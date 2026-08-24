# Pull через Refreshable Materialized View

Проверка варианта «внешняя нода сама ходит в основной кластер под read-only
пользователем и доливает данные». Всё, что ниже, измерено на стенде `s1`,
ClickHouse `26.3.17.110`. Полный протокол с запросами и выводом —
`scratch_queries_refreshable_mv.sql`.

Стенд: `main_cluster` (2 шарда × 2 реплики, `default.events_distributed` над
`ReplicatedMergeTree`), внешняя нода `ch-ext-s1r1` — **обычный одиночный узел**,
без общего Keeper, без реплицируемых таблиц, только `MergeTree` + RMV. TLS на
стенде не настроен, поэтому везде `remote()` и порт 9000 вместо
`remoteSecure()`/9440; на суть это не влияет.

## Вердикт

Схема работает ровно так, как описана. Один refresh по таблице в 6000 строк —
~90–105 мс. Read-only пользователя с `SETTINGS readonly = 1` хватает на всё,
включая чтение через `Distributed`. Из связности между кластерами — одно
исходящее TCP-соединение ext → main.

Но в приведённом сниппете два бага, и оба тихие.

## Баг 1: запятая в списке хостов удваивает данные

`remote('ch-main-1:9440,ch-main-2:9440', default.events_distributed, ...)` —
запятая объявляет **шарды**, а не реплики. `events_distributed` и так
разъезжается по всем шардам, поэтому она читается дважды, по разу с каждого
хоста:

| адресация | что означает | `count()` при 5000 строк в источнике |
|---|---|---|
| `'ch-main-s1r1:9000'` | один хост | 5000 |
| `'ch-main-s1r1:9000,ch-main-s2r1:9000'` | **два шарда** | **10000** |
| `'ch-main-s1r1:9000\|ch-main-s1r2:9000'` | две реплики одного шарда | 5000 |
| `'ch-main-s1r1:9000,ch-main-s2r1:9000'` над `events_local` | по хосту на шард | 5000 |

Правильно: `|` (реплики) поверх `Distributed`-таблицы — заодно получаем
прозрачный failover; или `,` (шарды) поверх **локальных** пошардовых таблиц.

Failover проверен отдельно: с `'s1r1|s1r2'` я погасил `ch-main-s1r1`, залил
300 строк через другой узел — они доехали на ext за один цикл, в
`system.view_refreshes` ни исключения, ни ретрая.

## Баг 2: `>=` в водяном знаке — это бесконечный источник дублей

`WHERE ts >= (SELECT max(ts) FROM ext.events)` каждый refresh заново
затягивает все строки, стоящие ровно на границе. Я запустил два одинаковых
RMV бок о бок, `>=` и `>`:

```
t+12s  events(>=)=5028   events_fixed(>)=5000
t+24s  events(>=)=5036   events_fixed(>)=5000
t+36s  events(>=)=5040   events_fixed(>)=5000
...
через ~9 минут:
       events(>=)=30468  events_fixed(>)=7000
ещё через 2.5 минуты:
       events(>=)=37968  events_fixed(>)=7000
```

Разгон произошёл, когда приехал батч из 500 строк с одинаковым
`ingested_at`: каждые 10 секунд эти 500 строк вставлялись заново. `>=` — не
«запас прочности», а утечка. Нужен `>`; если нужна семантика at-least-once,
дедуплицировать надо отдельно (`ReplacingMergeTree` по ключу), а не границей
сравнения.

## Инкрементальный долив: лаг и поздние вставки

Ключ — время вставки (`ingested_at DateTime DEFAULT now()`), лаг — 30 секунд.
Три батча одновременно:

| батч | `ingested_at` | что произошло |
|---|---|---|
| `batch2_ok` (1000) | `now() - 120s` | приехал на первом же refresh |
| `batch3_fresh` (500) | `now()` | придержан лагом, приехал через 30 с |
| `batch4_late` (200) | `now() - 3600s` | **не приехал никогда** |

То есть предупреждение из исходной заметки подтверждается буквально: строка,
записанная задним числом ниже водяного знака, теряется молча — ни ошибки, ни
следа в `system.view_refreshes`.

### Восстановление хвоста через REPLACE PARTITION

```sql
SYSTEM STOP VIEW ext.events_sync_fixed;          -- обязательно
CREATE TABLE IF NOT EXISTS ext.events_tail AS ext.events_fixed;
INSERT INTO ext.events_tail
SELECT ... FROM remote(...) WHERE toDate(ts) = toDate('2026-08-23');   -- 3243 строки
ALTER TABLE ext.events_fixed REPLACE PARTITION '2026-08-23' FROM ext.events_tail;
SYSTEM START VIEW ext.events_sync_fixed;
```

После этого ext = 6700 = main, потерянные 200 строк на месте, водяной знак не
сдвинулся. `SYSTEM STOP VIEW` здесь не формальность: `REPLACE PARTITION` может
опустить `max(ingested_at)`, и параллельный refresh перетянет целое окно заново.

## Полная перезагрузка справочника

`REFRESH EVERY ... ENGINE = MergeTree` (без `APPEND`) — данные живут во
внутренней таблице `.inner_id.<uuid>`, и каждый refresh собирает новую и
подменяет её. UUID внутренней таблицы меняется на каждом цикле:
`9d5aede2 → 635cf47e → ae2ca1ad`.

Проверил, что полная перезагрузка действительно переносит апдейты и удаления:
на источнике `ALTER ... UPDATE level = 999 WHERE user_id = 1` и
`ALTER ... DELETE WHERE user_id = 2` → на ext через один цикл `99` строк и
`level = 999`. (Мелочь по пути: в мутации реплицируемой таблицы нельзя
`now()` — `Code: 36 ... must use only deterministic functions`.)

А вот `APPEND`-вью такого не переносит вообще: после удаления 1000 строк на
источнике там осталось 5700, на ext — все 6700, включая удалённые.

## Скрытая цена: каждый refresh читает всю таблицу

Скалярный подзапрос с `max()` по локальной таблице вычисляется на ext и
подставляется в удалённый запрос литералом — фильтр реально уезжает в
источник, это видно в его `query_log`:

```
WHERE (`__table1`.`ingested_at` > _CAST('2026-08-23 21:46:06', 'Nullable(DateTime)'))
```

Но `ingested_at` не входит в первичный ключ (`ORDER BY (ts, user_id)`), поэтому
отсекать по нему нечего:

| фильтр | `read_rows` на источнике | строк вернулось |
|---|---|---|
| `ingested_at > X` | **6000** (вся таблица) | 300 |
| `ts > X` | 1431 | 116 |
| `ingested_at > X AND ts >= X - INTERVAL 1 HOUR` | 2543 | 300 |

На проде это full scan исходной таблицы каждые N секунд, навсегда. Лечится
либо добавлением грубой границы по PK/партиции в тот же `WHERE` (третья
строка), либо тем, чтобы ключ доливки был частью сортировки. Плюс каждый
refresh делает ещё и `DESC TABLE`.

## Отказы

| ситуация | поведение |
|---|---|
| неверный пароль | ловится **на `CREATE`** (`Code: 516 AUTHENTICATION_FAILED`), вью не создаётся |
| отозвали грант у живого вью | `exception` в `system.view_refreshes`, `retry` растёт (видел 3), читатели продолжают видеть последний удачный снапшот |
| грант вернули | само восстановилось: `retry = 0`, `exception` пустой, никаких действий оператора |
| упал первый хост в `'a\|b'` | прозрачный failover, ни ошибки, ни пропущенных строк |
| рестарт ext-ноды | все вью поднялись сами, без дублей и разрывов (`stop_refreshable_materialized_views_on_startup = 0`) |

`allow_experimental_refreshable_materialized_view` в 26.3 уже `1` по умолчанию —
`SET ... = 1` из времён 23.12–24.x не нужен.

## Пароль источника лежит на диске открытым текстом

`SHOW CREATE` показывает `'[HIDDEN]'`, в `query_log` пароль тоже не всплывает
(`0` совпадений). Но в файле метаданных вью — как есть:

```
$ grep -o "ro_export_user', '[^']*'" /var/lib/clickhouse/metadata/ext/events_sync_fixed.sql
ro_export_user', 'ro_export_pw_change_me'
```

Лечится named collection — тогда в метаданных вью пароля нет вовсе
(`grep -c` → `0`), а `system.named_collections` показывает `[HIDDEN]`:

```sql
CREATE NAMED COLLECTION main_src AS
    host = 'ch-main-s1r1:9000', user = 'ro_export_user', password = '***';

CREATE MATERIALIZED VIEW ext.dim_users_sync_nc
REFRESH EVERY 1 MINUTE ENGINE = MergeTree ORDER BY user_id
AS SELECT ... FROM remote(main_src, database = 'default', table = 'dim_users');
```

## Push-направление

Тоже работает: на ext заведён `ingest_user` только с `GRANT INSERT`, с main —
`INSERT INTO FUNCTION remote('ch-ext-s1r1:9000', ext.events_push, ...)`. Окно в
10 минут переехало один в один (800 строк), и обратно этот пользователь ничего
прочитать не может (`Code: 497 ... necessary to have the grant SELECT`).

Разница не в механике, а в том, где живёт учёт: при pull состояние («до какой
границы уже долито») выводится из самой приёмной таблицы на ext, при push его
надо вести на стороне main — какой интервал уже отправлен, что делать после
сбоя.

## Чем это отличается от репликации по Keeper

| | общий Keeper (S1) | pull через RMV |
|---|---|---|
| дырки в firewall | ext → keeper 9181, ext ↔ main 9009 | ext → main 9000/9440, одна, в одну сторону |
| кто кого знает | общий ансамбль Keeper, общие ZK-пути, общие interserver-креды | ext знает хост и read-only пользователя, main про ext не знает ничего |
| задержка | секунды, поток репликации | шаг расписания + лаг (у меня 10 с + 30 с) |
| консистентность | точная копия партов | at-least-once по окну, поздние строки надо добирать отдельно |
| DDL | ALTER реплицируемой таблицы переходит границу | схема на ext своя, ничего не переходит |
| нагрузка на источник | фоновая отдача партов | SELECT каждый цикл, при плохом ключе — full scan |

## Состояние стенда

Осталось на `ch-ext-s1r1`: база `ext` с `events` (вариант `>=`, 37968 строк —
оставил как иллюстрацию, вью остановлено через `SYSTEM STOP VIEW`, иначе таблица
растёт бесконечно), `events_fixed` (7000, корректный),
`events_tail`, `events_push`, вью `events_sync`, `events_sync_fixed`,
`dim_users_sync`, `dim_users_sync_nc`, named collection `main_src`, пользователь
`ingest_user`. Вью продолжают крутиться каждые 10 секунд.

На main: `default.events_local` / `events_distributed` / `dim_users`,
пользователь `ro_export_user`. Удалить всё — дропнуть базу `ext` на внешней
ноде и три таблицы + пользователя на main.
