# Query cache на реальной нагрузке: JOIN, IN, оконки через DBLink

Проверка профиля кэширования на наборе непростых, но обычных аналитических
запросов, выполняемых с внешнего узла в основной кластер. Стенд `s1`,
ClickHouse `26.3.17.110`. Настройки и их разбор — `note_query_cache_dblink.ru.md`.

## Конфигурация

### Серверный блок на ext (`docker/s1/config.d/common-ext.xml`)

```xml
<query_cache>
    <max_size_in_bytes>2147483648</max_size_in_bytes>            <!-- 2 GiB, было 1 GiB  -->
    <max_entries>8192</max_entries>                              <!-- было 1024          -->
    <max_entry_size_in_bytes>67108864</max_entry_size_in_bytes>  <!-- 64 MiB, было 1 MiB -->
    <max_entry_size_in_rows>100000000</max_entry_size_in_rows>   <!-- было 30M           -->
</query_cache>
```

Это верхние границы, а не преаллокация. Ключевая правка — `max_entry_size_in_bytes`:
именно дефолтный 1 MiB молча выбрасывает результаты аналитического размера.

### Профиль

```sql
CREATE SETTINGS PROFILE external_readers SETTINGS
    readonly                                       = 2,
    use_query_cache                                = 1 CONST,
    enable_reads_from_query_cache                  = 1,
    enable_writes_to_query_cache                   = 1,
    query_cache_min_query_duration                 = 0,
    query_cache_min_query_runs                     = 0,
    query_cache_nondeterministic_function_handling = 'save',
    query_cache_system_table_handling              = 'ignore',
    query_cache_max_size_in_bytes                  = 0,
    query_cache_max_entries                        = 0,
    query_cache_compress_entries                   = 1,
    query_cache_squash_partial_results             = 1,
    query_cache_ttl                                = 300,
    query_cache_share_between_users                = 0 CONST;

CREATE USER bi_analyst IDENTIFIED WITH sha256_password BY '***'
    SETTINGS PROFILE external_readers;
GRANT SELECT ON ext.events, ext.dim_users, ext.dim_segments TO bi_analyst;
```

### Объекты

На ext добавлены две таблицы к уже существовавшей `ext.events`:

```sql
-- справочник с main; он реплицирован на все узлы main, поэтому читается
-- через ОДНОШАРДОВЫЙ линк, иначе каждая строка вернётся дважды
CREATE TABLE ext.dim_users
(
    user_id UInt64, country LowCardinality(String), level UInt32, updated_at DateTime
)
ENGINE = Distributed('main_link_ref', 'default', 'dim_users');

-- справочник, живущий на самой внешней ноде
CREATE TABLE ext.dim_segments
(
    country LowCardinality(String), region LowCardinality(String), is_priority UInt8
)
ENGINE = MergeTree ORDER BY country;
```

Кластер `main_link_ref` — те же четыре узла main, но объявленные как один шард
с четырьмя репликами. `main_link` (2 шарда) остаётся для `events_local`.

Данные: `ext.events` — 12 400 строк, `ext.dim_users` — 99, `ext.dim_segments` — 4.

## Первое, обо что споткнулись: JOIN и IN требуют GLOBAL

Обычный `JOIN` между двумя таблицами линка не работает вообще:

```sql
SELECT u.country, count()
FROM ext.events AS e
INNER JOIN ext.dim_users AS u ON e.user_id = u.user_id
GROUP BY u.country;
```
```
Code: 81. DB::Exception: Received from ch-main-s1r1:9000.
Database ext does not exist. (UNKNOWN_DATABASE)
```

Причина простая: правая часть JOIN уезжает на шарды основного кластера как
есть, а базы `ext` там нет и быть не должно. То же самое с обычным `IN`:

```sql
WHERE user_id IN (SELECT user_id FROM ext.dim_users WHERE level > 5)
-- Code: 81 ... Database ext does not exist
```

Что помогает и что нет:

| приём | результат |
|---|---|
| `GLOBAL INNER JOIN` | **работает** |
| `GLOBAL IN` / `GLOBAL NOT IN` | **работает** |
| `SETTINGS distributed_product_mode = 'global'` | **не помогает** — та же ошибка 81 |
| `GLOBAL CROSS JOIN` | синтаксис принимается, но падает с той же ошибкой |

То есть для DBLink правило такое: любое обращение ко второй таблице внутри
распределённого запроса должно быть `GLOBAL`. Это не про кэш, это про сам
линк — но узнаётся сразу, как только запросы становятся сложнее одиночного
`GROUP BY`, и об этом надо предупредить аналитиков (или прятать JOIN'ы во вью).

## Результаты по запросам

Каждый запрос выполнен дважды подряд от `bi_analyst`. «main» — сколько
подзапросов реально доехало до основного кластера (считались на всех четырёх
его узлах); один запрос через двухшардовый линк = 2 подзапроса.

| # | что в запросе | 1-й прогон | 2-й прогон |
|---|---|---|---|
| q01 | `GROUP BY` + `avg` | main 2, 33 мс | main 0, **2 мс** |
| q02 | `GLOBAL JOIN` + `uniqExact` | main 3, 57 мс | main 0, **2 мс** |
| q03 | два `GLOBAL JOIN`: удалённый и локальный справочники | main 3, 95 мс | main 0, **1 мс** |
| q04 | `GLOBAL IN` + `GLOBAL NOT IN` | main 3, 73 мс | main 0, **3 мс** |
| q05 | `GLOBAL IN` с подзапросом | main 3, 326 мс | main 0, **1 мс** |
| q06 | `WITH` + оконная функция `sum() OVER` | main 2, 66 мс | main 0, **2 мс** |
| q07 | `UNION ALL` из двух веток | main 4, 53 мс | main 0, **1 мс** |
| q08 | `WHERE ts > now() - INTERVAL 7 DAY` | main 2, 28 мс | main 0, **1 мс** |
| q09 | `ARRAY JOIN range(20)`, 248 000 строк результата | main 2, 177 мс | main 0, **27 мс** |
| q10 | `GROUP BY` + `LIMIT 3 BY event_type` | main 2, 60 мс | main 0, **2 мс** |
| q11 | `GLOBAL JOIN` + скалярный подзапрос в `HAVING` | main 5, 82 мс | main 0, **1 мс** |
| q12 | только локальная `ext.dim_segments` | main 0, 10 мс | main 0, **3 мс** |

Ни одного исключения: **всё закэшировалось с первого прогона**, и на повторе
в основной кластер не ушло ничего. Включая оконные функции, `UNION ALL`,
`ARRAY JOIN`, скалярные подзапросы и `GLOBAL`-конструкции — то есть кэш
работает на уровне результата запроса и ему безразлично, насколько сложен план.

Суммарно по всем 12 запросам:

| | серверное время | подзапросов на main |
|---|---|---|
| холодный прогон | 1060 мс | 31 |
| прогретый | 46 мс | **0** |

(Настенное время прогона всего набора — 4.4 с против 3.2 с, но там доминирует
запуск `clickhouse-client` в контейнере на каждый запрос; смотреть надо на
`query_duration_ms`, как в таблице.)

## Что попало в кэш

```
QueryCacheEntries  12
QueryCacheBytes    10.37 MiB
```

| запрос | размер записи |
|---|---|
| q09 (248 000 строк) | **10.35 MiB** |
| q01, q06, q10 и прочие агрегаты | 256 B – 4 KiB |

Здесь и виден смысл поднятого серверного лимита: запись q09 — 10.35 MiB уже
**после** сжатия, то есть при дефолтном `max_entry_size_in_bytes = 1 MiB` этот
запрос не кэшировался бы вообще и молча ходил бы в основной кластер каждый раз.

## Как отличить попадание от промаха

Два независимых признака в `system.query_log` на ext:

```sql
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses'], read_rows
FROM system.query_log WHERE type = 'QueryFinish' AND user = 'bi_analyst';
```

- счётчики `QueryCacheHits` / `QueryCacheMisses` — прямо;
- `read_rows` схлопывается: на промахе это скан источника (12 400 – 24 800 строк),
  на попадании — ровно размер результата (10, 4, 20, 26 строк; у q09 — 248 000).
  Удобный косвенный индикатор, если смотреть по дашборду в целом.

## Границы: что мимо кэша

Ключ кэша — это запрос целиком. Смена литерала в фильтре создаёт новую запись:

```sql
... WHERE level > 5   -- запись 1
... WHERE level > 6   -- запись 2
... WHERE level > 7   -- запись 3
... WHERE level > 5   -- попадание в запись 1
```

Замер: 4 запроса → записей в кэше стало 12 → 15, на main ушло 9 подзапросов
(три промаха по 3) и один запрос обслужился из кэша. То есть дашборд с
фиксированным набором фильтров кэшируется прекрасно, а свободный выбор
диапазона дат/порогов — это столько записей, сколько комбинаций выберут
пользователи, и попадания начнутся только на повторах.

## Инвалидация по тегу

Работает точечно и не трогает остальной кэш:

```sql
-- дашбордные запросы помечаются тегом
SELECT ... FROM ext.events ... SETTINGS query_cache_tag = 'dash_events';
-- после загрузки данных на main:
SYSTEM DROP QUERY CACHE TAG 'dash_events';
```

Замер: повтор с тем же тегом → 0 подзапросов на main; после `DROP ... TAG` →
2 подзапроса, результат пересчитан. Тег виден в `system.query_cache.tag`.

Помните, что тег входит в ключ: один дашборд — один постоянный тег, иначе
каждый вариант тега заводит собственную запись.

## `now()` в запросе

q08 (`WHERE ts > now() - INTERVAL 7 DAY`) закэшировался и на повторе выдал
попадание — это заслуга `query_cache_nondeterministic_function_handling = 'save'`
в профиле. Ценой того, что момент `now()` заморожен внутри записи на весь TTL:
при `query_cache_ttl = 300` граница окна может отставать на пять минут. Для
«за последние 7 дней» это незаметно, для «за последний час» — уже нет.

Если такие запросы есть, выбор один из трёх: `'ignore'` (не кэшировать их
вовсе), короткий TTL, или подставлять границы окна литералами со стороны BI —
тогда запрос детерминирован и кэшируется честно.

## Итог

На этом наборе гипотеза подтверждается полностью: любой повторный запрос,
включая многоступенчатые с JOIN и оконками, обслуживается на внешнем узле за
1–3 мс и не порождает ни одного обращения к основному кластеру. Ограничения
ровно два, и оба не про кэш:

1. запросы к линку должны использовать `GLOBAL` для JOIN и IN, иначе они просто
   не выполняются;
2. кэшируется точный текст запроса, поэтому выигрыш получают дашборды с
   фиксированным набором запросов, а не свободная ad-hoc аналитика.

## Что осталось на стенде

- `docker/s1/config.d/common-ext.xml`: секция `<query_cache>` и кластер
  `main_link_ref` (4 узла main одним шардом, пользователь `ro_export_user`,
  **пароль открытым текстом**). Контейнер ext перезапущен, чтобы применить.
- На ext: таблицы `ext.dim_users`, `ext.dim_segments`, профиль
  `external_readers`, пользователь `bi_analyst`.
- Пользователи и профили из предыдущих раундов (`bi_user*`, `bi_ro*`,
  `bi_const`, `bi_quota`, `bi_nogrant`, `ingest_user`, пробные профили)
  удалены.
- Тексты всех двенадцати запросов — в приложении ниже.

## Приложение: запросы

Все выполнялись от `bi_analyst`, без `SETTINGS` в тексте — настройки
кэша целиком приходят из профиля.

**q01** — агрегат по типам событий

```sql
SELECT event_type, count() AS c, round(avg(value), 2) AS avg_v
FROM ext.events
GROUP BY event_type
ORDER BY c DESC
```

**q02** — GLOBAL JOIN со справочником с main + uniqExact

```sql
SELECT u.country AS country, count() AS c, uniqExact(e.user_id) AS users
FROM ext.events AS e
GLOBAL INNER JOIN ext.dim_users AS u ON e.user_id = u.user_id
GROUP BY country
ORDER BY c DESC
```

**q03** — два GLOBAL JOIN: удалённый справочник и локальный на ext

```sql
SELECT s.region AS region, s.is_priority AS prio, count() AS c, round(sum(e.value), 1) AS total
FROM ext.events AS e
GLOBAL INNER JOIN ext.dim_users AS u ON e.user_id = u.user_id
GLOBAL INNER JOIN ext.dim_segments AS s ON u.country = s.country
GROUP BY region, prio
ORDER BY c DESC
```

**q04** — GLOBAL IN и GLOBAL NOT IN в одном WHERE

```sql
SELECT count() AS c, round(avg(value), 3) AS avg_v
FROM ext.events
WHERE user_id GLOBAL IN (SELECT user_id FROM ext.dim_users WHERE level > 5)
  AND event_type GLOBAL NOT IN (SELECT country FROM ext.dim_segments)
```

**q05** — GLOBAL IN с подзапросом

```sql
SELECT count() AS c, round(avg(value), 3) AS avg_v
FROM ext.events
WHERE user_id GLOBAL IN (SELECT user_id FROM ext.dim_users WHERE level > 5)
```

**q06** — CTE + оконная функция

```sql
WITH daily AS
(
    SELECT toDate(ts) AS d, event_type, count() AS c
    FROM ext.events
    GROUP BY d, event_type
)
SELECT d, event_type, c, sum(c) OVER (PARTITION BY event_type ORDER BY d) AS running
FROM daily
ORDER BY event_type, d
LIMIT 20
```

**q07** — UNION ALL

```sql
SELECT 'purchase' AS bucket, count() AS c FROM ext.events WHERE event_type = 'purchase'
UNION ALL
SELECT 'other' AS bucket, count() AS c FROM ext.events WHERE event_type != 'purchase'
```

**q08** — недетерминированная now() в фильтре

```sql
SELECT count() AS c
FROM ext.events
WHERE ts > now() - INTERVAL 7 DAY
```

**q09** — ARRAY JOIN, крупный результат (248 000 строк, запись 10.35 MiB)

```sql
SELECT user_id, event_type, n, hex(sipHash128(user_id, ts, n)) AS h
FROM ext.events
ARRAY JOIN range(20) AS n
```

**q10** — GROUP BY + LIMIT n BY

```sql
SELECT event_type, user_id, max(value) AS mv
FROM ext.events
GROUP BY event_type, user_id
ORDER BY event_type ASC, mv DESC
LIMIT 3 BY event_type
```

**q11** — GLOBAL JOIN + скалярный подзапрос в HAVING

```sql
SELECT u.level AS level, count() AS c
FROM ext.events AS e
GLOBAL INNER JOIN ext.dim_users AS u ON e.user_id = u.user_id
GROUP BY level
HAVING c > (SELECT count() / 200 FROM ext.events)
ORDER BY level
```

**q12** — только локальная таблица ext, в main не ходит

```sql
SELECT region, count() AS c
FROM ext.dim_segments
GROUP BY region
ORDER BY region
```

