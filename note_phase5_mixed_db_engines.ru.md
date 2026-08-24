# Нужен ли `ENGINE = Replicated` на обоих кластерах?

**Коротко: нет.** Ни на обоих, ни вообще. Данные между узлами
реплицируются по совпадению `zookeeper_path` таблицы `ReplicatedMergeTree`.
Движок базы данных влияет только на то, *как* этот путь выводится, если его
не задать явно, и на то, как разъезжается DDL.

Проверено на `26.3.17.110`, стенд `s1`: `main_cluster` — 2 шарда × 2 реплики,
`ext_cluster` — 2 шарда × 1 реплика, общий ансамбль Keeper, общие
`interserver_http_credentials`. Все запросы — в
`scratch_queries_replicated_db.sql`, раздел `PHASE 5`.

## Постановка

Самый показательный случай — не «одинаковые движки», а **разные**:

| | `main_cluster` | `ext_cluster` |
|---|---|---|
| движок БД | `Atomic` (по умолчанию) | `Replicated('/clickhouse/databases/db_mix_ext', '{shard}', '{replica}')` |
| путь таблицы | `/clickhouse/tables/{shard}/db_mix/events` | тот же, задан явно |
| как разъезжается DDL | `ON CLUSTER main_cluster` | очередь самой БД, `ON CLUSTER` не нужен |

Таблица — та же `events`, что и в остальных примерах
(`event_date, event_time, user_id, event_type, value`,
`PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, user_id)`), плюс
`Distributed`-обёртка `db_mix.events` над своим кластером на каждой стороне.

Одна оговорка по пути: внутри `Replicated`-БД явный путь по умолчанию
запрещён —

```
Code: 36. It's not allowed to specify explicit zookeeper_path and replica_name
for ReplicatedMergeTree arguments in Replicated database. (BAD_ARGUMENTS)
```

лечится `database_replicated_allow_replicated_engine_arguments = 1`
(по умолчанию `0`). Это же поведение отдельно разобрано в `PHASE 3`.

## Что получилось

По 1000 строк вставлено с каждой стороны через свою `Distributed`-таблицу:

| узел | всего | `from_main_atomic_db` | `from_ext_replicated_db` |
|---|---|---|---|
| `s1-ch-main-s1r1` | 1038 | 518 | 520 |
| `s1-ch-main-s1r2` | 1038 | 518 | 520 |
| `s1-ch-ext-s1r1`  | 1038 | 518 | 520 |
| `s1-ch-main-s2r1` |  962 | 482 | 480 |
| `s1-ch-main-s2r2` |  962 | 482 | 480 |
| `s1-ch-ext-s2r1`  |  962 | 482 | 480 |

`SELECT count() FROM db_mix.events` — 2000 и с main, и с ext.

Набор реплик в Keeper — по одному на шард, и он перекрывает оба кластера:

```
/clickhouse/tables/1/db_mix/events/replicas -> ext_r1, main_r1, main_r2
/clickhouse/tables/2/db_mix/events/replicas -> ext_r1, main_r1, main_r2
```

`system.replicas`: `total_replicas = 3`, `active_replicas = 3` — одинаково
и на main, и на ext.

Точечная вставка 5 строк прямо в `events_local` на `s1-ch-ext-s1r1`
(в обход `Distributed`) разошлась ровно по своему шарду: 5 строк на
`main-s1r1`, `main-s1r2`, `ext-s1r1` и 0 на всех узлах второго шарда.

## Куда доезжает DDL в такой смешанной схеме

`ALTER TABLE db_mix.events_local ADD COLUMN source_note ...`, выполненный на
`ext-s1r1`, оказался **на всех 6 узлах**: очередь `Replicated`-БД доставила
его на оба ext-узла, а дальше лог репликации разнёс его по main-репликам
обоих шардов. Метаданные `ReplicatedMergeTree` реплицируются по тому же
пути в ZK, что и данные.

Из этого следует вывод, который легко упустить: **«обычные БД» не означает
«кластеры друг на друга не влияют»** — изменения схемы реплицируемой
таблицы всё равно переходят границу. Локальным остаётся всё, что не
является реплицируемой таблицей: `Distributed`-обёртки, вьюхи, словари.
После того же `ALTER` колонка `source_note` появилась в `events_local` на
всех 6 узлах и **ни на одной** `Distributed`-таблице — их надо править
руками на каждом кластере.

## Итог

Что должно совпадать для межкластерной репликации — только `zookeeper_path`
таблицы (плюс общий Keeper и общие interserver-креды). Рабочие комбинации:

| # | движки БД | путь таблицы | DDL | где |
|---|---|---|---|---|
| a | обычные БД на обоих кластерах | явный, общий | `ON CLUSTER` отдельно на каждый кластер | `scratch_queries.sql` |
| b | одна общая `Replicated`-БД на оба кластера | по умолчанию, `{uuid}` | один запрос на всё | `PHASE 2` |
| c | своя `Replicated`-БД на каждом кластере | явный, общий + флаг | отдельно на каждый кластер | `PHASE 3` |
| d | `Atomic` на main, `Replicated` на ext | явный, общий + флаг | отдельно на каждый кластер | `PHASE 5` |

Не работает ровно одна комбинация: `Replicated`-БД на каждом кластере со
**своими** путями и путём таблицы по умолчанию `{uuid}` — UUID у баз разные,
наборы реплик не пересекаются, репликации нет (`PHASE 1`).

Практически, если цель — только данные на `ext`, самый слабосвязанный
вариант — (a): кластеры остаются административно независимыми, каждая
сторона сама решает, когда применять DDL, и случайный `CREATE`/`DROP` на
одной стороне не прилетает на другую. Вариант (b) удобнее в
эксплуатации (один `CREATE`/`ALTER` вместо двух), но связывает кластеры
целиком, включая объекты, которые зависят от локального конфига узла
(`Distributed` над именем кластера, определённым только на одной стороне,
создаётся везде, но на другой стороне падает при чтении с
`Code: 701 CLUSTER_DOESNT_EXIST`).

## Состояние стенда

`db_mix` **оставлена** на обоих кластерах — можно посмотреть руками.
Запросы на удаление лежат в конце `PHASE 5` закомментированными:

```sql
DROP DATABASE IF EXISTS db_mix ON CLUSTER main_cluster SYNC;
DROP DATABASE IF EXISTS db_mix ON CLUSTER ext_cluster SYNC;
```

Также на кластерах остались `db_repl` и `db_xrepl` из фаз 1–3.
