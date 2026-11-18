# Module 09 — Capstone: real-time clickstream & IoT lakehouse (you build it)

Now you build a project end to end on your own, using everything from Modules 01–08. This is the project for your
résumé and GitHub. A starter data generator is in [`capstone_datagen.py`](capstone_datagen.py).

## Scenario
"StreamFit" sells smart fitness devices plus a mobile app. Data sources:
| Source | Format | Arrives | Notes |
|---|---|---|---|
| Device telemetry (heart rate, steps, battery) | JSON, every few seconds | continuously into a volume | late and out-of-order events, duplicate sends, firmware v2 adds a field |
| App clickstream (page views, workouts started/finished) | JSON | continuously | sessionize per user |
| Users (CRM) | CSV CDC | daily | SCD2 needed (plan changes: free → premium) |
| Devices registry | Delta table / CSV snapshot | daily | device → user mapping |

## Requirements
**Ingestion & modeling**
1. Bronze with **Auto Loader** for telemetry and clickstream, including schema evolution for the firmware v2 field.
2. Silver: dedupe events (`dropDuplicatesWithinWatermark`), parse and validate, quarantine, `dim_user` as **SCD2**.
3. Gold:
   - `gold_device_health_hourly`: avg/max heart rate, steps, low-battery alerts per device per hour (**watermarked streaming aggregation**)
   - `gold_user_sessions`: sessions (30-minute inactivity gap) with duration and workouts per session
   - `gold_daily_active_users` by plan (free/premium) using the **plan valid at event time** (point-in-time join to SCD2)
   - `gold_conversion_funnel`: install → first workout → premium upgrade

**Implementation**: build at least one layer with **Lakeflow Declarative Pipelines** (with expectations) and at least one with plain Structured Streaming + `foreachBatch`. Explain the trade-off in your README.

**Governance**: UC catalog per environment, PII masked (email, birthdate), row filter by region, table comments and tags.

**Ops**: a **Databricks Asset Bundle** with a job (pipeline task + notebook tasks + DQ gate), dev/prod targets, GitHub Actions CI with unit tests.

**Performance**: liquid clustering on the big tables. Show one before/after optimization with numbers.

**Serving**: an **AI/BI dashboard** (or Databricks SQL queries) on the gold tables. Bonus: a **Genie space** so people can ask questions in English.

## Grading rubric (score yourself)
| Area | 0 | 1 | 2 |
|---|---|---|---|
| Correctness | numbers wrong | mostly right | reconciles bronze → gold with checks |
| Incrementality | full reloads | some incremental | everything incremental + idempotent |
| Data quality | none | filters | expectations + quarantine + DQ gate + alerts |
| Late/dup data | ignored | deduped | watermarks + SCD2 point-in-time correct |
| Governance | none | grants | masks + filters + docs + lineage screenshots |
| Testing & CI/CD | none | unit tests | tests + bundle + CI deploy |
| Performance | none | clustered | measured before/after with explanation |
| Communication | none | README | README + architecture diagram + dashboard |

**14+ = interview-ready portfolio project.**

## Hints (open only when stuck)
<details><summary>Sessionization</summary>

`lag(ts) over (partition by user order by ts)` → `new_session = gap > 30 min` → `sum(new_session) over (...)` = session number.
In streaming, use `F.session_window("ts", "30 minutes")`.
</details>
<details><summary>Point-in-time join to SCD2</summary>

`events e JOIN dim_user u ON e.user_id = u.user_id AND e.ts >= u.valid_from AND (u.valid_to IS NULL OR e.ts < u.valid_to)`
</details>
<details><summary>Streaming dedupe</summary>

`.withWatermark("event_ts", "10 minutes").dropDuplicatesWithinWatermark(["event_id"])`
</details>
