# Module 07 — Lakeflow Jobs, Databricks Asset Bundles & CI/CD

## 1. Lakeflow Jobs (formerly Databricks Workflows)
A **job** is a DAG of **tasks**. Task types include notebook, Python script, Python wheel, SQL (query, file, dashboard refresh),
**pipeline** (Lakeflow Declarative Pipelines), dbt, JAR, *Run Job* (job of jobs), and control flow
(**If/else** condition, **For each** loop).

Key concepts:
| Concept | Notes |
|---|---|
| `depends_on` | Builds the DAG. Tasks without dependencies run in parallel. |
| Run-if | `ALL_SUCCESS` (default), `AT_LEAST_ONE_SUCCESS`, `ALL_DONE` (cleanup tasks), `NONE_FAILED`, … |
| Job parameters | Pushed to every task (`{{job.parameters.x}}`, widgets in notebooks). |
| Dynamic value refs | `{{job.run_id}}`, `{{job.start_time.iso_date}}`, `{{tasks.<key>.values.<name>}}` |
| Task values | `dbutils.jobs.taskValues.set/get` to pass small values between tasks. |
| Triggers | Cron schedule, **file arrival**, **table update**, continuous, manual/API. |
| Retries / timeouts | Per task. **Repair run** re-runs only failed tasks. |
| Compute | Serverless, or job clusters (cheaper than all-purpose; created per run). Share a job cluster across tasks. |
| Notifications | Email, Slack, webhooks on start/success/failure/duration-exceeded. |

## 2. Databricks Asset Bundles (DABs)
"Infrastructure as code" for Databricks: jobs, pipelines, dashboards, schemas, volumes, model endpoints, all described in YAML
next to your code, deployed by the **Databricks CLI** to any number of **targets** (dev/staging/prod).

```
07_jobs_and_bundles/
├─ databricks.yml                     bundle name, variables, targets (dev/prod)
├─ resources/shopstream_daily.job.yml the job: 6 tasks, schedule, params, notifications
├─ src/shopstream/                    importable Python package (transforms, datagen)
├─ src/notebooks/                     thin notebooks: I/O only, logic comes from src/shopstream
└─ tests/                             pytest, runs locally and in CI
```
`mode: development` prefixes resources with `[dev <you>]` and pauses schedules, so every developer gets an isolated copy.
`mode: production` enforces good practice (e.g. warns if not run as a service principal).

```bash
# one-time
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks auth login --host https://<your-workspace>.cloud.databricks.com

cd 07_jobs_and_bundles
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run shopstream_daily -t dev    # streams the run status to your terminal
databricks bundle summary -t dev
databricks bundle destroy -t dev
```
`databricks bundle init` scaffolds new projects from templates (`default-python`, `lakeflow-pipelines`, …).
`databricks bundle generate job --existing-job-id 123` turns a UI-built job into YAML.

## 3. CI/CD
See [`.github/workflows/shopstream-ci-cd.yml`](../.github/workflows/shopstream-ci-cd.yml):
1. **On every PR:** unit tests (pytest + local Spark), then `bundle validate`.
2. **On merge to main:** `bundle deploy -t prod` (optionally gated by a GitHub *environment* approval).
3. Auth from CI: a **service principal** with OAuth M2M (`DATABRICKS_CLIENT_ID`/`SECRET`) is the best practice. A PAT works for learning.

Testing pyramid for data pipelines:
- **Unit tests:** pure transformation functions on small hand-built DataFrames (fast, local). See `tests/`.
- **Integration tests:** deploy to a `staging` target and run the job against sample data.
- **Data tests in production:** expectations (Module 05), the DQ gate task (this module), Lakehouse Monitoring.

## 4. Project 07: ship ShopStream as code
1. Set your workspace URL in `databricks.yml`.
2. `databricks bundle deploy -t dev`, then open *Jobs & Pipelines*: your job `[dev you] shopstream_daily_dev` is there.
3. `databricks bundle run shopstream_daily -t dev` three times. Each run lands one new day and processes only that day.
4. Break it on purpose: set `dq_max_bad_pct` to `1` in `databricks.yml`, redeploy, run. The **DQ gate** fails, you get an email,
   and gold has already been refreshed. Discuss: should the gate run *before* gold? (Move it and redeploy.)
5. Use **Repair run** in the UI to re-run only the failed task.
6. Push the repo to GitHub, add secrets, and watch the workflow run tests and deploy.

### Your turn
- Add a **pipeline resource** that deploys the Module 05 pipeline, and a job task (`pipeline_task`) that triggers it.
  ```yaml
  resources:
    pipelines:
      shopstream_ldp:
        name: shopstream_ldp_${bundle.target}
        catalog: ${var.catalog}
        schema: ${var.schema}_ldp
        serverless: true
        libraries:
          - file: { path: ../src/pipeline/shopstream_pipeline.py }
          - file: { path: ../src/pipeline/shopstream_gold.sql }
        configuration:
          landing_path: /Volumes/${var.catalog}/${var.schema}/landing
  ```
- Add a **file arrival trigger** on `/Volumes/workspace/shopstream_dev/landing/orders/` instead of the cron schedule.
- Add a `staging` target and a third CI stage that runs the job there (`databricks bundle run -t staging`) before prod.
- Add a cleanup task with `run_if: ALL_DONE` that runs `OPTIMIZE` on silver.
