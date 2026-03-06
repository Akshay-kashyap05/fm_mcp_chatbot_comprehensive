# Apache Airflow: Overview and Integration for Scheduled Reports

This document explains **what Apache Airflow is**, **how it works**, and **how to use it** in this project to schedule daily analytics reports and email them to recipients at a specific time.

---

## What is Apache Airflow?

**Apache Airflow** is an open-source platform to **programmatically author, schedule, and monitor workflows**. You define workflows as **DAGs (Directed Acyclic Graphs)** in Python: each node is a **task** (e.g. “run a script”, “call an API”), and edges define the order and dependencies. Airflow’s **scheduler** runs tasks according to their schedule (cron or timetable) and handles retries, logging, and monitoring.

### Why use Airflow instead of cron?

| Cron | Airflow |
|------|--------|
| One line per schedule, no dependencies | DAGs with multiple tasks and dependencies |
| No built-in retries or alerting | Retries, failure emails, task-level logs |
| Hard to see history or reason about runs | Web UI: run history, logs, backfills |
| Different servers = different crontabs | Centralized scheduling and visibility |

For “run report every day at 8 AM and email it”, cron is enough. Airflow becomes useful when you want **visibility**, **retries**, **multiple reports or steps**, or **parameterized schedules** (e.g. different clients/fleets at different times).

---

## How Apache Airflow Works

### 1. Core components

- **Scheduler**  
  Watches DAGs and creates **DAG runs** when the schedule says it’s time. For each run, it creates **task instances** and queues them for execution.

- **Executor**  
  Decides *where* tasks run (e.g. same process, Celery workers, Kubernetes). For a single-machine setup, **LocalExecutor** or **SequentialExecutor** is typical.

- **Workers**  
  (With LocalExecutor/Celery) Processes that actually run the task code.

- **Web server**  
  Flask app that serves the UI: list DAGs, trigger runs, view logs, see success/failure.

- **Metadata database**  
  (Usually PostgreSQL or MySQL.) Stores DAG definitions, run state, task instances, logs metadata, users, etc. SQLite is only for quick local tryouts.

### 2. DAG (Directed Acyclic Graph)

A **DAG** is the workflow definition:

- **DAG** = a Python file that defines:
  - **Schedule**: when the workflow runs (e.g. `"0 8 * * *"` = 08:00 every day).
  - **Tasks**: units of work (operators like `BashOperator`, `PythonOperator`).
  - **Dependencies**: e.g. `task_b runs after task_a` (set with `>>` or `set_downstream`).

- **DAG run** = one “execution” of that DAG at a given logical date.

- **Task instance** = one execution of a single task within a DAG run.

“Directed acyclic” means: dependencies flow in one direction and there are no cycles, so the scheduler can always decide what to run next.

### 3. Execution flow (high level)

1. You add a Python file under the **DAGs folder** (e.g. `~/airflow/dags` or `airflow/dags` in this project).
2. The **scheduler** parses the file, loads the DAG, and registers it.
3. At each tick (e.g. every few seconds), the scheduler checks which DAGs are due.
4. For each due schedule, it creates a **DAG run** and then **task instances** for each task whose dependencies are met.
5. The **executor** assigns task instances to workers.
6. Workers run the task code (e.g. the DAG’s Python task that calls `src/report_builder`). Logs and status go to the metadata DB and UI.
7. On failure, Airflow can **retry** according to the task’s `retries` and `retry_delay`.

### 4. Scheduling (cron and timetables)

- **Cron**: `schedule="0 8 * * *"` means 08:00 every day (in the scheduler’s timezone).
- **Timetable** (Airflow 2.2+): more complex schedules (e.g. “last weekday of month”).
- **Logical date (execution_date)**: For a run at 08:00 on 2026-02-26, the logical date is usually 2026-02-26 00:00:00. Your script can use this to decide “yesterday” or “today” if needed.

---

## How this project uses Airflow

This repo uses **`src/report_builder.py`** for report generation (used by the MCP server when a user asks for an analytics summary; the server calls `build_pdf()` / `build_pdf_from_text()` and `send_report_email()`). There is no dependency on `report_job.py` on the server.

The integration point is: **an Airflow DAG runs on a schedule and, inside the task, calls the same code path** — it fetches analytics from the Sanjaya API, then uses `src.report_builder.build_pdf()` and `send_report_email()` to generate and email the PDF. So the report logic stays in `src/report_builder.py`; Airflow only **schedules** when that runs.

---

## Server setup (GPU server — install Airflow on the same machine as the project)

Use this when your project (and Ollama) already run on a GPU server and you want Airflow on that same server, **without Docker**.

### Files in this project that Airflow uses

| File / folder | Purpose |
|---------------|--------|
| `airflow/dags/sanjaya_daily_report.py` | The DAG definition (schedule + Python task that fetches data and calls report_builder). |
| `src/report_builder.py` | `build_pdf()`, `send_report_email()` — used by the DAG task and by the MCP server. |
| `src/sanjaya_client.py` | Sanjaya API client; the DAG uses it to fetch `basic_analytics`. |
| `src/time_parse.py` | `parse_time_range()` for converting "today" / "yesterday" to API timestamps. |
| `.env` | Credentials and config: `SANJAYA_*`, `EMAIL_*`, `REPORT_RECIPIENT`, `SANJAYA_DEFAULT_CLIENT`, `SANJAYA_DEFAULT_FLEET`. |
| `.ses_client.py` | Email sending (used by `report_builder.send_report_email`). |

There is no dependency on `report_job.py`; the DAG uses `src/report_builder.py` directly. Your MCP server and Ollama keep running as they do today.

### Step 1: SSH into the server and go to the project

```bash
ssh your-user@your-gpu-server
cd /path/to/fm_mcp_chatbot_comprehensive   # your project root
```

Remember this path — we’ll call it **PROJECT_ROOT**. Example: `/home/ubuntu/fm_mcp_chatbot_comprehensive`.

### Step 2: Create a separate virtualenv for Airflow (recommended)

Keeping Airflow in its own venv avoids conflicts with the project’s venv (MCP, Ollama, etc.):

```bash
cd /path/to/fm_mcp_chatbot_comprehensive
python3 -m venv venv_airflow
source venv_airflow/bin/activate
```

### Step 3: Install Airflow and dependencies

```bash
pip install --upgrade pip
pip install "apache-airflow>=2.10.0" psycopg2-binary
```

If you prefer to use the project’s existing `requirements.txt` plus Airflow, you can use the project venv and add `apache-airflow` and `psycopg2-binary` there instead; then in the steps below use that venv when running `airflow` commands.

### Step 4: Set AIRFLOW_HOME and disable examples

```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=false
```

To make this permanent, add those two lines to `~/.bashrc` or `~/.profile`, then `source ~/.bashrc`.

### Step 5: Point Airflow to your project’s DAGs folder

So Airflow loads the DAG from your repo (no copying files):

```bash
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/fm_mcp_chatbot_comprehensive/airflow/dags
```

Use the **real** path (e.g. `/home/ubuntu/fm_mcp_chatbot_comprehensive/airflow/dags`). Again, add to `~/.bashrc` if you want it every time.

### Step 6: Initialize the database and create an admin user

```bash
airflow db init
airflow users create --username admin --role Admin --email admin@example.com --firstname Admin --lastname User --password admin
```

Change the password in production. By default Airflow uses SQLite (`$AIRFLOW_HOME/airflow.db`). For heavy use you can switch to PostgreSQL later by setting `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`.

### Step 7: Start the scheduler and webserver

Run these in the background (e.g. with `screen`, `tmux`, or systemd):

```bash
airflow scheduler &
airflow webserver --port 8080
```

- **Scheduler**: decides when to run the DAG and runs the task.
- **Webserver**: UI at `http://<server-ip>:8080`. Log in with the admin user you created.

If the server has a firewall, open port 8080, or use SSH port forwarding from your PC: `ssh -L 8080:localhost:8080 your-user@your-gpu-server` and open `http://localhost:8080` in the browser.

### Step 8: Unpause the DAG in the UI

1. Open the Airflow UI in the browser.
2. Find the DAG **sanjaya_daily_report**.
3. Turn the toggle **ON** (unpause).

The DAG runs daily at 08:00 (server timezone). Each run executes a Python task that loads `.env`, fetches analytics from the Sanjaya API, then calls `src.report_builder.build_pdf()` and `send_report_email()`.

### When do I get reports? What email? How to see in the UI?

**When you’ll see reports**

- **Automatically**: The DAG is scheduled for **08:00 every day** in the server’s timezone (see `SCHEDULE_CRON = "0 8 * * *"` in the DAG file). After 08:00, the task runs, builds the PDF(s), and emails them. So you see reports **once per day**, shortly after 08:00.
- **Manually**: In the Airflow UI you can **trigger** the DAG anytime (see below). That run will generate and email reports immediately (no need to wait until 08:00).

**Which email gets the report**

- **Airflow runs** use the **`AIRFLOW_REPORT_RECIPIENT`** variable in your `.env`. In your project it’s set to `akshaykashyap7879@gmail.com`. You can use a comma-separated list for multiple addresses, e.g. `AIRFLOW_REPORT_RECIPIENT=user1@example.com,user2@example.com`.
- If `AIRFLOW_REPORT_RECIPIENT` is not set, the DAG falls back to **`REPORT_RECIPIENT`** (same as chat reports).

**How to see what’s happening in the Airflow UI**

1. **Open the UI** — Usually `http://<server>:8080` (or `http://localhost:8080` if you use SSH port forwarding). Log in (e.g. admin / the password you set).
2. **DAG list** — You’ll see **sanjaya_daily_report**. The **toggle** must be ON (green) for the schedule to run. “Last Run” and “Next Run” show when it ran or will run.
3. **Trigger a run now** — Click the DAG name **sanjaya_daily_report**, then click **“Trigger DAG”** (play button). A new run starts; you’ll see it in the list of runs.
4. **See run details** — Click a run (e.g. the date/time of the run). You’ll see the single task: **generate_and_email_report**. Click that task.
5. **Logs** — Click **“Log”** on the task. The log shows: loading prompts, which client/fleet/time were used, API calls, PDF path, and whether the email was sent. Any Python errors appear here too.

So: **when** = daily at 08:00 (or whenever you trigger); **email** = `AIRFLOW_REPORT_RECIPIENT` (or `REPORT_RECIPIENT`); **what’s happening** = DAG runs → task **generate_and_email_report** → check its **Log** in the UI.

### Step 9: (Optional) Change schedule or timezone

- **Schedule**: Edit `airflow/dags/sanjaya_daily_report.py` on the server and change `SCHEDULE_CRON` (e.g. `"0 9 * * *"` for 09:00). The scheduler picks up file changes automatically.
- **Report period**: Same file, variable `TIME_RANGE` (e.g. `"today"` or `"yesterday"`).

### Quick checklist on the server

- [ ] Project at **PROJECT_ROOT** with `src/report_builder.py`, `src/sanjaya_client.py`, `src/time_parse.py`, `.env`, `.ses_client.py`.
- [ ] `airflow/dags/sanjaya_daily_report.py` present (it’s in the repo).
- [ ] Airflow installed (separate venv or project venv); `AIRFLOW_HOME` and `AIRFLOW__CORE__DAGS_FOLDER` set.
- [ ] `airflow db init` and admin user created.
- [ ] Scheduler and webserver running; DAG **sanjaya_daily_report** unpaused.

The DAG infers **PROJECT_ROOT** as the parent of `airflow/dags` (i.e. your project root), so no extra config is needed as long as the DAG file lives at `fm_mcp_chatbot_comprehensive/airflow/dags/sanjaya_daily_report.py`. The task uses `src/report_builder.py` directly (no `report_job.py`).

---

## Adding Airflow to this project

### Option A: Run Airflow in Docker (recommended for a clean setup)

1. **Install Docker and Docker Compose** on the machine where you want the scheduler and UI.

2. **Use the official image** (example with `docker-compose`):

   Create `docker-compose.airflow.yaml` (or merge into your existing compose file) so that:
   - The **project root** (where `src/`, `.env`, and `.ses_client.py` live) is mounted into the Airflow containers.
   - The **DAGs folder** points to this project’s `airflow/dags` (or a path that contains the DAG file below).

   Example (customize image tag and paths as needed):

   ```yaml
   version: "3"
   services:
     airflow:
       image: apache/airflow:2.10.0-python3.10
       environment:
         AIRFLOW__CORE__EXECUTOR: LocalExecutor
         AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
         AIRFLOW__CORE__LOAD_EXAMPLES: "false"
         AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
         AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "false"
       volumes:
         - ./airflow/dags:/opt/airflow/dags
         - .:/opt/reports_project   # project root with src/, .env, .ses_client.py
       user: "50000:0"
       depends_on:
         - postgres
       # ... (webserver, scheduler, triggerer as separate services or one service)
     postgres:
       image: postgres:13
       environment:
         POSTGRES_USER: airflow
         POSTGRES_PASSWORD: airflow
         POSTGRES_DB: airflow
       volumes:
         - airflow_db_data:/var/lib/postgresql/data
   volumes:
     airflow_db_data: {}
   ```

   Important: the DAG we provide uses `PythonOperator` and imports from `src.report_builder` and `src.sanjaya_client`; the project root must be mounted (e.g. `/opt/reports_project`) and that path set as Airflow Variable `sanjaya_report_project_root` so the task can find `src/` and `.env`.

3. **Start Airflow** (scheduler + webserver + postgres) according to the image’s docs, then open the UI (often http://localhost:8080). Default login is often `admin` / `admin` (change in production).

4. **Copy the DAG file** from this repo’s `airflow/dags/` into the mounted DAGs folder (e.g. `./airflow/dags/sanjaya_daily_report.py`). Ensure the path and `report_job.py` invocation inside the DAG match the mount (e.g. `PROJECT_ROOT = "/opt/reports_project"`).

5. **Unpause the DAG** in the UI. It will run daily at the configured time (e.g. 08:00) and execute `report_job.py`, which builds the PDF and sends the email via your existing `.ses_client` and `REPORT_RECIPIENT`.

### Option B: Run Airflow on the host (no Docker)

1. **Create a virtualenv** (or use the project’s venv) and install Airflow and a DB driver, e.g.:

   ```bash
   pip install "apache-airflow>=2.10.0" psycopg2-binary
   ```

2. **Set `AIRFLOW_HOME`** (e.g. `~/airflow`). Initialize the DB:

   ```bash
   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow users create --username admin --role Admin --email admin@example.com --firstname Admin --lastname User --password admin
   ```

3. **Configure** `airflow.cfg` (or env vars):
   - `dags_folder` = path to this project’s `airflow/dags` (or symlink `~/airflow/dags` → `.../fm_mcp_chatbot_comprehensive/airflow/dags`).
   - `executor` = `LocalExecutor` (or `SequentialExecutor` for minimal setup).
   - Optionally point to a real DB (PostgreSQL) instead of SQLite for production.

4. **Start scheduler and webserver**:

   ```bash
   airflow scheduler &
   airflow webserver --port 8080
   ```

5. Place the provided DAG file in the configured `dags_folder`. The DAG infers the project root from the DAG file path (or use Airflow Variable `sanjaya_report_project_root`). Ensure the project has `src/report_builder.py`, `src/sanjaya_client.py`, `src/time_parse.py`, and `.env`. Unpause the DAG in the UI.

---

## DAG file: `sanjaya_daily_report.py`

The DAG in `airflow/dags/sanjaya_daily_report.py` does the following:

- **Schedule**: Daily at a fixed time (e.g. 08:00) — change `SCHEDULE_CRON` in the file.
- **Task**: One `PythonOperator` that adds the project root to `sys.path`, loads `.env`, then either runs in **prompt-driven mode** or **fallback mode** (see below).
- **Recipients**: Controlled by `REPORT_RECIPIENT` in `.env` and `send_report_email()` in `src/report_builder.py`.

There is no use of `report_job.py`; the DAG uses **`src/report_builder.py`** directly.

### Where the DAG gets prompts (priority order)

1. **File (from chat)**  
   When a user asks for a report in **chat_client** (via MCP `sanjaya_chat`), the server generates the report and **appends that exact prompt** to `scheduled_report_prompts.json` in the project root (`src.scheduled_prompts`). The DAG reads this file **first**. So prompts you give in chat are automatically used by Airflow on its next run (scheduled or manual) — same prompt, same NLU, same basic_analytics call, so no double entry and no wrong data.

2. **Airflow Variable**  
   If the file is missing or empty, the DAG uses the Variable **`sanjaya_report_prompts`** (JSON array of prompt strings). Set it in the UI: Admin → Variables → Key: `sanjaya_report_prompts`, Value: e.g. `["total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant"]`.

3. **Fallback**  
   If neither has prompts, the DAG generates **one** report using `.env` (`SANJAYA_DEFAULT_CLIENT`, `SANJAYA_DEFAULT_FLEET`) and `TIME_RANGE` in the DAG (e.g. "yesterday").

Each prompt is parsed with the same NLU as chat (`src.nlu.parse_query`) to get client_name, fleet_name, time_phrase, timezone; then basic_analytics is called and a PDF is built and emailed.

### Fallback mode (single report from .env)

If the prompts file is empty and `sanjaya_report_prompts` is not set or empty, the DAG generates one report using `SANJAYA_DEFAULT_CLIENT`, `SANJAYA_DEFAULT_FLEET`, and `TIME_RANGE` from the DAG file (e.g. "yesterday").

### basic_analytics vs route_analytics

The scheduled report uses **basic_analytics** only; `build_pdf()` expects that payload (trips, availability, utilization, etc.). **route_analytics** is used by the MCP server for specific metrics (takt time, route utilization, obstacle time). Including route data in the scheduled PDF would require extending `src/report_builder.py` to accept or merge a route_analytics payload; the DAG can be extended to call `route_analytics` and pass that data when the builder supports it.

---

## End-to-end workflow: where prompts come from and how data flows

### 1. Where the prompt comes from (priority order)

| Priority | Source | Description |
|----------|--------|-------------|
| **1** | **File: `scheduled_report_prompts.json`** | When a user asks for a report in **chat** (MCP `sanjaya_chat`), the server appends that exact prompt to `scheduled_report_prompts.json` in the project root (see `src/scheduled_prompts.py` → `add_prompt_for_airflow()`). The DAG reads this file first. |
| **2** | **Airflow Variable: `sanjaya_report_prompts`** | If the file is missing or empty, the DAG uses the Variable (JSON array of prompt strings). Set in UI: Admin → Variables. |
| **3** | **Fallback** | If neither has prompts, the DAG generates **one** report using `.env` (`SANJAYA_DEFAULT_CLIENT`, `SANJAYA_DEFAULT_FLEET`) and `TIME_RANGE` in the DAG (e.g. `"yesterday"`). |

### 2. How the prompt is used (same as chat)

1. **Parse** — Each prompt is parsed with the same NLU as chat (`src.nlu.parse_query`) to get: `client_name`, `fleet_name`, `time_phrase` (e.g. "today", "yesterday"), `timezone`.
2. **Resolve dates** — `parse_time_range(time_phrase, time_zone=timezone)` is called **at DAG run time** with the run’s **logical date** as `now`. So "today" and "yesterday" refer to the **day the DAG runs**, not the day the prompt was saved.
3. **Fetch data** — `SanjayaAPI.basic_analytics(client_name, start_time, end_time, timezone, fleet_name, ...)` calls the Sanjaya API. Data comes from the **endpoints** behind `SANJAYA_BASE_URL`.
4. **Build and send** — `build_pdf(data, ...)` generates the PDF; `send_report_email(...)` emails it to `REPORT_RECIPIENT`.

### 3. Why reports can have empty data

- **API returns no data** for the requested client/fleet/date (e.g. no trips in that range).
- **Wrong client/fleet** in the prompt or in `.env` defaults.
- **Stale fixed date**: if the stored prompt (or NLU) produced a **fixed date** (e.g. "2026-02-26") instead of a relative phrase ("yesterday"), every run keeps using that date. The DAG now forces **relative** time phrases for scheduled runs when the prompt does not contain one, so each run uses "yesterday" (or `TIME_RANGE`) and gets fresh data.
- **Auth/config**: missing or wrong `SANJAYA_USERNAME` / `SANJAYA_PASSWORD` or `SANJAYA_BASE_URL`.

### 4. Correct workflow for dynamic dates (data that changes with the day)

To have **endpoint data** in reports and **dates that change with the run date**:

1. **Use relative time in prompts** — e.g. "Summary for client X and fleet Y **today**" or "Total trips for client X and fleet Y **yesterday**". The DAG resolves these at run time to the current run date.
2. **Scheduled run uses run date** — The DAG passes the Airflow **logical date** into `parse_time_range` as `now`, so "today"/"yesterday" are always relative to the run.
3. **Avoid fixed-date prompts for daily reports** — Prompts like "report for 10 Jan 2026" will keep using that date. For daily changing data, use "today" or "yesterday" or leave time out (DAG uses `TIME_RANGE`).
4. **Ensure API and env** — Correct `SANJAYA_*` and `REPORT_RECIPIENT` in `.env`; the API must return data for the requested client/fleet and time range.
5. **Where endpoint data appears** — The report is built from the **basic_analytics** API response. Whatever that endpoint returns for the requested range is what appears in the PDF.

---

## Summary

- **What Airflow is**: A workflow scheduler and orchestrator where you define DAGs (tasks + schedule + dependencies) in Python and get a UI, retries, and logs.
- **How it works**: Scheduler creates DAG runs and task instances; executor/workers run the tasks; metadata and logs are stored and shown in the UI.
- **How you add it to this project**: Install Airflow (Docker or host), point its DAGs folder to `airflow/dags`, use the provided DAG. The DAG task calls `src/report_builder.py` daily. Reports are sent by your existing email path to `REPORT_RECIPIENT` at the time you set in the DAG schedule.

For “autogenerated reports to recipients thru mail every day at specific time”, the DAG schedule (e.g. `0 8 * * *`) is that “specific time”; the rest is your existing `src/report_builder.py` + `send_report_email()` flow (no `report_job.py`).
