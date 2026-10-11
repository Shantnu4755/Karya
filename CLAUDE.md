# CLAUDE.md: Karya

Job queue built from scratch on PostgreSQL: SKIP LOCKED claiming, heartbeats, exponential backoff, dead-letter. Part of a six-project portfolio platform that runs on one free Oracle ARM VM.

- **Source of truth:** the PDF `~/Downloads/Portfolio Projects Complete Build & Free Deployment Plan.pdf`.
- **Progress and deviations:** `../PROGRESS.md`. Update it after every step. Any change to the PDF's design goes under "Deviations from the plan" with the reason; never change the design silently.

## Rules (PDF "Coding rules")
1. Class-based views only (`BaseView`, a Flask `MethodView`) with **only `get()` and `post()`**. Updates and deletes are a POST to an action endpoint (for example `POST /claims/<id>/approve`). `tests/test_view_rules.py` enforces it.
2. Layers: **View** (HTTP only) → **Service** (business logic) → **Repository** (DB access) → **Model**. A view never writes SQL or opens sessions; a repository never imports Flask.
3. Errors: raise `AppError` subclasses from `app/exceptions.py`. One global handler returns `{"error": {"code", "message", "details", "request_id"}}`. No bare `except:`, no `print`, no swallowed errors (log with `logger.exception` when a loop must continue).
4. Every request is validated with Pydantic (`BaseView.parse_body` / `parse_query`) before it reaches a service.
5. Config from environment variables only (`app/config.py`, fails fast at boot). `.env.example` is committed, `.env` never is.
6. Structured JSON logs with `request_id` on every line. Allowed extra fields: `code`, `error_message`, `job_id`, `service`.
7. pytest for every service class, against real PostgreSQL (never SQLite). External calls (LLM, Telegram, Docker, HTTP) are mocked. CI fails under 80% coverage.

Also: Python 3.12, type hints everywhere, `ruff check .` clean, small commits prefixed `feat:` `fix:` `test:` `docs:` `chore:`. Push after every verified change (the local folder was once deleted by an outside process; GitHub is the backup).

## Layout (PDF 2.1)
`app/{__init__,config,extensions,exceptions,error_handlers,logging_setup}.py`, `app/models`, `app/schemas`, `app/repositories`, `app/services`, `app/views` (`base.py`, `health.py`, `__init__.py` with `register_routes`), `migrations/` (Alembic), `tests/`, `wsgi.py`, `gunicorn.conf.py`, `Dockerfile`, `ui/` (React UI, PDF 3A).

## Commands
```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.lock -r requirements-dev.txt
# local test databases: docker container "portfolio-test-pg" on port 55432 (password "test")
export TEST_DATABASE_URL=postgresql+psycopg://postgres:test@localhost:55432/karya_test
ruff check . && pytest --cov=app --cov-report=term-missing
DATABASE_URL=... alembic revision --autogenerate -m "describe change" && alembic upgrade head
docker build -t karya .
```
Dependencies: runtime packages go in `requirements.txt`, test tools in `requirements-dev.txt`. `requirements.lock` is the frozen runtime set (`pip freeze` of a runtime-only install, or of the built image), so pytest never ships in the image.

## Karya specifics (PDF 3.2)
- **Table `jobs`:** id bigserial, queue, task, payload jsonb, status (`queued` `running` `done` `dead`), attempts, max_attempts (default 5), run_at, locked_by, heartbeat_at, last_error, idempotency_key (unique, nullable), created_at. Index `(queue, status, run_at)`. Plus `job_attempts` (one row per attempt).
- **Endpoints:** `POST /jobs` (`{queue, task, payload, delay_seconds, max_attempts, idempotency_key}`), `GET /jobs?status=&task=&queue=`, `GET /jobs/<id>`, `POST /jobs/<id>/retry`, `POST /jobs/retry` (bulk), `GET /queues/stats`, `GET /tasks`.
- **Worker:** `python -m app.worker`, same image as the API. Claims with `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)`. Heartbeat thread every 15 s. Reaper re-queues `running` jobs with no heartbeat for 60 s (dead when attempts are used up). Backoff 10 s, 20 s, 40 s... capped at 1 h; `dead` after max_attempts. Writes check `locked_by` (leases). SIGTERM lets the current job finish.
- **Tests to write:** two workers claiming at once never get the same job; a raising job is retried then becomes dead; a job whose heartbeat stops is re-queued by the reaper; SIGTERM lets the current job finish.
- **UI accent:** sky blue (hue 235).
