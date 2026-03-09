# HH Parser

## Structure
- `config.py`: environment-based settings.
- `schema.py`: DB schema (vacancies, skills, links, salary features, parse runs).
- `hh_client.py`: HH API client.
- `transform.py`: normalize HH payload to internal vacancy model.
- `repository.py`: upsert data into DB.
- `pipeline.py`: load vacancies with run tracking.
- `it_queries.py`: default IT search queries.

## Local Run (one-shot)
1. Copy `.env.example` to `.env` and set `DB_PASSWORD`.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Start IT parser loop:
   - `python run.py`
4. Show DB overview:
   - `python show_data.py`

## Autonomous IT Loop
- Run forever:
  - `python run.py`
- Run one cycle only:
  - set `IT_RUN_ONCE=true` in `.env`

Key loop settings in `.env`:
- `IT_PAGES`, `IT_PER_PAGE`
- `IT_TARGET_PER_QUERY` (target count per query per cycle, default 100)
- `IT_LOOP_INTERVAL_MINUTES`
- `IT_WITH_SALARY_ONLY`
- `IT_QUERIES` (comma-separated, optional)
- `IT_RECREATE_ON_START` (use `true` only when you want full reset)
- `HH_SLEEP_BETWEEN_REQUESTS_SEC`, `HH_RETRY_ATTEMPTS`, `HH_RETRY_BACKOFF_SEC`
- `HH_403_COOLDOWN_THRESHOLD`, `HH_403_COOLDOWN_SEC` (global cooldown on repeated 403)

## Deploy To Server (Docker)
1. Install Docker + Docker Compose plugin on server.
2. Copy project to server.
3. Create `.env` from `.env.example` and set production password.
4. Start service:
   - `docker compose up -d --build`
5. Check logs:
   - `docker compose logs -f hh_parser`

## Deploy To Server (systemd, no Docker)
1. Create venv and install:
   - `python3 -m venv .venv`
   - `. .venv/bin/activate`
   - `pip install -r requirements.txt`
2. Create service file `/etc/systemd/system/hh-parser.service`:
   - `WorkingDirectory=/opt/hh-parser`
   - `ExecStart=/opt/hh-parser/.venv/bin/python /opt/hh-parser/run.py`
   - `Restart=always`
3. Enable and start:
   - `sudo systemctl daemon-reload`
   - `sudo systemctl enable --now hh-parser`
4. Logs:
   - `sudo journalctl -u hh-parser -f`
