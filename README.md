# Chess Dagster

[WIP] This is still a very early work-in-progress -- to remain a judgement free zone until this is more official and the code more solid.

Dagster + dbt project for ingesting chess.com data into Postgres and building models on top of it. 

**Prereqs**
- Python 3.12
- Postgres

**Setup**
1. Create a virtual environment and install dependencies:
```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```
2. Create `.env` from the example and set your Postgres connection:
```powershell
Copy-Item .env.example .env
```
3. Configure dbt profiles (choose one):
- Set `DBT_PROFILES_DIR` to a folder containing `profiles.yml`
- Or place `profiles.yml` in `.dbt/` at the repo root
- Or use your user profile at `%USERPROFILE%\.dbt`

**Run Dagster**
```powershell
.\.venv\Scripts\dagster.exe dev -w workspace.yaml
```

**Run dbt Directly (optional)**
```powershell
.\.venv\Scripts\dbt.exe build --project-dir dbt
```

**Project Layout**
- `dagster/` Dagster code location, assets, and sensors
- `dbt/` dbt project
- `workspace.yaml` Dagster workspace entrypoint

**Config**
- `POSTGRES_URL` is required in `.env`
- `DBT_*` env vars are required for dbt profiles (see `.env.example`)

**License**
MIT. See `LICENSE`.
