from pathlib import Path
import sys

from dotenv import load_dotenv
load_dotenv()

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    load_assets_from_modules,
)

# dagster runs from repo root
_src_dir = str(Path(__file__).parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from assets import src_chesscom_admin as chesscom_admin_assets
from assets import src_chesscom_player as chesscom_player_assets
from assets import src_chesscom_games as chesscom_games_assets
from assets.dbt import dbt_assets, dbt_resource
from sensors.src_chesscom import src_chesscom_games_job, chesscom_new_games_sensor

all_assets = load_assets_from_modules(
    [chesscom_admin_assets, chesscom_player_assets, chesscom_games_assets]
)

assets = [*all_assets, dbt_assets]
automation_condition_sensor = AutomationConditionSensorDefinition(
    name="default_automation_condition_sensor",
    target=AssetSelection.all(),
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60 * 60,
)
sensors = [automation_condition_sensor, chesscom_new_games_sensor]
jobs = [
    src_chesscom_games_job,
    chesscom_player_assets.src_chesscom_player_job,
    chesscom_admin_assets.src_chesscom_swap,
]
schedules = [chesscom_player_assets.src_chesscom_schedule]
resources = {"dbt": dbt_resource}

defs = Definitions(
    assets=assets,
    sensors=sensors,
    jobs=jobs,
    schedules=schedules,
    resources=resources,
)
