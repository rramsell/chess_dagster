import os
from pathlib import Path
from typing import Any

from dagster import AssetKey, AutoMaterializePolicy, AutoMaterializeRule, MetadataValue

try:
    from dagster_dbt import DbtCliResource, DagsterDbtTranslator, DbtProject, dbt_assets as dbt_assets_decorator
    from dagster_dbt.asset_utils import default_asset_key_fn
except ImportError as exc:
    raise ImportError(
        "dagster-dbt is required to load dbt assets. Install it in your environment."
    ) from exc


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


DBT_PROJECT_DIR = _repo_root() / "dbt"
_env_profiles_dir = os.getenv("DBT_PROFILES_DIR")
if _env_profiles_dir:
    DBT_PROFILES_DIR = Path(_env_profiles_dir)
else:
    _project_profiles = DBT_PROJECT_DIR / ".dbt"
    _repo_profiles = _repo_root() / ".dbt"
    if _project_profiles.exists():
        DBT_PROFILES_DIR = _project_profiles
    elif _repo_profiles.exists():
        DBT_PROFILES_DIR = _repo_profiles
    else:
        DBT_PROFILES_DIR = Path.home() / ".dbt"

_required_dbt_env = [
    "DBT_HOST",
    "DBT_PORT",
    "DBT_USER",
    "DBT_PASSWORD",
    "DBT_DBNAME",
    "DBT_SCHEMA",
]
_missing_dbt_env = [name for name in _required_dbt_env if not os.getenv(name)]
if _missing_dbt_env:
    missing = ", ".join(_missing_dbt_env)
    raise RuntimeError(
        f"Missing required DBT env vars: {missing}. "
        "Set them in your environment or `.env`."
    )

_env_dbt_executable = os.getenv("DBT_CLI_PATH") or os.getenv("DBT_EXECUTABLE")
if _env_dbt_executable:
    DBT_EXECUTABLE = Path(_env_dbt_executable)
else:
    _venv = _repo_root() / ".venv" / "Scripts" / "dbt.exe"
    _venv_dbt = _repo_root() / ".venv-dbt" / "Scripts" / "dbt.exe"
    if _venv.exists():
        DBT_EXECUTABLE = _venv
    elif _venv_dbt.exists():
        DBT_EXECUTABLE = _venv_dbt
    else:
        DBT_EXECUTABLE = None


_RULE_MAP = {
    "materialize_on_missing": AutoMaterializeRule.materialize_on_missing(),
    "materialize_on_parent_updated": AutoMaterializeRule.materialize_on_parent_updated(),
    "materialize_on_required_for_freshness": AutoMaterializeRule.materialize_on_required_for_freshness(),
    "skip_on_parent_outdated": AutoMaterializeRule.skip_on_parent_outdated(),
    "skip_on_parent_missing": AutoMaterializeRule.skip_on_parent_missing(),
    "skip_on_all_parents_not_updated": getattr(
        AutoMaterializeRule, "skip_on_not_all_parents_updated", lambda: None
    )(),
    "skip_on_not_all_parents_updated": getattr(
        AutoMaterializeRule, "skip_on_not_all_parents_updated", lambda: None
    )(),
    "skip_on_required_but_nonexistent_parents": AutoMaterializeRule.skip_on_required_but_nonexistent_parents(),
    "skip_on_backfill_in_progress": AutoMaterializeRule.skip_on_backfill_in_progress(),
}


def _resolve_auto_materialize_policy(spec: Any) -> AutoMaterializePolicy | None:
    if spec is None:
        return None

    if isinstance(spec, str):
        spec_lower = spec.lower()
        if spec_lower == "eager":
            return AutoMaterializePolicy.eager()
        if spec_lower == "lazy":
            return AutoMaterializePolicy.lazy()
        if spec_lower in {"none", "disabled", "false"}:
            return None

    if isinstance(spec, dict):
        if spec.get("enabled") is False:
            return None

        policy_type = str(spec.get("type", "eager")).lower()
        if policy_type == "lazy":
            policy = AutoMaterializePolicy.lazy()
        else:
            policy = AutoMaterializePolicy.eager()

        with_rules = spec.get("with_rules", []) or []
        without_rules = spec.get("without_rules", []) or []

        if with_rules:
            policy = policy.with_rules(*[_RULE_MAP[name] for name in with_rules if name in _RULE_MAP])
        if without_rules:
            policy = policy.without_rules(
                *[_RULE_MAP[name] for name in without_rules if name in _RULE_MAP]
            )

        return policy

    return None


class ChessDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        # Allow for specified dagster asset_keys set in dbt meta
        dbt_meta = dbt_resource_props.get("config", {}).get("meta", {}) or dbt_resource_props.get(
            "meta", {}
        )
        dagster_meta = dbt_meta.get("dagster", {}) if isinstance(dbt_meta, dict) else {}
        asset_key_config = dagster_meta.get("asset_key", [])
        if asset_key_config:
            return AssetKey(asset_key_config)

        if dbt_resource_props.get("resource_type") == "model":
            schema = dbt_resource_props.get("schema") or dbt_resource_props.get("config", {}).get(
                "schema"
            )
            alias = dbt_resource_props.get("alias") or dbt_resource_props.get("name")
            if schema:
                return AssetKey([schema, alias])
            return AssetKey([alias])

        return default_asset_key_fn(dbt_resource_props)

    def _merged_meta(self, dbt_resource_props: dict) -> dict:
        merged: dict[str, Any] = {}
        for meta_source in (
            dbt_resource_props.get("meta"),
            dbt_resource_props.get("config", {}).get("meta"),
        ):
            if isinstance(meta_source, dict):
                merged.update(meta_source)
        return merged

    def _dagster_meta(self, dbt_resource_props: dict) -> dict:
        meta = self._merged_meta(dbt_resource_props)
        dagster_meta = meta.get("dagster", {})
        return dagster_meta if isinstance(dagster_meta, dict) else {}

    def get_metadata(self, dbt_resource_props: dict) -> dict[str, Any]:
        metadata = super().get_metadata(dbt_resource_props)
        meta = self._merged_meta(dbt_resource_props)
        if meta:
            metadata["dbt_meta"] = MetadataValue.json(meta)
        return metadata

    def get_auto_materialize_policy(
        self, dbt_resource_props: dict
    ) -> AutoMaterializePolicy | None:
        dagster_meta = self._dagster_meta(dbt_resource_props)
        return _resolve_auto_materialize_policy(dagster_meta.get("auto_materialize_policy"))

    def get_automation_condition(self, dbt_resource_props: dict):
        policy = self.get_auto_materialize_policy(dbt_resource_props)
        return policy.to_automation_condition() if policy else None


_dbt_resource_kwargs = {
    "profiles_dir": str(DBT_PROFILES_DIR),
}
if DBT_EXECUTABLE is not None:
    _dbt_resource_kwargs["dbt_executable"] = str(DBT_EXECUTABLE)

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=dbt_project, **_dbt_resource_kwargs)


@dbt_assets_decorator(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=ChessDagsterDbtTranslator(),
    project=dbt_project,
)
def dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
