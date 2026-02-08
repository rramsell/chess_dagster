from __future__ import annotations

import asyncio
import inspect
import json
import os
from datetime import datetime, timezone

import aiohttp
from sqlalchemy import create_engine, text
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    define_asset_job,
    get_dagster_logger,
)

from chess_guru import ChesscomAPI
from utilities.utils import load_players_from_yaml, utc_now

DEFAULT_USER_AGENT = "chess-guru (chess.com API)"
REQUEST_TIMEOUT_SECONDS = 30


def _is_not_found_exception(exc: Exception) -> bool:
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status == 404
    return "404" in str(exc).lower()


def _not_found_from_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None

    message = payload.get("message") or payload.get("error")
    if isinstance(message, str) and "not found" in message.lower():
        return message

    code = payload.get("code")
    if code == 0 and isinstance(message, str):
        return message

    return None


async def _call_api_method(method, username: str):
    try:
        return await method(username=username)
    except TypeError:
        pass

    try:
        return await method(username)
    except TypeError:
        pass

    return await method()


def _chesscom_method_names() -> list[str]:
    names: list[str] = []
    for name, member in inspect.getmembers(ChesscomAPI):
        if not name in {
            'get_player', 
            'get_archives', 
            'get_player_stats', 
            'get_games_to_move', 
            'get_tournaments'
        }:
            continue
        if callable(member):
            names.append(name)
    return sorted(set(names))


def _table_basename(method_name: str) -> str:
    return method_name.replace("get_", "")


def _table_name(method_name: str) -> str:
    safe_name = _table_basename(method_name)
    return f"src_chesscom.{safe_name}"


def _insert_rows(engine, table_name: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    sql = text(f"""
        insert into {table_name} (
            method,
            username,
            ingested_at_utc,
            payload,
            error
        )
        values (
            :method,
            :username,
            :ingested_at_utc,
            cast(:payload as jsonb),
            :error
        )
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)


def _build_chesscom_asset(method_name: str):
    asset_name = _table_basename(method_name)

    @asset(name=asset_name, key_prefix=["src_chesscom"])
    def _asset():
        logger = get_dagster_logger()
        players = [p for p in load_players_from_yaml()]
        usernames = [
            getattr(p, "username", None)
            for p in players
            if getattr(p, "username", None)
        ]
        ingested_at_dt = utc_now()
        ingested_at = ingested_at_dt.isoformat()

        async def fetch_all():
            results: dict[str, object] = {}
            errors: dict[str, str] = {}

            if not usernames:
                return results, errors

            user_agent = os.getenv("CHESS_GURU_USER_AGENT", DEFAULT_USER_AGENT)
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

            async with aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": user_agent},
            ) as session:
                try:
                    api = ChesscomAPI(session, user_agent=user_agent)
                except TypeError:
                    api = ChesscomAPI(session)

                method = getattr(api, method_name)

                for username in usernames:
                    try:
                        payload = await _call_api_method(method, username)
                        not_found_message = _not_found_from_payload(payload)
                        if not_found_message:
                            errors[username] = f"not_found: {not_found_message}"
                            results[username] = None
                            logger.warning(
                                "chesscom %s not found for username=%s",
                                method_name,
                                username,
                            )
                            continue

                        results[username] = payload
                    except Exception as exc:
                        errors[username] = str(exc)
                        if _is_not_found_exception(exc):
                            logger.warning(
                                "chesscom %s not found for username=%s",
                                method_name,
                                username,
                            )
                        else:
                            logger.exception(
                                "chesscom %s failed for username=%s",
                                method_name,
                                username,
                            )

            return results, errors

        results, errors = asyncio.run(fetch_all())

        POSTGRES_URL = os.getenv("POSTGRES_URL")
        if not POSTGRES_URL:
            raise ValueError("Missing env var POSTGRES_URL")

        engine = create_engine(POSTGRES_URL)
        table_name = _table_name(method_name)

        rows: list[dict] = []
        for username in usernames:
            payload = results.get(username)
            rows.append(
                {
                    "method": method_name,
                    "username": username,
                    "ingested_at_utc": ingested_at_dt,
                    "payload": json.dumps(payload)
                    if payload is not None
                    else None,
                    "error": errors.get(username),
                }
            )

        _insert_rows(engine, table_name, rows)

        return {
            "method": method_name,
            "players_seen": len(usernames),
            "results": results,
            "errors": errors,
            "ingested_at_utc": ingested_at,
        }

    return _asset


CHESSCOM_METHOD_NAMES = _chesscom_method_names()
CHESSCOM_ASSET_NAMES = [_table_basename(name) for name in CHESSCOM_METHOD_NAMES]
CHESSCOM_ASSETS = []

for _method_name in CHESSCOM_METHOD_NAMES:
    _asset_def = _build_chesscom_asset(_method_name)
    CHESSCOM_ASSETS.append(_asset_def)
    globals()[_method_name] = _asset_def


_asset_keys = [AssetKey(["src_chesscom", name]) for name in CHESSCOM_ASSET_NAMES]

src_chesscom_player_job = define_asset_job(
    "src_chesscom",
    selection=AssetSelection.keys(*_asset_keys),
)

src_chesscom_schedule = ScheduleDefinition(
    name="src_chesscom",
    job=src_chesscom_player_job,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
