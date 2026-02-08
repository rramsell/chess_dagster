from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiohttp
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    define_asset_job,
    sensor,
)

from chess_guru import ChesscomAPI
from utilities.utils import load_chess_players, utc_now


def _load_players_from_yaml():
    yml_path = Path(__file__).resolve().parents[1] / "assets" / "asset_definitions" / "chess_players.yml"
    return load_chess_players(yml_path)


def _latest_end_time_utc(engine, username: str) -> datetime | None:
    sql = text("""
        select max(end_time_utc) as max_end_time
        from src_chesscom.games
        where username = :username
    """)

    try:
        with engine.connect() as conn:
            row = conn.execute(sql, {"username": username}).first()
            return row[0] if row and row[0] is not None else None
    except ProgrammingError as exc:
        if "src_chesscom.games" in str(exc):
            return None
        raise


def _extract_games(payload: dict) -> list[dict]:
    out: list[dict] = []
    months = payload.get("months", {}) or {}

    for _, month_obj in months.items():
        out.extend(month_obj.get("games", []) or [])

    return out


src_chesscom_games_job = define_asset_job(
    "src_chesscom_games",
    selection=AssetSelection.keys(AssetKey(["src_chesscom", "games"])),
)


@sensor(
    job=src_chesscom_games_job,
    minimum_interval_seconds=60*5, 
    default_status=DefaultSensorStatus.RUNNING
)
def chesscom_new_games_sensor(context):
    POSTGRES_URL = os.getenv("POSTGRES_URL")
    if not POSTGRES_URL:
        yield SkipReason("Missing env var POSTGRES_URL")
        return

    players = [p for p in _load_players_from_yaml()]
    if not players:
        yield SkipReason("No players configured.")
        return

    engine = create_engine(POSTGRES_URL)

    last_end_by_user: dict[str, datetime | None] = {}
    for p in players:
        username = getattr(p, "username", None)
        if not username:
            continue
        last_end_by_user[username] = _latest_end_time_utc(engine, username)

    async def detect_new_games() -> dict[str, dict]:
        async with aiohttp.ClientSession() as session:
            api = ChesscomAPI(session)
            results: dict[str, dict] = {}

            for p in players:
                username = getattr(p, "username", None)
                if not username:
                    continue

                last_end = last_end_by_user.get(username)
                from_ts = last_end + timedelta(seconds=1) if last_end else None

                try:
                    payload = await api.get_games(
                        username=username,
                        from_ts=from_ts,
                        to_ts=utc_now(),
                    )
                except Exception as exc:
                    context.log.warning(
                        "chesscom get_games failed for username=%s: %s",
                        username,
                        exc,
                    )
                    continue

                games = _extract_games(payload)
                if not games:
                    continue

                max_end = None
                for g in games:
                    end_time = g.get("end_time")
                    if isinstance(end_time, (int, float)):
                        g_dt = datetime.fromtimestamp(end_time, tz=timezone.utc)
                        if max_end is None or g_dt > max_end:
                            max_end = g_dt

                results[username] = {
                    "new_count": len(games),
                    "max_end": max_end,
                }

            return results

    results = asyncio.run(detect_new_games())

    if not results:
        yield SkipReason("No new chess.com games detected.")
        return

    for username, info in results.items():
        max_end = info.get("max_end")
        run_key = f"chesscom_games:{username}:{max_end.isoformat() if max_end else 'unknown'}"
        context.log.info("New games detected for %s (count=%s).", username, info["new_count"])
        yield RunRequest(
            run_key=run_key,
            run_config={
                "ops": {
                    "src_chesscom__games": {
                        "config": {"usernames": [username]},
                    }
                }
            },
            tags={"username": username},
        )
