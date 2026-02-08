from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone, timedelta

import aiohttp
from sqlalchemy import create_engine, text
from dagster import AssetKey, Field, asset, get_dagster_logger

from chess_guru import ChesscomAPI
from utilities.utils import load_players_from_yaml, utc_now


def _latest_end_time_utc(engine, username: str) -> datetime | None:
    sql = text("""
        select max(end_time_utc) as max_end_time
        from src_chesscom.games
        where username = :username
    """)

    with engine.connect() as conn:
        row = conn.execute(sql, {"username": username}).first()
        return row[0] if row and row[0] is not None else None


def _extract_games(payload: dict) -> list[dict]:
    """
    chess_guru.get_games() returns a payload with payload["months"][<month>]["games"].
    Flatten to list[game_dict].
    """
    out: list[dict] = []
    months = payload.get("months", {}) or {}

    for _, month_obj in months.items():
        out.extend(month_obj.get("games", []) or [])

    return out


def _upsert_rows(engine, rows: list[dict]) -> int:
    if not rows:
        return 0

    sql = text("""
        insert into src_chesscom.games (
            username,
            player_name,
            game_url,
            end_time_utc,
            ingested_at_utc,
            payload,
            error
        )
        values (
            :username,
            :player_name,
            :game_url,
            :end_time_utc,
            :ingested_at_utc,
            cast(:payload as jsonb),
            :error
        )
        on conflict (username, game_url)
        do update set
            player_name = excluded.player_name,
            end_time_utc = excluded.end_time_utc,
            ingested_at_utc = excluded.ingested_at_utc,
            payload = excluded.payload,
            error = excluded.error
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)


@asset(
    key=AssetKey(["src_chesscom", "games"]),
    config_schema={"usernames": Field([str], is_required=False)},
)
def chesscom_games(context) -> dict:
    """
    Incremental ingest for Chess.com games.
    Can be triggered by a sensor or run manually.
    """
    logger = get_dagster_logger()

    POSTGRES_URL = os.getenv("POSTGRES_URL")
    if not POSTGRES_URL:
        raise ValueError("Missing env var POSTGRES_URL")

    engine = create_engine(POSTGRES_URL)

    target_usernames = set(context.op_config.get("usernames", []) or [])
    players = [
        p
        for p in load_players_from_yaml()
        if not target_usernames or getattr(p, "username", None) in target_usernames
    ]

    async def ingest_all() -> dict:
        ingested_at = utc_now()
        summary = {
            "players_seen": len(players),
            "players_ingested": 0,
            "games_upserted": 0,
        }

        async with aiohttp.ClientSession() as session:
            api = ChesscomAPI(session)

            for p in players:
                username = getattr(p, "username", None)
                if not username:
                    continue

                last_end = _latest_end_time_utc(engine, username)

                from_ts = None
                if last_end is not None:
                    from_ts = last_end + timedelta(seconds=1)

                logger.info("ingest username=%s from_ts=%s", username, from_ts)

                try:
                    payload = await api.get_games(
                        username=username,
                        from_ts=from_ts,
                        to_ts=utc_now(),
                    )
                except Exception as exc:
                    logger.warning(
                        "chesscom get_games failed for username=%s: %s",
                        username,
                        exc,
                    )
                    continue

                games = _extract_games(payload)
                if not games:
                    continue

                rows: list[dict] = []
                for g in games:
                    game_url = g.get("url")
                    if not game_url:
                        continue

                    end_time = g.get("end_time")
                    end_time_utc = (
                        datetime.fromtimestamp(end_time, tz=timezone.utc)
                        if isinstance(end_time, (int, float))
                        else None
                    )

                    rows.append(
                        {
                            "username": username,
                            "player_name": getattr(p, "player_name", None),
                            "game_url": game_url,
                            "end_time_utc": end_time_utc,
                            "ingested_at_utc": ingested_at,
                            "payload": json.dumps(g),
                            "error": None,
                        }
                    )

                upserted = _upsert_rows(engine, rows)

                summary["players_ingested"] += 1
                summary["games_upserted"] += upserted

        return summary

    return asyncio.run(ingest_all())
