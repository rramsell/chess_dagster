from __future__ import annotations

import os

from dagster import AssetKey, AssetSelection, asset, define_asset_job
from sqlalchemy import create_engine, text


def _run_ddl(statements: list[str]) -> None:
    postgres_url = os.getenv("POSTGRES_URL")
    if not postgres_url:
        raise ValueError("Missing env var POSTGRES_URL")

    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


@asset(name="src_chesscom_player_swap", key_prefix=["admin"])
def src_chesscom_player_swap() -> dict[str, str]:
    statements = [
        "create schema if not exists src_chesscom",
        "drop table if exists src_chesscom.player cascade",
        """
        create table src_chesscom.player (
            method text not null,
            username text not null,
            ingested_at_utc timestamptz not null,
            payload jsonb,
            error text
        )
        """,
    ]
    _run_ddl(statements)
    return {"table": "src_chesscom.player", "status": "recreated"}


@asset(name="src_chesscom_archives_swap", key_prefix=["admin"])
def src_chesscom_archives_swap() -> dict[str, str]:
    statements = [
        "create schema if not exists src_chesscom",
        "drop table if exists src_chesscom.archives cascade",
        """
        create table src_chesscom.archives (
            method text not null,
            username text not null,
            ingested_at_utc timestamptz not null,
            payload jsonb,
            error text
        )
        """,
    ]
    _run_ddl(statements)
    return {"table": "src_chesscom.archives", "status": "recreated"}


@asset(name="src_chesscom_games_swap", key_prefix=["admin"])
def src_chesscom_games_swap() -> dict[str, str]:
    statements = [
        "create schema if not exists src_chesscom",
        "drop table if exists src_chesscom.games cascade",
        """
        create table src_chesscom.games (
            username text not null,
            player_name text,
            game_url text not null,
            end_time_utc timestamptz,
            ingested_at_utc timestamptz not null,
            payload jsonb,
            error text,
            unique (username, game_url)
        )
        """,
    ]
    _run_ddl(statements)
    return {"table": "src_chesscom.games", "status": "recreated"}


@asset(name="src_chesscom_player_stats_swap", key_prefix=["admin"])
def src_chesscom_player_stats_swap() -> dict[str, str]:
    statements = [
        "create schema if not exists src_chesscom",
        "drop table if exists src_chesscom.player_stats cascade",
        """
        create table src_chesscom.player_stats (
            method text not null,
            username text not null,
            ingested_at_utc timestamptz not null,
            payload jsonb,
            error text
        )
        """,
    ]
    _run_ddl(statements)
    return {"table": "src_chesscom.player_stats", "status": "recreated"}


@asset(name="src_chesscom_games_to_move_swap", key_prefix=["admin"])
def src_chesscom_games_to_move_swap() -> dict[str, str]:
    statements = [
        "create schema if not exists src_chesscom",
        "drop table if exists src_chesscom.games_to_move cascade",
        """
        create table src_chesscom.games_to_move (
            method text not null,
            username text not null,
            ingested_at_utc timestamptz not null,
            payload jsonb,
            error text
        )
        """,
    ]
    _run_ddl(statements)
    return {"table": "src_chesscom.games_to_move", "status": "recreated"}


@asset(name="src_chesscom_tournaments_swap", key_prefix=["admin"])
def src_chesscom_tournaments_swap() -> dict[str, str]:
    statements = [
        "create schema if not exists src_chesscom",
        "drop table if exists src_chesscom.tournaments cascade",
        """
        create table src_chesscom.tournaments (
            method text not null,
            username text not null,
            ingested_at_utc timestamptz not null,
            payload jsonb,
            error text
        )
        """,
    ]
    _run_ddl(statements)
    return {"table": "src_chesscom.tournaments", "status": "recreated"}


_admin_asset_keys = [
    AssetKey(["admin", "src_chesscom_player_swap"]),
    AssetKey(["admin", "src_chesscom_archives_swap"]),
    AssetKey(["admin", "src_chesscom_games_swap"]),
    AssetKey(["admin", "src_chesscom_player_stats_swap"]),
    AssetKey(["admin", "src_chesscom_games_to_move_swap"]),
    AssetKey(["admin", "src_chesscom_tournaments_swap"]),
]

src_chesscom_swap = define_asset_job(
    "src_chesscom_swap",
    selection=AssetSelection.keys(*_admin_asset_keys),
)
