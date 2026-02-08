from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

ALLOWED_PLATFORMS = {"chesscom", "lichess"}


@dataclass(frozen=True)
class ChessPlayer:
    username: str
    online_platform: str = "chesscom"
    player_name: Optional[str] = None

def load_yaml(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _none_if_blank(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def _validate_platform(platform: str | None) -> str:
    if platform is None:
        return "chesscom"

    plt = platform.strip().lower()

    if not plt:
        return "chesscom"

    if plt not in ALLOWED_PLATFORMS:
        raise ValueError(
            f"Invalid online_platform '{platform}'. Must be one of {ALLOWED_PLATFORMS}"
        )

    return plt


def load_chess_players(yml_path: str | Path) -> list[ChessPlayer]:
    data = load_yaml(yml_path)

    players: list[ChessPlayer] = []
    for p in data.get("players", []):
        username = _none_if_blank(p.get("username"))
        if not username:
            raise ValueError("Each player must have a username")

        players.append(
            ChessPlayer(
                username=username,
                online_platform=_validate_platform(p.get("online_platform")),
                player_name=_none_if_blank(p.get("player_name")),
            )
        )

    return players


def load_players_from_yaml() -> list[ChessPlayer]:
    yml_path = Path(__file__).resolve().parents[1] / "assets" / "asset_definitions" / "chess_players.yml"
    return load_chess_players(yml_path)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
