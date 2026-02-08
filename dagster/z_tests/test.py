import asyncio
import aiohttp
from datetime import datetime, timezone

from chess_guru import ChesscomAPI


def ts_to_utc_str(ts: int | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def game_ts(game: dict) -> int:
    # prefer start_time if it exists, else fall back to end_time
    return int(game.get("start_time") or game.get("end_time") or 0)


async def main(username: str):
    async with aiohttp.ClientSession() as session:
        api = ChesscomAPI(session, user_agent="latest-game-script/1.0")

        archives = await api.get_archives(username)
        month_urls = archives.get("archives", []) or []
        if not month_urls:
            print("No archives found for:", username)
            return

        # newest 2 months just in case
        latest_month_urls = month_urls[-2:]

        games = []
        for url in latest_month_urls:
            async with session.get(url) as resp:
                resp.raise_for_status()
                payload = await resp.json()
            games.extend(payload.get("games", []) or [])

        games = [g for g in games if game_ts(g) > 0]
        if not games:
            print("No games found for:", username)
            return

        latest = max(games, key=game_ts)

        print("username:", username)
        print("latest_game_url:", latest.get("url"))
        print("latest_time_utc:", ts_to_utc_str(game_ts(latest)))

        white = latest.get("white", {}) or {}
        black = latest.get("black", {}) or {}
        print("white:", white.get("username"), white.get("rating"), white.get("result"))
        print("black:", black.get("username"), black.get("rating"), black.get("result"))


if __name__ == "__main__":
    asyncio.run(main("wfree364"))
