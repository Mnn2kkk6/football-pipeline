"""Extract: lấy dữ liệu StatsBomb open-data và ghi raw JSON vào S3.

Layout: raw/{year}/{league}/{match_id}/{events|lineups|match}.json
Idempotent: trận nào đã có trên S3 thì bỏ qua (chạy lại không tải trùng).
"""
import logging
import re
import time
import requests

from src import storage

log = logging.getLogger(__name__)
BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


def _get(path: str):
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE}/{path}", timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.warning("GET %s lỗi (lần %d): %s", path, attempt + 1, e)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Không tải được {path}")


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def extract(competition_id: int, season_id: int, limit: int | None = None) -> dict:
    client = storage.get_client()
    storage.ensure_bucket(client)
    matches = _get(f"matches/{competition_id}/{season_id}.json")
    matches = [m for m in matches if m.get("match_status") == "available"]
    if limit:
        matches = matches[:limit]

    new, skipped = 0, 0
    for m in matches:
        mid = m["match_id"]
        year = m["match_date"][:4]
        league = _slug(m["competition"]["competition_name"])
        prefix = f"raw/{year}/{league}/{mid}"

        if storage.object_exists(client, f"{prefix}/events.json"):
            skipped += 1
            continue

        storage.put_json(client, f"{prefix}/match.json", m)
        storage.put_json(client, f"{prefix}/lineups.json", _get(f"lineups/{mid}.json"))
        # events ghi cuối cùng: là "dấu hiệu hoàn tất" của một trận
        storage.put_json(client, f"{prefix}/events.json", _get(f"events/{mid}.json"))
        new += 1
        log.info("Đã nạp match %s -> %s", mid, prefix)

    result = {"new": new, "skipped": skipped, "total": len(matches)}
    log.info("Extract xong: %s", result)
    return result


if __name__ == "__main__":
    import os
    logging.basicConfig(level=logging.INFO)
    extract(int(os.getenv("COMPETITION_ID", 43)), int(os.getenv("SEASON_ID", 3)))