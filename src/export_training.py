"""Xuất dataset huấn luyện xG (CSV) từ analytics.fact_shots lên S3: exports/xg_training.csv"""
import io
import logging
import psycopg2
from src import config, storage

QUERY = """
COPY (
  SELECT shot_id, match_id, shot_x, shot_y, distance_to_goal, shot_angle_deg,
         shot_body_part, shot_technique, shot_type, shot_first_time::int AS shot_first_time,
         under_pressure::int AS under_pressure, is_goal::int AS label_goal, shot_xg AS statsbomb_xg
  FROM analytics.fact_shots
  WHERE shot_type <> 'Penalty' OR shot_type IS NULL
) TO STDOUT WITH CSV HEADER
"""


def export():
    buf = io.StringIO()
    with psycopg2.connect(**config.PG) as conn, conn.cursor() as cur:
        cur.copy_expert(QUERY, buf)
    data = buf.getvalue().encode("utf-8")
    client = storage.get_client()
    client.put_object(Bucket=config.S3_BUCKET, Key="exports/xg_training.csv", Body=data)
    logging.getLogger(__name__).info("Đã xuất %d bytes -> exports/xg_training.csv", len(data))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    export()
