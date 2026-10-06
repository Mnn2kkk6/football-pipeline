"""Transform: đọc JSON thô từ S3 -> flatten -> làm sạch -> ghi Parquet (S3) + load PostgreSQL (schema raw)."""
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F

from src import config

log = logging.getLogger(__name__)

NAME = "struct<id:int,name:string>"
EVENT_SCHEMA = f"""
  id string, index int, period int, minute int, second int, type {NAME},
  possession int, possession_team {NAME}, play_pattern {NAME}, team {NAME},
  player {NAME}, position {NAME}, location array<double>, under_pressure boolean,
  pass struct<recipient:{NAME}, length:double, angle:double, height:struct<name:string>,
              end_location:array<double>, outcome:struct<name:string>,
              body_part:struct<name:string>, type:struct<name:string>>,
  shot struct<end_location:array<double>, statsbomb_xg:double, outcome:struct<name:string>,
              body_part:struct<name:string>, technique:struct<name:string>,
              type:struct<name:string>, first_time:boolean>
"""
MATCH_SCHEMA = """
  match_id long, match_date string, kick_off string,
  competition struct<competition_id:int, competition_name:string, country_name:string>,
  season struct<season_id:int, season_name:string>,
  home_team struct<home_team_id:int, home_team_name:string>,
  away_team struct<away_team_id:int, away_team_name:string>,
  home_score int, away_score int
"""
LINEUP_SCHEMA = """
  team_id int, team_name string,
  lineup array<struct<player_id:int, player_name:string, player_nickname:string,
                      jersey_number:int, country:struct<id:int,name:string>>>
"""


def build_spark() -> SparkSession:
    b = (
        SparkSession.builder.appName("football-transform")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID or "")
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY or "")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if config.S3_ENDPOINT_URL:  # MinIO / S3-compatible
        b = (b.config("spark.hadoop.fs.s3a.endpoint", config.S3_ENDPOINT_URL)
              .config("spark.hadoop.fs.s3a.path.style.access", "true")
              .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                      str(config.S3_ENDPOINT_URL.startswith("https")).lower()))
    return b.getOrCreate()


def _read(spark, name: str, schema: str) -> DataFrame:
    path = f"s3a://{config.S3_BUCKET}/raw/*/*/*/{name}.json"
    df = spark.read.schema(schema).option("multiLine", True).json(path)
    return df.withColumn(
        "match_id_file",
        F.regexp_extract(F.input_file_name(), r"/(\d+)/" + name + r"\.json$", 1).cast("long"),
    )


def flatten_events(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("id").alias("event_id"),
        F.col("match_id_file").alias("match_id"),
        F.col("index").alias("event_index"),
        "period",
        F.col("minute").alias("event_minute"),
        F.col("second").alias("event_second"),
        F.col("type.name").alias("event_type"),
        F.col("team.id").alias("team_id"),
        F.col("team.name").alias("team_name"),
        "possession",
        F.col("possession_team.id").alias("possession_team_id"),
        F.col("play_pattern.name").alias("play_pattern"),
        F.col("player.id").alias("player_id"),
        F.col("player.name").alias("player_name"),
        F.col("position.name").alias("position"),
        F.col("location")[0].alias("x"),
        F.col("location")[1].alias("y"),
        F.coalesce("under_pressure", F.lit(False)).alias("under_pressure"),
        # pass
        F.col("pass.recipient.id").alias("pass_recipient_id"),
        F.col("pass.recipient.name").alias("pass_recipient_name"),
        F.col("pass.length").alias("pass_length"),
        F.col("pass.angle").alias("pass_angle"),
        F.col("pass.height.name").alias("pass_height"),
        F.col("pass.end_location")[0].alias("pass_end_x"),
        F.col("pass.end_location")[1].alias("pass_end_y"),
        F.col("pass.outcome.name").alias("pass_outcome"),   # null = đường chuyền thành công
        F.col("pass.body_part.name").alias("pass_body_part"),
        # shot
        F.col("shot.end_location")[0].alias("shot_end_x"),
        F.col("shot.end_location")[1].alias("shot_end_y"),
        F.col("shot.statsbomb_xg").alias("shot_xg"),
        F.col("shot.outcome.name").alias("shot_outcome"),
        F.col("shot.body_part.name").alias("shot_body_part"),
        F.col("shot.technique.name").alias("shot_technique"),
        F.col("shot.type.name").alias("shot_type"),
        F.col("shot.first_time").alias("shot_first_time"),
    )


def clean_events(flat: DataFrame):
    """Trả về (clean, rejected). Quy tắc DQ: có id, có cầu thủ, toạ độ nằm trong sân 120x80,
    Pass phải có toạ độ đích, Shot phải có toạ độ kết thúc."""
    is_pass = F.col("event_type") == "Pass"
    is_shot = F.col("event_type") == "Shot"
    valid = (
        F.col("event_id").isNotNull()
        & F.col("match_id").isNotNull()
        & F.col("player_id").isNotNull()
        & F.col("x").between(0, 120)
        & F.col("y").between(0, 80)
        & (~is_pass | (F.col("pass_end_x").between(0, 120) & F.col("pass_end_y").between(0, 80)))
        & (~is_shot | F.col("shot_end_x").isNotNull())
    )
    flat = flat.withColumn("_valid", F.coalesce(valid, F.lit(False)))
    clean = flat.filter("_valid").drop("_valid").dropDuplicates(["event_id"])
    rejected = flat.filter(~F.col("_valid")).drop("_valid")
    return clean, rejected


def write_pg(df: DataFrame, table: str) -> None:
    (df.write.format("jdbc")
       .option("url", config.jdbc_url())
       .option("dbtable", table)
       .option("user", config.PG["user"])
       .option("password", config.PG["password"])
       .option("driver", "org.postgresql.Driver")
       .option("truncate", "true")   # giữ nguyên bảng để view dbt không bị gãy
       .mode("overwrite")
       .save())


def main():
    logging.basicConfig(level=logging.INFO)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    bucket = config.S3_BUCKET

    # --- events ---
    flat = flatten_events(_read(spark, "events", EVENT_SCHEMA))
    clean, rejected = clean_events(flat)
    clean.cache()
    n_clean, n_rej = clean.count(), rejected.count()
    log.info("events: giữ %d, loại %d (%.2f%%)", n_clean, n_rej, 100 * n_rej / max(n_clean + n_rej, 1))
    write_pg(rejected, "raw.events_rejected")
    write_pg(clean, "raw.events")

    # --- matches ---
    m = _read(spark, "match", MATCH_SCHEMA).select(
        "match_id", F.to_date("match_date").alias("match_date"), "kick_off",
        F.col("competition.competition_id").alias("competition_id"),
        F.col("competition.competition_name").alias("competition_name"),
        F.col("season.season_name").alias("season_name"),
        F.col("home_team.home_team_id").alias("home_team_id"),
        F.col("home_team.home_team_name").alias("home_team_name"),
        F.col("away_team.away_team_id").alias("away_team_id"),
        F.col("away_team.away_team_name").alias("away_team_name"),
        "home_score", "away_score",
    ).dropDuplicates(["match_id"])
    write_pg(m, "raw.matches")

    # --- players (từ lineups) ---
    p = (_read(spark, "lineups", LINEUP_SCHEMA)
         .select(F.col("match_id_file").alias("match_id"), "team_id", "team_name",
                 F.explode("lineup").alias("pl"))
         .select("match_id", "team_id", "team_name",
                 F.col("pl.player_id").alias("player_id"),
                 F.col("pl.player_name").alias("player_name"),
                 F.col("pl.player_nickname").alias("player_nickname"),
                 F.col("pl.country.name").alias("country_name"),
                 F.col("pl.jersey_number").alias("jersey_number"))
         .filter("player_id is not null"))
    write_pg(p, "raw.players")

    spark.stop()


if __name__ == "__main__":
    main()
