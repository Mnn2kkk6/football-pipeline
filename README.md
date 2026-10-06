# Football Analytics Data Pipeline

Pipeline dữ liệu tự động cho phân tích bóng đá: thu thập dữ liệu sự kiện trận đấu từ **StatsBomb Open Data**, làm sạch bằng **PySpark**, lưu vào **PostgreSQL**, mô hình hóa bằng **dbt**, điều phối bằng **Airflow** và trực quan hóa bằng **Metabase**. Toàn bộ chạy local bằng Docker Compose.

Dữ liệu mặc định: **FIFA World Cup 2018** (64 trận, ~228 nghìn sự kiện, ~63 nghìn đường chuyền, 1.706 cú sút).

## Kiến trúc

```
StatsBomb Open Data (GitHub)
        │  Airflow: extract_to_s3  (idempotent, chỉ tải trận mới)
        ▼
S3 (SeaweedFS)  raw/{year}/{league}/{match_id}/{events,lineups,match}.json
        │  Airflow: spark_transform_load  (flatten + data quality)
        ▼
PostgreSQL  schema raw:  events | events_rejected | matches | players
        │  Airflow: dbt_run → dbt_test
        ▼
PostgreSQL  schema analytics:  fact_passes | fact_shots | dim_players | mart_*
        │                              │
        ▼                              ▼
Metabase (dashboard)        S3 exports/xg_training.csv  (train mô hình xG)
```

| Thành phần | Công nghệ |
|---|---|
| Nguồn dữ liệu | StatsBomb Open Data |
| Điều phối | Apache Airflow 2.9.3 (chế độ `standalone`) |
| Data lake | SeaweedFS (tương thích S3), dùng thay S3/MinIO khi chạy local |
| Xử lý | PySpark 3.5.1 (Java 17) |
| Data warehouse | PostgreSQL 16 |
| Transformation | dbt-core 1.8.8 + dbt-postgres 1.8.2 |
| Visualization | Metabase |

## Cấu trúc thư mục

```
football-pipeline/
├── docker-compose.yml
├── s3.json                    # tài khoản S3 cho SeaweedFS (xem bên dưới)
├── .env / .env.example
├── docker/airflow/Dockerfile  # Airflow + Java + PySpark + JAR + dbt (venv riêng)
├── dags/football_pipeline_dag.py
├── src/
│   ├── config.py
│   ├── storage.py             # boto3: tạo bucket, ghi/kiểm tra object
│   ├── extract.py             # StatsBomb → S3
│   ├── transform_spark.py     # S3 → flatten → làm sạch → Postgres
│   └── export_training.py     # fact_shots → exports/xg_training.csv
├── sql/init.sql               # tạo schema raw, analytics
└── dbt/
    ├── dbt_project.yml, profiles.yml
    ├── models/staging/        # stg_events, stg_matches (view)
    ├── models/marts/          # fact_*, dim_*, mart_* (table) + schema.yml
    └── tests/generic/         # test toạ độ nằm trong sân
```

## Yêu cầu

- Docker Desktop (Windows / macOS / Linux), còn trống khoảng 6 GB ổ đĩa.
- Kết nối Internet ổn định ở lần build đầu (tải image Airflow, Java, PySpark, JAR ~300 MB, dbt).

## Chạy nhanh

Các lệnh dưới đây viết cho PowerShell; chạy trong thư mục chứa `docker-compose.yml`.

**1. Tạo file `s3.json`** (khai báo tài khoản S3 trùng với key trong `.env`; cách viết này tránh BOM làm hỏng JSON):

```powershell
$json = '{"identities":[{"name":"admin","credentials":[{"accessKey":"local","secretKey":"localsecret"}],"actions":["Admin","Read","Write","List","Tagging"]}]}'
[System.IO.File]::WriteAllText("$PWD\s3.json", $json, (New-Object System.Text.UTF8Encoding($false)))
```

Trên Linux/macOS: `echo '<nội dung JSON ở trên>' > s3.json`.

**2. Khởi động**

```powershell
docker compose up -d --build
```

Lần build đầu mất 10–20 phút.

**3. Đăng nhập Airflow** — http://localhost:8080, user `admin`:

```powershell
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

**4. Chạy pipeline**

```powershell
docker compose exec airflow airflow dags unpause football_pipeline
docker compose exec airflow airflow dags trigger football_pipeline
```

Theo dõi ở tab **Grid** trên giao diện Airflow. Thứ tự task: `extract_to_s3` → `spark_transform_load` → `dbt_run` → `dbt_test` → `export_xg_training_csv`. Cả 5 ô chuyển xanh đậm là hoàn tất (task Spark mất vài phút).

**5. Kiểm tra dữ liệu**

```powershell
docker compose exec postgres psql -U football -d football -c "select count(*) from analytics.fact_shots;" -c "select * from analytics.mart_possession limit 5;"
```

**6. Kết nối Metabase** — http://localhost:3001, thêm cơ sở dữ liệu PostgreSQL:

| Trường | Giá trị |
|---|---|
| Host | `postgres` |
| Cổng | `5432` |
| Tên cơ sở dữ liệu / người dùng / mật khẩu | `football` |
| Schemas | chỉ `analytics` |

## Cổng dịch vụ

| Dịch vụ | Địa chỉ |
|---|---|
| Airflow | http://localhost:8080 |
| Metabase | http://localhost:3001 |
| PostgreSQL (từ máy host, ví dụ DBeaver) | `localhost:15432` |
| S3 API (SeaweedFS) | http://localhost:8333 (key `local` / `localsecret`) |

Giữa các container, dùng tên dịch vụ: `postgres:5432`, `s3:8333`. Nếu cổng bị chiếm, sửa phần `ports` trong `docker-compose.yml` (hoặc biến `PG_HOST_PORT` trong `.env`).

## Mô hình dữ liệu

| Bảng (schema `analytics`) | Nội dung |
|---|---|
| `fact_passes` | toạ độ đầu/cuối, độ dài, góc, chuyền thành công?, chuyền tiến (≥10 yard)? |
| `fact_shots` | toạ độ sút, khoảng cách tới khung thành, góc sút (độ), xG, bàn thắng? |
| `dim_players` | tên, quốc tịch, đội, số áo, số trận có tên trong đội hình |
| `mart_player_match_stats` | phong độ cầu thủ theo trận: chuyền, tỷ lệ chuyền chính xác, sút, bàn thắng, xG, hành động phòng ngự |
| `mart_possession` | tỷ lệ kiểm soát bóng mỗi đội theo trận (tính theo tỷ trọng số đường chuyền) |
| `mart_pass_network` | cạnh mạng chuyền bóng + vị trí trung bình của hai cầu thủ |

Hệ toạ độ StatsBomb: sân 120 × 80, khung thành đối phương ở `x = 120`, hai cột dọc ở `y = 36` và `y = 44`.

- Khoảng cách tới khung thành: `√((120 − x)² + (y − 40)²)`
- Góc sút: `atan2(8·(120 − x), (120 − x)² + (y − 40)² − 16)` (đổi sang độ)

## Chất lượng dữ liệu

PySpark loại một sự kiện khi: thiếu `event_id`/`match_id`/`player_id`, toạ độ null hoặc ngoài sân 120×80, Pass thiếu điểm đích, Shot thiếu điểm kết thúc. Các dòng bị loại (khoảng 1,7%) được lưu ở bảng `raw.events_rejected` để kiểm tra, không bị mất.

dbt test (9 test): `unique` / `not_null` cho khoá chính, `shot_xg` không null, toạ độ `start_x` nằm trong [0, 120].

## SQL mẫu cho Metabase

```sql
-- Top cầu thủ theo xG so với bàn thắng thực tế
SELECT player_name, team_name, SUM(xg) AS xg, SUM(goals) AS goals, SUM(shots) AS shots
FROM analytics.mart_player_match_stats
GROUP BY player_name, team_name
ORDER BY xg DESC
LIMIT 15;

-- Kiểm soát bóng mỗi trận
SELECT m.match_date, m.home_team_name || ' - ' || m.away_team_name AS match,
       p.team_name, p.possession_pct
FROM analytics.mart_possession p
JOIN analytics.stg_matches m USING (match_id)
ORDER BY m.match_date, match;

-- Heatmap điểm bắt đầu đường chuyền (ô 10x10 yard)
SELECT FLOOR(start_x/10)*10 AS x_bin, FLOOR(start_y/10)*10 AS y_bin, COUNT(*) AS passes
FROM analytics.fact_passes
GROUP BY 1, 2;
```

Metabase không vẽ được hình sân bóng, nên heatmap ở đây là lưới số có màu. Muốn pass network và heatmap trên hình sân thật, dùng Python (`mplsoccer`) đọc từ `mart_pass_network` và `fact_passes`.

## Đổi giải đấu

Danh sách giải và mùa nằm trong `data/competitions.json` của repo [statsbomb/open-data](https://github.com/statsbomb/open-data). Trong Airflow: **Admin → Variables**, đặt `competition_id` và `season_id` (mặc định 43 / 3 = World Cup 2018). Lần chạy kế tiếp, `extract` chỉ tải những trận chưa có trên S3.

## Chạy từng bước thủ công (debug)

```powershell
# Extract
docker compose exec airflow bash -c "cd /opt/airflow && python -m src.extract"
# Spark: flatten, làm sạch, nạp Postgres
docker compose exec airflow bash -c "cd /opt/airflow && python -m src.transform_spark 2>&1 | tail -30"
# dbt
docker compose exec airflow bash -c "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt run --profiles-dir ."
docker compose exec airflow bash -c "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt test --profiles-dir ."
```

Xem log một task Airflow:

```powershell
docker compose exec airflow bash -c 'tail -n 40 $(ls -t /opt/airflow/logs/dag_id=football_pipeline/run_id=*/task_id=TEN_TASK/*.log | head -1)'
```

## Xử lý sự cố

| Triệu chứng | Nguyên nhân và cách xử lý |
|---|---|
| `pull access denied for minio/mc` hoặc `quay.io/minio ... 401` | MinIO đã gỡ image khỏi Docker Hub và Quay.io. Project dùng SeaweedFS (`chrislusf/seaweedfs`) thay thế. |
| `no such host` khi pull/build | Lỗi DNS của Docker Desktop. `docker pull` lại image gốc; nếu vẫn lỗi, thêm `"dns": ["8.8.8.8", "1.1.1.1"]` trong Docker Desktop → Settings → Docker Engine. |
| `port is already allocated` (5432, 5433, 3000…) | Cổng máy host đã bị chiếm. Đổi số cổng bên trái trong `ports` của `docker-compose.yml`. |
| `JAVA_GATEWAY_EXITED` kèm `SSLHandshakeException` khi tải từ `repo1.maven.org` | Spark tải JAR lúc chạy bị rớt mạng. Project đã đóng sẵn 3 JAR (`hadoop-aws`, `aws-java-sdk-bundle`, `postgresql`) vào image và không dùng `spark.jars.packages`. Nếu vẫn thấy `resolving dependencies` trong log, file `src/transform_spark.py` còn là bản cũ. |
| `Signed request requires setting up SeaweedFS S3 authentication` | Thiếu `s3.json` hoặc chưa mount vào service `s3`. Xem phần Chạy nhanh, bước 1. |
| `404 Not Found` ở `copyFile` khi Spark ghi Parquet lên S3 | S3A "đổi tên" bằng copy + xóa, SeaweedFS không hỗ trợ tốt. Project không ghi Parquet lên S3; dữ liệu bị loại lưu ở `raw.events_rejected`. |
| dbt cài nhầm `dbt-core 2.0.0rc` | Phải ghim `dbt-core==1.8.8` cùng `dbt-postgres==1.8.2` (đã có trong Dockerfile). |
| Sau `docker compose up --build`, DAG tắt và mất lịch sử chạy | Airflow `standalone` lưu metadata bằng SQLite trong container. Chạy lại `airflow dags unpause football_pipeline` rồi `trigger`. Mật khẩu admin cũng có thể đổi. Dữ liệu trên S3 và Postgres không mất. |
| Các bảng `analytics.*` chưa tồn tại | `dbt_run` chưa chạy. Chạy dbt thủ công như phần trên để xem lỗi, hoặc trigger lại DAG. |

## Giới hạn và hướng mở rộng

- Spark hiện đọc lại toàn bộ dữ liệu thô và ghi đè `raw.*` mỗi lần chạy (ổn với dữ liệu open-data). Với dữ liệu lớn nên chuyển sang xử lý incremental theo trận.
- `standalone` dùng SQLite và `SequentialExecutor`, chỉ phù hợp phát triển. Production nên dùng LocalExecutor/Celery với metadata DB là Postgres.
- Tỷ lệ kiểm soát bóng tính theo số đường chuyền, chưa theo thời gian cầm bóng.
- Thêm các loại sự kiện Carry, Pressure; thêm dữ liệu 360 frames / tracking.
- Train mô hình xG (logistic regression, XGBoost) từ `exports/xg_training.csv` và so sánh với `statsbomb_xg`.
- Chuyển sang AWS S3: xóa `S3_ENDPOINT_URL` trong `.env` và điền key AWS thật. Chuyển sang BigQuery: thay sink JDBC và dùng `dbt-bigquery`.

## Nguồn dữ liệu

Dữ liệu từ [StatsBomb Open Data](https://github.com/statsbomb/open-data). Khi công bố kết quả dùng dữ liệu này, hãy ghi nguồn StatsBomb theo điều khoản sử dụng của họ.
<img width="1899" height="759" alt="image" src="https://github.com/user-attachments/assets/5d84b9e8-56cc-407a-80bd-fa20055117a9" />
<img width="1500" height="374" alt="image" src="https://github.com/user-attachments/assets/8b5bec28-5db2-4da3-bc8f-61679fbe1cd5" />
<img width="1487" height="375" alt="image" src="https://github.com/user-attachments/assets/d0d5e3b3-8d58-47db-beca-6313719c982a" />



