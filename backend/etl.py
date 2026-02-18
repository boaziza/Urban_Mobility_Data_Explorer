import hashlib
import sqlite3
from pathlib import Path

import pandas as pd

try:
    import shapefile  # pyshp
except ImportError:
    shapefile = None

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "mobility.db"
LOOKUP_CSV = ROOT / "data" / "taxi_zone_lookup.csv"
TRIP_CSV = ROOT / "data" / "yellow_tripdata_2019-01.csv"
TRIP_PARQUET = ROOT / "data" / "yellow_tripdata_2019-01.parquet"
SCHEMA_SQL = ROOT / "sql" / "schema.sql"
ZONE_SHP_PRIMARY = ROOT / "data" / "taxi_zones" / "taxi_zones.shp"
ZONE_SHP_FALLBACK = ROOT / "taxi_zones" / "taxi_zones.shp"

CHUNK_SIZE = 200_000


def hash_row(values):
    s = "|".join(str(v) for v in values)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def build_db():
    conn = sqlite3.connect(DB_PATH)
    with open(SCHEMA_SQL, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()


def load_zones():
    conn = sqlite3.connect(DB_PATH)
    zones = pd.read_csv(LOOKUP_CSV)
    zones.columns = [c.strip('"') for c in zones.columns]
    zones = zones.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
        }
    )
    zones["borough"] = zones["borough"].fillna("Unknown")
    zones["zone"] = zones["zone"].fillna("Unknown")
    zones["service_zone"] = zones["service_zone"].fillna("Unknown")
    zones.to_sql("dim_zone", conn, if_exists="append", index=False)
    conn.close()


def _shape_to_wkt(shape_obj):
    points = shape_obj.points
    if not points:
        return None

    parts = list(shape_obj.parts) + [len(points)]
    rings = []
    for i in range(len(parts) - 1):
        ring = points[parts[i] : parts[i + 1]]
        if not ring:
            continue
        if ring[0] != ring[-1]:
            ring = ring + [ring[0]]
        ring_txt = ",".join(f"{x} {y}" for x, y in ring)
        rings.append(f"(({ring_txt}))")

    if not rings:
        return None
    return f"MULTIPOLYGON({','.join(rings)})"


def load_zone_geometry():
    if shapefile is None:
        print("Skipping spatial load: install 'pyshp' to ingest taxi_zones shapefile.")
        return

    shp_path = ZONE_SHP_PRIMARY if ZONE_SHP_PRIMARY.exists() else ZONE_SHP_FALLBACK
    if not shp_path.exists():
        print("Skipping spatial load: taxi_zones.shp not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    reader = shapefile.Reader(str(shp_path))
    field_names = [f[0] for f in reader.fields[1:]]

    inserted = 0
    for sr in reader.shapeRecords():
        if hasattr(sr.record, "as_dict"):
            rec = sr.record.as_dict()
        else:
            rec = dict(zip(field_names, sr.record))
        rec_keys = {str(k).lower(): k for k in rec.keys()}
        loc_key = rec_keys.get("locationid") or rec_keys.get("location_id")
        if not loc_key:
            continue
        try:
            location_id = int(rec[loc_key])
        except (ValueError, TypeError):
            continue

        wkt = _shape_to_wkt(sr.shape)
        if not wkt:
            continue
        min_x, min_y, max_x, max_y = sr.shape.bbox
        conn.execute(
            """INSERT OR REPLACE INTO zone_geometry
               (location_id, wkt, min_x, min_y, max_x, max_y)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (location_id, wkt, min_x, min_y, max_x, max_y),
        )
        inserted += 1

    conn.commit()
    conn.close()
    print(f"Loaded {inserted} zone geometries.")


def upsert_time(conn, pickup_dt):
    row = conn.execute(
        "SELECT time_id FROM dim_time WHERE pickup_datetime = ?",
        (pickup_dt,),
    ).fetchone()
    if row:
        return row[0]
    ts = pd.to_datetime(pickup_dt)
    pickup_date = ts.strftime("%Y-%m-%d")
    pickup_hour = int(ts.hour)
    pickup_weekday = int(ts.weekday())
    pickup_month = int(ts.month)
    is_weekend = 1 if pickup_weekday >= 5 else 0
    cur = conn.execute(
        """INSERT INTO dim_time (pickup_datetime, pickup_date, pickup_hour, pickup_weekday, pickup_month, is_weekend)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (pickup_dt, pickup_date, pickup_hour, pickup_weekday, pickup_month, is_weekend),
    )
    return cur.lastrowid


def clean_chunk(df):
    df = df.copy()
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
    df["tip_amount"] = pd.to_numeric(df["tip_amount"], errors="coerce").fillna(0.0)
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").fillna(1)
    df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce").fillna(0)
    df["RatecodeID"] = pd.to_numeric(df["RatecodeID"], errors="coerce").fillna(0)

    df["duration_min"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60.0
    df["avg_speed_mph"] = df["trip_distance"] / (df["duration_min"] / 60.0)
    df["avg_speed_mph"] = df["avg_speed_mph"].replace([float("inf"), float("-inf")], pd.NA)
    df["tip_pct"] = df.apply(lambda r: (r["tip_amount"] / r["fare_amount"]) if r["fare_amount"] > 0 else 0.0, axis=1)
    df["is_peak_hour"] = df["tpep_pickup_datetime"].dt.hour.isin([7, 8, 9, 16, 17, 18, 19]).astype(int)

    rejects = []
    valid = pd.Series([True] * len(df))

    mask = df["tpep_pickup_datetime"].isna() | df["tpep_dropoff_datetime"].isna()
    rejects.append((mask, "missing_or_invalid_datetime"))

    mask = (df["duration_min"] <= 0) | (df["duration_min"] > 180)
    rejects.append((mask, "invalid_duration"))

    mask = (df["trip_distance"] < 0) | (df["trip_distance"] > 60)
    rejects.append((mask, "distance_outlier"))

    mask = (df["fare_amount"] < 0) | (df["fare_amount"] > 500)
    rejects.append((mask, "fare_outlier"))

    mask = (df["avg_speed_mph"].isna()) | (df["avg_speed_mph"] <= 0) | (df["avg_speed_mph"] > 80)
    rejects.append((mask, "speed_outlier"))

    for m, _ in rejects:
        valid = valid & (~m)

    bad_rows = []
    for m, reason in rejects:
        part = df[m]
        if len(part) > 0:
            for _, r in part.iterrows():
                bad_rows.append(
                    {
                        "source_hash": hash_row(
                            [
                                r.get("VendorID"),
                                r.get("tpep_pickup_datetime"),
                                r.get("tpep_dropoff_datetime"),
                                r.get("PULocationID"),
                                r.get("DOLocationID"),
                                r.get("fare_amount"),
                            ]
                        ),
                        "reject_reason": reason,
                        "raw_pickup_datetime": str(r.get("tpep_pickup_datetime")),
                        "raw_dropoff_datetime": str(r.get("tpep_dropoff_datetime")),
                        "raw_trip_distance": str(r.get("trip_distance")),
                        "raw_fare_amount": str(r.get("fare_amount")),
                    }
                )

    clean = df[valid].copy()
    clean["pickup_str"] = clean["tpep_pickup_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return clean, bad_rows


def iter_trip_chunks():
    if TRIP_PARQUET.exists():
        try:
            import pyarrow.parquet as pq
        except ImportError as ex:
            raise RuntimeError(
                "Parquet file found but pyarrow is not installed. Install pyarrow or use CSV input."
            ) from ex
        pqf = pq.ParquetFile(TRIP_PARQUET)
        for batch in pqf.iter_batches(batch_size=CHUNK_SIZE):
            yield batch.to_pandas()
        return

    if not TRIP_CSV.exists():
        raise FileNotFoundError(
            "Trip dataset not found. Provide either data/yellow_tripdata_2019-01.parquet or data/yellow_tripdata_2019-01.csv"
        )
    for chunk in pd.read_csv(TRIP_CSV, chunksize=CHUNK_SIZE):
        yield chunk


def load_trips():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("BEGIN")

    for chunk in iter_trip_chunks():
        clean, bad_rows = clean_chunk(chunk)

        for b in bad_rows:
            conn.execute(
                """INSERT INTO reject_log (source_hash, reject_reason, raw_pickup_datetime, raw_dropoff_datetime, raw_trip_distance, raw_fare_amount)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    b["source_hash"],
                    b["reject_reason"],
                    b["raw_pickup_datetime"],
                    b["raw_dropoff_datetime"],
                    b["raw_trip_distance"],
                    b["raw_fare_amount"],
                ),
            )

        for _, r in clean.iterrows():
            t_id = upsert_time(conn, r["pickup_str"])
            source_hash = hash_row(
                [
                    r.get("VendorID"),
                    r.get("pickup_str"),
                    str(r.get("tpep_dropoff_datetime")),
                    int(r.get("PULocationID")),
                    int(r.get("DOLocationID")),
                    float(r.get("fare_amount")),
                    float(r.get("trip_distance")),
                ]
            )

            conn.execute(
                """INSERT OR IGNORE INTO fact_trip
                (vendor_id, time_id, pu_location_id, do_location_id, passenger_count, trip_distance,
                 duration_min, fare_amount, tip_amount, total_amount, payment_type, ratecode_id,
                 avg_speed_mph, tip_pct, is_peak_hour, source_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    int(r.get("VendorID")) if pd.notna(r.get("VendorID")) else None,
                    t_id,
                    int(r.get("PULocationID")),
                    int(r.get("DOLocationID")),
                    float(r.get("passenger_count")),
                    float(r.get("trip_distance")),
                    float(r.get("duration_min")),
                    float(r.get("fare_amount")),
                    float(r.get("tip_amount")),
                    float(r.get("total_amount")),
                    int(r.get("payment_type")),
                    int(r.get("RatecodeID")),
                    float(r.get("avg_speed_mph")),
                    float(r.get("tip_pct")),
                    int(r.get("is_peak_hour")),
                    source_hash,
                ),
            )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    build_db()
    load_zones()
    load_zone_geometry()
    load_trips()
    print("ETL complete.")
