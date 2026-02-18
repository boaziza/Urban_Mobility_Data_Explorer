PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS reject_log;
DROP TABLE IF EXISTS fact_trip;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS zone_geometry;
DROP TABLE IF EXISTS dim_zone;

CREATE TABLE dim_zone (
  location_id INTEGER PRIMARY KEY,
  borough TEXT NOT NULL,
  zone TEXT NOT NULL,
  service_zone TEXT NOT NULL
);

CREATE TABLE zone_geometry (
  location_id INTEGER PRIMARY KEY,
  wkt TEXT NOT NULL,
  min_x REAL NOT NULL,
  min_y REAL NOT NULL,
  max_x REAL NOT NULL,
  max_y REAL NOT NULL,
  FOREIGN KEY(location_id) REFERENCES dim_zone(location_id)
);

CREATE TABLE dim_time (
  time_id INTEGER PRIMARY KEY AUTOINCREMENT,
  pickup_datetime TEXT NOT NULL,
  pickup_date TEXT NOT NULL,
  pickup_hour INTEGER NOT NULL,
  pickup_weekday INTEGER NOT NULL,
  pickup_month INTEGER NOT NULL,
  is_weekend INTEGER NOT NULL,
  UNIQUE(pickup_datetime)
);

CREATE TABLE fact_trip (
  trip_id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_id INTEGER,
  time_id INTEGER NOT NULL,
  pu_location_id INTEGER NOT NULL,
  do_location_id INTEGER NOT NULL,
  passenger_count REAL,
  trip_distance REAL NOT NULL,
  duration_min REAL NOT NULL,
  fare_amount REAL NOT NULL,
  tip_amount REAL NOT NULL,
  total_amount REAL NOT NULL,
  payment_type INTEGER,
  ratecode_id INTEGER,
  avg_speed_mph REAL NOT NULL,
  tip_pct REAL NOT NULL,
  is_peak_hour INTEGER NOT NULL,
  source_hash TEXT NOT NULL UNIQUE,
  FOREIGN KEY(time_id) REFERENCES dim_time(time_id),
  FOREIGN KEY(pu_location_id) REFERENCES dim_zone(location_id),
  FOREIGN KEY(do_location_id) REFERENCES dim_zone(location_id),
  CHECK(trip_distance >= 0),
  CHECK(duration_min > 0),
  CHECK(fare_amount >= 0)
);

CREATE TABLE reject_log (
  reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_hash TEXT,
  reject_reason TEXT NOT NULL,
  raw_pickup_datetime TEXT,
  raw_dropoff_datetime TEXT,
  raw_trip_distance TEXT,
  raw_fare_amount TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_pu ON fact_trip(pu_location_id);
CREATE INDEX idx_fact_do ON fact_trip(do_location_id);
CREATE INDEX idx_fact_time ON fact_trip(time_id);
CREATE INDEX idx_fact_payment ON fact_trip(payment_type);
CREATE INDEX idx_time_date_hour ON dim_time(pickup_date, pickup_hour);
CREATE INDEX idx_zone_geom_bbox ON zone_geometry(min_x, min_y, max_x, max_y);
