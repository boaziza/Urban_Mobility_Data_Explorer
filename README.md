# Urban Mobility Data Explorer

Enterprise Web Development Summative Project using NYC TLC Yellow Taxi data to explore city mobility patterns through a full-stack dashboard.

## Team
- **Team Name:** EWDGroup10
- **Members:**
  - IZA Prince Boaz
  - Kwizera Bodgar
  - Ineza Merveille Shekina
  - Ishimwe Sam Nelson
  - Obanijesu Williams Okunola

## Project Links
- **GitHub Repository:** https://github.com/boaziza/Urban_Mobility_Data_Explorer.git
- **Deployed Backend API:** https://urban-mobility-data-explorer-hqlv.onrender.com
- **Team Task Sheet:** https://docs.google.com/spreadsheets/d/1Spu3KrzAqDeETqtlrAyjqn9MeJZbof1f0aOhHezKqu0/edit?usp=sharing
- **Video Walkthrough (5 min):** `ADD_YOUTUBE_LINK_HERE`
- **PDF Technical Report:** `ADD_REPORT_LINK_OR_FILENAME_HERE`

## Overview
This system ingests raw NYC taxi trip records, cleans and normalizes them, stores them in a relational SQLite database, and exposes a Flask API consumed by a JavaScript dashboard for interactive urban mobility analysis.

### Core Goals Implemented
- ETL pipeline for TLC trip data + taxi zone lookup + taxi zone shapefile metadata
- Data cleaning with reject logging for anomalies/outliers
- Normalized relational schema with indexes
- REST API for filtering, aggregation, detail views, and insight endpoints
- Interactive dashboard with KPIs, charts, sortable tables, and derived insights
- Manual algorithm/data structure implementation for route ranking

## Architecture
```text
NYC TLC Files (.parquet/.csv + lookup.csv + shapefile)
                  |
                  v
          backend/etl.py (clean + feature engineering + reject log)
                  |
                  v
              mobility.db (SQLite)
   [dim_zone, zone_geometry, dim_time, fact_trip, reject_log]
                  |
                  v
            backend/app.py (Flask REST API)
                  |
                  v
       frontend/index.html + app.js + Chart.js + styles.css
```

## Repository Structure
```text
Urban_Mobility_Data_Explorer/
├── backend/
│   ├── app.py              # Flask API
│   ├── etl.py              # ETL + cleaning + feature engineering
│   ├── algorithms.py       # Manual grouping + merge sort route ranking
│   └── db.py               # DB connection helper
├── frontend/
│   ├── index.html          # Dashboard UI
│   ├── app.js              # API calls + rendering + interactions
│   └── styles.css          # Styling
├── data/
│   └── taxi_zone_lookup.csv
├── taxi_zones/             # shapefile components
├── sql/
│   └── schema.sql          # relational schema + indexes
├── mobility.db             # generated/loaded SQLite database
└── requirements.txt
```

## Dataset Requirements
This project expects the following TLC components:
1. `yellow_tripdata` fact data (`.parquet` preferred or `.csv` fallback)
2. `taxi_zone_lookup.csv` dimension mapping
3. `taxi_zones` shapefile metadata

### Expected data paths
- `data/yellow_tripdata_2019-01.parquet` **or** `data/yellow_tripdata_2019-01.csv`
- `data/taxi_zone_lookup.csv`
- `taxi_zones/taxi_zones.shp` (+ `.dbf`, `.shx`, etc.)

## Data Cleaning and Feature Engineering
Implemented in `backend/etl.py`.

### Cleaning Rules
- Invalid/missing pickup/dropoff datetime removed
- Duration outliers removed (`<= 0` or `> 180` minutes)
- Distance outliers removed (`< 0` or `> 60` miles)
- Fare outliers removed (`< 0` or `> 500`)
- Speed outliers removed (`<= 0` or `> 80 mph`)
- Duplicate prevention through `source_hash` unique constraint

### Derived Features
- `duration_min` (dropoff - pickup)
- `avg_speed_mph` (`trip_distance / duration`)
- `tip_pct` (`tip_amount / fare_amount` when fare > 0)
- `is_peak_hour` (pickup hour in `[7,8,9,16,17,18,19]`)

### Transparency / Reject Log
All suspicious/excluded records are logged in `reject_log` with:
- `source_hash`
- `reject_reason`
- raw datetime/distance/fare fields

## Database Design
Schema: `sql/schema.sql`

### Tables
- `dim_zone` (zone metadata)
- `zone_geometry` (WKT geometry + bounding box)
- `dim_time` (normalized pickup time dimensions)
- `fact_trip` (trip fact table)
- `reject_log` (audit of removed records)

### Indexing
Indexes include:
- `fact_trip(pu_location_id)`
- `fact_trip(do_location_id)`
- `fact_trip(time_id)`
- `fact_trip(payment_type)`
- `dim_time(pickup_date, pickup_hour)`
- `zone_geometry(min_x, min_y, max_x, max_y)`

## Algorithm / DSA Requirement
Manual implementation is in `backend/algorithms.py`:
- Custom route grouping (`manual_group_count_route`)
- Custom merge sort (`merge_sort_desc`)
- Top-K route extraction (`top_k_routes_manual`)

No built-in advanced grouping/sorting helpers are used for this route ranking flow.

## API Endpoints
Base URL (deployed): `https://urban-mobility-data-explorer-hqlv.onrender.com/api`

- `GET /health`
- `GET /filter-options`
- `GET /summary`
- `GET /hourly-trips`
- `GET /top-zones?k=10`
- `GET /top-routes?k=10`
- `GET /trips?limit=50&offset=0&sort=pickup_datetime&order=desc`
- `GET /zones/heatmap?metric=pickups|dropoffs`
- `GET /insights`

Supported filter query params (where applicable):
- `start_date`, `end_date`, `borough`, `payment_type`
- `min_distance`, `max_distance`, `min_fare`, `max_fare`

## Frontend Features
- Global filtering by date, borough, payment type, distance, and fare range
- KPI cards (trips, revenue, avg distance, avg speed)
- Hourly trips line chart
- Top pickup zones bar chart
- Top routes table (manual algorithm results)
- Sortable trip detail table
- Plain-language insight cards

## Local Setup (Fully Runnable)

### 1. Prerequisites
- Python 3.10+
- `pip`
- (Optional) virtual environment

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Build and Load Database (ETL)
From project root:
```bash
python backend/etl.py
```
This creates/refreshes `mobility.db` using `sql/schema.sql` and data files.

### 4. Run Backend API
```bash
python backend/app.py
```
Local API default: `http://localhost:5000/api`

### 5. Run Frontend
Option A (simple static server from root):
```bash
python -m http.server 5500
```
Open: `http://localhost:5500/frontend/`

Option B: open `frontend/index.html` directly in browser.

## Frontend API Configuration
Current `frontend/app.js` uses deployed API:
```js
const API = "https://urban-mobility-data-explorer-hqlv.onrender.com/api";
```
For local backend testing, change to:
```js
const API = "http://localhost:5000/api";
```

## Assignment Deliverables Checklist
- [x] Full codebase (backend + frontend)
- [x] README with setup and run instructions
- [x] Database schema (`sql/schema.sql`)
- [x] Team participation sheet link
- [ ] Video walkthrough link inserted
- [ ] PDF technical report added/linked

## Documentation Report Guide (2-3 pages)
Use this structure in your PDF:
1. Problem framing and dataset analysis
2. System architecture and design decisions
3. Algorithm/data structure (manual) + pseudo-code + complexity
4. Three insights with visuals and interpretation
5. Reflection and future work

## Suggested Video Walkthrough Script (5 minutes)
1. Problem and dataset context
2. Architecture diagram and stack choices
3. ETL and cleaning/feature engineering logic
4. API demonstration (filters + endpoints)
5. Frontend insights and story from visualizations

## Notes
- This README is intentionally focused on reproducibility and technical clarity for grading.
- Replace placeholders before submission:
  - `ADD_YOUTUBE_LINK_HERE`
  - `ADD_REPORT_LINK_OR_FILENAME_HERE`
