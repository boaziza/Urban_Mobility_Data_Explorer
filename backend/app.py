from flask import Flask, jsonify, request
from flask_cors import CORS

from algorithms import top_k_routes_manual
from db import get_conn

app = Flask(__name__)
CORS(app)


def _int_or_none(v):
    if v is None or v == "":
        return None
    return int(v)


def _float_or_none(v):
    if v is None or v == "":
        return None
    return float(v)


def _build_filters(args):
    clauses = []
    params = []

    start_date = args.get("start_date")
    end_date = args.get("end_date")
    borough = args.get("borough")
    payment_type = _int_or_none(args.get("payment_type"))
    min_distance = _float_or_none(args.get("min_distance"))
    max_distance = _float_or_none(args.get("max_distance"))
    min_fare = _float_or_none(args.get("min_fare"))
    max_fare = _float_or_none(args.get("max_fare"))

    if start_date:
        clauses.append("t.pickup_date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("t.pickup_date <= ?")
        params.append(end_date)
    if borough:
        clauses.append("z.borough = ?")
        params.append(borough)
    if payment_type is not None:
        clauses.append("f.payment_type = ?")
        params.append(payment_type)
    if min_distance is not None:
        clauses.append("f.trip_distance >= ?")
        params.append(min_distance)
    if max_distance is not None:
        clauses.append("f.trip_distance <= ?")
        params.append(max_distance)
    if min_fare is not None:
        clauses.append("f.fare_amount >= ?")
        params.append(min_fare)
    if max_fare is not None:
        clauses.append("f.fare_amount <= ?")
        params.append(max_fare)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


@app.get("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/api/filter-options")
def filter_options():
    conn = get_conn()
    boroughs = conn.execute("SELECT DISTINCT borough FROM dim_zone ORDER BY borough").fetchall()
    date_row = conn.execute("SELECT MIN(pickup_date) min_date, MAX(pickup_date) max_date FROM dim_time").fetchone()
    conn.close()
    return jsonify(
        {
            "boroughs": [x["borough"] for x in boroughs],
            "min_date": date_row["min_date"],
            "max_date": date_row["max_date"],
            "payment_types": [
                {"id": 1, "label": "Credit card"},
                {"id": 2, "label": "Cash"},
                {"id": 3, "label": "No charge"},
                {"id": 4, "label": "Dispute"},
                {"id": 5, "label": "Unknown"},
                {"id": 6, "label": "Voided trip"},
            ],
        }
    )


@app.get("/api/summary")
def summary():
    where_sql, params = _build_filters(request.args)
    conn = get_conn()
    row = conn.execute(
        f"""SELECT COUNT(*) trips,
                  ROUND(SUM(f.total_amount),2) revenue,
                  ROUND(AVG(f.trip_distance),2) avg_distance,
                  ROUND(AVG(f.avg_speed_mph),2) avg_speed
           FROM fact_trip f
           JOIN dim_time t ON f.time_id = t.time_id
           JOIN dim_zone z ON f.pu_location_id = z.location_id
           {where_sql}""",
        params,
    ).fetchone()
    conn.close()
    return jsonify(dict(row))


@app.get("/api/hourly-trips")
def hourly():
    where_sql, params = _build_filters(request.args)
    conn = get_conn()
    rows = conn.execute(
        f"""SELECT t.pickup_hour hour, COUNT(*) trips
           FROM fact_trip f
           JOIN dim_time t ON f.time_id = t.time_id
           JOIN dim_zone z ON f.pu_location_id = z.location_id
           {where_sql}
           GROUP BY t.pickup_hour
           ORDER BY t.pickup_hour""",
        params,
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.get("/api/top-zones")
def top_zones():
    k = int(request.args.get("k", 10))
    where_sql, params = _build_filters(request.args)
    conn = get_conn()
    rows = conn.execute(
        f"""SELECT z.location_id, z.zone, z.borough, COUNT(*) trips
           FROM fact_trip f
           JOIN dim_zone z ON f.pu_location_id = z.location_id
           JOIN dim_time t ON f.time_id = t.time_id
           {where_sql}
           GROUP BY z.location_id
           ORDER BY trips DESC
           LIMIT ?""",
        params + [k],
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.get("/api/top-routes")
def top_routes():
    k = int(request.args.get("k", 10))
    where_sql, params = _build_filters(request.args)
    conn = get_conn()
    rows = conn.execute(
        f"""SELECT f.pu_location_id, f.do_location_id
            FROM fact_trip f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_zone z ON f.pu_location_id = z.location_id
            {where_sql}""",
        params,
    ).fetchall()
    conn.close()
    pairs = [(r["pu_location_id"], r["do_location_id"]) for r in rows]
    return jsonify(top_k_routes_manual(pairs, k))


@app.get("/api/trips")
def trips():
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    sort = request.args.get("sort", "pickup_datetime")
    order = request.args.get("order", "desc").lower()
    if limit > 500:
        limit = 500
    if offset < 0:
        offset = 0
    if order not in ("asc", "desc"):
        order = "desc"

    sortable = {
        "pickup_datetime": "t.pickup_datetime",
        "distance": "f.trip_distance",
        "fare": "f.fare_amount",
        "total": "f.total_amount",
        "duration": "f.duration_min",
    }
    sort_sql = sortable.get(sort, "t.pickup_datetime")

    where_sql, params = _build_filters(request.args)
    conn = get_conn()
    rows = conn.execute(
        f"""SELECT t.pickup_datetime,
                   f.trip_distance,
                   f.fare_amount,
                   f.total_amount,
                   f.duration_min,
                   f.payment_type,
                   pu.zone pu_zone,
                   do.zone do_zone,
                   pu.borough pu_borough,
                   do.borough do_borough
            FROM fact_trip f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_zone pu ON f.pu_location_id = pu.location_id
            JOIN dim_zone do ON f.do_location_id = do.location_id
            JOIN dim_zone z ON f.pu_location_id = z.location_id
            {where_sql}
            ORDER BY {sort_sql} {order}
            LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.get("/api/zones/heatmap")
def zones_heatmap():
    metric = request.args.get("metric", "pickups")
    if metric not in ("pickups", "dropoffs"):
        metric = "pickups"
    where_sql, params = _build_filters(request.args)
    location_col = "f.pu_location_id" if metric == "pickups" else "f.do_location_id"

    conn = get_conn()
    rows = conn.execute(
        f"""SELECT z.location_id, z.zone, z.borough, COUNT(*) trip_count
            FROM fact_trip f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_zone z ON {location_col} = z.location_id
            {where_sql}
            GROUP BY z.location_id""",
        params,
    ).fetchall()
    geoms = conn.execute(
        "SELECT location_id, wkt, min_x, min_y, max_x, max_y FROM zone_geometry"
    ).fetchall()
    conn.close()

    counts = {r["location_id"]: dict(r) for r in rows}
    payload = []
    for g in geoms:
        row = counts.get(g["location_id"])
        payload.append(
            {
                "location_id": g["location_id"],
                "zone": row["zone"] if row else "Unknown",
                "borough": row["borough"] if row else "Unknown",
                "trip_count": row["trip_count"] if row else 0,
                "bbox": [g["min_x"], g["min_y"], g["max_x"], g["max_y"]],
                "wkt": g["wkt"],
            }
        )
    return jsonify(payload)


@app.get("/api/insights")
def insights():
    where_sql, params = _build_filters(request.args)
    tip_where = where_sql
    if tip_where:
        tip_where = f"{tip_where} AND f.fare_amount > 0"
    else:
        tip_where = "WHERE f.fare_amount > 0"
    conn = get_conn()
    borough = conn.execute(
        f"""SELECT z.borough, COUNT(*) trips
           FROM fact_trip f
           JOIN dim_zone z ON f.pu_location_id = z.location_id
           JOIN dim_time t ON f.time_id = t.time_id
           {where_sql}
           GROUP BY z.borough
           ORDER BY trips DESC
           LIMIT 1""",
        params,
    ).fetchone()
    tip = conn.execute(
        f"""SELECT f.payment_type, ROUND(AVG(f.tip_pct) * 100, 2) avg_tip_pct
           FROM fact_trip f
           JOIN dim_time t ON f.time_id = t.time_id
           JOIN dim_zone z ON f.pu_location_id = z.location_id
           {tip_where}
           GROUP BY f.payment_type
           ORDER BY avg_tip_pct DESC""",
        params,
    ).fetchall()
    peak = conn.execute(
        f"""SELECT t.pickup_hour, COUNT(*) trips
           FROM fact_trip f
           JOIN dim_time t ON f.time_id = t.time_id
           JOIN dim_zone z ON f.pu_location_id = z.location_id
           {where_sql}
           GROUP BY t.pickup_hour
           ORDER BY trips DESC
           LIMIT 1""",
        params,
    ).fetchone()
    conn.close()
    return jsonify(
        {
            "top_pickup_borough": dict(borough) if borough else None,
            "tip_behavior_by_payment": [dict(x) for x in tip],
            "peak_hour": dict(peak) if peak else None,
        }
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
