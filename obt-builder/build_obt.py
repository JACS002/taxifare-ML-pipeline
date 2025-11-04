#!/usr/bin/env python3
import os, argparse, psycopg2
from textwrap import dedent

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "nyc_taxi")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "postgres")
SCHEMA_RAW = os.getenv("PG_SCHEMA_RAW", "raw")
SCHEMA_AN  = os.getenv("PG_SCHEMA_ANALYTICS", "analytics")

def conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)

def exec_sql(sql, params=None):
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, params or [])

def ensure_tables():
    exec_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_AN};")
    # ⚠️ Orden de columnas destino DEFINITIVO (usaremos el mismo en INSERT)
    exec_sql(dedent(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_AN}.obt_trips (
        service text,
        year int,
        month int,
        vendor_id int,
        pickup_ts timestamp,
        dropoff_ts timestamp,
        passenger_count int,
        trip_distance double precision,
        ratecode_id int,
        pu_location_id int,
        do_location_id int,
        pu_borough text,
        pu_zone text,
        do_borough text,
        do_zone text,
        payment_type int,
        fare_amount double precision,
        extra double precision,
        mta_tax double precision,
        tip_amount double precision,
        tolls_amount double precision,
        improvement_surcharge double precision,
        congestion_surcharge double precision,
        airport_fee double precision,
        cbd_congestion_fee double precision,
        trip_type double precision,
        total_amount double precision,
        run_id text,
        ingested_at_utc timestamp
    );
    """))
    exec_sql(f"CREATE INDEX IF NOT EXISTS idx_obt_ym ON {SCHEMA_AN}.obt_trips(year, month, service);")
    exec_sql(f"CREATE INDEX IF NOT EXISTS idx_obt_pu_do ON {SCHEMA_AN}.obt_trips(pu_location_id, do_location_id);")

def delete_partition(y, m, services):
    """
    Elimina la partición (year, month, service) antes de insertar (idempotencia).
    """
    for svc in services:
        sql = f"""
        DELETE FROM {SCHEMA_AN}.obt_trips
        WHERE year = %s AND month = %s AND service = %s;
        """
        with conn() as c:
            with c.cursor() as cur:
                cur.execute(sql, (y, m, svc))
        print(f"🧹 Borrada partición {svc}-{y}-{m:02d} en analytics.obt_trips")


def has_column(schema: str, table: str, column: str) -> bool:
    sql = """
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s AND column_name = %s
    LIMIT 1;
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, (schema, table, column))
            return cur.fetchone() is not None

def insert_partition(y, m, services, run_id):
    """
    Inserta (y,m) en analytics.obt_trips unificando yellow/green.
    - Limpieza en OBT via WHERE (no RAW)
    - Rango de fechas forzado (2015..2025 y mes 1..12)
    - Columnas opcionales detectadas dinámicamente
    - Alias 'doz' para dropoff zones
    - Columnas destino explícitas (orden estable)
    """
    target_cols = """
    (service, year, month, vendor_id, pickup_ts, dropoff_ts, passenger_count, trip_distance,
     ratecode_id, pu_location_id, do_location_id, pu_borough, pu_zone, do_borough, do_zone,
     payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
     congestion_surcharge, airport_fee, cbd_congestion_fee, trip_type, total_amount, run_id, ingested_at_utc)
    """

    def select_service(svc: str) -> str:
        raw_table = f"{svc}_taxi_trip"

        # columnas opcionales en RAW
        exists_airport = has_column(SCHEMA_RAW, raw_table, "airport_fee")        if svc == "yellow" else False
        exists_cbd     = has_column(SCHEMA_RAW, raw_table, "cbd_congestion_fee") if svc == "yellow" else False
        exists_trip    = has_column(SCHEMA_RAW, raw_table, "trip_type")          if svc == "green"  else False

        if svc == "yellow":
            pk, dk = "tpep_pickup_datetime", "tpep_dropoff_datetime"
            airport_fee_expr = 't."airport_fee"'        if exists_airport else "NULL"
            cbd_expr         = 't."cbd_congestion_fee"' if exists_cbd     else "NULL"
            trip_type_expr   = "NULL"
        else:  # green
            pk, dk = "lpep_pickup_datetime", "lpep_dropoff_datetime"
            airport_fee_expr = "NULL"
            cbd_expr         = "NULL"
            trip_type_expr   = 't."trip_type"'          if exists_trip    else "NULL"

        # SELECT alineado al orden de target_cols + filtros de calidad y rango de fechas
        return f"""
        SELECT
          '{svc}'::text AS service,
          t.year, t.month,
          t."VendorID"::int as vendor_id,
          t."{pk}" as pickup_ts,
          t."{dk}" as dropoff_ts,
          t.passenger_count::int,
          t.trip_distance::double precision,
          t."RatecodeID"::int as ratecode_id,
          t."PULocationID"::int as pu_location_id,
          t."DOLocationID"::int as do_location_id,
          pu."Borough"::text as pu_borough,
          pu."Zone"::text   as pu_zone,
          doz."Borough"::text as do_borough,
          doz."Zone"::text    as do_zone,
          t.payment_type::int,
          t.fare_amount::double precision,
          t.extra::double precision,
          t.mta_tax::double precision,
          t.tip_amount::double precision,
          t.tolls_amount::double precision,
          t.improvement_surcharge::double precision,
          t.congestion_surcharge::double precision,
          {airport_fee_expr}::double precision        AS airport_fee,
          {cbd_expr}::double precision                AS cbd_congestion_fee,
          {trip_type_expr}::double precision          AS trip_type,
          t.total_amount::double precision,
          t.run_id::text,
          t.ingested_at_utc
        FROM {SCHEMA_RAW}.{raw_table} t
        LEFT JOIN {SCHEMA_RAW}.taxi_zone_lookup pu  ON pu."LocationID"  = t."PULocationID"
        LEFT JOIN {SCHEMA_RAW}.taxi_zone_lookup doz ON doz."LocationID" = t."DOLocationID"
        WHERE
          -- partición objetivo
          t.year = %(y)s AND t.month = %(m)s AND t.service = '{svc}'
          -- rango de fechas permitido (defensivo si el parquet trae años erróneos)
          AND t.year BETWEEN 2015 AND 2025
          AND t.month BETWEEN 1 AND 12
          -- limpieza básica
          AND t."{pk}" IS NOT NULL AND t."{dk}" IS NOT NULL
          AND t."{pk}" <= t."{dk}"
          AND t.trip_distance >= 0
          AND t.total_amount >= 0
          AND (t.passenger_count IS NULL OR (t.passenger_count BETWEEN 0 AND 8))
          AND (t."RatecodeID"   IS NULL OR t."RatecodeID"   IN (1,2,3,4,5,6,99))
          AND (t.payment_type   IS NULL OR t.payment_type   IN (0,1,2,3,4,5,6))
        """

    union_sql = " UNION ALL ".join([select_service(s) for s in services])
    final_sql = f"INSERT INTO {SCHEMA_AN}.obt_trips {target_cols} {union_sql};"

    with conn() as c:
        with c.cursor() as cur:
            cur.execute(final_sql, {"y": y, "m": m})


def run(mode, y0, y1, services, run_id, overwrite):
    ensure_tables()
    years = range(y0, y1+1)
    for y in years:
        for m in range(1, 13):
            if overwrite:
                delete_partition(y, m, services)
            insert_partition(y, m, services, run_id)

def parse_services(s):
    return [x.strip() for x in s.split(",") if x.strip()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="full", choices=["full","by-partition"])
    ap.add_argument("--year-start", type=int, required=True)
    ap.add_argument("--year-end", type=int, required=True)
    ap.add_argument("--services", type=str, default="yellow,green")
    ap.add_argument("--run-id", type=str, default="obt-run")
    ap.add_argument("--overwrite", type=str, default="true")  # "true"/"false"
    args = ap.parse_args()

    services = parse_services(args.services)
    overwrite = (str(args.overwrite).lower() == "true")
    run(args.mode, args.year_start, args.year_end, services, args.run_id, overwrite)

if __name__ == "__main__":
    main()
