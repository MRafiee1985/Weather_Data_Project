import logging
import json
from datetime import datetime
import contextlib

# Import utilities from our local package.  The original code imported from a
# module named ``get_api`` and relied on the external ``psycopg2`` package.  In
# the refactored project ``fetch_data`` and ``connect_to_db`` live in
# ``api_request.api_request`` and use only the Python standard library.
from api_request.api_request import fetch_data, mock_fetch_data, connect_to_db

# -------------------------
# Configure Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -------------------------
# Create (or Recreate) Table
# -------------------------
def create_table(conn):
    """
    Drop the old weather_data table (if any) and create a fresh one
    with the correct columns.
    """
    cursor = conn.cursor()
    use_ctx = hasattr(cursor, "__enter__")
    try:
        cm = cursor if use_ctx else contextlib.nullcontext(cursor)
        with cm as cur:
            # Create the destination table if it does not already exist.  The
            # schema mirrors the fields used in ``insert_records``.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT,
                    temperature FLOAT,
                    weather_descriptions TEXT,
                    wind_speed FLOAT,
                    local_time TEXT,
                    utc_offset TEXT
                );
                """
            )
        conn.commit()
        logging.info("✅ Table 'weather_data' recreated with correct schema.")
    except Exception as e:  # pragma: no cover - exercised via tests
        logging.error(f"❌ Failed to create table: {e}")
        raise
    finally:
        if not use_ctx:
            try:
                cursor.close()
            except Exception:
                pass

# -------------------------
# Insert Record
# -------------------------
def insert_records(conn, data):
    """
    Insert one record of weather data into the ``weather_data`` table.
    Maps JSON ``localtime`` → DB ``local_time``.
    """
    cursor = None
    use_ctx = False
    try:
        location = data.get('location', {})
        current = data.get('current', {})

        # Required keys within the location portion of the payload.
        required_keys_loc = ['name', 'country', 'localtime', 'utc_offset']
        missing = [k for k in required_keys_loc if k not in location]
        if missing:
            raise ValueError(f"⚠️ Missing required keys: {missing}")

        city = location['name']
        utc_offset = location['utc_offset']
        local_time = location['localtime']
        temperature = current.get('temperature')
        weather_descriptions = json.dumps(current.get('weather_descriptions', []))
        wind_speed = current.get('wind_speed')

        cursor = conn.cursor()
        use_ctx = hasattr(cursor, "__enter__")
        cm = cursor if use_ctx else contextlib.nullcontext(cursor)
        with cm as cur:
            cur.execute(
                """
                INSERT INTO weather_data
                    (city, temperature, weather_descriptions, wind_speed, local_time, utc_offset)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (city, temperature, weather_descriptions, wind_speed, local_time, utc_offset),
            )
        conn.commit()
        logging.info("✅ Weather data inserted successfully.")
    except Exception as e:  # pragma: no cover - exercised via tests
        logging.error(f"❌ Unexpected error during insertion: {e}")
        raise
    finally:
        if cursor is not None and not use_ctx:
            try:
                cursor.close()
            except Exception:
                pass

# -------------------------
# Main ETL Logic
# -------------------------
def main(use_mock: bool = False):
    """
    Fetch weather data, recreate table, then insert one row.
    """
    conn = None
    try:
        data =  fetch_data() if not use_mock else mock_fetch_data()
        # payload =  mock_fetch_data() if use_mock else fetch_data()
         

        conn = connect_to_db()
        create_table(conn)           # always drops & recreates
        insert_records(conn, data)
    except Exception as e:
        logging.error(f"❌ ETL process failed: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("🔒 Database connection closed.")

# -------------------------
# Script Entry Point
# -------------------------
if __name__ == "__main__":
    main(use_mock=False)  # Set to False for production use

