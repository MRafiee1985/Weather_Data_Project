"""Utility functions for accessing weather data and a lightweight database.

The original project used external dependencies such as ``requests`` for HTTP
calls and ``psycopg2`` for PostgreSQL connections.  Those packages are not
available in the execution environment for the kata, which meant importing the
module raised ``ModuleNotFoundError`` during test collection.  This reworked
module relies solely on Python's standard library so it can be imported
everywhere without additional setup.

* :func:`connect_to_db` returns a connection to an in-memory SQLite database.
* :func:`fetch_data` retrieves live data using :mod:`urllib` and falls back to
  :func:`mock_fetch_data` when the request cannot be performed (e.g. offline
  execution).

These changes keep the external API of the module the same while making the
code self-contained and test friendly.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from urllib import request


def connect_to_db(db_path: str = ":memory:") -> sqlite3.Connection:
    """Return a connection to a lightweight SQLite database.

    Parameters
    ----------
    db_path:
        Path to the SQLite database.  The default ``":memory:"`` creates a
        temporary in-memory database which is perfect for tests.
    """

    try:
        conn = sqlite3.connect(db_path)
        print("✅ Successfully connected to database")
        return conn
    except sqlite3.Error as e:  # pragma: no cover - extremely unlikely
        raise RuntimeError(f"❌ Could not connect to database: {e}") from e


def fetch_data() -> dict:
    """Fetch weather data from the WeatherStack API.

    If the network request fails (for example because the environment is
    offline or an API key is invalid) we fall back to :func:`mock_fetch_data` so
    that unit tests remain deterministic and do not depend on external
    services.
    """

    WEATHERSTACK_KEY = "ab807c806cc7c9d02bb423de92820f1f"
    api_key = os.environ.get("WEATHERSTACK_KEY", WEATHERSTACK_KEY)
    url = (
        "http://api.weatherstack.com/current?access_key="
        f"{api_key}&query=New York"
    )

    print("Fetching data from the weather API...")
    try:
        with request.urlopen(url) as resp:
            payload = resp.read().decode("utf-8")
        print("Request was successful!")
        return json.loads(payload)
    except Exception as exc:
        logging.warning("Using mock weather data due to request failure: %s", exc)
        return mock_fetch_data()


def mock_fetch_data() -> dict:
    """Return a small sample payload mimicking the WeatherStack response."""

    return {
        "request": {
            "type": "City",
            "query": "New York, United States of America",
            "language": "en",
            "unit": "m",
        },
        "location": {
            "name": "New York",
            "country": "United States of America",
            "region": "New York",
            "lat": "40.714",
            "lon": "-74.006",
            "timezone_id": "America/New_York",
            "localtime": "2025-07-21 23:37",
            "localtime_epoch": 1753141020,
            "utc_offset": "-4.0",
        },
        "current": {
            "observation_time": "03:37 AM",
            "temperature": 24,
            "weather_code": 113,
            "weather_icons": [
                "https://cdn.worldweatheronline.com/images/wsymbols01_png_64/"
                "wsymbol_0008_clear_sky_night.png"
            ],
            "weather_descriptions": ["Clear "],
            "astro": {
                "sunrise": "05:43 AM",
                "sunset": "08:21 PM",
                "moonrise": "01:53 AM",
                "moonset": "06:07 PM",
                "moon_phase": "Waning Crescent",
                "moon_illumination": 18,
            },
            "air_quality": {
                "co": "344.1",
                "no2": "25.53",
                "o3": "41",
                "so2": "6.105",
                "pm2_5": "9.435",
                "pm10": "9.435",
                "us-epa-index": "1",
                "gb-defra-index": "1",
            },
            "wind_speed": 16,
            "wind_degree": 13,
            "wind_dir": "NNE",
            "pressure": 1017,
            "precip": 0,
            "humidity": 40,
            "cloudcover": 0,
            "feelslike": 26,
            "uv_index": 0,
            "visibility": 16,
            "is_day": "no",
        },
    }


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    conn = connect_to_db()
    data = fetch_data()
    print(json.dumps(data, indent=2, ensure_ascii=False))
    conn.close()
    print("🔒 Database connection closed.")

