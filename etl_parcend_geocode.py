import json
import os
import time
from typing import Optional, Tuple

import oracledb
import requests
from dotenv import load_dotenv

SELECT_SQL = "SELECT codparc, nomeparc, endereco FROM parcend WHERE endereco IS NOT NULL"
INSERT_SQL = (
    "INSERT INTO rependparc (codparc, nomeparc, endereco, latitude, longitude) "
    "VALUES (:codparc, :nomeparc, :endereco, :latitude, :longitude)"
)
BATCH_SIZE = 100
REQUEST_DELAY_SECONDS = 0.15
MAX_BACKOFF_SECONDS = 5
MAX_OVER_QUERY_LIMIT_RETRIES = 3


def build_dsn() -> str:
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    service_name = os.environ["DB_SERVICE_NAME"]
    return oracledb.makedsn(host, port, service_name=service_name)


def geocode_address(address: str, api_key: str) -> Tuple[Optional[float], Optional[float]]:
    params = {"address": address, "key": api_key}
    backoff_seconds = 1

    for attempt in range(MAX_OVER_QUERY_LIMIT_RETRIES + 1):
        response = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        status = payload.get("status")

        if status == "OVER_QUERY_LIMIT":
            if attempt >= MAX_OVER_QUERY_LIMIT_RETRIES:
                print(f"OVER_QUERY_LIMIT after {attempt + 1} attempts for '{address}'")
                return None, None
            sleep_time = min(backoff_seconds, MAX_BACKOFF_SECONDS)
            print(f"OVER_QUERY_LIMIT: backing off {sleep_time}s for '{address}'")
            time.sleep(sleep_time)
            backoff_seconds *= 2
            continue

        if status != "OK":
            print(f"Geocode status {status} for '{address}'")
            return None, None

        results = payload.get("results", [])
        if not results:
            print(f"No results for '{address}'")
            return None, None

        location = results[0].get("geometry", {}).get("location", {})
        latitude = location.get("lat")
        longitude = location.get("lng")
        if latitude is None or longitude is None:
            print(f"Missing lat/lng for '{address}'")
            return None, None

        return float(latitude), float(longitude)

    return None, None


def main() -> None:
    load_dotenv()

    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_MAPS_API_KEY is required")

    dsn = build_dsn()
    connection = None
    select_cursor = None
    insert_cursor = None
    processed = 0
    total_read = 0
    total_inserted = 0
    total_skipped = 0
    total_api_failures = 0
    total_db_failures = 0

    try:
        connection = oracledb.connect(
            user=os.environ["DB_USERNAME"],
            password=os.environ["DB_PASSWORD"],
            dsn=dsn,
        )
        select_cursor = connection.cursor()
        insert_cursor = connection.cursor()

        select_cursor.execute(SELECT_SQL)
        for codparc, nomeparc, endereco in select_cursor:
            total_read += 1
            try:
                if endereco is None or not str(endereco).strip():
                    total_skipped += 1
                    continue

                latitude, longitude = geocode_address(endereco, api_key)
                if latitude is None or longitude is None:
                    total_api_failures += 1
                    time.sleep(REQUEST_DELAY_SECONDS)
                    continue

                insert_cursor.execute(
                    INSERT_SQL,
                    {
                        "codparc": codparc,
                        "nomeparc": nomeparc,
                        "endereco": endereco,
                        "latitude": latitude,
                        "longitude": longitude,
                    },
                )
                processed += 1
                total_inserted += 1

                if processed % BATCH_SIZE == 0:
                    connection.commit()
            except (requests.RequestException, json.JSONDecodeError) as exc:
                total_api_failures += 1
                print(f"Error processing codparc={codparc}: {exc}")
            except oracledb.Error as exc:
                total_db_failures += 1
                print(f"Error processing codparc={codparc}: {exc}")
            finally:
                time.sleep(REQUEST_DELAY_SECONDS)

        connection.commit()
        print(
            "Resumo: "
            f"total_lidos={total_read}, "
            f"inseridos={total_inserted}, "
            f"pulados={total_skipped}, "
            f"falhas_api={total_api_failures}, "
            f"falhas_banco={total_db_failures}"
        )
    finally:
        if select_cursor is not None:
            select_cursor.close()
        if insert_cursor is not None:
            insert_cursor.close()
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
