import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from datetime import UTC, datetime
import json

URL = "https://opensky-network.org/api/states/all"


def bronze_ingest(**kwargs):

    session = requests.Session()

    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)

    response = session.get(URL, timeout=30)

    response.raise_for_status()

    data = response.json()

    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")

    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f)

    kwargs["ti"].xcom_push(
        key="bronze_path",
        value=str(path)
    )

    print(f"Saved file to {path}")
