import pandas as pd
from pathlib import Path


def gold_aggregate(**context):
    silver_file = context["ti"].xcom_pull(
        task_ids="silver_transform", key="silver_path")

    if silver_file is None:
        raise ValueError("Silver file path not found in XCom")

    df_silver = pd.read_csv(silver_file)

    gold_path = Path("/opt/airflow/data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)

    df_gold = df_silver.groupby("origin_country").agg(
        total_flights=("icao24", "count"),
        avg_velocity=("velocity", "mean"),
        on_ground=("on_ground", "mean"),
        avg_geo_altitude=("geo_altitude", "mean")
    ).reset_index()

    execution_date = context["ds_nodash"]
    print(execution_date)
    output_file = gold_path / f"gold_{execution_date}.csv"
    df_gold.to_csv(output_file, index=False)

    print("Gold aggregation completed successfully.")
