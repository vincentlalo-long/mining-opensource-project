"""Create the Trino database, gold dataset and one chart through the Superset REST API."""

import base64
import json
import os

import requests

BASE_URL = "http://127.0.0.1:8088"
DATABASE_NAME = "Trino Gold"
DATASET_TABLE = "event_volume_daily"
DATASET_SCHEMA = "gold"
CHART_NAME = "Gold event volume by type"
SQLALCHEMY_URI = "trino://admin@trino:8080/iceberg/gold"


def main():
    session = requests.Session()
    login = session.post(
        f"{BASE_URL}/api/v1/security/login",
        json={
            "username": "admin",
            "password": os.environ["SUPERSET_ADMIN_PASSWORD"],
            "provider": "db",
            "refresh": False,
        },
        timeout=30,
    )
    login.raise_for_status()
    session.headers.update({
        "Authorization": f"Bearer {login.json()['access_token']}",
        "Referer": BASE_URL,
    })
    csrf = session.get(f"{BASE_URL}/api/v1/security/csrf_token/", timeout=30)
    csrf.raise_for_status()
    session.headers.update({"X-CSRFToken": csrf.json()["result"]})

    def api(method, path, **kwargs):
        response = session.request(method, f"{BASE_URL}{path}", timeout=60, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text}")
        return response.json() if response.text else {}

    def collection(name):
        return api("GET", f"/api/v1/{name}/?q=(page_size:100)")["result"]

    database = next((item for item in collection("database") if item["database_name"] == DATABASE_NAME), None)
    if database is None:
        database = api("POST", "/api/v1/database/", json={
            "database_name": DATABASE_NAME,
            "sqlalchemy_uri": SQLALCHEMY_URI,
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
        })["id"]
    else:
        database = database["id"]

    dataset = next(
        (item for item in collection("dataset")
         if item["schema"] == DATASET_SCHEMA
         and item["table_name"] == DATASET_TABLE
         and (item.get("database") or {}).get("id", item.get("database_id")) == database),
        None,
    )
    if dataset is None:
        dataset = api("POST", "/api/v1/dataset/", json={
            "database": database,
            "schema": DATASET_SCHEMA,
            "table_name": DATASET_TABLE,
        })["id"]
    else:
        dataset = dataset["id"]

    metric = {
        "expressionType": "SIMPLE",
        "column": {"column_name": "event_count"},
        "aggregate": "SUM",
        "label": "Event count",
    }
    params = {
        "datasource": f"{dataset}__table",
        "viz_type": "echarts_timeseries_bar",
        "metrics": [metric],
        "groupby": [],
        "x_axis": "event_type",
        "row_limit": 100,
        "order_desc": True,
        "show_legend": False,
        "adhoc_filters": [],
    }
    query_context = {
        "datasource": {"id": dataset, "type": "table"},
        "force": False,
        "queries": [{
            "columns": ["event_type"],
            "metrics": [metric],
            "orderby": [[metric, False]],
            "row_limit": 100,
            "series_limit": 0,
            "order_desc": True,
        }],
        "form_data": params,
        "result_format": "json",
        "result_type": "full",
    }

    chart_payload = {
        "slice_name": CHART_NAME,
        "viz_type": "echarts_timeseries_bar",
        "datasource_id": dataset,
        "datasource_type": "table",
        "params": json.dumps(params),
        "query_context": json.dumps(query_context),
    }
    chart = next((item for item in collection("chart") if item["slice_name"] == CHART_NAME), None)
    if chart is None:
        chart = api("POST", "/api/v1/chart/", json=chart_payload)["id"]
    else:
        chart = api("PUT", f"/api/v1/chart/{chart['id']}", json=chart_payload)["id"]

    data = api("GET", f"/api/v1/chart/{chart}/data/?force=true")
    rows = data["result"][0]["data"]
    if not rows:
        raise RuntimeError(f"Superset returned no chart rows: {data}")

    result = {
        "database_id": database,
        "dataset_id": dataset,
        "chart_id": chart,
        "chart_url": f"http://localhost:8088/explore/?slice_id={chart}",
        "chart_rows": len(rows),
        "chart_sample": rows[:3],
    }
    print("SERVING_BOOTSTRAP_JSON=" + base64.b64encode(json.dumps(result).encode()).decode())


if __name__ == "__main__":
    main()
