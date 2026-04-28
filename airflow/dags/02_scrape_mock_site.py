"""
AniData Lab - DAG 02 : Scraping du mock-site
Scrape automatiquement le mock-site AniDex en local et sauvegarde les données en JSON.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def check_mock_site_reachable():
    """Vérifie que le mock-site est accessible avant de lancer le scraping."""
    import requests

    url = "http://mock-site"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        print(f"Mock-site accessible : {url} (HTTP {response.status_code})")
    except Exception as exc:
        raise RuntimeError(f"Mock-site inaccessible ({url}) : {exc}") from exc


def run_scraper(**context):
    """Lance le scraping complet du mock-site et pousse le chemin du fichier via XCom."""
    import sys
    sys.path.insert(0, "/opt/airflow/plugins")

    from anidata_scraper.scraper import scrape_to_file

    output_path = scrape_to_file(
        output_dir="/opt/airflow/output",
        base_url="http://mock-site",
        enrich=True,
    )

    print(f"Fichier produit : {output_path}")
    context["ti"].xcom_push(key="scraped_file", value=output_path)
    return output_path


def log_scraping_result(**context):
    """Affiche un résumé des données scrapées."""
    import json
    from pathlib import Path

    filepath = context["ti"].xcom_pull(task_ids="scrape_site", key="scraped_file")
    if not filepath or not Path(filepath).exists():
        print("Aucun fichier trouvé.")
        return

    data = json.loads(Path(filepath).read_text(encoding="utf-8"))
    stats = data.get("stats", {})

    print("\n" + "=" * 50)
    print("  RÉSULTAT DU SCRAPING")
    print("=" * 50)
    print(f"  Source      : {data.get('source')}")
    print(f"  Scraped at  : {data.get('scraped_at')}")
    print(f"  Animes      : {stats.get('animes_count', 0)}")
    print(f"  News        : {stats.get('news_count', 0)}")
    print(f"  Sans score  : {stats.get('missing_scores', 0)}")
    print(f"  Sans studio : {stats.get('missing_studios', 0)}")
    print(f"  Fichier     : {filepath}")
    print("=" * 50 + "\n")


with DAG(
    dag_id="02_scrape_mock_site",
    default_args=default_args,
    description="Scrape le mock-site AniDex en local et sauvegarde en JSON",
    schedule_interval="@daily",
    start_date=datetime(2026, 3, 26),
    catchup=False,
    tags=["anidata", "scraping"],
) as dag:

    task_check_site = PythonOperator(
        task_id="check_mock_site",
        python_callable=check_mock_site_reachable,
        doc_md="Vérifie que le mock-site est accessible",
    )

    task_scrape = PythonOperator(
        task_id="scrape_site",
        python_callable=run_scraper,
        doc_md="Scrape le catalogue et les news du mock-site",
    )

    task_log = PythonOperator(
        task_id="log_results",
        python_callable=log_scraping_result,
        doc_md="Affiche le résumé du scraping",
    )

    task_check_site >> task_scrape >> task_log
