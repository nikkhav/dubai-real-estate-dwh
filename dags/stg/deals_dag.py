import logging
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from lib.pg_connect import PgConnect
from stg.deals_loader import DealsLoader, DealsOriginRepository, DealsDestRepository

log = logging.getLogger(__name__)

URL = "https://gateway.dubailand.gov.ae/open-data/transactions/export/csv"


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['stg', 'source', 'loader'],
    is_paused_upon_creation=False
)
def stg_deals_dag():
    dwh_pg_connect = PgConnect("PG_DWH_CONNECTION")

    @task(task_id="stg_load_deals")
    def stg_load_deals(logical_date=None):
        repo_origin = DealsOriginRepository(URL)
        repo_dest = DealsDestRepository()
        loader = DealsLoader(repo_origin, dwh_pg_connect, repo_dest, log)
        loaded_count = loader.load_deals(logical_date)
        return f"stg_deals_loaded: {loaded_count} rows"

    stg_load_deals = stg_load_deals()

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="docker-compose run --rm dbt dbt run",
    )

    stg_load_deals >> run_dbt


stg_deals_dag = stg_deals_dag()
