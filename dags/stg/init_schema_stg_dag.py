import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib.schema_init import SchemaDdl
from lib.pg_connect import PgConnect

log = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['stg', 'schema', 'ddl'],
    is_paused_upon_creation=False
)
def stg_init_schema_dag():
    dwh_pg_connect = PgConnect("PG_DWH_CONNECTION")

    # Забираем путь до каталога с SQL-файлами из переменных Airflow.
    ddl_path = Variable.get("STG_DDL_FILES_PATH")

    @task(task_id="stg_schema_init")
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path)
        return "stg_schema_initialized"

    init_schema = schema_init()

    init_schema


stg_init_schema_dag = stg_init_schema_dag()
