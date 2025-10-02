import re
import uuid
from datetime import datetime, UTC
from logging import Logger
from io import StringIO
from typing import List

import requests
import pandas as pd
from psycopg2.extensions import connection as Connection
from pydantic import BaseModel

from lib.dict_util import json2str
from lib.pg_connect import PgConnect
from settings_repository import EtlSettingsRepository, EtlSetting


class DealObj(BaseModel):
    transaction_number: str
    instance_date: datetime
    payload: str
    source_loaded_at: datetime
    ingestion_id: uuid.UUID


class DealsOriginRepository:
    def __init__(self, url: str):
        self.url = url

    def list_deals(self, target_date: datetime, batch_limit: int) -> List[DealObj]:
        date_str = target_date.strftime("%m/%d/%Y")
        skip = 0
        all_deals: List[DealObj] = []

        while True:
            body = {
                "parameters": {
                    "P_FROM_DATE": date_str,
                    "P_TO_DATE": date_str,
                    "P_GROUP_ID": "",
                    "P_IS_OFFPLAN": "",
                    "P_IS_FREE_HOLD": "",
                    "P_AREA_ID": "",
                    "P_USAGE_ID": "",
                    "P_PROP_TYPE_ID": "",
                    "P_TAKE": str(batch_limit),
                    "P_SKIP": str(skip),
                    "P_SORT": "INSTANCE_DATE_ASC"
                },
                "command": "transactions",
                "labels": {
                    "TRANSACTION_NUMBER": "TRANSACTION_NUMBER",
                    "INSTANCE_DATE": "INSTANCE_DATE",
                    "GROUP_EN": "GROUP_EN",
                    "PROCEDURE_EN": "PROCEDURE_EN",
                    "IS_OFFPLAN_EN": "IS_OFFPLAN_EN",
                    "IS_FREE_HOLD_EN": "IS_FREE_HOLD_EN",
                    "USAGE_EN": "USAGE_EN",
                    "AREA_EN": "AREA_EN",
                    "PROP_TYPE_EN": "PROP_TYPE_EN",
                    "PROP_SB_TYPE_EN": "PROP_SB_TYPE_EN",
                    "TRANS_VALUE": "TRANS_VALUE",
                    "PROCEDURE_AREA": "PROCEDURE_AREA",
                    "ACTUAL_AREA": "ACTUAL_AREA",
                    "ROOMS_EN": "ROOMS_EN",
                    "PARKING": "PARKING",
                    "NEAREST_METRO_EN": "NEAREST_METRO_EN",
                    "NEAREST_MALL_EN": "NEAREST_MALL_EN",
                    "NEAREST_LANDMARK_EN": "NEAREST_LANDMARK_EN",
                    "TOTAL_BUYER": "TOTAL_BUYER",
                    "TOTAL_SELLER": "TOTAL_SELLER",
                    "MASTER_PROJECT_EN": "MASTER_PROJECT_EN",
                    "PROJECT_EN": "PROJECT_EN"
                }
            }

            res = requests.post(self.url, json=body, headers={"Content-Type": "application/json"})
            res.raise_for_status()
            df = pd.read_csv(StringIO(res.text))
            for column in df.columns:  # Need to remove Byte Order Marker at beginning of first column name
                new_column_name = re.sub(r"[^0-9a-zA-Z.,-/_ ]", "", column)
                df.rename(columns={column: new_column_name}, inplace=True)
            if df.empty:
                break

            batch_ingestion_id = uuid.uuid4()

            deals = [
                DealObj(
                    transaction_number=row["TRANSACTION_NUMBER"],
                    instance_date=pd.to_datetime(row["INSTANCE_DATE"]),
                    payload=row.to_json(),
                    source_loaded_at=datetime.now(UTC),
                    ingestion_id=batch_ingestion_id
                )
                for _, row in df.iterrows()
            ]
            all_deals.extend(deals)

            if len(df) < batch_limit:
                break

            skip += batch_limit

        return all_deals


class DealsDestRepository:
    def insert_deal(self, conn: Connection, deal: DealObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.raw_deals(transaction_number, payload, source_loaded_at, ingestion_id)
                VALUES (%(transaction_number)s, %(payload)s, %(source_loaded_at)s, %(ingestion_id)s)
                ON CONFLICT (transaction_number) DO UPDATE
                SET
                    payload = EXCLUDED.payload,
                    source_loaded_at = EXCLUDED.source_loaded_at,
                    ingestion_id = EXCLUDED.ingestion_id;
                """,
                {
                    "transaction_number": deal.transaction_number,
                    "payload": deal.payload,
                    "source_loaded_at": deal.source_loaded_at,
                    "ingestion_id": deal.ingestion_id
                },
            )


class DealsLoader:
    WF_KEY = "deals_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 1000
    LOG_THRESHOLD = 100

    def __init__(self, origin: DealsOriginRepository, pg_dest: PgConnect, dest: DealsDestRepository, log: Logger) -> None:
        self.origin = origin
        self.dest = dest
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository(schema="stg")
        self.log = log

    def load_deals(self, target_date: datetime) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: target_date.isoformat()},
                )

            load_queue = self.origin.list_deals(target_date, self.BATCH_LIMIT)
            if not load_queue:
                self.log.info("No deals for this day.")
                return 0

            for i, deal in enumerate(load_queue, start=1):
                self.dest.insert_deal(conn, deal)
                if i % self.LOG_THRESHOLD == 0:
                    self.log.info(f"Inserted {i} deals...")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = target_date.isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished for {target_date}")
            return len(load_queue)
