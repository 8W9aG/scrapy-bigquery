"""The pipelines used for sending items to BigQuery."""
import base64
import datetime
import json
import typing
import uuid

import scrapy
from bigquery_schema_generator.generate_schema import SchemaGenerator
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.oauth2 import service_account
from scrapy.exceptions import NotConfigured


class BigQueryPipeline:
    """The pipeline for inserting rows into BigQuery."""

    bigquery_dataset_key = "BIGQUERY_DATASET"
    bigquery_table_key = "BIGQUERY_TABLE"

    def __init__(self, service_account_info: typing.Dict, dataset_id: str):
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        self.client = bigquery.Client(credentials=credentials)
        self.project_id = service_account_info["project_id"]
        try:
            dataset = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
            self.client.create_dataset(dataset, timeout=30)
        except Conflict:
            # It already exists
            pass
        self.tables_created: typing.Set[str] = set()
        self.schema_generator = SchemaGenerator(input_format="dict")
        self.session_id = str(uuid.uuid4())
        self.item_cache = {}

    @classmethod
    def from_crawler(cls, crawler) -> typing.Any:
        service_account_json = base64.b64decode(
            crawler.settings.get("BIGQUERY_SERVICE_ACCOUNT")
        ).decode()
        try:
            gcp_service_account = json.loads(service_account_json)
        except json.decoder.JSONDecodeError:
            raise NotConfigured(
                "Could not decode BIGQUERY_SERVICE_ACCOUNT, disabling BigQuery middleware"
            )
        dataset = crawler.settings.get("BIGQUERY_DATASET")
        return cls(service_account_info=gcp_service_account, dataset_id=dataset)

    def process_item(self, item: typing.Dict, spider: scrapy.Spider) -> typing.Dict:
        table_id, item = self.table_id(item, spider)
        self.ensure_table_created(table_id, item, spider)
        for item_key in item:
            if isinstance(item[item_key], datetime.date):
                item[item_key] = item[item_key].strftime("%Y-%m-%d")
        if table_id not in self.item_cache:
            self.item_cache[table_id] = []
        self.item_cache[table_id].append(item)
        self.flush_items(spider)
        return item

    def close_spider(self, spider):
        self.flush_items(spider, force=True)

    def table_id(
        self, item: typing.Dict, spider: scrapy.Spider
    ) -> typing.Tuple[str, typing.Dict]:
        """Generate a table ID."""

        def extract_and_delete(key: str):
            if key in item:
                value = item[key]
                del item[key]
                return value
            return spider.settings[key]

        dataset = extract_and_delete(self.bigquery_dataset_key)
        table = extract_and_delete(self.bigquery_table_key)
        if spider.settings.get("BIGQUERY_ADD_SCRAPED_TIME", False):
            item["scraped_time"] = datetime.datetime.now()
        if spider.settings.get("BIGQUERY_ADD_SCRAPER_NAME", False):
            item["scraper"] = spider.name
        if spider.settings.get("BIGQUERY_ADD_SCRAPER_SESSION", False):
            item["scraper_session_id"] = self.session_id
        for key in item:
            if isinstance(item[key], datetime.datetime):
                item[key] = str(item[key])
        return f"{self.project_id}.{dataset}.{table}", item

    def ensure_table_created(
        self, table_id: str, item: typing.Dict, spider: scrapy.Spider
    ) -> None:
        """Ensures that the BigQuery table is created."""
        if table_id in self.tables_created:
            return
        try:
            schema_map, error_logs = self.schema_generator.deduce_schema(
                input_data=[item]
            )
            for error in error_logs:
                spider.logger.error(
                    f"Problem generating BigQuery Schema {error['line_number']}: {error['msg']}"
                )
            table = bigquery.Table(
                table_id, schema=self.schema_generator.flatten_schema(schema_map)
            )
            self.client.create_table(table)
        except Conflict:
            pass
        except Exception:
            pass
        self.tables_created.add(table_id)

    def flush_items(self, spider: scrapy.Spider, force=False):
        """Flush the items cache if it is big enough for the settings."""
        for table_id in self.item_cache:
            items = self.item_cache[table_id]
            if not items:
                continue
            if len(items) >= spider.settings.get("BIGQUERY_ITEM_BATCH", 1) or force:
                errors = self.client.insert_rows_json(table_id, items)
                if errors:
                    spider.logger.error(f"Error inserting rows to BigQuery: {errors}")
                self.item_cache[table_id] = []
