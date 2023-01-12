# scrapy-bigquery

<a href="https://pypi.org/project/scrapy-bigquery/">
    <img alt="PyPi" src="https://img.shields.io/pypi/v/scrapy-bigquery">
</a>

A Big Query pipeline to store items into [Google BigQuery](https://cloud.google.com/bigquery/).

## Dependencies :globe_with_meridians:

- [Python 3.7](https://www.python.org/downloads/release/python-370/)
- [Scrapy 2.4.0](https://scrapy.org/)
- [Google Cloud Bigquery 2.23.2](https://pypi.org/project/google-cloud-bigquery/)
- [Bigquery Schema Generator 1.4](https://github.com/bxparks/bigquery-schema-generator)

## Installation :inbox_tray:

This is a python package hosted on pypi, so to install simply run the following command:

`pip install scrapy-bigquery`

## Settings

### BIGQUERY_DATASET (Required)

The name of the bigquery dataset to post to.

### BIGQUERY_TABLE (Required)

The name of the bigquery table in the dataset to post to.

### BIGQUERY_SERVICE_ACCOUNT (Required)

The base64'd JSON of the [Google Service Account](https://cloud.google.com/iam/docs/service-accounts) used to authenticate with Google BigQuery. You can generate it from a service account like so:

`cat service-account.json | jq . -c | base64`

### BIGQUERY_ADD_SCRAPED_TIME (Optional)

Whether to add the time the item was scraped to the item when posting it to BigQuery. This will add current datetime to the column `scraped_time` in the BigQuery table.

### BIGQUERY_ADD_SCRAPER_NAME (Optional)

Whether to add the name of the scraper to the item when posting it to BigQuery. This will add the scrapers name to the column `scraper` in the BigQuery table.

### BIGQUERY_ADD_SCRAPER_SESSION (Optional)

Whether to add the session ID of the scraper to the item when posting it to BigQuery. This will add the scrapers session ID to the column `scraper_session_id` in the BigQuery table.

### BIGQUERY_ITEM_BATCH (Optional)

The number of items to batch process when inserting into BigQuery. The higher this number the faster the pipeline will process items.

### BIGQUERY_FIELDS_TO_SAVE (Optional)

A list of item fields to save to BigQuery. If this is not set, all fields of an item will be saved.

## Usage example :eyes:

In order to use this plugin simply add the following settings and substitute your variables:

```
BIGQUERY_DATASET = "my-dataset"
BIGQUERY_TABLE = "my-table"
BIGQUERY_SERVICE_ACCOUNT = "eyJ0eX=="
ITEM_PIPELINES = {
    "bigquerypipeline.pipelines.BigQueryPipeline": 301
}
BIGQUERY_FIELDS_TO_SAVE = ["name", "age"] # Optional. This will only save the name and age fields of an item to BigQuery.
```

The pipeline will attempt to create a dataset/table if none exist by inferring the type from the dictionaries it processes, however be aware that this can be flaky (especially if you have nulls in the dictionary), so it is recommended you create the table prior to running.

If you want to specify a table for a specific item, you can add the keys "BIGQUERY_DATASET" and "BIGQUERY_TABLE" to the item you pass back to the pipeline. This will override where the item is posted, allowing you to handle more than one item type in a scraper. The keys/values here will not be part of the final item in the table.

## License :memo:

The project is available under the [MIT License](LICENSE).
