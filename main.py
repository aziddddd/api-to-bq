'''
Creator : Azid
Objective: This is image to use for any pipeline that retrieve data from any custom database API via HTTPS requests to BigQuery

Flag:
    --url https://your.api.com/data.json
    --method GET
    --headers '{"Authorization": "your_auth_token"}'
    --project-id your-project-id
    --dataset-id your-dataset-id
    --table-id your-table-id
    --location US
    --sa your-sa-key.json
    --url-type equal_based    # dict_based / equal_based 
    --timestamp-column stamp
    --date-columns '["date"]'
    --filter-params '{"url_condition1": "condition1", "url_condition2": "condition2"}'
    --write-disposition WRITE_APPEND    # WRITE_APPEND / WRITE_TRUNCATE
    --time-partitioning '{"type_": "DAY", "field": "stamp"}'
    --topic topic1

=======================================

time python3 ./main.py \
--url 'https://your.api.com/data/PAGE_NUMBER' \
--method GET \
--project-id your-project-id \
--dataset-id your-dataset-id \
--table-id your-table-id \
--location US \
--sa your-sa-key.json \
--url-type equal_based \
--timestamp-column stamp \
--write-disposition WRITE_APPEND \
--date-columns '["date1", "date2"]' \
--time-partitioning '{"type_": "DAY", "field": "stamp"}' \
--paginations true \
--topic topic1

time python3 ./main.py \
--url https://api.met.gov.my/v2/data \
--method GET \
--headers '{"Authorization": "your_auth_token"}' \
--project-id your-project-id \
--dataset-id your-dataset-id \
--table-id your-table-id \
--location US \
--temp-table true \
--sa your-sa-key.json \
--url-type equal_based \
--write-disposition WRITE_APPEND \
--date-columns '["date"]' \
--filter-params '{"url_condition1": "condition1", "url_condition2": "condition2", "start_date": "2022-01-01", "end_date": "2022-12-31"}' \
--time-partitioning '{"type_": "DAY", "field": "date"}' \
--topic topic2

'''

from lib.source import Source
import argparse
import json
import os

def main():
    parser = argparse.ArgumentParser(
        description='Script to pull data from any custom database API via HTTPS requests'
    )
    parser.add_argument('--url', type=str, required=True)
    parser.add_argument('--method', type=str, default="GET")
    parser.add_argument('--headers', default=None)
    parser.add_argument('--project-id', type=str, required=True)
    parser.add_argument('--dataset-id', type=str, required=True)
    parser.add_argument('--table-id', type=str, required=True)
    parser.add_argument('--location', type=str, required=True)
    parser.add_argument('--temp-table', type=str, default="false")
    parser.add_argument('--sa', type=str, required=True)
    parser.add_argument('--url-type', type=str, default="equal_based")
    parser.add_argument('--timestamp-column', type=str)
    parser.add_argument('--date-columns', type=str, default="[]")
    parser.add_argument('--filter-params', type=str, default="{}")
    parser.add_argument('--write-disposition', type=str, default="WRITE_APPEND")
    parser.add_argument('--time-partitioning', type=str, default="{}")
    parser.add_argument('--paginations', type=str, default="false")
    parser.add_argument('--topic', type=str, required=True)
    args = parser.parse_args()

    credentials_path = '/secret2/credentials.json'
    # if credential is mounted, there should be key named 'api_key'.
    if os.path.exists(credentials_path):
        with open(credentials_path, 'r') as openfile:
            credentials = json.load(openfile)
    # if credential is not mounted, means no credentials required.
    else:
        credentials = {'api_key': 'no_api_key_required'}

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f'/secret1/{args.sa}'
    os.environ["CLOUDSDK_PYTHON"] = "python3"

    source = Source(
        url=args.url,
        method=args.method,
        headers=args.headers,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_id=args.table_id,
        location=args.location,
        temp_table=args.temp_table,
        api_key=credentials.get('api_key', 'no_api_key_provided'),
        url_type=args.url_type,
        timestamp_column=args.timestamp_column,
        date_columns=args.date_columns,
        filter_params=args.filter_params,
        write_disposition=args.write_disposition,
        time_partitioning=args.time_partitioning,
        paginations=args.paginations,
        topic=args.topic,
    )
    source.make_requests()
    source.upload_to_bq()

if __name__ == '__main__':
    main()