from google.cloud import bigquery
import pandas as pd
import subprocess
import urllib3
import json
import re

class SourceError(Exception):
    """ An exception class for Source """
    pass

class Source(object):
    """ A class to pull data from any custom database API via HTTPS requests and upload to BigQuery """
    def __init__(
        self,
        url: str='',
        method: str='GET',
        headers: str=None,
        project_id: str=None,
        dataset_id: str=None,
        table_id: str=None,
        location: str=None,
        temp_table: str='false', # true/false
        api_key: str=None,
        url_type: str='dict_based', # dict_based/equal_based
        timestamp_column: str='stamp',
        date_columns: str=None,
        filter_params: str=None,
        write_disposition: str=None,
        time_partitioning: str=None,
        paginations: str='false',
        topic: str='topic1',
    ):
        if not headers:
            headers_dict = None
        elif isinstance(headers, str):
            headers_dict = json.loads(headers)
        else:
            raise SourceError('Provided headers is not JSON serializable.')
        self.headers = headers_dict

        if isinstance(date_columns, str):
            date_columns_list = json.loads(date_columns)
        else:
            raise SourceError('Provided date_columns is not JSON serializable.')
        self.date_columns = date_columns_list

        if isinstance(filter_params, str):
            filter_params_dict = json.loads(filter_params)
        else:
            raise SourceError('Provided filter_params is not JSON serializable.')
        self.filter_params = filter_params_dict

        if isinstance(time_partitioning, str):
            time_partitioning_dict = json.loads(time_partitioning)
        else:
            raise SourceError('Provided time_partitioning is not JSON serializable.')
        self.time_partitioning = time_partitioning_dict

        self.method = method
        self.timestamp_column = timestamp_column
        self.write_disposition = write_disposition
        self.url_type = url_type
        self.paginations = paginations
        self.topic = topic
        self.data = []

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.location = location
        self.temp_table = temp_table

        if api_key == 'no_api_key_required':
            self.api_key = ''
        elif api_key == 'no_api_key_provided':
            raise SourceError('No API key is provided.')
        else:
            self.api_key = api_key

        self.check_essential_resources()
        self.upload_flag = True

        self.url = self.apply_filter(url)

        if self.method == 'GET':
            self.req_params = {
                'method': self.method,
                'url': self.url,
                'headers': self.headers
            }
        elif self.method == 'POST':
            self.req_params = {
                'method': self.method,
                'url': self.url,
                'body': body,
                'headers': self.headers
            }
        else:
            raise SourceError('Invalid HTTPS method is provided.')

    def check_essential_resources(self):
        """ Check if the outbound dataset is exist, if not exist then create one """
        client = bigquery.Client(
            project=self.project_id,
            location=self.location,
        )
        datasets = [i.dataset_id for i in client.list_datasets()]
        full_dataset_id = f"{self.project_id}.{self.dataset_id}"

        # if dataset not exist, then create new one.
        if self.dataset_id not in datasets:
            print(f"Dataset '{self.dataset_id}' is not exists, creating a new one..")
            dataset = bigquery.Dataset(full_dataset_id)
            dataset.location = self.location
            dataset = client.create_dataset(dataset, timeout=30)
            print("Created dataset, dataset_id :", full_dataset_id)
        else:
            print("Dataset exists, dataset_id :", full_dataset_id)

    def get_latest_timestamp(self):
        """ Retrieve the latest timestamp if table exist """
        client = bigquery.Client(
            project=self.project_id,
            location=self.location,
        )
        # check if the table exist or not
        tables = [i.table_id for i in client.list_tables(self.dataset_id)]

        # if table not exist, means no latest timestamp (cold start)
        self.latest_timestamp = ''
        if not self.table_id in tables:
            print(f"'{self.table_id}' table is not exist yet")
        else:
            print("Table exists, table_id :", self.table_id)
            # get latest timestamp from bq table itself
            query = f"SELECT MAX({self.timestamp_column}) AS max FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`"
            bq_job = client.query(query)
            bq_job.result()
            max_df = bq_job.to_dataframe()
            
            if len(max_df) > 0:
                _latest_timestamp = max_df.loc[0,'max']
                if isinstance(_latest_timestamp, pd._libs.tslibs.timestamps.Timestamp):
                    max_df['max'] = pd.to_datetime(max_df['max']).astype(int)/ 10**9
                    _latest_timestamp = int(max_df.loc[0,'max'])
                self.latest_timestamp = _latest_timestamp

        print(f'Latest timestamp : {self.latest_timestamp}')


    def apply_filter(self, url):
        """ Apply filter to URL as required """

        # dict-based url
        if self.url_type == 'dict_based':
            def clean_filter(f):
                f = json.dumps(f)
                f = re.sub(r'(?<!: )"(\S*?)"', '\\1', f).replace(' ','').replace('"', '%22')
                return f
            
            self._filter = clean_filter(self.filter_params)
            if self._filter != '{}':
                url += '?'+ self._filter
            return url

        # equal-based url
        elif self.url_type == 'equal_based':
            if self.topic == 'topic1':
                self.filter_params.update({'k': self.api_key})
            url += '?'+ '&'.join([f'{key}={val}' for (key, val) in self.filter_params.items()])
            return url
        else:
            return url

    def _requestor(
        self,
        req_params
    ):
        http = urllib3.PoolManager()

        self.request = http.request(**req_params)
        self.response = json.loads(self.request.data.decode('utf-8'))

        if self.response:
            if isinstance(self.response, dict):
                targets = ['results', 'response']
                for target in targets:
                    if target in self.response:
                        data = self.response.get(target, {})
                        break
            elif isinstance(self.response, list):
                data = self.response

            if len(data) > 0 :
                self.data.extend(data)

    def make_requests(self):
        """ Make the HTTPS request """
        try:
            print(f'Performing {self.method} request...')

            if self.paginations == 'true':
                count, check = 1, True
                while check:
                    req_params = self.req_params.copy()
                    req_params['url'] = req_params['url'].replace('PAGE_NUMBER', str(count))
                    self._requestor(req_params=req_params)
                    if not self.response:
                        check = False
                    else:
                        print(f"Pulled data from : {req_params['url']}")
                        count += 1
            elif self.paginations == 'false':
                self._requestor(req_params=self.req_params)
            else:
                raise SourceError('Invalid paginations is provided.')

            if len(self.data) > 0 :
                self.transform()
            else:
                print(f'No new records to pull.')
                self.upload_flag = False
        except Exception as err:
            raise SourceError(err)

    def transform(self):
        """ Apply data transformation to URL as required """
        df = pd.DataFrame(self.data)

        try:
            df = df.drop_duplicates(keep='last')
        except TypeError as err:
            pass

        if self.topic == 'topic1':
            self.get_latest_timestamp()
            if self.latest_timestamp:
                df = df[df[self.timestamp_column]>self.latest_timestamp]

        if self.date_columns:
            print(f'date_columns : {self.date_columns}')
            print('Transforming date columns properly...')
            for col in self.date_columns:
                df[col]= pd.to_datetime(df[col])
                df[col] = df[col].astype(str)
        else:
            print('No date_columns provided for cleanup.')

        self.data = json.loads(df.to_json(orient='records'))

    def upload_to_bq(self):
        """ Upload processed data to BigQuery """
        try:
            # Upload to BQ
            if not self.upload_flag:
                print(f'No new records to upload.')
                return

            if self.temp_table == 'true':
                self.dataset_id = 'TEMP'
                table_name = f'{self.table_id}_STG'
                print(f'Saving to temporary BQ path... : {self.project_id}.{self.dataset_id}.{table_name}')
            elif self.temp_table == 'false':
                table_name = self.table_id
            else:
                raise SourceError('Invalid temp_table is provided.')

            client = bigquery.Client(
                project=self.project_id,
                location=self.location,
            )

            table = f'{self.project_id}.{self.dataset_id}.{table_name}'

            with open(f'schema/{self.topic}.json', 'r') as openfile:
                schema_obj = json.load(openfile)

            job_config_params ={
                'schema' : [
                    bigquery.SchemaField( # non-nested field
                        name=i.get('name',''),
                        field_type=i.get('field_type',''),
                        mode=i.get('mode',''),
                        fields=()
                    ) if 'fields' not in i else \
                    bigquery.SchemaField( # nested field
                        name=i.get('name',''),
                        field_type=i.get('field_type',''),
                        mode=i.get('mode',''),
                        fields=[bigquery.SchemaField(**field_details) for field_details in i.get('fields','')],
                    ) \
                    for i in schema_obj
                ],
                'create_disposition': bigquery.CreateDisposition.CREATE_IF_NEEDED
            }

            if self.write_disposition == 'WRITE_TRUNCATE':
                _write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            elif self.write_disposition == 'WRITE_APPEND':
                _write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:
                raise SourceError('Invalid WriteDisposition method is provided.')

            job_config_params['write_disposition'] = _write_disposition

            if self.time_partitioning:
                _time_partitioning = bigquery.table.TimePartitioning(**self.time_partitioning)
                job_config_params['time_partitioning'] = _time_partitioning

            job_config = bigquery.LoadJobConfig(**job_config_params)

            load_job = client.load_table_from_json(
                json_rows=self.data,
                destination=table,
                project=self.project_id,
                location=self.location,
                job_config=job_config,
            ) # Make an API request.

            load_job.result() # Waits for the job to complete.
            print(f'{len(self.data)} new records are uploaded to {table}.')

        except Exception as err:
            raise SourceError(err)