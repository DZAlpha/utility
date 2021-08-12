import influxdb_client
import pandas as pd
from influxdb_client.client.write_api import SYNCHRONOUS


def args_val(def_params, required_params, **params):
    '''
    Fills parameteres dictionary with predefined default values if they were not provided
    Also checks if all required parameters are present
    input:
        def_params: dict
        required_params: list
        **params: dict
    output:
        params: dict
    '''
    for key in def_params:
        if key not in params:
            params[key] = def_params[key]
            
    missing_params = list(set(required_params) - set(params))
    if len(missing_params) > 0:
        raise ValueError('Missing params: {}'.format(missing_params))
    return params


class InfluxClient:
    '''
    Client that provides API for reading and writing to an Influx database. 
    '''
    def __init__(
        self, 
        url,
        bucket, 
        org, 
        token
    ):
        self.bucket=bucket
        self.org=org
        self.token=token
        self.url=url

        self.client=influxdb_client.InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org
        )

        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        
    def save(self, currency, metric, time, **kwargs):
        p = influxdb_client.Point(currency).tag("metric", metric).time(time) #time must be pd.to_datetime(xxx, unit='ms')
        for field in kwargs:
            p = p.field(field, kwargs[field])
            self.write_api.write(self.bucket, self.org, record=p)
            
    
    @staticmethod
    def parse_query(**query_params):
        '''
        returns query constructed from given parameters 
        input:
            query_params: dict with parameters
                expected values: 
                    metric: string
                    from_time: string of format yyyy-mm-dd-hh:mm
                    to_time: string of format yyyy-mm-dd-hh:mm
        output:
            parsed_query: string
        '''
        def_params = {'bucket': 'my-bucket', 'currency': 'BTCUSDT'}
        required_params = ['metric', 'from_time', 'to_time']
        params = args_val(def_params, required_params, **query_params)
        
        from_time = pd.Timestamp(params['from_time']).isoformat()
        to_time = (pd.Timestamp(params['to_time']) + pd.Timedelta(seconds=1)).isoformat()
        
        parsed_query = 'from (bucket:"{}")'.format(params['bucket'])\
                + '|> range(start: {}Z, stop: {}Z)'.format(from_time, to_time)\
                + '|> filter(fn: (r) => r["_measurement"] == "{}")'.format(params['currency'])\
                + '|> filter(fn: (r) => r["metric"] == "{}")'.format(params['metric'])
        
        if 'fields' in params:
            fields_filter =""
            for field in params['fields']:
                if len(fields_filter) > 0:
                    fields_filter += " or "
                fields_filter += 'r["_field"] == "{}"'.format(field)
            parsed_query += '|> filter(fn: (r) => {})'.format(fields_filter)
        return parsed_query
    
    def execute_query(self, query):
        return self.query_api.query_data_frame(org=self.org, query=query)


