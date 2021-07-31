import pandas as pd
import time
from datetime import timedelta, datetime



def to_string(date_time):
    '''
    Returns a datetime object converted to string
    valid for InluxClient. 
    '''
    return date_time.strftime("%Y-%m-%d-%H:%M")


def get_time_range(time: pd.Timestamp, interval: int):
    '''
    Returns a tuple of two pd.Timestamps (start, end)
    which denote the time range to feed to Influx.
    Args:
        time: pd.Timestamp when the new record was introduced
        interval: interval of the observations in minutes
    '''
    return time - timedelta(minutes=interval), time 