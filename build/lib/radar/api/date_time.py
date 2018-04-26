from datetime import datetime
import numpy as np
import pandas as pd
TIMESTAMP_FORMAT_WITH_MILLISECONDS = "%Y-%m-%d %H:%M:%S.%f"
TIMESTAMP_FORMAT_ACTIGRAPH = "%Y/%m/%d %H:%M:%S.%f"
TIMESTAMP_FORMAT_WITHOUT_MILLISECONDS = "%Y-%m-%d %H:%M:%S"

def str_to_datetime_mhealth(stri):
    l = len(stri)
    if(l == 23):
        us = int(stri[20:23]) * 1000
        result = datetime(
                int(stri[0:4]),
                int(stri[5:7]),
                int(stri[8:10]),
                int(stri[11:13]),
                int(stri[14:16]),
                int(stri[17:19]),
                us
                )
    elif(l == 19):
        result = datetime(
                int(stri[0:4]),
                int(stri[5:7]),
                int(stri[8:10]),
                int(stri[11:13]),
                int(stri[14:16]),
                int(stri[17:19]),
                0
                )
    else:
        raise ValueError("Unrecognized mhealth datetime format")
    
    return result

def str_to_datetime_actigraph(str):
    return datetime.strptime(str, TIMESTAMP_FORMAT_ACTIGRAPH)

def str_to_datetime_no_milliseconds(str):
    return datetime.strptime(str, TIMESTAMP_FORMAT_WITHOUT_MILLISECONDS)

def datetime_to_str_with_milliseconds(dt):
    return dt.strftime(TIMESTAMP_FORMAT_WITH_MILLISECONDS)[:-3]

def datetime64_to_str_with_milliseconds(dt):
    ts = pd.to_datetime(dt)
    return datetime_to_str_with_milliseconds(ts)

def datetime64_to_seconds(dt):
    '''
		naive datetime conversion
		Naive means no time zone is specified, the unix origin has no time zone specified as well
	'''
    return (dt - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's')

def datetime64_to_milliseconds(dt):
    return datetime64_to_seconds(dt) * 1000

def datetime_to_seconds(dt):
    '''
		naive datetime conversion
		Naive means no time zone is specified, the unix origin has no time zone specified as well
	'''
    if type(dt) is datetime:
        dt = np.datetime64(dt)
    elif isinstance(dt, list):
        dt = np.array(dt, dtype='datetime64[s]')
    return datetime64_to_seconds(dt)

def datetime_to_milliseconds(dt):
    return datetime_to_seconds(dt) * 1000

def seconds_to_datetime64(ts):
    ts_indices = pd.to_datetime(ts, unit='s')
    return ts_indices.values

def milliseconds_to_datetime64(ts):
    ts_indices = pd.to_datetime(ts, unit='ms')
    return ts_indices.values

def current_timestamp_str(format='%Y%m%d%H%M%S'):
    return datetime.now().strftime(format)