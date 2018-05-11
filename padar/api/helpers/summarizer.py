import numpy as np
import pandas as pd
from ..numeric_feature import enmo
def summarize_annotation(df):
    by_groups = []
    sort_by = []
    if 'pid' in df.columns:
        by_groups.append('pid')
        sort_by.append('pid')
    if 'annotator' in df.columns:
        by_groups.append('annotator')
        sort_by.append('annotator')
    by_groups.append(df.columns[3])
    result = df.groupby(by_groups).apply(lambda row: np.sum(row.iloc[:,2] - row.iloc[:,1]))
    result = result.reset_index()
    if len(sort_by) > 0:
        result = result.sort_values(by=sort_by)
    result = result.rename(columns={0: 'DURATION_IN_SECONDS'})
    result['DURATION_IN_SECONDS'] = result['DURATION_IN_SECONDS']/ np.timedelta64(1, 's')
    return result

def summarize_sensor(df, method='enmo', window=5):
    by_groups = []
    if 'pid' in df.columns:
        by_groups.append('pid')
    if 'sid' in df.columns:
        by_groups.append('sid')
    if 'location' in df.columns:
        by_groups.append('location')
    result = df.groupby(by=[pd.Grouper(key=df.columns[0], freq=str(window) + 's')] + by_groups).apply(lambda row: enmo(row.iloc[:,1:4].values))
    result = result.reset_index()
    if len(by_groups) > 0:
        result = result.sort_values(by=by_groups)
    result = result.rename(columns={0: method})
    result = result[[result.columns[0], method] + by_groups]
    return result
    
