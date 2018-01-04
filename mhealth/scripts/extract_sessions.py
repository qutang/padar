"""
  script to extract session times from annotation files each hour
"""

import os
import pandas as pd
import mhealth.api as mh

def main(file, verbose=False, **kwargs):
  file = os.path.abspath(file)
  df = pd.read_csv(file, parse_dates=[0, 1, 2], infer_datetime_format=True)
  result = run_extract_sessions(df, verbose=verbose, **kwargs)
  result['pid'] = mh.extract_pid(file)
  result['date'] = mh.extract_date(file)
  result['hour'] = mh.extract_hour(file)
  return result

def run_extract_sessions(df, verbose=False, **kwargs):
  st = df.iloc[0, 1]
  et = df.iloc[df.shape[0] - 1, 2]
  result = pd.DataFrame(data={'START_TIME': st, 'STOP_TIME': et}, index=[0])
  return result