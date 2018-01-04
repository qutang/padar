"""
  preprocess pipeline for multilocation paper 2017
  [Insert citation]
"""

import os
import pandas as pd
import mhealth.scripts as scripts
import mhealth.api as mh

def main(file, verbose=True, static_chunk_file=None, session_file=None, **kwargs):
  file = os.path.abspath(file)
  df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)

  pid = mh.extract_pid(file)
  sid = mh.extract_id(file)
  date = mh.extract_date(file)
  hour = mh.extract_hour(file)

  pipeline = list()

  pipeline.append({
    'name': 'calibration',
    'func': scripts.calibrate_accel.run_calibrate_accel,
    'kwargs': {
      'static_chunk_file': static_chunk_file,
      'pid': pid,
      'sid': sid
    }
  })

  pipeline.append({
    'name': 'clip',
    'func': scripts.clipper.run_clipper,
    'kwargs': {
      'session_file': session_file,
      'pid': pid
    }
  })

  result = df.copy(deep=True)
  for pipe in pipeline:
    
    print('Execute ' + pipe['name'] + " on file: " + file)
    print(result.shape)
    func = pipe['func']
    kwargs = pipe['kwargs']
    result = func(result, verbose=verbose, **kwargs)
    print(result.shape)

  # save to individual file
  output_file = file.replace('MasterSynced', 'Derived/preprocessed')
  if not os.path.exists(os.path.dirname(output_file)):
    os.makedirs(os.path.dirname(output_file))
  result.to_csv(output_file, index=False, float_format='%.3f')
  if verbose:
    print('Saved preprocessed data to ' + output_file)
  
  # we don't need to concatenate results, so return an empty dataframe
  return pd.DataFrame()