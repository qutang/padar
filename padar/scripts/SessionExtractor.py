"""
  Script to extract session times from annotation or sensor files each hour. Use it before `FeatureSetPreparer`.

  If your dataset has annotation files, use annotation files to extract sessions. Otherwise, use sensor files to extract sessions.

  Usage:
    pad -p <PID> -r <root> process -p <PATTERN> --par -o <OUTPUT_FILEPATH> SessionExtractor

    process options:
      --output, -o <filepath>: The output filepath (relative to participant's folder or root folder) where the script will save the extracted sessions information from one or more participants to.

  Examples:

    1. Extract sessions information from annotation files for participant SPADES_1 and save the sessions information to 'sessions.csv' in the 'Derived' folder of SPADES_1

      pad -p SPADES_1 process SessionExtractor --par -p MasterSynced/**/*.annotation.csv -o Derived/sessions.csv

    2. Extract sessions information from Actigraph sensor files for participant SPADES_1 and save the sessions information to 'sessions.csv' in the 'Derived' folder of SPADES_1

      pad -p SPADES_1 process SessionExtractor --par -p MasterSynced/**/Actigraph*.sensor.csv -o Derived/sessions.csv

    3. Extract sessions information from annotation files for all participants and save the sessions information to 'sessions.csv' in the 'DerivedCrossParticipants' folder of the whole dataset

      pad process SessionExtractor --par -p SPADES_*/MasterSynced/**/*.annotation.csv -o DerivedCrossParticipants/sessions.csv

    2. Extract sessions information from Actigraph sensor files for all participants and save the sessions information to 'sessions.csv' in the 'DerivedCrossParticipants' folder of the whole dataset

      pad process SessionExtractor --par -p SPADES_*/MasterSynced/**/Actigraph*.sensor.csv -o Derived/sessions.csv
"""

import os
import pandas as pd
import numpy as np
from .BaseProcessor import AnnotationProcessor, SensorProcessor, Processor

def build(**kwargs):
  return SessionExtractor(**kwargs).run_on_file

class SessionExtractor(Processor):
  def __init__(self, verbose=True, independent=True, violate=False):
    Processor.__init__(self, verbose=verbose, independent=independent, violate=violate)
    self.sensorProcessor = SensorProcessor(verbose=verbose, independent=independent)
    self.annotationProcessor = AnnotationProcessor(verbose=verbose, independent=independent)
    self.name = 'SessionExtractor'

  def _load_file(self, file, prev_file=None, next_file=None):
    if self.meta['file_type'] == 'sensor':
      return self.sensorProcessor._load_file(file, prev_file=prev_file, next_file=next_file)
    elif self.meta['file_type'] == 'annotation':
      return self.annotationProcessor._load_file(file, prev_file=prev_file, next_file=next_file)

  def _merge_data(self, data, prev_data=None, next_data=None):
    if self.meta['file_type'] == 'sensor':
      return self.sensorProcessor._merge_data(data, prev_data=prev_data, next_data=next_data)
    elif self.meta['file_type'] == 'annotation':
      return self.annotationProcessor._merge_data(data, prev_data=prev_data, next_data=next_data)

  def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
    if self.meta['file_type'] == 'sensor':
      st_col = 0
      et_col = 0
    elif self.meta['file_type'] == 'annotation':
      st_col = 1
      et_col = 2
    st = combined_data.iloc[0, st_col].to_datetime64()
    et = combined_data.iloc[combined_data.shape[0] - 1, et_col].to_datetime64()
    st = st.astype('datetime64[s]')
    et = et.astype('datetime64[s]') + np.timedelta64(1, unit='s')
    result_data = pd.DataFrame(data={'START_TIME': st, 'STOP_TIME': et}, index=[0])
    return result_data

  def _post_process(self, result_data):
    result_data['pid'] = self.meta['pid']
    result_data['date'] = self.meta['date']
    result_data['hour'] = self.meta['hour']
    result_data['annotator'] = self.meta['sid']
    return result_data