"""
  script to extract session times from annotation files each hour
  Usage:
    mh -r . process SessionExtractor --par --pattern SPADES_*/MasterSynced/**/*.annotation.csv --indepedent 1 > DerivedCrossParticipants/sessions.csv
"""

import os
import pandas as pd
from .BaseProcessor import AnnotationProcessor

def build(**kwargs):
  return SessionExtractor(**kwargs).run_on_file

class SessionExtractor(AnnotationProcessor):
  def __init__(self, verbose=True, independent=True):
    AnnotationProcessor.__init__(self, verbose=verbose, independent=independent)
    self.name = 'SessionExtractor'

  def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
    st = combined_data.iloc[0, 1]
    et = combined_data.iloc[combined_data.shape[0] - 1, 2]
    result_data = pd.DataFrame(data={'START_TIME': st, 'STOP_TIME': et}, index=[0])
    return result_data

  def _post_process(self, result_data):
    result_data['pid'] = self.meta['pid']
    result_data['date'] = self.meta['date']
    result_data['hour'] = self.meta['hour']
    result_data['annotator'] = self.meta['sid']
    return result_data