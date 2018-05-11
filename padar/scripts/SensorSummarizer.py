"""
Script to apply different numerical transformation to raw sensor data
"""

import os
import pandas as pd
from ..api import filter as mf 
from ..api import utils as mu
from ..api.helpers import summarizer
from .BaseProcessor import SensorProcessor

def build(**kwargs):
    return SensorSummarizer(**kwargs).run_on_file

class SensorSummarizer(SensorProcessor):
    def __init__(self, verbose=True, independent=True, violate=False, method='enmo', window_size=5, location_mapping_file=None, setname='Summarization'):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
        self.name = 'SensorSummarizer' + "_" + method
        self.setname = setname
        self.method = method
        self.window_size = window_size
        self.location_mapping_file = location_mapping_file

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        if self.method == 'enmo':
            result_data = summarizer.summarize_sensor(combined_data, method=self.method, window=self.window_size)
        return result_data

    def _post_process(self, result_data):
        output_file = mu.generate_output_filepath(self.file, self.setname, 'feature', self.method)
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        result_data.to_csv(output_file, index=False, float_format='%.3f')
        if self.verbose:
            print('Saved summarization data to ' + output_file)
        result_data['pid'] = self.meta['pid']
        result_data['sid'] = self.meta['sid']
        result_data['location'] = mu.get_location_from_sid(self.meta['pid'], self.meta['sid'], self.location_mapping_file)
        return result_data