"""
Script to interpolate sensor files automatically. Can handle large gaps in data samples (set by gap_threshold, default is 1 second)

Usage:
    Production:
        Whole dataset:
            `mh -r . process --verbose --par --pattern SPADES_*/MasterSynced/**/*.sensor.csv SensorResampler --new_sr 80`
        Single participant:
            `mh -r . -p SPADES_1 process --par --pattern MasterSynced/**/*.sensor.csv SensorResampler --new_sr 80`
    Debug: 
        `mh -r . -p SPADES_1 process --verbose --pattern MasterSynced/**/*.sensor.csv SensorResampler --new_sr 80`
"""

import os
import pandas as pd
from ..api.interpolate import interpolate
from ..api import utils as mu
from .BaseProcessor import SensorProcessor

def build(**kwargs):
    return SensorResampler(**kwargs).run_on_file

class SensorResampler(SensorProcessor):
    def __init__(self, verbose=True, independent=False, new_sr=None, gap_threshold=1, setname='Resampled'):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = 'SensorResampler'
        self.gap_threshold = gap_threshold
        self.new_sr = new_sr
        self.setname = setname

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        if self.new_sr is None:
            if self.verbose:
                print("Warning: sampling rate is not set, interpolation will be skipped and original data will be saved")
            mask = (combined_data.iloc[:,0] >= data_start_indicator) & (combined_data.iloc[:,0] <= data_stop_indicator)
            result_data = combined_data.loc[mask,:]
        else:
            prev_mask = combined_data.iloc[:,0] < data_start_indicator
            prev_data = combined_data.loc[prev_mask,:]
            next_mask = combined_data.iloc[:,0] > data_stop_indicator
            next_data = combined_data.loc[next_mask,:]
            mask = (combined_data.iloc[:,0] >= data_start_indicator) & (combined_data.iloc[:,0] <= data_stop_indicator)
            data = combined_data.loc[mask,:]
            result_data = interpolate(data, verbose=self.verbose, prev_df=prev_data, next_df=next_data, sr=self.new_sr, start_time=data_start_indicator, stop_time=data_stop_indicator, gap_threshold=self.gap_threshold)
        return result_data

    def _post_process(self, result_data):
        output_file = mu.generate_output_filepath(self.file, self.setname, 'sensor')
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        result_data.to_csv(output_file, index=False, float_format='%.3f')
        if self.verbose:
            print('Saved interpolated data to ' + output_file)
        return pd.DataFrame()