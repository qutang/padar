"""
Script to filter sensor files

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
import mhealth.api.filter as mf 
import mhealth.api.utils as mu
from .BaseProcessor import SensorProcessor

def build(**kwargs):
    return SensorFilter(**kwargs).run_on_file

class SensorFilter(SensorProcessor):
    def __init__(self, verbose=True, independent=False, order=4, ftype='butter', btype='lowpass', cutoffs=None, setname='Filtered'):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = 'SensorFilter'
        self.ftype = ftype
        self.btype = btype
        self.setname = setname
        self.order = order
        self.cutoffs = cutoffs

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        ftype = self.ftype
        sr = mu._sampling_rate(combined_data)
        if self.cutoffs is None:
            if self.verbose:
                print("Cut off is not set, return the original data")
            mask = (combined_data.iloc[:,0] >= data_start_indicator) & (combined_data.iloc[0,1] <= data_stop_indicator)
            result_data = combined_data.loc[mask,:]
        if ftype == 'butter':
            result_data = mf.butterworth(combined_data, sr, self.cutoffs, self.order, self.dtype)
            mask = (result_data.iloc[:,0] >= data_start_indicator) & (result_data.iloc[0,1] <= data_stop_indicator)
            result_data = result_data.loc[mask,:]
        return result_data

    def _post_process(self, result_data):
        output_file = mu.generate_output_filepath(self.file, self.setname, 'sensor')
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        result_data.to_csv(output_file, index=False, float_format='%.3f')
        if self.verbose:
            print('Saved filtered data to ' + output_file)
        return pd.DataFrame()