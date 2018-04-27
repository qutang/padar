"""
Script to compute features used for posture and activity recognition in multilocation paper. 

features:
    x,y,z median angle
    x,y,z angle range

Usage:
    Production: 
        On all participants
            `mh -r . process OrientationFeatureComputer --par --pattern MasterSynced/**/Actigraph*.sensor.csv > DerivedCrossParticipants/Orientation.feature.csv`
        On single participant
            `mh -r . -p SPADES_1 process OrientationFeatureComputer --par --pattern MasterSynced/**/Actigraph*.sensor.csv > SPADES_1/Derived/Orientation.feature.csv`

    Debug:
        `mh -r . -p SPADES_1 process OrientationFeatureComputer --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv --setname test_orientationfeatures`
"""

import os
import pandas as pd
import numpy as np
from ..api import numeric_feature as mnf
from ..api import windowing as mw
from ..api import utils as mu
from .BaseProcessor import SensorProcessor

def build(**kwargs):
    return OrientationFeatureComputer(**kwargs).run_on_file

class OrientationFeatureComputer(SensorProcessor):
    def __init__(self, verbose=True, independent=False, setname='Feature', session_file='DerivedCrossParticipants/sessions.csv', ws=12800, ss=12800, subwins=4):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = 'OrientationFeatureComputer'
        self.setname = setname
        self.session_file = session_file
        self.ws = ws
        self.ss = ss
        self.subwins = 4
    
    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        st, et = mu.get_st_et(combined_data, self.meta['pid'], self.session_file, st_col=0, et_col=0)
        ws = self.ws
        ss = self.ss
        subwins = self.subwins

        if self.verbose:
            print('Session start time: ' + str(st))
            print('Session stop time: ' + str(et))

        sr = mu._sampling_rate(combined_data)

        features = [
            lambda x: mnf.accelerometer_orientation_features(x, subwins=subwins)
        ]

        feature_names = [
            "MEDIAN_X_ANGLE",
            "MEDIAN_Y_ANGLE",
            "MEDIAN_Z_ANGLE",
            "RANGE_X_ANGLE",
            "RANGE_Y_ANGLE",
            "RANGE_Z_ANGLE"
        ]

        windows = mw.get_sliding_window_boundaries(start_time=st, stop_time=et, window_duration=ws, step_size=ss)
        chunk_windows_mask = (windows[:,0] >= data_start_indicator) & (windows[:,0] < data_stop_indicator)
        chunk_windows = windows[chunk_windows_mask,:]
        if len(chunk_windows) == 0:
            return pd.DataFrame()
        result_data = mw.apply_to_sliding_windows(df=combined_data, sliding_windows=chunk_windows, window_operations=features, operation_names=feature_names, return_dataframe=True)
        return result_data

    def _post_process(self, result_data):
        output_path = mu.generate_output_filepath(self.file, self.setname, 'feature', 'Orientation')
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
            
        result_data.to_csv(output_path, index=False, float_format='%.6f')
        if self.verbose:
            print('Saved feature data to ' + output_path)
        result_data['pid'] = self.meta['pid']
        result_data['sid'] = self.meta['sid']
        return result_data