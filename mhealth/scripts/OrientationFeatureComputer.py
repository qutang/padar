"""
Script to compute features used for posture and activity recognition in multilocation paper. 

features:
    x,y,z median angle
    x,y,z angle range

Usage:
    Production: 
        On all participants
            `mh -r . process multilocation_2017.compute_features --par --pattern MasterSynced/**/Actigraph*.sensor.csv > DerivedCrossParticipants/multilocation_2017.feature.csv`
        On single participant
            `mh -r . -p SPADES_1 process multilocation_2017.compute_features --par --pattern MasterSynced/**/Actigraph*.sensor.csv > SPADES_1/Derived/multilocation_2017.feature.csv`

    Debug:
        `mh -r . -p SPADES_1 process multilocation_2017.compute_features --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv`
"""

import os
import pandas as pd
import numpy as np
import mhealth.api.numeric_feature as mnf
import mhealth.api.windowing as mw
import mhealth.api.utils as mu
from ..BaseProcessor import SensorProcessor

def build(**kwargs):
	return OrientationFeatureComputer(**kwargs).run_on_file

class OrientationFeatureComputer(SensorProcessor):
    def __init__(self, verbose=True, independent=False, setname='Feature', session_file=None, ws=12800, ss=12800, threshold=0.2, subwins=4):
        SensorProcessor.__init__(verbose=verbose, independent=independent)
        self.name = 'OrientationFeatureComputer'
        self.setname = setname
        self.session_file = session_file
        self.ws = ws
        self.ss = ss
        self.threshold = 0.2
        self.subwins = 4
    
    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        st, et = mu.get_st_et(combined_data, self.meta['pid'], self.session_file, st_col=0, et_col=0)
		ws = self.ws
		ss = self.ss
		threshold = self.threshold
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
        chunk_windows_mask = (windows[:,0] >= data_start_indicator) & (windows[:,0] <= data_stop_indicator)
        chunk_windows = windows[chunk_windows_mask,:]

        result_data = mw.apply_to_sliding_windows(df=combined_data, sliding_windows=chunk_windows, window_operations=features, operation_names=feature_names, return_dataframe=True)
        return result_data

	def _post_process(self, result_data):
		output_path = mu.generate_output_filepath(self.file, self.setname, 'feature', 'Orientation')
		if not os.path.exists(os.path.dirname(output_path)):
			os.makedirs(os.path.dirname(output_path))
			
		result_data.to_csv(output_path, index=False, float_format='%.6f')
		if self.verbose:
			print('Saved feature data to ' + output_path)
		result_data['pid'] = pid
		result_data['sid'] = sid
		return result_data