"""
Script to compute features (based on VM and orientation) used for posture and activity recognition in multilocation paper. 

VM features:
    preprocess:
        20Hz butterworth lowpass
    features:
        "MEAN"
        'STD'
        'MAX'
        'DOM_FREQ'
        'DOM_FREQ_POWER_RATIO'
        'HIGHEND_FREQ_POWER_RATIO'
        'RANGE'
        'ACTIVE_SAMPLE_PERC'
        'NUMBER_OF_ACTIVATIONS'
        'ACTIVATION_INTERVAL_VAR'

Orientation features:
    preprocess:
        20Hz butterworth lowpass
        manual orientation fix
    features:
        x,y,z median angle
        x,y,z angle range
Usage:
    Production: 
        On all participants
            `mh -r . process multilocation_2017.FeatureSetPreparer --par --pattern MasterSynced/**/Actigraph*.sensor.csv > DerivedCrossParticipants/multilocation_posture_and_activity.feature.csv`
        On single participant
            `mh -r . -p SPADES_1 process multilocation_2017.FeatureSetPreparer --par --pattern MasterSynced/**/Actigraph*.sensor.csv > SPADES_1/Derived/multilocation_posture_and_activity.feature.csv`

    Debug:
        `mh -r . -p SPADES_1 process multilocation_2017.FeatureSetPreparer --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv --setname test_featureset`
"""

import os
import pandas as pd
import numpy as np
import scipy.signal as signal
import mhealth.api.numeric_feature as mnf
import mhealth.api.numeric_transformation as mnt
import mhealth.api.filter as mf
import mhealth.api.windowing as mw
import mhealth.api.utils as mu
from ..BaseProcessor import SensorProcessor
from ..ManualOrientationNormalizer import ManualOrientationNormalizer
from ..SensorFilter import SensorFilter
from ..TimeFreqFeatureComputer import TimeFreqFeatureComputer
from ..OrientationFeatureComputer import OrientationFeatureComputer

def build(**kwargs):
    return FeatureSetPreparer(**kwargs).run_on_file

class FeatureSetPreparer(SensorProcessor):
    def __init__(self, verbose=True, independent=False, setname='Feature', session_file="DerivedCrossParticipants/sessions.csv", ws=12800, ss=12800, threshold=0.2, subwins=4):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = 'AccelerometerFeatureComputer'
        self.setname = setname
        self.session_file = session_file
        self.subwins = subwins
        self.sensorFilter = SensorFilter(verbose=verbose, independent=independent, order=4, low_cutoff=None, high_cutoff=20)
        self.manualOrientationNormalizer = ManualOrientationNormalizer(verbose=verbose, independent=independent, orientation_fix_file='DerivedCrossParticipants/orientation_fix_map.csv')
        self.timeFreqFeatureComputer = TimeFreqFeatureComputer(verbose=verbose, independent=independent, session_file=session_file, ws=ws, ss=ss, threshold=threshold)
        self.orientationFeatureComputer = OrientationFeatureComputer(verbose=verbose, independent=independent, session_file=session_file, ws=ws, ss=ss, subwins=subwins)
    
    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        self.sensorFilter.set_meta(self.meta)
        self.manualOrientationNormalizer.set_meta(self.meta)
        self.timeFreqFeatureComputer.set_meta(self.meta)
        self.orientationFeatureComputer.set_meta(self.meta)
        st, et = mu.get_st_et(combined_data, self.meta['pid'], self.session_file, st_col=0, et_col=0)
        if self.verbose:
            print('Session start time: ' + str(st))
            print('Session stop time: ' + str(et))

        sr = mu._sampling_rate(combined_data)

        # 20 Hz lowpass filter on vector magnitude data and original data
        vm_data = mnt.vector_magnitude(combined_data.values[:,1:4]).ravel()
        vm_data = pd.DataFrame(vm_data, columns=['VM'])
        vm_data.insert(0, 'HEADER_TIME_STAMP', combined_data.iloc[:, 0].values)
        vm_data_filtered = self.sensorFilter._run_on_data(vm_data, data_start_indicator, data_stop_indicator)
        combined_data_filtered = self.sensorFilter._run_on_data(combined_data, data_start_indicator, data_stop_indicator)
        # b, a = signal.butter(4, lowpass_cutoff/sr, 'low')
        # vm_data_filtered = signal.filtfilt(b, a, vm_data)
        # combined_data_filtered = signal.filtfilt(b, a, combined_df.values[:,1:4], axis=0)
        # vm_df = pd.DataFrame(data={"HEADER_TIME_STAMP": combined_df.iloc[:,0].values, "VM": vm_data_filtered})
        # combined_df.values[:,1:4] = combined_data_filtered

        # manual fix orientation
        combined_data_prepared = self.manualOrientationNormalizer._run_on_data(combined_data_filtered, data_start_indicator, data_stop_indicator)

        timefreq_feature_df = self.timeFreqFeatureComputer._run_on_data(vm_data_filtered, data_start_indicator, data_stop_indicator)
        orientation_feature_df = self.orientationFeatureComputer._run_on_data(combined_data_prepared, data_start_indicator, data_stop_indicator)

        feature_df = timefreq_feature_df.merge(orientation_feature_df, on = ['START_TIME', 'STOP_TIME'])
        return feature_df
    
    def _post_process(self, result_data):
        output_path = mu.generate_output_filepath(self.file, self.setname, 'feature', 'PostureAndActivity')
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
            
        result_data.to_csv(output_path, index=False, float_format='%.6f')
        if self.verbose:
            print('Saved feature data to ' + output_path)

        result_data['pid'] = self.meta['pid']
        result_data['sid'] = self.meta['sid']
        return result_data