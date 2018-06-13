"""
Script to compute features (based on VM and orientation) used for posture and activity recognition classifier in multilocation paper. This feature set may also be used to build classifier to detect activity groups and activity intensities. The VM part may also be used for sedentary and ambulation classifier. 

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

Prerequiste:
    Better to run `SessionExtractor` script first

Usage:
	pad -p <PID> -r <root> process -p <PATTERN> --par -o <OUTPUT_FILEPATH> multilocation_2017.FeatureSetPreparer <options>

    process options:
        --output, -o <filepath>: the output filepath (relative to participant's folder or root folder) that the script will save concatenated feature set data to. If it is not provided, concatenated feature set results will not be saved.

	script options:
	
		--sessions <path>: the filepath (relative to root folder or absolute path) that contains the sessions information (the start and end time of a data collection session for a participant) found by `SessionExtractor`. If this file is not provided, the start and end time of the dataset will be the start and end time of the current file.

        --location_mapping <path>: the filepath (relative to root folder or absolute path) that contains the location mapping information (mapping from sensor id to sensor location). If this file is not provided, location information will not be appended to the output.

        --orientation_fixes <path>: the filepath (relative to root folder or absolute path) that contains the ground truth of orientation fix information (swap or flip between x, y and z axes). If this file is not provided, orientation fix will be skipped.

        --ws <number>: window size in milliseconds. The size of window to extract features. Default is 12800ms (12.8s)

        --ss <number>: step size in milliseconds. The size of sliding step between adjacent window. Default is 12800ms (12.8s), indicating there is no overlapping between adjacent feature windows.

        --threshold <number>: the threshold in g value to compute activation related features. Default is 0.2g.

        --subwins <number>: the number of sub windows in a feature window, which is used to compute location features (also used in orientation feature computation). Default is 4.

        --high_cutoff <number>: the lowpass butterworth filter cutoff frequency applied before computing features. Default is 20Hz. This value should be smaller than half of the sampling rate.
		
		--output_folder <folder name>: the folder name that the script will save feature set data to in a participant's Derived folder. User must provide this information in order to use the script.
		
	output:
		The command will print the concatenated feature set file in pandas dataframe to console. The command will also save features to hourly files to <output_folder> if this parameter is provided.

Examples:

	1.  Compute features for each of the Actigraph raw data files for participant SPADES_1 in parallel and save each to a folder named 'Features' in the 'Derived' folder of SPADES_1 and then save the concatenated feature set data to 'PostureAndActivity.feature.csv' in 'Derived' folder of SPADES_1
	
    	pad -p SPADES_1 process multilocation_2017.FeatureSetPreparer --par -p MasterSynced/**/Actigraph*.sensor.csv --output_folder Features --sessions SPADES_1/Derived/sessions.csv --location_mapping SPADES_1/Derived/location_mapping.csv -o Derived/PostureAndActivity.feature.csv

	2. Compute features for each of the Actigraph raw data files for all participants in a dataset in parallel and save each to a folder named 'Features' in the 'Derived' folder of each participant and then save the concatenated feature set data to 'PostureAndActivity.feature.csv' in 'DerivedCrossParticipants' folder of the dataset.

		pad process AccelerometerCalibrator --par -p MasterSynced/**/Actigraph*.sensor.csv -output_folder Features --sessions DerivedCrossParticipants/sessions.csv --location_mapping DerivedCrossParticipants/location_mapping.csv -o DerivedCrossParticipants/PostureAndActivity.feature.csv
"""

import os
import pandas as pd
import numpy as np
import scipy.signal as signal
from ...api import numeric_feature as mnf
from ...api import numeric_transformation as mnt
from ...api import filter as mf
from ...api import windowing as mw
from ...api import utils as mu
from ..BaseProcessor import SensorProcessor
from ..ManualOrientationNormalizer import ManualOrientationNormalizer
from ..SensorFilter import SensorFilter
from ..TimeFreqFeatureComputer import TimeFreqFeatureComputer
from ..OrientationFeatureComputer import OrientationFeatureComputer
from ...utility import logger

def build(**kwargs):
    return FeatureSetPreparer(**kwargs).run_on_file

class FeatureSetPreparer(SensorProcessor):
    def __init__(self, verbose=True, independent=False, violate=False, output_folder=None, 
    sessions=None, 
    location_mapping =None, 
    orientation_fixes=None, 
    ws=12800, ss=12800, threshold=0.2, subwins=4, high_cutoff=20):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
        self.name = 'AccelerometerFeatureComputer'
        self.output_folder = output_folder
        self.sessions = sessions
        self.orientation_fixes = orientation_fixes
        self.subwins = subwins
        self.sensorFilter = SensorFilter(verbose=verbose, independent=independent, order=4, low_cutoff=None, high_cutoff=high_cutoff)

        self.manualOrientationNormalizer = ManualOrientationNormalizer(verbose=verbose, independent=independent, orientation_fixes=self.orientation_fixes)

        self.timeFreqFeatureComputer = TimeFreqFeatureComputer(verbose=verbose, independent=independent, sessions=sessions, ws=ws, ss=ss, threshold=threshold)

        self.orientationFeatureComputer = OrientationFeatureComputer(verbose=verbose, independent=independent, sessions=sessions, ws=ws, ss=ss, subwins=subwins)
        
        self.location_mapping = location_mapping
    
    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        if combined_data.empty:
            return pd.DataFrame()
        
        self.sensorFilter.set_meta(self.meta)
        self.manualOrientationNormalizer.set_meta(self.meta)
        self.timeFreqFeatureComputer.set_meta(self.meta)
        self.orientationFeatureComputer.set_meta(self.meta)
        st, et = mu.get_st_et(combined_data, self.meta['pid'], self.sessions, st_col=0, et_col=0)
        if self.verbose:
            logger.debug('Session start time: ' + str(st))
            logger.debug('Session stop time: ' + str(et))
            logger.debug('File start time: ' + str(data_start_indicator))
            logger.debug('File stop time: ' + str(data_stop_indicator))

        sr = mu._sampling_rate(combined_data)

        # 20 Hz lowpass filter on vector magnitude data and original data
        vm_data = mnt.vector_magnitude(combined_data.values[:,1:4]).ravel()
        vm_data = pd.DataFrame(vm_data, columns=['VM'])
        vm_data.insert(0, 'HEADER_TIME_STAMP', combined_data.iloc[:, 0].values)
        vm_data_filtered = self.sensorFilter._run_on_data(vm_data, data_start_indicator, data_stop_indicator)
        combined_data_filtered = self.sensorFilter._run_on_data(combined_data, data_start_indicator, data_stop_indicator)

        # manual fix orientation
        if self.orientation_fixes is not None and os.path.exists(self.orientation_fixes):
            combined_data_prepared = self.manualOrientationNormalizer._run_on_data(combined_data_filtered, data_start_indicator, data_stop_indicator)
        else:
            combined_data_prepared = combined_data_filtered.copy()

        timefreq_feature_df = self.timeFreqFeatureComputer._run_on_data(vm_data_filtered, data_start_indicator, data_stop_indicator)
        orientation_feature_df = self.orientationFeatureComputer._run_on_data(combined_data_prepared, data_start_indicator, data_stop_indicator)
        if timefreq_feature_df.empty and orientation_feature_df.empty:
            return pd.DataFrame()
        feature_df = timefreq_feature_df.merge(orientation_feature_df)
        return feature_df
    
    def _post_process(self, result_data):
        if self.output_folder is None:
            return result_data
        output_path = mu.generate_output_filepath(self.file, self.output_folder, 'feature', 'PostureAndActivity')
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
        location = mu.get_location_from_sid(self.meta['pid'], self.meta['sid'], self.location_mapping)
        result_data.to_csv(output_path, index=False, float_format='%.9f')
        if self.verbose:
            logger.info('Saved feature data to ' + output_path)

        result_data['pid'] = self.meta['pid']
        result_data['sid'] = self.meta['sid']
        result_data['location'] = location
        return result_data