"""
Script to compute class labels for general dataset, relying on a generated class_mapping file

Usage:

	pad -p <PID> -r <root> process -p <PATTERN> --par -o <OUTPUT_FILEPATH> ClassLabelAssigner <options>

    process options:
        --output, -o <filepath>: the output filepath (relative to participant's folder or root folder) that the script will save the converted class labels to. If it is not provided, concatenated class label results will not be saved.

	script options:
	
		--sessions <path>: the filepath (relative to root folder or absolute path) that contains the sessions information (the start and end time of a data collection session for a participant) found by `SessionExtractor`. If this file is not provided, the start and end time of the dataset will be the start and end time of the current file.

        --ws <number>: window size in milliseconds. The size of window to extract class labels. Default is 12800ms (12.8s)

        --ss <number>: step size in milliseconds. The size of sliding step between adjacent window. Default is 12800ms (12.8s), indicating there is no overlapping between adjacent feature windows.
		
		--output_folder <folder name>: the folder name that the script will save class label data to in a participant's Derived folder. If not provided, hourly class label data will not be saved
		
	output:

		The command will print the concatenated class label file in pandas dataframe to console. The command will also save class label data to hourly files to <output_folder> if this parameter is provided.

Examples:

	1. Convert annotations to class labels for SPADES_1 and save the results to `processed` in `Derived` folder of SPADES_1 hourly and the concatenated result to `Derived` folder of SPADES_1

		pad -p SPADES_1 process ClassLabelAssigner --par -p MasterSynced/**/*.annotation.csv -o Derived/SPADESInLab.annotation.csv --sessions SPADES_1/Derived/sessions.csv --output_folder processed

	1. Convert annotations to class labels for all participants and save the results to `processed` in `Derived` folder of each participant hourly and the concatenated result to `DerivedCrossParticipants` folder

		pad process ClassLabelAssigner --par -p SPADES_*/MasterSynced/**/*.annotation.csv -o DerivedCrossParticipants/SPADESInLab.annotation.csv --sessions DerivedCrossParticipants/sessions.csv --output_folder processed
	
"""

import os
import pandas as pd
import numpy as np
from ...api import windowing as mw
from ...api import utils as mu
from ...api import date_time as mdt
from ..BaseProcessor import AnnotationProcessor
from ...utility import logger

def build(**kwargs):
	return ClassLabelAssigner(**kwargs).run_on_file

class ClassLabelAssigner(AnnotationProcessor):
	def __init__(self, verbose=True, violate=False, independent=False, ws=12800, ss=12800, sessions=None, class_map=None, output_folder=None):
		AnnotationProcessor.__init__(self, verbose=verbose, violate=False, independent=independent)
		self.name = 'ClassLabelAssigner'
		self.output_folder = output_folder
		self.sessions = sessions
		self.class_map = class_map
		self.ws = ws
		self.ss = ss
	
	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		st, et = mu.get_st_et(combined_data, self.meta['pid'], self.sessions, st_col=1, et_col=2)
		if self.verbose:
			logger.info('Session start time: ' + str(st))
			logger.info('Session stop time: ' + str(et))

		if self.class_map is not None:
			class_mapping = pd.read_csv(self.class_map)
		# save current file's start and stop time
		ws, ss = self.ws, self.ss
		windows = mw.get_sliding_window_boundaries(st, et, ws, ss)
		chunk_windows_mask = (windows[:,0] >= data_start_indicator) & (windows[:,0] < data_stop_indicator)
		chunk_windows = windows[chunk_windows_mask,:]
		transformers = [
			lambda x: find_class_map(x, class_mapping)
		]

		transformer_names = class_mapping.columns[1:]
		
		result_data = mw.apply_to_sliding_windows(df=combined_data, sliding_windows=chunk_windows, window_operations=transformers, operation_names=transformer_names, start_time_col=1, stop_time_col=2, send_time_cols=True, return_dataframe=True, empty_row_placeholder='unknown')

		return result_data

	def _post_process(self, result_data):
		output_path = mu.generate_output_filepath(self.file, self.output_folder, 'class')
		if not os.path.exists(os.path.dirname(output_path)):
			os.makedirs(os.path.dirname(output_path))
		result_data.to_csv(output_path, index=False)
		if self.verbose:
			logger.info('Saved class labels to ' + output_path)
		result_data['pid'] = self.meta['pid']
		result_data['annotator'] = self.meta['sid']
		return result_data

def find_class_map(annotations, class_mapping):
	start_times = np.array(annotations[:,1], dtype='datetime64[ms]')
	stop_times = np.array(annotations[:,2], dtype='datetime64[ms]')
	time_diffs = (stop_times - start_times) / np.timedelta64(1, 's')
	labels = np.unique(annotations[:,3])
	labels = map(lambda label: label.lower().strip(), labels)
	labels.sort()
	joined_label = '-'.join(labels)
	if(np.any(time_diffs < ws / 1000)):
		return "transition"
	else:
		mapping = class_mapping[class_mapping.iloc[:,0] == joined_label,:]
		if(mapping.shape[0] > 1):
			logger.error('Found more than one mapping in the provided class mapping file for: ' + joined_label + ', please review your class mapping file')
			exit(1)
		elif(mapping.shape[0] == 0:
			logger.warn(joined_label + " is not in the provided mapping file, return None")
			result = ['unknown'] * len(class_mapping.columns[1:])
			return result
		else:
			return mapping.values