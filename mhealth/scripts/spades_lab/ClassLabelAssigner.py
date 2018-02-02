"""
Script to compute class labels for SPADES lab dataset. 

Posture label:
	Upright: standing variations, walking variations and running variations
	Sitting: sitting variations and reclining variations
	Lying: lying on the back

Activity label:
	

Indoor/Outdoor label:
	Indoor, Outdoor

Four class label:
	Ambulation: walking and running
	Cycling: cycling
	Sedentary: sitting, standing and lying
	Others: the rest

Usage:
	Production: 
		On all participants
			`mh -r . process spades_lab.ClassLabelAssigner --par --pattern SPADES_*/MasterSynced/**/SPADESInLab*.annotation.csv --setname Preprocessed > DerivedCrossParticipants/SPADESInLab.class.csv`
		On single participant
			`mh -r . -p SPADES_1 process spades_lab.ClassLabelAssigner --par --pattern MasterSynced/**/SPADESInLab*.annotation.csv --setname Preprocessed > SPADES_1/Derived/SPADESInLab.class.csv`

	Debug:
		`mh -r . -p SPADES_1 process spades_lab.ClassLabelAssigner --verbose --pattern MasterSynced/**/SPADESInLab*.annotation.csv --setname Preprocessed`
"""

import os
import pandas as pd
import numpy as np
import mhealth.api.windowing as mw
import mhealth.api.utils as mu
import mhealth.api.date_time as mdt
from ..BaseProcessor import AnnotationProcessor

def build(**kwargs):
	return ClassLabelAssigner(**kwargs).run_on_file

class ClassLabelAssigner(AnnotationProcessor):
	def __init__(self, verbose=True, independent=False, ws=12800, ss=12800, session_file="DerivedCrossParticipants/sessions.csv", setname='Classlabel'):
		AnnotationProcessor.__init__(self, verbose=verbose, independent=independent)
		self.name = 'ClassLabelAssigner'
		self.setname = setname
		self.session_file = session_file
		self.ws = ws
		self.ss = ss
	
	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		st, et = mu.get_st_et(combined_data, self.meta['pid'], self.session_file, st_col=1, et_col=2)
		if self.verbose:
			print('Session start time: ' + str(st))
			print('Session stop time: ' + str(et))
		# save current file's start and stop time
		ws, ss = self.ws, self.ss
		windows = mw.get_sliding_window_boundaries(st, et, ws, ss)
		chunk_windows_mask = (windows[:,0] >= data_start_indicator) & (windows[:,0] <= data_stop_indicator)
		chunk_windows = windows[chunk_windows_mask,:]
		transformers = [
			lambda x: np.array([_to_posture(x, ws)], dtype=object),
			lambda x: np.array([_to_indoor_outdoor(x, ws)], dtype=object),
			lambda x: np.array([_to_activity(x, ws)], dtype=object),
			lambda x: np.array([_to_hand_gesture(x, ws)], dtype=object)
		]

		transformer_names = [
			'posture',
			'indoor_outdoor',
			'activity',
			'hand_gesture'
		]
		
		result_data = mw.apply_to_sliding_windows(df=combined_data, sliding_windows=chunk_windows, window_operations=transformers, operation_names=transformer_names, start_time_col=1, stop_time_col=2, send_time_cols=True, return_dataframe=True, empty_row_placeholder='unknown')

		return result_data

	def _post_process(self, result_data):
		output_path = mu.generate_output_filepath(self.file, self.setname, 'class')
		if not os.path.exists(os.path.dirname(output_path)):
			os.makedirs(os.path.dirname(output_path))
		result_data.to_csv(output_path, index=False)
		if self.verbose:
			print('Saved ' + output_path)
		result_data['pid'] = self.meta['pid']
		result_data['annotator'] = self.meta['sid']
		return result_data

def main(file, verbose=True, prev_file=None, next_file=None, ws=12800, ss=12800, session_file="DerivedCrossParticipants/sessions.csv", **kwargs):
	file = os.path.abspath(file)
	if verbose:
		print("Compute labels for " + file)
	df = pd.read_csv(file, parse_dates=[0, 1, 2], infer_datetime_format=True)
	pid = utils.extract_pid(file)
	if verbose:
		print("Prev file is " + str(prev_file))
		print("Next file is " + str(next_file))

	if not os.path.exists(prev_file):
		prev_file = None
	if not os.path.exists(next_file):
		next_file = None

	session_file = os.path.abspath(session_file)
	if session_file is None or pid is None:
		st = df.iloc[0, 1]
		et = df.iloc[df.shape[0]-1, 2]
	else:
		session_df = pd.read_csv(session_file, parse_dates=[0, 1], infer_datetime_format=True)
		selected_sessions = session_df.loc[session_df['pid'] == pid, :]
		if selected_sessions.shape[0] == 0:
			st = df.iloc[0, 1]
			et = df.iloc[df.shape[0]-1, 2]
		else:
			st = selected_sessions.iloc[0, 0]
			et = selected_sessions.iloc[selected_sessions.shape[0] - 1, 1]

	if verbose:
		print('Session start time: ' + str(st))
		print('Session stop time: ' + str(et))


	result_df = run_compute_class_labels(df, verbose=verbose, prev_file=prev_file, next_file=next_file, st=st, et=et, ws=ws, ss=ss, **kwargs)
	
	output_path = file.replace("MasterSynced", "Derived/preprocessed").replace("annotation", "class")
	if not os.path.exists(os.path.dirname(output_path)):
		os.makedirs(os.path.dirname(output_path))
	
	result_df.to_csv(output_path, index=False)
	if verbose:
		print('Saved ' + output_path)

	result_df['pid'] = pid
	return result_df

def run_compute_class_labels(df, verbose=True, prev_file=None, next_file=None, st=None, et=None, ws=12800, ss=12800, **kwargs):

	columns = df.columns
	
	# save current file's start and stop time
	chunk_st = df.iloc[0, 1].to_datetime64().astype('datetime64[h]')
	
	if chunk_st < st.to_datetime64():
		chunk_st = st.to_datetime64()
	chunk_et = df.iloc[df.shape[0]-1, 1].to_datetime64().astype('datetime64[h]') + np.timedelta64(1, 'h')
	
	if chunk_et > et.to_datetime64():
		chunk_et = et.to_datetime64()

	if prev_file is not None and prev_file != 'None':
		prev_df = pd.read_csv(prev_file, parse_dates=[0, 1, 2], infer_datetime_format=True)
		if len(columns) < len(prev_df.columns):
			columns = prev_df.columns
	else:
		prev_df = pd.DataFrame()
	if next_file is not None and next_file != 'None':
		next_df = pd.read_csv(next_file, parse_dates=[0, 1, 2], infer_datetime_format=True)
		if len(columns) < len(next_df.columns):
			columns = next_df.columns
	else:
		next_df = pd.DataFrame()

	combined_df = pd.concat([prev_df, df, next_df], axis=0, ignore_index=True)
	combined_df = combined_df[columns]

	windows = windowing.get_sliding_window_boundaries(st, et, ws, ss)

	chunk_windows_mask = (windows[:,0] >= chunk_st) & (windows[:,0] <= chunk_et)
	chunk_windows = windows[chunk_windows_mask,:]
	transformers = [
		lambda x: np.array([_to_posture(x, ws)], dtype=object),
		lambda x: np.array([_to_indoor_outdoor(x, ws)], dtype=object),
		lambda x: np.array([_to_activity(x, ws)], dtype=object),
		lambda x: np.array([_to_hand_gesture(x, ws)], dtype=object)
	]

	transformer_names = [
		'posture',
		'indoor_outdoor',
		'activity',
		'hand_gesture'
	]
	
	class_df = windowing.apply_to_sliding_windows(df=combined_df, sliding_windows=chunk_windows, window_operations=transformers, operation_names=transformer_names, start_time_col=1, stop_time_col=2, send_time_cols=True, return_dataframe=True, empty_row_placeholder='unknown')

	return class_df

def _to_posture(annotations, ws):
	start_times = np.array(annotations[:,1], dtype='datetime64[ms]')
	stop_times = np.array(annotations[:,2], dtype='datetime64[ms]')
	time_diffs = (stop_times - start_times) / np.timedelta64(1, 's')
	labels = np.unique(annotations[:,3])
	label = ','.join(labels)
	label = label.lower().strip()
	if(np.any(time_diffs < ws / 1000)):
		return "transition"
	else:
		if 'walk' in label or 'stand' in label or 'run' in label or 'jog' in label or 'still' in label or 'jump' in label or 'laundry' in label or 'sweep' in label or 'shelf' in label or 'frisbee' in label or 'stair' in label or 'vending' in label or 'elevator' in label or 'escalator' in label:
			return "upright"
		elif 'sit' in label or 'reclin' in label or 'bik' in label or 'cycl' in label:
			return 'sitting'
		elif 'lying' in label:
			return 'lying'
		else:
			return 'unknown'

def _to_activity(annotations, ws):
	start_times = np.array(annotations[:,1], dtype='datetime64[ms]')
	stop_times = np.array(annotations[:,2], dtype='datetime64[ms]')
	time_diffs = (stop_times - start_times) / np.timedelta64(1, 's')
	labels = np.unique(annotations[:,3])
	label = ','.join(labels)
	label = label.lower().strip()
	if(np.any(time_diffs < ws / 1000)):
		return "transition"
	else:
		if label == 'jumping jacks':
			return "jumping jacks"
		elif "sitting" in label and 'writing' in label:
			return 'sitting and writing'
		elif 'stand' in label and 'writ' in label:
			return 'standing and writing'
		elif 'sit' in label and 'story' in label:
			return "sitting and talking"
		elif "reclin" in label and 'story' in label:
			return 'reclining and talking'
		elif ('reclin' in label or 'sit' in label) and ('text' in label):
			return 'reclining and using phone'
		elif "stand" in label and "story" in label and 'wait' not in label:
			return 'standing naturally'
		elif 'sit' in label and 'web' in label:
			return 'sitting and keyboard typing'
		elif "stand" in label and "web" in label:
			return "standing and keyboard typing"
		elif 'stair' in label and 'down' in label:
			return "walking down stairs"
		elif 'stair' in label and 'up' in label and 'phone' in label:
			return 'walking up stairs and phone talking'
		elif 'stair' in label and 'up' in label:
			return 'walking up stairs'
		elif 'mbta' in label and 'stand' in label:
			return 'standing on train'
		elif 'mbta' in label and 'sit' in label:
			return 'sitting on train'
		elif 'bik' in label and 'outdoor' in label:
			return "biking outdoor"
		elif 'bik' in label and 'stationary' in label:
			return "stationary biking"
		elif 'treadmill' in label and '1' in label and 'arms' in label:
			return "treadmill walking at 1 mph with arms on desk"
		elif 'treadmill' in label and '2' in label and 'arms' in label:
			return "treadmill walking at 2 mph with arms on desk"
		elif 'treadmill' in label and '3.5' in label and 'text' in label and 'arms' not in label:
			return "treadmill walking at 3-3.5 mph and using phone"
		elif 'treadmill' in label and 'phone' in label and 'arms' not in label:
			return "treadmill walking at 3-3.5 mph and talking with phone"
		elif 'treadmill' in label and 'bag' in label and 'arms' not in label:
			return "treadmill walking at 3-3.5 mph and carrying a bag"
		elif 'treadmill' in label and 'story' in label and 'arms' not in label:
			return "treadmill walking at 3-3.5 mph talking"
		elif 'treadmill' in label and 'drink' in label and 'arms' not in label:
			return 'treadmill walking at 3-3.5 mph and carrying a drink'
		elif ('treadmill' in label or 'walk' in label) and ('3.5' in label or '3' in label) and 'arms' not in label:
			return 'treadmill walking at 3-3.5 mph'
		elif 'treadmill' in label and '5.5' in label:
			return 'treadmill running at 5.5 mph 5% grade'
		elif 'laundry' in label:
			return 'standing and folding towels'
		elif 'sweep' in label:
			return 'standing and sweeping'
		elif 'frisbee' in label:
			return 'frisbee'
		elif 'shelf' in label and 'load' in label:
			return 'standing and shelf reloading or unloading'
		elif 'lying' in label:
			return "lying on the back"
		elif 'elevator' in label and 'up' in label:
			return 'elevator up'
		elif 'elevator' in label and 'down' in label:
			return 'elevator down'
		elif 'escalator' in label and 'up' in label:
			return "escalator up"
		elif 'escalator' in label and 'down' in label:
			return "escalator down"
		elif "walk" in label and 'bag' in label and 'story' in label:
			return "self-paced walking and talking with bag"
		elif "walk" in label and 'bag' in label:
			return "self-paced walking with bag"
		elif "walk" in label and 'story' in label:
			return "self-paced walking and talking"
		elif "walk" in label and 'text' in label:
			return "self-paced walking and texting"
		elif label == 'walking':
			return 'self-paced walking'
		elif 'outdoor' in label and 'stand' in label:
			return "standing naturally"
		elif 'vend' in label:
			return "using vending machine"
		elif 'light' in label and 'stand' in label:
			return "standing for stop light"
		elif label == 'standing':
			return 'standing naturally'
		elif 'sit' in label and 'wait' in label:
			return 'sitting naturally'
		elif label == 'sitting' or ('sit' in label and 'still' in label):
			return "sitting still"
		elif label == "still" or 'standing' == label:
			return "standing naturally"
		else:
			return 'unknown'

def _to_indoor_outdoor(annotations, ws):
	start_times = np.array(annotations[:,1], dtype='datetime64[ms]')
	stop_times = np.array(annotations[:,2], dtype='datetime64[ms]')
	time_diffs = (stop_times - start_times) / np.timedelta64(1, 's')
	labels = np.unique(annotations[:,3])
	label = ','.join(labels)
	label = label.lower().strip()
	if(np.any(time_diffs < ws / 1000)):
		return "transition"
	else:
		if 'outdoor' in label or 'city' in label or 'light' in label or 'escalator' in label or 'mbta' in label or 'frisbee' in label or 'stair' in label or 'vending' in label or 'elevator' in label or 'wait' in label:
			return "outdoor"
		elif 'treadmill' in label or 'jog' in label or 'run' in label or 'writ' in label or 'laundry' in label or 'shelf' in label or 'web' in label or 'lying' in label or 'reclin' in label or 'jump' in label or 'desk' in label or 'mph' in label or 'kpm' in label or 'grade' in label or 'sweep' in label or 'sit' in label or 'still' in label or 'station' in label:
			return 'indoor'
		else:
			return 'unknown'

def _to_hand_gesture(annotations, ws):
	start_times = np.array(annotations[:,1], dtype='datetime64[ms]')
	stop_times = np.array(annotations[:,2], dtype='datetime64[ms]')
	time_diffs = (stop_times - start_times) / np.timedelta64(1, 's')
	labels = np.unique(annotations[:,3])
	label = ','.join(labels)
	label = label.lower().strip()
	if(np.any(time_diffs < ws / 1000)):
		return "transition"
	