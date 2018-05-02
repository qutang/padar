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
			`mh -r . process spades_lab.ClassLabelAssigner --par --pattern SPADES_*/MasterSynced/**/SPADESInLab*.annotation.csv --setname Preprocessed > DerivedCrossParticipants/Feature_sets/SPADESInLab.class.csv`
		On single participant
			`mh -r . -p SPADES_1 process spades_lab.ClassLabelAssigner --par --pattern MasterSynced/**/SPADESInLab*.annotation.csv --setname Preprocessed > SPADES_1/Derived/SPADESInLab.class.csv`

	Debug:
		`mh -r . -p SPADES_1 process spades_lab.ClassLabelAssigner --verbose --pattern MasterSynced/**/SPADESInLab*.annotation.csv --setname Preprocessed`
"""

import os
import pandas as pd
import numpy as np
from ...api import windowing as mw
from ...api import utils as mu
from ...api import date_time as mdt
from ..BaseProcessor import AnnotationProcessor

def build(**kwargs):
	return ClassLabelAssigner(**kwargs).run_on_file

class ClassLabelAssigner(AnnotationProcessor):
	def __init__(self, verbose=True, violate=False, independent=False, ws=12800, ss=12800, session_file="DerivedCrossParticipants/sessions.csv", setname='Classlabel'):
		AnnotationProcessor.__init__(self, verbose=verbose, violate=False, independent=independent)
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
		chunk_windows_mask = (windows[:,0] >= data_start_indicator) & (windows[:,0] < data_stop_indicator)
		chunk_windows = windows[chunk_windows_mask,:]
		transformers = [
			lambda x: np.array([_to_posture(x, ws)], dtype=object),
			lambda x: np.array([_to_four_classes(x, ws)], dtype=object),
			lambda x: np.array([_to_mdcas(x, ws)], dtype=object),
			lambda x: np.array([_to_indoor_outdoor(x, ws)], dtype=object),
			lambda x: np.array([_to_activity(x, ws)], dtype=object),
			lambda x: np.array([_to_intensity(x, ws)], dtype=object),
			lambda x: np.array([_to_hand_gesture(x, ws)], dtype=object)
		]

		transformer_names = [
			'posture',
			'four_classes',
			'MDCAS',
			'indoor_outdoor',
			'activity',
			'activity_intensity',
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

def _to_intensity(annotations, ws):
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
			met = 8.0
		elif "sitting" in label and 'writing' in label:
			met = 1.3
		elif 'stand' in label and 'writ' in label:
			met = 1.8
		elif 'sit' in label and 'story' in label:
			met = 1.5
		elif "reclin" in label and 'story' in label:
			met = 1.3
		elif ('reclin' in label or 'sit' in label) and ('text' in label):
			met = 1.3
		elif "stand" in label and "story" in label and 'wait' not in label:
			met = 1.3
		elif 'sit' in label and 'web' in label:
			met = 1.3
		elif "stand" in label and "web" in label:
			met = 1.8
		elif 'stair' in label and 'down' in label:
		  met = 3.5
		elif 'stair' in label and 'up' in label and 'phone' in label:
			met = 5.0
		elif 'stair' in label and 'up' in label:
			met = 5.0
		elif 'mbta' in label and 'stand' in label:
			met = 1.5
		elif 'mbta' in label and 'sit' in label:
			met = 1.3
		elif 'bik' in label and 'outdoor' in label:
			met = 6.8
		elif 'bik' in label and 'stationary' in label:
			met = 3.5
		elif 'treadmill' in label and '1' in label and 'arms' in label:
			met = 2.0
		elif 'treadmill' in label and '2' in label and 'arms' in label:
			met = 2.8
		elif 'treadmill' in label and '3.5' in label and 'text' in label and 'arms' not in label:
			met = 4.3
		elif 'treadmill' in label and 'phone' in label and 'arms' not in label:
			met = 4.0
		elif 'treadmill' in label and 'bag' in label and 'arms' not in label:
			met = 4.5
		elif 'treadmill' in label and 'story' in label and 'arms' not in label:
			met = 4.0
		elif 'treadmill' in label and 'drink' in label and 'arms' not in label:
			met = 4.0
		elif ('treadmill' in label or 'walk' in label) and ('3.5' in label or '3' in label) and 'arms' not in label:
			met = 4.0
		elif 'treadmill' in label and '5.5' in label:
			met = 10.5
		elif 'laundry' in label:
			met = 2.0
		elif 'sweep' in label:
			met = 3.3
		elif 'frisbee' in label:
			met = 3.0
		elif 'shelf' in label and 'load' in label:
			met = 3.5
		elif 'lying' in label:
			met = 1.3
		elif 'elevator' in label and 'up' in label:
			met = 1.5
		elif 'elevator' in label and 'down' in label:
			met = 1.5
		elif 'escalator' in label and 'up' in label:
			met = 1.5
		elif 'escalator' in label and 'down' in label:
			met = 1.5
		elif "walk" in label and 'bag' in label and 'story' in label:
			met = 3.8
		elif "walk" in label and 'bag' in label:
			met = 3.8
		elif "walk" in label and 'story' in label:
			met = 3.8
		elif "walk" in label and 'text' in label:
			met = 3.8
		elif label == 'walking':
			met = 3.8
		elif 'outdoor' in label and 'stand' in label:
			met = 1.5
		elif 'vend' in label:
			met = 2.5
		elif 'light' in label and 'stand' in label:
			met = 1.5
		elif label == 'standing':
			met = 1.5
		elif 'sit' in label and 'wait' in label:
			met = 1.3
		elif label == 'sitting' or ('sit' in label and 'still' in label):
			met = 1.3
		elif label == "still" or 'standing' == label:
			met = 1.5
		else:
			return 'unknown'
	if met <= 1.5:
		return "sedentary"
	elif met <= 4.0:
		return "light"
	elif met <= 7.0:
		return "moderate"
	else:
		return "vigorous"

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

def _to_four_classes(annotations, ws):
	start_times = np.array(annotations[:,1], dtype='datetime64[ms]')
	stop_times = np.array(annotations[:,2], dtype='datetime64[ms]')
	time_diffs = (stop_times - start_times) / np.timedelta64(1, 's')
	labels = np.unique(annotations[:,3])
	label = ','.join(labels)
	label = label.lower().strip()
	if(np.any(time_diffs < ws / 1000)):
		return "transition"
	else:
		if label == 'jumping jacks' or 'laundry' in label or 'sweep' in label or 'frisbee' in label or 'shelf' in label or 'vend' in label:
			return "others"
		elif "sit" in label or "stand" in label or "lying" in label or "reclin" in label or 'elevator' in label or 'escalator' in label:
			return 'sedentary'
		elif "stair" in label or "treadmill" in label or "walk" in label:
			return "ambulation"
		elif 'bik' in label:
			return "cycling"
		else:
			return 'unknown'

def _to_mdcas(annotations, ws):
	start_times = np.array(annotations[:,1], dtype='datetime64[ms]')
	stop_times = np.array(annotations[:,2], dtype='datetime64[ms]')
	time_diffs = (stop_times - start_times) / np.timedelta64(1, 's')
	labels = np.unique(annotations[:,3])
	label = ','.join(labels)
	label = label.lower().strip()
	if(np.any(time_diffs < ws / 1000)):
		return "transition"
	else:
		if label == 'jumping jacks' or 'laundry' in label or 'sweep' in label or 'frisbee' in label or 'shelf' in label or 'vend' in label or 'bik' in label:
			return "others"
		elif "sit" in label or "stand" in label or "reclin" in label or 'elevator' in label or 'escalator' in label:
			return 'sedentary'
		elif "stair" in label or "treadmill" in label or "walk" in label or "run" in label:
			return "ambulation"
		elif "lying" in label:
			return "sleep"
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
	else:
		if label == 'jumping jacks':
			return "jumping jacks"
		elif "sitting" in label and 'writing' in label:
			return 'writing (dom)'
		elif 'stand' in label and 'writ' in label:
			return 'writing (dom)'
		elif 'sit' in label and 'story' in label:
			return "talking"
		elif "reclin" in label and 'story' in label:
			return 'talking'
		elif ('reclin' in label or 'sit' in label) and ('text' in label):
			return 'using phone'
		elif "stand" in label and "story" in label and 'wait' not in label:
			return 'talking'
		elif 'sit' in label and 'web' in label:
			return 'keyboard typing'
		elif "stand" in label and "web" in label:
			return "keyboard typing"
		elif 'stair' in label and 'down' in label:
			return "free"
		elif 'stair' in label and 'up' in label and 'phone' in label:
			return 'phone talking'
		elif 'stair' in label and 'up' in label:
			return 'free'
		elif 'mbta' in label and 'stand' in label:
			return 'still'
		elif 'mbta' in label and 'sit' in label:
			return 'free'
		elif 'bik' in label and 'outdoor' in label:
			return "biking"
		elif 'bik' in label and 'stationary' in label:
			return "biking"
		elif 'treadmill' in label and '1' in label and 'arms' in label:
			return "arms on desk"
		elif 'treadmill' in label and '2' in label and 'arms' in label:
			return "arms on desk"
		elif 'treadmill' in label and '3.5' in label and 'text' in label and 'arms' not in label:
			return "using phone"
		elif 'treadmill' in label and 'phone' in label and 'arms' not in label:
			return "phone talking"
		elif 'treadmill' in label and 'bag' in label and 'arms' not in label:
			return "carrying suitcase"
		elif 'treadmill' in label and 'story' in label and 'arms' not in label:
			return "talking"
		elif 'treadmill' in label and 'drink' in label and 'arms' not in label:
			return 'carrying a drink'
		elif ('treadmill' in label or 'walk' in label) and ('3.5' in label or '3' in label) and 'arms' not in label:
			return 'free'
		elif 'treadmill' in label and '5.5' in label:
			return 'free'
		elif 'laundry' in label:
			return 'folding towels'
		elif 'sweep' in label:
			return 'sweeping'
		elif 'frisbee' in label:
			return 'frisbee'
		elif 'shelf' in label and 'load' in label:
			return 'shelf reloading or unloading'
		elif 'lying' in label:
			return "still"
		elif 'elevator' in label and 'up' in label:
			return 'free'
		elif 'elevator' in label and 'down' in label:
			return 'free'
		elif 'escalator' in label and 'up' in label:
			return "free"
		elif 'escalator' in label and 'down' in label:
			return "free"
		elif "walk" in label and 'bag' in label and 'story' in label:
			return "talking"
		elif "walk" in label and 'bag' in label:
			return "free"
		elif "walk" in label and 'story' in label:
			return "talking"
		elif "walk" in label and 'text' in label:
			return "using phone"
		elif label == 'walking':
			return 'free'
		elif 'outdoor' in label and 'stand' in label:
			return "free"
		elif 'vend' in label:
			return "using vending machine"
		elif 'light' in label and 'stand' in label:
			return "free"
		elif label == 'standing':
			return 'free'
		elif 'sit' in label and 'wait' in label:
			return 'free'
		elif label == 'sitting' or ('sit' in label and 'still' in label):
			return "still"
		elif label == "still" or 'standing' == label:
			return "still"
		else:
			return 'unknown'