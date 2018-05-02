"""

Base class for a runnable script

"""

import pandas as pd
import numpy as np
from .. import api as mhapi
import os

class Processor:
	def __init__(self, verbose=True, violate=False, independent=True):
		self.verbose = verbose
		self.independent = independent
		self.violate = violate
		self.name = 'BaseProcessor'
	
	def run_on_file(self, file, prev_file=None, next_file=None):
		self.file = file
		if self.independent:
			prev_file = None
			next_file = None
		self._extract_meta(file)
		data, prev_data, next_data = self._load_file(file, prev_file=prev_file, next_file=next_file)
		combined_data, data_start_indicator, data_stop_indicator = self._merge_data(data, prev_data=prev_data, next_data=next_data)
		result_data = self._run_on_data(combined_data, data_start_indicator, data_stop_indicator)
		result_data = self._post_process(result_data)
		return result_data

	def set_meta(self, meta):
		self.meta = meta

	def _extract_meta(self, file):
		file = os.path.normpath(os.path.abspath(file))
		pid = mhapi.extract_pid(file)
		if not self.violate:
			data_type = mhapi.extract_datatype(file)
			file_type = mhapi.extract_file_type(file)
			sensor_type = mhapi.extract_sensortype(file)
			sid = mhapi.extract_id(file)
			date = mhapi.extract_date(file)
			hour = mhapi.extract_hour(file)
			meta = dict(
				pid=pid,
				data_type=data_type,
				file_type=file_type,
				sensor_type=sensor_type,
				sid=sid,
				date=date,
				hour=hour
			)
		else:
			meta = dict(
				pid=pid
			)
		self.meta = meta

	def _load_file(self, file, prev_file=None, next_file=None):
		raise NotImplementedError("Subclass must implement this method")

	def _merge_data(self, data, prev_data=None, next_data=None):
		raise NotImplementedError("Subclass must implement this method")

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		raise NotImplementedError("Subclass must implement this method")

	def _post_process(self, result_data):
		return result_data

	def __str__(self):
		return self.name

class SensorProcessor(Processor):
	def __init__(self, verbose=True, violate=False, independent=True):
		Processor.__init__(self, verbose=verbose, violate=violate, independent=independent)
		self.name = 'SensorProcessor'
	
	def _load_file(self, file, prev_file=None, next_file=None):
		file = os.path.normpath(os.path.abspath(file))
		df = mhapi.helpers.importer.import_sensor_file_mhealth(file)
		if self.verbose:
			print("file: " + file)
			print("previous file: " + str(prev_file))
			print("next file: " + str(next_file))
		if prev_file is not None and prev_file != "None":
			prev_file = os.path.normpath(os.path.abspath(prev_file))
			prev_df = mhapi.helpers.importer.import_sensor_file_mhealth(prev_file)
		else:
			prev_df = pd.DataFrame()
		if next_file is not None and next_file != "None":
			next_file = os.path.normpath(os.path.abspath(next_file))
			next_df = mhapi.helpers.importer.import_sensor_file_mhealth(next_file)
		else:
			next_df = pd.DataFrame()
		if self.verbose:
			print(df.dtypes)
		return df, prev_df, next_df
	
	def _merge_data(self, data, prev_data=None, next_data=None):
		if data.empty:
			return pd.DataFrame(), None, None

		columns = data.columns
		if prev_data is None:
			prev_data = pd.DataFrame()
		if next_data is None:
			next_data = pd.DataFrame()
		
		combined_data = pd.concat([prev_data, data, next_data], axis=0, ignore_index=True)
		combined_data = combined_data[columns]
		data_start_indicator = data.iloc[0, 0].to_datetime64().astype('datetime64[h]')
		# file could be longer than an hour
		data_stop_indicator = data.iloc[-1, 0].to_datetime64().astype('datetime64[h]') + np.timedelta64(1, 'h')
		return combined_data, data_start_indicator, data_stop_indicator

class AnnotationProcessor(Processor):
	def __init__(self, verbose=True, violate=False, independent=True):
		Processor.__init__(self, verbose, violate, independent)
		self.name = 'AnnotationProcessor'
	
	def _load_file(self, file, prev_file=None, next_file=None):
		file = os.path.normpath(os.path.abspath(file))
		df = pd.read_csv(file, parse_dates=[0,1,2], infer_datetime_format=True)
		if prev_file is not None and prev_file != "None":
			prev_file = os.path.normpath(os.path.abspath(prev_file))
			prev_df = pd.read_csv(prev_file, parse_dates=[0,1,2], infer_datetime_format=True)
		else:
			prev_df = pd.DataFrame()
		if next_file is not None and next_file != "None":
			next_file = os.path.normpath(os.path.abspath(next_file))
			next_df = pd.read_csv(next_file, parse_dates=[0,1,2], infer_datetime_format=True)
		else:
			next_df = pd.DataFrame()
		return df, prev_df, next_df

	def _merge_data(self, data, prev_data=None, next_data=None):
		columns = data.columns
		combined_data = pd.concat([prev_data, data, next_data], axis=0, ignore_index=True)
		combined_data = combined_data[columns]
		data_start_indicator = data.iloc[0, 1].to_datetime64().astype('datetime64[h]')
		data_stop_indicator = data.iloc[data.shape[0]-1, 1].to_datetime64().astype('datetime64[h]') + np.timedelta64(1, 'h')
		return combined_data, data_start_indicator, data_stop_indicator

class FeatureClassProcessor(Processor):
	def __init__(self, verbose=True):
		Processor.__init__(verbose)
		self.name = 'FeatureClassProcessor'
	
	def _load_file(self, file, prev_file=None, next_file=None):
		file = os.path.normpath(os.path.abspath(file))
		df = pd.read_csv(file, parse_dates=[0,1], infer_datetime_format=True)
		if prev_file != None or prev_file != "None":
			prev_file = os.path.normpath(os.path.abspath(prev_file))
			prev_df = pd.read_csv(prev_file, parse_dates=[0,1], infer_datetime_format=True)
		else:
			prev_df = pd.DataFrame()
		if next_file != None or next_file != "None":
			next_file = os.path.normpath(os.path.abspath(next_file))
			next_df = pd.read_csv(next_file, parse_dates=[0,1], infer_datetime_format=True)
		else:
			next_df = pd.DataFrame()
		return df, prev_df, next_df

	def _merge_data(self, data, prev_data=None, next_data=None):
		columns = data.columns
		combined_data = pd.concat([prev_data, data, next_data], axis=0, ignore_index=True)
		combined_data = combined_data[columns]
		data_start_indicator = data.iloc[0, 0].to_datetime64().astype('datetime64[h]')
		data_stop_indicator = data.iloc[data.shape[0]-1, 0].to_datetime64().astype('datetime64[h]') + np.timedelta64(1, 'h')
		return combined_data, data_start_indicator, data_stop_indicator