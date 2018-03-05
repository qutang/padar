'''
'''
from ..numeric_transformation import vector_magnitude, accelerometer_orientation
from ..windowing import get_sliding_window_boundaries
import numpy as np
import pandas as pd
from ._calibraxis import Calibraxis
from ..utils import clip_dataframe

class Calibrator():
	def __init__(self, accel_df, angle_diff=30, max_points=12, g_threshold=0.01, chunk_size=1000):
		self._data = accel_df
		self._angle_diff = angle_diff
		self._max_points = max_points
		self._g_threshold = g_threshold
		self._chunk_size = chunk_size
		self._calibration_chunks = []
		self._calibrated_data = pd.DataFrame()

	@property
	def calibrated(self):
		return self._calibrated_data

	@property
	def static(self):
		return self._calibration_chunks

	def set_static(self, static_chunks):
		self._calibration_chunks = static_chunks
		return self

	def find(self):
		df = self._data
		st = df['HEADER_TIME_STAMP'].values[0]
		et = df['HEADER_TIME_STAMP'].values[-1]
		windows = get_sliding_window_boundaries(st, et, window_duration=self._chunk_size, step_size=self._chunk_size)
		nrows = windows.shape[0]
		indices = range(0, nrows)
		calibration_chunks = []
		calibration_chunks_orientations = []
		count = 0
		for i in indices:
			st = windows[i, 0]
			et = windows[i, 1]
			chunk = clip_dataframe(df, st, et)
			chunk_values = chunk.values[:,1:]
			chunk_values = chunk_values.astype(np.float64)
			# check if current chunk is static
			if self._is_static(chunk_values):
				# check if current chunk has a different enough orientation than existing ones
				chunk_mean = np.mean(chunk_values, axis=0)
				chunk_orientation = np.rad2deg(accelerometer_orientation(chunk_mean))
				if self._is_different_orientation(chunk_orientation, calibration_chunks_orientations):
					chunk.insert(3, 'WINDOW_ID', i)
					chunk.insert(4, 'COUNT', count)
					calibration_chunks.append(chunk)
					calibration_chunks_orientations.append(chunk_orientation)
					count = count + 1
					# if there are already max_points calibration points, break the loop
					if count > self._max_points - 1:
						break
	
		# merge into a single data frame
		if len(calibration_chunks) > 0:
			calibration_chunks = pd.concat(calibration_chunks, axis=0, join='inner')
		else:
			calibration_chunks = pd.DataFrame()
		self._calibration_chunks = calibration_chunks
		return self

	def run(self, verbose=False):
		df = self._data
		if len(self._calibration_chunks) == 0:
			calibrated_df = df.copy(deep=True)
			self._calibrated_data = calibrated_df
			return self

		# get mean values of each calibration chunks
		if 'date' in self._calibration_chunks.columns:
			calibration_points = self._calibration_chunks.groupby(['WINDOW_ID', 'COUNT', 'date', 'hour']).mean()
		else:
			calibration_points = self._calibration_chunks.groupby(['WINDOW_ID', 'COUNT']).mean()
		# run calibration algorithm
		calibrator = Calibraxis(verbose=False)
		calibrator.add_points(calibration_points.values)
		calibrator.calibrate_accelerometer()

		if(calibrator.scale_factor_matrix is None):
			print("Calibration fails, provided calibration points cannot converge")
			print("Use original data")
			self._calibrated_data = df
			return self

		calibrated_df_list = calibrator.batch_apply(df.values[:,1:])

		# convert back into dataframe
		calibrated_df_values = np.asarray(calibrated_df_list, dtype=np.float64)
		calibrated_df = df.copy(deep=True)
		calibrated_df.ix[:,1:] = calibrated_df_values
		self._calibrated_data = calibrated_df
		return self

	def _is_static(self, chunk):
		if np.all(np.abs(np.mean(chunk, axis=0, dtype=np.float64)) < 0.00001):
			return False
		axis_stds = np.std(chunk, axis=0, dtype=np.float64)
		if(np.all(axis_stds <= self._g_threshold)):
			return True
		else:
			return False

	def _is_different_orientation(self, current_orientation, calibration_points_orientations):
		for orientation in calibration_points_orientations:
			orientation_diff = np.abs(orientation - current_orientation)
			if(np.all(orientation_diff < self._angle_diff)):
				return False
		return True