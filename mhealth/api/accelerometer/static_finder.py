from ..numeric_transformation import vector_magnitude, accelerometer_orientation, unitize
from ..windowing import get_sliding_window_boundaries
import numpy as np
import pandas as pd
from ..utils import clip_dataframe

class StaticFinder():
    def __init__(self, df, chunk_size=1000, g_threshold=0.01, reference_vector = unitize(np.array([1., 1., 1.]))):
        self._reference_vector = reference_vector
        self._df = df
        self._chunk_size = chunk_size
        self._g_threshold = g_threshold

    @property
    def static(self):
        return self._static_vectors
        
    def find(self):
        '''
            find static chunks that have similar direction with unit length as the baseline
        '''
        df = self._df
        st = df.iloc[:, 0].values[0]
        et = df.iloc[:, 0].values[-1]
        windows = get_sliding_window_boundaries(st, et, window_duration=self._chunk_size, step_size=self._chunk_size)
        nrows = windows.shape[0]
        indices = range(0, nrows)
        static_vectors = []
        static_vectors_orientations = []
        count = 0
        for i in indices:
            st = windows[i, 0]
            et = windows[i, 1]
            chunk = clip_dataframe(df, st, et)
            chunk_values = chunk.values[:,1:]
            chunk_values = chunk_values.astype(np.float64)
            # check if current chunk is static
            if self._is_static(chunk_values) and self._is_unit(chunk_values):
                chunk_mean = unitize(np.mean(chunk_values, axis=0))
                chunk_angle = np.arccos(np.dot(chunk_mean, self._reference_vector) / np.linalg.norm(chunk_mean) / np.linalg.norm(self._reference_vector))
                chunk.insert(3, 'WINDOW_ID', i)
                chunk.insert(4, 'COUNT', count)
                chunk_mean_df = pd.DataFrame(data=np.reshape(chunk_mean, (1, 3)), columns=['X', 'Y', 'Z'])
                chunk_mean_df.insert(0, 'START_TIME', st)
                chunk_mean_df.insert(1, 'STOP_TIME', et)
                chunk_mean_df.insert(chunk_mean_df.shape[1], 'ANGLE', chunk_angle)
                static_vectors.append(chunk_mean_df)
                count = count + 1
    
        # merge into a single data frame
        if len(static_vectors) > 0:
            static_vectors = pd.concat(static_vectors, axis=0, join='inner')
        else:
            static_vectors = pd.DataFrame()
        self._static_vectors = static_vectors
        return self
    
    def _is_static(self, chunk):
        if np.all(np.abs(np.mean(chunk, axis=0, dtype=np.float64)) < 0.00001):
            return False
        axis_stds = np.std(chunk, axis=0, dtype=np.float64)
        if(np.all(axis_stds <= self._g_threshold)):
            return True
        else:
            return False

    def _is_unit(self, chunk):
        vm = vector_magnitude(np.mean(chunk, axis=0, dtype=np.float64))
        if np.abs(vm - 1.0) < 0.02:
            return True
        else:
            return False

    def _is_similar_orientation(self, current_orientation, calibration_points_orientations):
        for orientation in calibration_points_orientations:
            orientation_diff = np.abs(orientation - current_orientation)
            if(np.all(orientation_diff < self._angle_diff)):
                return False
        return True