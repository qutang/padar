from scipy import signal, interpolate
import numpy as np
from .detect_peaks import detect_peaks
import pandas as pd
from .numeric_transformation import vector_magnitude

def active_perc(X, threshold):
    """
    The percentage of active samples, active samples are samples whose value is beyond certain threshold
    """
    thres_X = X >= threshold
    active_samples = np.sum(thres_X, axis=0)
    active_perc = active_samples / np.float(thres_X.shape[0])
    return(active_perc)


def activation_count(X, threshold):
    """
    The number of times signal go across up the active threshold
    """
    thres_X = X >= threshold
    active_samples = np.sum(thres_X, axis=0)
    thres_X_num = thres_X.astype(np.float64)
    active_crossings_X = np.diff(
        np.insert(thres_X_num, 0, np.zeros([1, X.shape[1]]), axis=0), axis=0) > 0
    active_crossings = np.sum(active_crossings_X, axis=0)
    result = np.divide(active_crossings, active_samples)
    return(result)


def activation_std(X, threshold):
    """
    The standard deviation of the durations of actived durations
    """
    if type(X) == pd.DataFrame:
        X = X.values
    thres_X = X >= threshold
    cumsum_X = np.cumsum(thres_X, axis=0)
    thres_X_num = thres_X.astype(np.float64)
    rise_marker_X = np.diff(
        np.insert(thres_X_num, 0, np.zeros([1, X.shape[1]]), axis=0), axis=0) > 0
    active_crossings = np.sum(rise_marker_X, axis=0)
    zero_marker = active_crossings <= 2
    fall_marker_X = np.diff(
        np.append(thres_X, np.zeros([1, X.shape[1]]), axis=0), axis=0) < 0
    rise_X = np.sort(np.multiply(
        cumsum_X, rise_marker_X, dtype=np.float), axis=0)
    fall_X = np.sort(np.multiply(
        cumsum_X, fall_marker_X, dtype=np.float), axis=0)
    activation_dur_X = fall_X - rise_X + 1
    activation_dur_X[activation_dur_X == 1.] = np.nan
    activation_std = np.nanstd(activation_dur_X, axis=0)
    activation_std[zero_marker] = 0
    activation_std = activation_std / X.shape[0]
    return(activation_std)

def mean(X):
    return np.nanmean(X, axis=0)

def std(X):
    return np.nanstd(X, axis=0)

def positive_amplitude(X):
    return np.nanmax(X, axis=0)
    
def negative_amplitude(X):
    return np.nanmin(X, axis=0)

def amplitude_range(X):
    return positive_amplitude(X) - negative_amplitude(X)

def amplitude(X):
    return np.nanmax(np.abs(X), axis=0)

def mean_distance(X):
    '''
    Compute mean distance, the mean of the absolute difference between value and mean
    '''
    return mean(np.abs(X - mean(X)), axis=0)

def accelerometer_orientation_features(X, subwins=4):
	result = []
	win_length = int(np.floor(X.shape[0] / subwins))
	for i in range(0, subwins):
		indices = range(i * win_length,min([(i + 1) * win_length, X.shape[0]-1]))
		subwin_X = X[indices,:]
		subwin_mean = np.array(np.mean(subwin_X, axis=0), dtype=np.float)
		oreintation_angles = np.arccos(subwin_mean / vector_magnitude(subwin_mean))
		result.append(np.reshape(oreintation_angles, (1, oreintation_angles.shape[0])))
	angles = np.concatenate(result, axis=0)
	median_angles = np.median(angles, axis=0)
	range_angles = np.max(angles, axis=0) - np.min(angles, axis=0)
	angle_features = np.concatenate((median_angles, range_angles))
	return angle_features


"""
=======================================================================
Frequency features
=======================================================================
"""
'''Frequency domain features for numerical time series data'''

def frequency_features(X, sr, freq_range=None, top_n_dominant = 1):
    '''compute frequency features for each axis, result will be aligned in the order of f1,f2,...,p1,p2,..,pt for each axis
    '''
    freq, Sxx = _spectrum(X, sr, freq_range)
    result = []
    if len(Sxx.shape) == 1:
        Sxx = np.reshape(Sxx, (Sxx.shape[0], 1))
    elif len(Sxx.shape) == 0:
        return result
    
    for n in range(0, Sxx.shape[1]):
        # Get dominant frequencies
        freq_peaks, Sxx_peaks = _peaks(Sxx[:,n], freq)
        result_freq = freq_peaks[0:top_n_dominant]
        result_Sxx = Sxx_peaks[0:top_n_dominant]
        if result_freq.shape[0] < top_n_dominant:
            result_freq = np.append(result_freq, np.zeros((top_n_dominant - result_freq.shape[0],)))
            result_Sxx = np.append(result_Sxx, np.zeros((top_n_dominant - result_Sxx.shape[0],)))
        # Get total power
        total_power = [np.sum(Sxx[:, n])]

        # Get power of band > 3.5Hz
        highend_power = [np.sum(Sxx[freq > 3.5, n])]
    
        result = np.concatenate((result, result_freq, result_Sxx, total_power, highend_power))
    return result

def _spectrum(X, sr, freq_range=None):
    freq, time, Sxx = signal.spectrogram(X, fs = sr, window='hamming',  nperseg=X.shape[0], noverlap=0, detrend='constant', return_onesided=True, scaling='density' , axis=0, mode='psd')
    # interpolate to get values in the freq_range
    if freq_range != None:
        interpolate_f = interpolate(freq, Sxx)
        Sxx_interpolated = interpolate_f(freq_range)
    else:
        freq_range = freq
        Sxx_interpolated = Sxx
    Sxx_interpolated = np.squeeze(Sxx_interpolated)
    return (freq_range, Sxx_interpolated)

def _peaks(y, x, sort='descend'):
    y = y.flatten()
    locs = detect_peaks(y)
    y_peaks = y[locs]
    x_peaks = x[locs]
    sorted_locs = np.argsort(y_peaks, kind='quicksort')
    if sort == 'descend':
        sorted_locs = sorted_locs[::-1][:len(sorted_locs)]
    y_sorted_peaks = y_peaks[sorted_locs]
    x_sorted_peaks = x_peaks[sorted_locs]
    return (x_sorted_peaks, y_sorted_peaks)

def enmo(X):
    return np.mean(np.clip(vector_magnitude(X) - 1, a_min=0,a_max=None))