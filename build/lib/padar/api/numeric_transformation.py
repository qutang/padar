from scipy import signal
import numpy as np
from datetime import datetime
from .date_time import datetime64_to_milliseconds, datetime_to_milliseconds
from scipy.interpolate import interp1d

def as_2d_array(func):
    def wrapper(X, *args, **kwargs):
        if len(X.shape) == 1:
            X_2d = np.reshape(X, (1, X.shape[0]))
            result = func(X_2d, *args, **kwargs)
            return result[0,:]
        elif len(X.shape) == 2:
            return func(X, *args, **kwargs)
        else:
            raise NotImplementedError("Input should be 1D or 2D array")
    return wrapper

@as_2d_array
def vector_magnitude(X):
    X = X.astype(np.float64)
    result = np.sqrt(np.sum(X**2, axis=1))
    return np.reshape(result, (1, result.shape[0]))

@as_2d_array
def unitize(X):
    X_unitized = np.divide(X, vector_magnitude(X))
    return(X_unitized)

@as_2d_array
def accelerometer_orientation(X):
	return np.arccos(unitize(X))

@as_2d_array
def interpolate(X, ts, new_sr):
	'''
		Make sampling rate consistent
		ts: numpy array in unix (seconds) or datetime64 timestamps
	'''

	if isinstance(ts[0], pd.Timestamp):
		ts = pd.to_datetime(ts)

	if type(ts[0]) is datetime:
		ts_in_unix = datetime_to_milliseconds(ts)
	elif type(ts[0]) is np.datetime64:
		ts_in_unix = datetime64_to_milliseconds(ts)
	else:
		ts_in_unix = ts
	
	new_count = np.ceil(new_sr * (ts_in_unix[-1] - ts_in_unix[0]))
	new_ts_in_unix = np.linspace(ts_in_unix[0], ts_in_unix[-1], num=new_count)
	
	new_X = np.apply_along_axis(
    _interpolate_1d, axis=0, arr=X, x=ts_in_unix, new_x=new_ts_in_unix)

	return new_X, new_ts_in_unix

def _interpolate_1d(y, x, new_x):
	fitted = interp1d(x, y)
	new_y = fitted(new_x)
	return new_y

@as_2d_array
def change_orientation(X, x_axis_change = 'X', y_axis_change = 'Y', z_axis_change = 'Z'):
    X_clone = np.copy(X)
    x = np.copy(X_clone[:,0])
    y = np.copy(X_clone[:,1])
    z = np.copy(X_clone[:,2])
    if x_axis_change == 'X':
        X_clone[:,0] = x
    elif x_axis_change == '-X':
        X_clone[:,0] = -x
    elif x_axis_change == 'Y':
        X_clone[:,0] = y
    elif x_axis_change == '-Y':
        X_clone[:,0] = -y
    elif x_axis_change == 'Z':
        X_clone[:,0] = z
    elif x_axis_change == '-Z':
        X_clone[:,0] = -z
    
    if y_axis_change == 'X':
        X_clone[:,1] = x
    elif y_axis_change == '-X':
        X_clone[:,1] = -x
    elif y_axis_change == 'Y':
        X_clone[:,1] = y
    elif y_axis_change == '-Y':
        X_clone[:,1] = -y
    elif y_axis_change == 'Z':
        X_clone[:,1] = z
    elif y_axis_change == '-Z':
        X_clone[:,1] = -z

    if z_axis_change == 'X':
        X_clone[:,2] = x
    elif z_axis_change == '-X':
        X_clone[:,2] = -x
    elif z_axis_change == 'Y':
        X_clone[:,2] = y
    elif z_axis_change == '-Y':
        X_clone[:,2] = -y
    elif z_axis_change == 'Z':
        X_clone[:,2] = z
    elif z_axis_change == '-Z':
        X_clone[:,2] = -z

    return X_clone