import pandas as pd

def import_sensor_file_mhealth(filepath, verbose=False):
	df = pd.read_csv(filepath, 
		dtype=str,
		error_bad_lines=False, 
		warn_bad_lines=False, 
		skip_blank_lines=True, 
		low_memory=False,
		comment='#')
	df.iloc[:,0] = pd.to_datetime(df.iloc[:,0], infer_datetime_format=True, errors='coerce').values.astype('datetime64[ms]')
	df.iloc[:,1:] = df.iloc[:,1:].apply(pd.to_numeric, errors='coerce')
	if verbose:
		print('na rows:' + str(df.shape[0] - df.dropna().shape[0]))
	df = df.dropna()
	return df
	