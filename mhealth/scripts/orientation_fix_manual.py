"""
Script to fix accelerometer orientation given a manual orientation fix file (info about axis flip or swap)

This should be ran with feature computation pipeline or after preprocessing (run on preprocessed data)

Usage:
	Production
		`mh -r . process --par --verbose --pattern SPADES_*/Derived/preprocessed/**/Actigraph*.sensor.csv orientation_fix_manual --orientation_fix_file DerivedCrossParticipants/orientation_fix_map.csv`
		`mh -r . -p SPADES_1 process --par --verbose --pattern Derived/preprocessed/**/Actigraph*.sensor.csv orientation_fix_manual --orientation_fix_file DerivedCrossParticipants/orientation_fix_map.csv `

	Debug
		`mh -r . -p SPADES_1 process --verbose --pattern Derived/preprocessed/**/Actigraph*.sensor.csv orientation_fix_manual --orientation_fix_file DerivedCrossParticipants/orientation_fix_map.csv`
"""

import os
import pandas as pd
import mhealth.api.utils as utils
import mhealth.api.numeric_transformation as transformation

def main(file, verbose=True, orientation_fix_file=None, **kwargs):
	file = os.path.normpath(os.path.abspath(file))
	df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
	pid = int(utils.extract_pid(file).split("_")[1])
	sid = utils.extract_id(file)
	if verbose:
		print("Process " + file)
	result = run_manual_orientation_fix(df, verbose=verbose, orientation_fix_file=orientation_fix_file, pid=pid, sid=sid, **kwargs)
	if 'MasterSynced' in file:
		output_file = file.replace('MasterSynced', 'Derived/manual_orientation_fix')
	elif 'Derived' in file:
		derived_folder_name = utils.extract_derived_folder_name(file)
		output_file = file.replace(derived_folder_name, 'manual_orientation_fix')
	if not os.path.exists(os.path.dirname(output_file)):
		os.makedirs(os.path.dirname(output_file))
	result.to_csv(output_file, index=False, float_format='%.3f')
	if verbose:
		print('Saved manually orientation fixed data to ' + output_file)
		print("")
	return pd.DataFrame()

def run_manual_orientation_fix(df, verbose=True, orientation_fix_file=None, pid=None, sid=None, **kwargs):
	if orientation_fix_file is None or pid is None or sid is None:
		x_axis_change = "X"
		y_axis_change = "Y"
		z_axis_change = "Z"
	else:
		orientation_fix_map = pd.read_csv(orientation_fix_file)
		selection_mask = (orientation_fix_map.iloc[:,0] == pid) & (orientation_fix_map.iloc[:,1] == sid)
		selected_fix_map = orientation_fix_map.loc[selection_mask, :]
		if selected_fix_map.shape[0] == 1:
			x_axis_change = selected_fix_map.iloc[0, 3]
			y_axis_change = selected_fix_map.iloc[0, 4]
			z_axis_change = selected_fix_map.iloc[0, 5]
			if verbose:
				print("Orientation fix: " + x_axis_change + "," + y_axis_change + "," + z_axis_change)
		else:
			if verbose:
				print("Does not find orientation fix mapping info for SPADES_" + str(pid) + ":" + sid)
			x_axis_change = "X"
			y_axis_change = "Y"
			z_axis_change = "Z"
	
	fixed_values = transformation.change_orientation(df.values[:,1:4], x_axis_change=x_axis_change, y_axis_change=y_axis_change, z_axis_change=z_axis_change)
	result_df = df.copy(deep=True)
	result_df.iloc[:,1:4] = fixed_values
	return result_df