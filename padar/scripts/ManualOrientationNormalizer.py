"""
Script to fix accelerometer orientation given a manual orientation fix file (info about axis flip or swap)

This should be ran with feature computation pipeline or after preprocessing (run on preprocessed data)

Usage:
    Production
        `mh -r . process --par --verbose --pattern SPADES_*/Derived/preprocessed/**/Actigraph*.sensor.csv ManualOrientationNormalizer --orientation_fix_file DerivedCrossParticipants/orientation_fix_map.csv --setname MON`
        `mh -r . -p SPADES_1 process --par --verbose --pattern Derived/preprocessed/**/Actigraph*.sensor.csv ManualOrientationNormalizer --orientation_fix_file DerivedCrossParticipants/orientation_fix_map.csv --setname MON`

    Debug
        `mh -r . -p SPADES_1 process --verbose --pattern Derived/preprocessed/**/Actigraph*.sensor.csv ManualOrientationNormalizer --orientation_fix_file DerivedCrossParticipants/orientation_fix_map.csv --setname MON`
"""

import os
import pandas as pd
from ..api import utils as mu
from ..api import numeric_transformation as mnt
from .BaseProcessor import SensorProcessor

def build(**kwargs):
    return ManualOrientationNormalizer(**kwargs).run_on_file

class ManualOrientationNormalizer(SensorProcessor):
    def __init__(self, verbose=True, independent=True, orientation_fix_file=None, setname='manual_orientation_normalization'):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = 'ManualOrientationNormalizer'
        self.orientation_fix_file = orientation_fix_file
        self.setname = setname

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        orientation_fix_file = self.orientation_fix_file
        if orientation_fix_file is None or orientation_fix_file == "None" or pid is None or sid is None:
            x_axis_change = "X"
            y_axis_change = "Y"
            z_axis_change = "Z"
        else:
            pid = self.meta['pid']
            sid = self.meta['sid']
            orientation_fix_map = pd.read_csv(orientation_fix_file)
            selection_mask = (orientation_fix_map.iloc[:,0] == pid) & (orientation_fix_map.iloc[:,1] == sid)
            selected_fix_map = orientation_fix_map.loc[selection_mask, :]
            if selected_fix_map.shape[0] == 1:
                x_axis_change = selected_fix_map.iloc[0, 3]
                y_axis_change = selected_fix_map.iloc[0, 4]
                z_axis_change = selected_fix_map.iloc[0, 5]
                if self.verbose:
                    print("Orientation fix: " + x_axis_change + "," + y_axis_change + "," + z_axis_change)
            else:
                if self.verbose:
                    print("Does not find orientation fix mapping info for " + str(pid) + ":" + sid)
                x_axis_change = "X"
                y_axis_change = "Y"
                z_axis_change = "Z"
        
        fixed_values = mnt.change_orientation(combined_data.values[:,1:4], x_axis_change=x_axis_change, y_axis_change=y_axis_change, z_axis_change=z_axis_change)
        result_df = combined_data.copy(deep=True)
        result_df.iloc[:,1:4] = fixed_values
        return result_df

    def _post_process(self, result_data):
        output_path = mu.generate_output_filepath(self.file, self.setname, 'sensor')
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
        result_data.to_csv(output_path, index=False, float_format='%.3f')
        if self.verbose:
            print('Saved manually orientation fixed data to ' + output_path)
        return pd.DataFrame()