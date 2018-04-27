"""
script to synchronize timestamps for Actigraph sensors in spades lab dataset based on the offset_mapping.csv file in DerivedCrossParticipants folder

Usage:
Production:
    Whole dataset:
        `mh -r . process --verbose --par --pattern SPADES_*/MasterSynced/**/Actigraph*.sensor.csv spades_lab.TimestampSyncer --sync_file DerivedCrossParticipants/offset_mapping.csv`
    Single participant:
        `mh -r . -p SPADES_1 process --par --pattern MasterSynced/**/Actigraph*.sensor.csv spades_lab.TimestampSyncer --sync_file DerivedCrossParticipants/offset_mapping.csv`
Debug: 
    `mh -r . -p SPADES_1 process --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv spades_lab.TimestampSyncer --sync_file DerivedCrossParticipants/offset_mapping.csv`
"""

import os
import pandas as pd
import numpy as np
from ...api import utils as mu
from ..BaseProcessor import SensorProcessor

def build(**kwargs):
    return TimestampSyncer(**kwargs).run_on_file

class TimestampSyncer(SensorProcessor):
    def __init__(self, verbose=True, independent=True, sync_file=None, setname='Synced'):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = "TimestampSyncer"
        self.sync_file = sync_file
        self.setname = setname

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        result_data = combined_data.copy(deep=True)
        pid = self.meta['pid']
        sync_file = self.sync_file
        if pid is None:
            raise ValueError("You must provide a valid pid")
        
        if sync_file is not None:
            sync_file = os.path.abspath(sync_file)
            offset_mapping = pd.read_csv(sync_file)
            selected_offset = offset_mapping.loc[offset_mapping['PID'] == pid,:]
            offset = selected_offset.iloc[0,1]
        else:
            offset = 0
        
        if self.verbose:
            print("Offset is: " + str(offset) + " seconds")
        
        result_data.iloc[:,0] = result_data.iloc[:,0] + pd.to_timedelta(offset, unit='s')
        return result_data

    def _post_process(self, result_data):
        output_file = mu.generate_output_filepath(self.file, self.setname, 'sensor')
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        result_data.to_csv(output_file, index=False, float_format='%.3f')
        if self.verbose:
            print('Saved synced data to ' + output_file)
        return pd.DataFrame()

def main(file, verbose=True, sync_file=None, **kwargs):
    file = os.path.abspath(file)
    if verbose:
        print("Process " + file)
    df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
    pid = utils.extract_pid(file)
    pid = int(pid.split("_")[1])

def run_sync_timestamp(df, verbose=True, sync_file=None, pid=None):
    result = df.copy(deep=True)
    if pid is None:
        raise ValueError("You must provide a valid pid")
    if sync_file is not None:
        sync_file = os.path.abspath(sync_file)
        offset_mapping = pd.read_csv(sync_file)
        selected_offset = offset_mapping.loc[offset_mapping['PID'] == pid,:]
        offset = selected_offset.iloc[0,1]
    else:
        offset = 0
    if verbose:
        print("Offset is: " + str(offset) + " seconds")
    
    result.iloc[:,0] = result.iloc[:,0] + pd.to_timedelta(offset, unit='s')
    return result
    