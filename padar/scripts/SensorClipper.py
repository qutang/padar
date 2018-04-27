"""
Script to run on a sensor data file for clipping

Usage:
    mh -r . -p SPADES_1 process ClipProcessor --pattern Actigraph*.sensor.csv --verbose --session_file DerivedCrossParticipants/sessions.csv --setname test_processor --independent 1
"""

import os
import pandas as pd
import numpy as np
from .. import api as mhapi
from .BaseProcessor import SensorProcessor

def build(**kwargs):
    clipper = SensorClipper(**kwargs)
    return clipper.run_on_file

class SensorClipper(SensorProcessor):
    def __init__(self, verbose=True, independent=True, session_file=None, start_time=None, stop_time=None, setname='clipped'):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = 'SensorClipper'
        self.session_file = session_file
        self.start_time = start_time
        self.stop_time = stop_time
        self.setname = setname

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        if self.verbose:
            print("Start clipping data...")
            print("Original data frame period:")
            print("Start time: " + str(data_start_indicator))
            print("Stop time: " + str(data_stop_indicator))
        pid = self.meta['pid']
        start_time = self.start_time
        stop_time = self.stop_time
        session_file = self.session_file

        if start_time is None and stop_time is None:
            if session_file is not None and pid is not None:
                session_file = os.path.normpath(os.path.abspath(session_file))
                session_df = pd.read_csv(session_file, parse_dates=[0, 1], infer_datetime_format=True)
                selected_sessions = session_df.loc[session_df['pid'] == pid, :]
                if selected_sessions.shape[0] == 0:
                    start_time = None
                    stop_time = None
                else:
                    start_time = np.min(selected_sessions.iloc[:, 0])
                    stop_time = np.max(selected_sessions.iloc[:, 1])
    
        if start_time is not None:
            if type(start_time) is str:
                st = pd.to_datetime(
                    start_time, infer_datetime_format=True).to_datetime64().astype(
                        'datetime64[ms]')
            elif type(start_time) is pd.Timestamp:
                st = start_time.to_datetime64().astype('datetime64[ms]')
            else:
                raise ValueError("Unknown timestamp type: " + str(type(start_time)))
            if st < data_start_indicator:
                st = data_start_indicator
        else:
            st = start_time

        if stop_time is not None:
            if type(stop_time) is str:
                et = pd.to_datetime(
                    stop_time, infer_datetime_format=True).to_datetime64().astype(
                        'datetime64[ms]')
            elif type(start_time) is pd.Timestamp:
                et = stop_time.to_datetime64().astype('datetime64[ms]')
            else:
                raise ValueError("Unknown timestamp type: " + str(type(stop_time)))
            if et > data_stop_indicator:
                et = data_stop_indicator
        else:
            et = stop_time

        clipped_df = mhapi.clip_dataframe(combined_data, start_time=st, stop_time=et)
        if self.verbose:
            print("Finish clipping data...")
            print("Clipped data frame period:")
            print("Start time: " + str(st))
            print("Stop time: " + str(et))
        return clipped_df

    def _post_process(self, result_data):
        output_path = mhapi.generate_output_filepath(self.file, self.setname, 'sensor')
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
        result_data.to_csv(output_path, index=False, float_format='%.3f')
        if self.verbose:
            print("Saved clipped data frame to " + output_path)
        return pd.DataFrame()

