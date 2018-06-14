"""
script to synchronize timestamps for Actigraph sensors in spades lab dataset based on the offset_mapping.csv file in DerivedCrossParticipants folder

Usage:
Production:
    Whole dataset:
        `mh -r . process --verbose --par --pattern SPADES_*/MasterSynced/**/Actigraph*.sensor.csv spades_lab.TimestampSyncer --offsets DerivedCrossParticipants/offset_mapping.csv`
    Single participant:
        `mh -r . -p SPADES_1 process --par --pattern MasterSynced/**/Actigraph*.sensor.csv spades_lab.TimestampSyncer --offsets DerivedCrossParticipants/offset_mapping.csv`
Debug: 
    `mh -r . -p SPADES_1 process --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv spades_lab.TimestampSyncer --offsets DerivedCrossParticipants/offset_mapping.csv`
"""

import os
import pandas as pd
import numpy as np
from ..api import utils as mu
from .BaseProcessor import SensorProcessor
from ..utility import logger

def build(**kwargs):
    return TimestampSyncer(**kwargs).run_on_file

class TimestampSyncer(SensorProcessor):
    def __init__(self, verbose=True, independent=True, violate=False, offsets=None, output_folder=None):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = "TimestampSyncer"
        self.offsets = offsets
        self.output_folder = output_folder

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        result_data = combined_data.copy(deep=True)
        pid = self.meta['pid']
        offsets = self.offsets
        if pid is None:
            logger.error("You must provide a valid pid")
            exit(1)
        
        if offsets is not None:
            offsets = os.path.abspath(offsets)
            offset_mapping = pd.read_csv(offsets)
            selected_offset = offset_mapping.loc[offset_mapping['PID'] == pid,:]
            offset = selected_offset.iloc[0,1]
            if self.verbose:
                logger.info("Offset is: " + str(offset) + " seconds")
        else:
            offset = 0
            logger.warn("offset_mapping file is not provided, skip timestamp syncing")
        
        result_data.iloc[:,0] = result_data.iloc[:,0] + pd.to_timedelta(offset, unit='s')
        return result_data

    def _post_process(self, result_data):
        output_file = mu.generate_output_filepath(self.file, self.output_folder, 'sensor')
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        result_data.to_csv(output_file, index=False, float_format='%.9f')
        if self.verbose:
            logger.info('Saved synced data to ' + output_file)
        return pd.DataFrame()
    