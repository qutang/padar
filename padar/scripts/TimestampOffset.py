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
    return TimestampOffset(**kwargs).run_on_file

class TimestampOffset(SensorProcessor):
    def __init__(self, verbose=True, independent=True, violate=False, offset=0):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
        self.name = "TimestampOffset"
        self.offset = float(offset)

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        result_data = combined_data.copy(deep=True)
        
        if self.verbose:
            logger.info("Offset is: " + str(self.offset) + " seconds")
        
        result_data.iloc[:,0] = result_data.iloc[:,0] + pd.to_timedelta(self.offset, unit='s')
        return result_data

    def _post_process(self, result_data):
        return result_data
    