"""
  Preprocess pipeline for a set of dataset collected under mhealth convention. Supported datasets:

  1. SPADES lab
  2. SPADES 2-day
  3. CamSPADES lab
  4. CamSPADES 1-day

  Preprocess will run following operations in order:

  1. Accelerometer calibration
  2. Timestamp syncing between sensors
  3. Clipping based on session information

  Usage:
    pad -p <PID> -r <root> process -p <PATTERN> --par AccelerometerPreprocessor <options>

    options:
	
		--static_chunks <path>: the filepath (relative to root folder or absolute path) that contains the static chunks found by `StaticFinder`. If this information is not provided, calibration will be skipped.

        --offsets <path>: the filepath (relative to root folder or absolute path) that contains the offset_mapping information. If this information is not provided, timestamp syncing will be skipped.

        --sessions <path>: the filepath (relative to root folder or absolute path) that contains the sessions information found by `SessionExtractor`. If this information is not provided, clipping will be skipped.
		
		--output_folder <folder name>: the folder name that the script will save the preprocessed data to in a participant's Derived folder. User must provide this information in order to use the script.
		
	output:
		The command will not print any output to console. The command will save the preprocessed hourly files to the <output_folder>

  Examples:

	1.  Preprocess the Actigraph raw data files for participant SPADES_1 in parallel and save it to a folder named 'preprocessed' in the 'Derived' folder of SPADES_1. Timestamp syncing will be skipped.
	
    	pad -p SPADES_1 process AccelerometerPreprocessor --par -p MasterSynced/**/Actigraph*.sensor.csv --output_folder preprocessed --static_chunks SPADES_1/Derived/static_chunks.csv --sessions SPADES_1/Derived/sessions.csv

    2.  Preprocess the Actigraph raw data files for participant SPADES_1 in parallel and save it to a folder named 'preprocessed' in the 'Derived' folder of SPADES_1. Calibration will be skipped.
	
    	pad -p SPADES_1 process AccelerometerPreprocessor --par -p MasterSynced/**/Actigraph*.sensor.csv --output_folder preprocessed --offsets SPADES_1/Derived/offset_mapping.csv --sessions SPADES_1/Derived/sessions.csv  

	3.  Preprocess the Actigraph raw data files for all participants in a dataset in parallel and save it to a folder named 'preprocessed' in the 'Derived' folder of each participant

        pad process AccelerometerPreprocessor --par -p MasterSynced/**/Actigraph*.sensor.csv -output_folder preprocessed --static_chunks DerivedCrossParticipants/static_chunks.csv --offsets DerivedCrossParticipants/offset_mapping.csv --sessions DerivedCrossParticipants/sessions.csv  
"""

import os
import pandas as pd
from .. import api as mhapi
from ..api import utils as mu
from .TimestampSyncer import TimestampSyncer
from .BaseProcessor import SensorProcessor
from .AccelerometerCalibrator import AccelerometerCalibrator
from .SensorClipper import SensorClipper
from ..utility import logger

def build(**kwargs):
    return AccelerometerProcessor(**kwargs).run_on_file

class AccelerometerProcessor(SensorProcessor):
    def __init__(self, verbose=True, independent=False, violate=False, output_folder=None, static_chunks=None, offsets=None, sessions=None):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
        self.name = 'AccelerometerProcessor'
        self.output_folder = output_folder
        self.static_chunks = static_chunks
        self.offsets = offsets
        self.sessions = sessions

    def _build_pipeline(self):
        self.pipeline = list()
        if self.static_chunks is not None:
            calibrator = AccelerometerCalibrator(verbose=self.verbose, independent=self.independent, violate=self.violate, static_chunks=self.static_chunks)
            self.pipeline.append(calibrator)
        else:
            logger.warn('static chunks are not provided, skip calibration')

        if self.offsets is not None:
            syncer = TimestampSyncer(verbose=self.verbose, independent=self.independent, violate=self.violate, offsets=self.offsets)
            self.pipeline.append(syncer)
        else:
            logger.warn('offsets are not provided, skip timestamp syncing')

        if self.sessions is not None:
            clipper = SensorClipper(verbose=self.verbose, independent=self.independent, violate=self.violate, sessions=self.sessions)
            self.pipeline.append(clipper)
        else:
            logger.warn('sessions are not provided, skip clipping')
        
        if len(self.pipeline) == 0:
            logger.warn('All preprocessing operations are skipped, return the original data')

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        result_data = combined_data.copy(deep=True)
        self._build_pipeline()
        for pipe in self.pipeline:
            logger.info('Execute ' + str(pipe) + " on file: " + self.file)
            pipe.set_meta(self.meta)
            result_data = pipe._run_on_data(result_data, data_start_indicator, data_stop_indicator)
            logger.debug(result_data.shape)
        return result_data

    def _post_process(self, result_data):
        output_file = mu.generate_output_filepath(self.file, self.output_folder, 'sensor')
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        if result_data.empty:
            logger.warn("result data is empty, skip saving hourly data")
            return pd.DataFrame()
        result_data.to_csv(output_file, index=False, float_format='%.9f')
        if self.verbose:
            logger.info('Saved preprocessed accelerometer data to ' + output_file)
        
        # we don't need to concatenate results, so return an empty dataframe
        return pd.DataFrame()