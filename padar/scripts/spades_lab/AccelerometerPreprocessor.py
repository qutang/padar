"""
  preprocess pipeline for multilocation paper 2017

  1. calibration
  2. timestamp syncing
  3. clipping

  [Insert citation]
  Usage:
     Production
        `mh -r . process --par --verbose --pattern SPADES_*/MasterSynced/**/Actigraph*.sensor.csv spades_lab.AccelerometerPreprocessor --setname Preprocessed`
        `mh -r . -p SPADES_1 process --par --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv spades_lab.AccelerometerPreprocessor --setname Preprocessed`

    Debug
         `mh -r . -p SPADES_1 process --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv spades_lab.AccelerometerPreprocessor --setname Preprocessed`
"""

import os
import pandas as pd
from ... import scripts as ms
from ... import api as mhapi
from ...api import utils as mu
from .TimestampSyncer import TimestampSyncer
from ..BaseProcessor import SensorProcessor

def build(**kwargs):
    return AccelerometerProcessor(**kwargs).run_on_file

class AccelerometerProcessor(SensorProcessor):
    def __init__(self, verbose=True, independent=False, setname='Proprocessed'):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = 'AccelerometerProcessor'
        self.setname = setname

    def _build_pipeline(self):
        self.pipeline = list()
        calibrator = ms.AccelerometerCalibrator(verbose=self.verbose, independent=self.independent, static_chunk_file='DerivedCrossParticipants/static_chunks.csv')
        self.pipeline.append(calibrator)

        syncer = TimestampSyncer(verbose=self.verbose, independent=self.independent, sync_file='DerivedCrossParticipants/offset_mapping.csv')
        self.pipeline.append(syncer)

        clipper = ms.SensorClipper(verbose=self.verbose, independent=self.independent, session_file='DerivedCrossParticipants/sessions.csv')
        self.pipeline.append(clipper)

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        result_data = combined_data.copy(deep=True)
        self._build_pipeline()
        for pipe in self.pipeline:
            print('Execute ' + str(pipe) + " on file: " + self.file)
            pipe.set_meta(self.meta)
            result_data = pipe._run_on_data(result_data, data_start_indicator, data_stop_indicator)
            print(result_data.shape)
        return result_data

    def _post_process(self, result_data):
        output_file = mu.generate_output_filepath(self.file, self.setname, 'sensor')
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        if result_data.empty:
            print("result data is empty, skip saving data script")
            return pd.DataFrame()
        result_data.to_csv(output_file, index=False, float_format='%.3f')
        if self.verbose:
            print('Saved preprocessed accelerometer data to ' + output_file)
        
        # we don't need to concatenate results, so return an empty dataframe
        return pd.DataFrame()