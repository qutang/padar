"""
Script to apply different numerical transformation to raw sensor data
"""

import os
import pandas as pd
from ..api import filter as mf 
from ..api import utils as mu
from ..api import numeric_transformation as mnt
from .BaseProcessor import SensorProcessor

def build(**kwargs):
    return SensorTransformer(**kwargs).run_on_file

class SensorTransformer(SensorProcessor):
    def __init__(self, verbose=True, independent=True, transform='vm', setname='Transformed'):
        SensorProcessor.__init__(self, verbose=verbose, independent=independent)
        self.name = 'SensorTransformer' + "_" + transform
        self.setname = setname
		self.transform = transform

    def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
        ftype = self.ftype
        if transform == 'vm':
			vm_data = mnt.vector_magnitude(combined_data.values[:,1:4]).ravel()
			result_data = combined_data.loc[:,0].copy(deep=True)
			result_data["VM"] = vm_data
        return result_data

    def _post_process(self, result_data):
        output_file = mu.generate_output_filepath(self.file, self.setname, 'sensor', self.transform)
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        result_data.to_csv(output_file, index=False, float_format='%.3f')
        if self.verbose:
            print('Saved transformed data to ' + output_file)
        return pd.DataFrame()