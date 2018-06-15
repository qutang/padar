"""
Script to concatenate annotation files

Example:
	1. Concatenate all annotation files for all participants and save the results to `DerivedCrossParticipants` folder

		pad process AnnotationConcatenator -p SPADES_*/MasterSynced/**/*.annotation.csv -o DerivedCrossParticipants/SPADESInLab.annotation.csv

	2. Concatenate all annotation files for SPADES_1 and save the results to `Derived` folder of SPADES_1

		pad -p SPADES_1 process AnnotationConcatenator -p MasterSynced/**/*.annotation.csv -o Derived/SPADESInLab.annotation.csv
"""

from .BaseProcessor import AnnotationProcessor
from ..api import utils as mu
import os
import shutil
import pandas as pd

def build(**kwargs):
	return AnnotationConcatenator(**kwargs).run_on_file

class AnnotationConcatenator(AnnotationProcessor):
	def __init__(self, verbose=True, independent=True, violate=False):
		AnnotationProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
		self.name = 'AnnotationConcatenator'

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		# combined_data is actually the filename
		return combined_data

	def _post_process(self, result_data):
		result_data['protocol'] = self.meta['sensor_type']
		result_data['annotator'] = self.meta['sid']
		result_data['pid'] = self.meta['pid']
		return result_data