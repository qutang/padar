"""
Script to compute features (based on VM and orientation) used for posture and activity recognition in multilocation paper. 

VM features:
	preprocess: 20Hz butterworth lowpass filter
	features:
		...
		...
		...

Orientation features:
	preprocess:
		manual orientation fixing
		20Hz butterworth lowpass filter
	features:
		x,y,z median angle
		x,y,z angle range
Usage:

"""

import os
import pandas as pd

def main(file, verbose=True, prev_file=None, next_file=None, **kwargs):
	file = os.path.abspath(file)
	df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)