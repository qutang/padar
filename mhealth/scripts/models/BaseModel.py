'''
Base class for activity recognition model
'''
import pandas as pd
from sklearn.externals import joblib
import pickle

class BaseModel:
    def __init__(self, verbose, feature_set, class_set):
        self._verbose = verbose
        self._feature_set_file = feature_set
        self._class_set_file = class_set
        self._name = 'BaseModel'
        self._description = 'Base class of activity recognition model'

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description

    def _load_set(self):
        self._feature_set = pd.read_csv(self._feature_set_file, infer_datetime_format=True, parse_dates=[0, 1])
        self._class_set = pd.read_csv(self._class_set_file, infer_datetime_format=True, parse_dates=[0, 1])

    def _train(self, **kwargs):
        self._bundle = None
        raise NotImplementedError("Must be implemented in sub classes")

    def _cv(self, n_folds=10, n_repeats=5):
        raise NotImplementedError("Must be implemented in sub classes")
    
    def train(self, **kwargs):
        self._load_set()
        self._train(**kwargs)
        return self

    def cv(self, **kwargs):
        self._load_set()
        return self._cv(**kwargs)

    def export_model(self, output_format, output):
        if output_format == 'joblib':
            joblib.dump(self._bundle, output)
        elif output_format == 'pickle':
            with open(output, 'w') as f:
                pickle.dump(self._bundle, f)

    def load_model(self, input_format, input):
        if input_format == 'joblib':
            self._bundle = joblib.load(input)
        elif input_format == 'pickle':
            with open(input, 'r') as f:
                self._bundle = pickle.load(f)
