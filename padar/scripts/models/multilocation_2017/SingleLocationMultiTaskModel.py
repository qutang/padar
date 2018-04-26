
from ..BaseModel import BaseModel
from mhealth.api import utils as mu
import numpy as np
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.utils import shuffle

class SingleLocationMultiTaskModel(BaseModel):
    def __init__(self, verbose, feature_set, class_set):
        BaseModel.__init__(self, verbose, feature_set, class_set)
        self._name = "Single Location Multi-Task Model"
        self._description = "AR model trained on single limb location using shared knowledge to learn multiple recognitions tasks, all tasks share the same model structure"

    def _train(self, **kwargs):
        '''
            kwargs['location']: the limb location used for training
            kwargs['location_mapping_file']: the location mapping file used to find sensor ID from limb location
            kwargs['classes']: class type used for training, if multiple, separate by comma
                For example: 
                    Use "posture,activity" for training a multi-task model for both posture and activity
        '''


        fs = self._feature_set.copy(deep=True)
        cs = self._class_set.copy(deep=True)

        # location mask
        self._location = kwargs['location']
        location_mask = fs['location'] == location

        # class mask
        selected_classes = kwargs['classes'].split(',')

        # validate dataset
        train_df = fs[location_mask,:]
        class_df = cs[location_mask,:]
        if train_df.shape[0] != class_df.shape[0]:
            raise ValueError("Training data should have the same amount with class labels")
        if np.any(train_df.iloc[:,0].values != class_df.iloc[:,0].values):
            raise ValueError("Training data and class labels should be sorted and have exactly the same windows")

        # clean up dataset
        train_set = train_df.drop(columns = train_df.columns[0:2] + ['pid', 'sid', 'location']).values
        class_set = class_df.loc[:, selected_classes].values

        # preprocess traing set
        # 1. standardize
        # 2. 
        self._scaler = preprocessing.StandardScaler().fit(train_set)
        train_set = self._scaler.transform(train_set)
        
        # preprocess class set
        # 1. discard unknown
        # 2. 
        unknown_mask = np.any(class_set == 'unknown', axis=1)
        class_set = class_set[np.logical_not(unknown_mask),:]

        # randomize
        shuffled_train_set, shuffled_class_set = shuffle(train_set, class_set)

        self._paras = {
            max_depth: 10, 
            random_state: 0, 
            criterion: 'entropy', 
            n_estimators: 20, 
            n_jobs: -1, 
            verbose: 1
        }

        clf = RandomForestClassifier(**paras)
        clf.fit(shuffled_train_set, shuffled_class_set)

        self._trained_model = clf

        self._bundle = {
            'model': clf,
            'paras': self._paras,
            'scaler': self._scaler,
            'location': self._location,
            'train_set_file': self._feature_set_file,
            'class_set_file': self._class_set_file
        }

        return self
        

        
