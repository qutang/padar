
from ..BaseModel import BaseModel 

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

        # clean up dataset
        fs.drop(columns = fs.columns[0:2], inplace=True)
        cs.drop(columns = cs.columns[0:2], inplace=True)

        