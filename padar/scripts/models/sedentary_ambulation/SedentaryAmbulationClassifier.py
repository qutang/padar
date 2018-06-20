from ..BaseModel import BaseModel
from ....api import utils as mu
import numpy as np
from sklearn import preprocessing
import sklearn.svm as svm
from sklearn.utils import shuffle
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import RepeatedKFold
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import accuracy_score
import scipy

def init(verbose, feature_set, class_set):
    return SedentaryAmbulationClassifier(verbose, feature_set, class_set)

class SedentaryAmbulationClassifier(BaseModel):
    def __init__(self, verbose, feature_set, class_set):
        BaseModel.__init__(self, verbose, feature_set, class_set)
        self._name = "Sedentary and ambulation classifier"
        self._description = "AR model trained on acceleromete data from single wrist or hip to classify cycling, sedentary, ambulation and others"

    def _convert_to_matrix(self, feature_df, class_df):
        if feature_df.shape[0] != class_df.shape[0]:
            raise ValueError("Training data should have the same amount with class labels")
        if np.any(feature_df.iloc[:,0].values != class_df.iloc[:,0].values):
            raise ValueError("Training data and class labels should be sorted and have exactly the same windows")

        # clean up dataset
        feature_set = feature_df.drop(labels = feature_df.columns[0:2].tolist() + ['pid', 'sid', 'location'], axis=1).values
        class_set = class_df.loc[:, "four_classes"].values

        # 1. discard unknown and transition
        unknown_mask = np.logical_or(class_set == 'unknown', class_set == 'transition')
        class_set = class_set[np.logical_not(unknown_mask)]
        feature_set = feature_set[np.logical_not(unknown_mask),:]
        return feature_set, class_set

    def _get_feature_names(feature_df):
        return feature_df.drop(labels = feature_df.columns[0:2].tolist() + ['pid', 'sid', 'location'], axis=1).columns

    def _preprocess_set(self, train_set, class_set):
        self._scaler = preprocessing.MinMaxScaler((-1, 1))
        train_set = self._scaler.fit_transform(train_set)
    
        # randomize
        shuffled_train_set, shuffled_class_set = shuffle(train_set, class_set)

        if shuffled_train_set.shape[0] != shuffled_class_set.shape[0]:
            raise ValueError("Training data should have the same amount with class labels")

        return shuffled_train_set, shuffled_class_set

    def _preprocess(self, train_df, class_df):
        train_set, class_set = self._convert_to_matrix(train_df, class_df)
        return self._preprocess_set(train_set, class_set)

    def _preprocess_test(self, test_df, **kwargs):
        if kwargs['no_groups']:
            df = test_df.copy(deep=True)
        else:
            df = test_df.copy(deep=True).drop(labels=['pid', 'sid', 'location'], axis=1)
        df = df.dropna()
        test_df = test_df.iloc[df.index,:]
        if kwargs['no_groups']:
            labels = test_df.columns[0:2].tolist()
        else:
            labels = test_df.columns[0:2].tolist() + ['pid', 'sid', 'location']
        test_set = test_df.drop(labels = labels, axis=1).values
        # preprocess test set
        # 1. standardize
        test_set = self._scaler.transform(test_set)
        return df.index, test_set

    def _cv(self, n_folds=10, n_repeats=5):
        feature_df = self._feature_set.copy(deep=True)
        class_df = self._class_set.copy(deep=True)
        feature_set, class_set = self._convert_to_matrix(feature_df, class_df)
        print('sample size: %d, %d' % (feature_set.shape[0], class_set.shape[0]))
        print('number of features: %d' % feature_set.shape[1])
        print('feature names: ')
        print(self._get_feature_names(feature_df).join('\n'))
        self._paras = {
            'C': 262.48813329580832, 
            'class_weight': None, 
            'gamma': 0.17399718482493676, 
            'kernel': 'rbf', 
            'tol': 1e-05,
            'verbose': True,
            'probability': True
        }
        
        skf = StratifiedKFold(n_splits=n_folds)
        i = 1
        for train_inds, test_inds in rkf.split(feature_set, class_set):
            print('%d fold validation' % i)
            best_clf = svm.SVC(**self._paras)
            X = feature_set.iloc[train_inds,:]
            y = class_set.iloc[train_inds]
            X_train, y_train = self._preprocess_set(X, y)
            X_test = self._scaler.transform(feature_set.iloc[test_inds,:])
            y_test = feature_set.iloc[test_inds]

            print('training sample size: %d' % X_train.shape[0])
            print('testing sample size: %d' % X_test.shape[0])

            trained = best_clf.fit(X_train, y_train)
            y_predict = trained.predict(X_test)
            print(str(i) + ' fold accuracy: ' + str(accuracy_score(y_test, y_predict)))
            i = i + 1
        return self

    def _train(self, **kwargs):
        '''
        '''
        train_df = self._feature_set.copy(deep=True)
        class_df = self._class_set.copy(deep=True)

        shuffled_train_set, shuffled_class_set = self._preprocess(train_df, class_df)        

        print(shuffled_train_set[0:10,:])
        print(shuffled_class_set[0:10])

        self._paras = {
            'C': 262.48813329580832, 
            'class_weight': None, 
            'gamma': 0.17399718482493676, 
            'kernel': 'rbf', 
            'tol': 1e-05,
            'verbose': True,
            'probability': True
        }
        if kwargs['hyper_search'] in ['True', '1']:
            tuned_parameters = {
                'C': scipy.stats.expon(scale=100), 
                'gamma': scipy.stats.expon(scale=.1),
                'kernel': ['rbf'], 
                'class_weight':['balanced', None],
                'tol': [0.00001]
            }

            clf = svm.SVC()

            scoring = 'f1_macro'

            hyper_clfs = RandomizedSearchCV(clf, tuned_parameters, cv=5, n_iter=20, scoring=scoring, n_jobs=7, verbose=3, refit=False)

            hyper_clfs.fit(shuffled_train_set, shuffled_class_set)

            print("# Tuning hyper-parameters for %s" % scoring)
            print()

            print("Best parameters set found on development set:")
            print()
            print(hyper_clfs.best_params_)
            print()
            print("Scores on development set:")
            print()
            means = hyper_clfs.cv_results_['mean_test_score']
            stds = hyper_clfs.cv_results_['std_test_score']
            for mean, std, params in zip(means, stds, hyper_clfs.cv_results_['params']):
                print("%0.3f (+/-%0.03f) for %r"
                    % (mean, std * 2, params))
            print()
            self._paras = hyper_clfs.best_params_

        best_clf = svm.SVC(**self._paras)
        best_clf.fit(shuffled_train_set, shuffled_class_set)
        self._trained_model = best_clf

        self._bundle = {
            'model': best_clf,
            'paras': self._paras,
            'scaler': self._scaler,
            'train_set_file': self._feature_set_file,
            'class_set_file': self._class_set_file
        }

        return self
        
    def _test(self, test_df, **kwargs):
        nonnull_indices, test_set = self._preprocess_test(test_df, no_groups=kwargs['no_groups'])
        
        labels = self._trained_model.classes_
        
        if kwargs['prob'] in ['True', '1', 1, True]:
            pred_prob_set = self._trained_model.predict_proba(test_set)
            pred_max_prob_set = np.max(pred_prob_set, axis=1)
            pred_set_label_indices = np.argmax(pred_prob_set, axis=1)
            pred_set = labels[pred_set_label_indices]
        else:
            pred_set = self._trained_model.predict(test_set)
        if kwargs['no_groups']:
            cols = test_df.columns[0:2].tolist()
        else:
            cols = test_df.columns[0:2].tolist() + ['pid', 'sid', 'location']
        pred_df = test_df[cols]
        pred_df = pred_df.iloc[nonnull_indices,:]
        pred_df['SEDENTARY_AMBULATION_PREDICTION'] = pred_set
        if kwargs['prob'] in ['True', '1', 1, True]:
            pred_df['SEDENTARY_AMBULATION_PREDICTION_PROB'] = pred_max_prob_set
        self._pred_df = pred_df
        return self
        

        
        
        
