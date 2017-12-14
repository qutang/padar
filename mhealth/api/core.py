import os
import glob
import re
import pandas as pd
import numpy as np

class M:
    """[summary]
    
    [description]
    """
    
    def __init__(self, root):
        self._root = str.strip(root)

    @property
    def participants(self):
        """[summary]
        
        [description]
        
        Returns:
            [type] -- [description]
        """
        return self._get_participants()
    
    def sensors(self, pid):
        pid_folder = self._root + '/' + pid
        return self._get_sensors(pid_folder)

    def annotators(self, pid):
        pid_folder = self._root + '/' + pid
        return self._get_annotators(pid_folder)

    def folder_size(self, pid):
        total = 0
        for entry in os.scandir(self._root + '/' + pid):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += self.folder_size(entry.path)
        return total

    def total_size(self, pattern):
        selected_files = glob.glob(os.path.join(self._root, pattern), recursive=True)
        sizes = [os.path.getsize(file) for file in selected_files]
        return np.sum(sizes)

    def file_sizes(self, pattern, by):
        selected_files = glob.glob(os.path.join(self._root, pattern), recursive=True)
        hours = [self._extract_hour(file) for file in selected_files]
        dates = [self._extract_date(file) for file in selected_files]
        sizes = [os.path.getsize(file) for file in selected_files]
        size_df = pd.DataFrame({"date": dates, "hour": hours, "size": sizes}, index=None)
        if by == 'day':
            day_df = size_df.groupby(by='date')['size'].sum()
            day_df = day_df.reset_index()
            return day_df
        elif by == 'hour':
            return size_df
        else:
            raise ValueError('Unrecognized argument by: ' + by)

    def _get_annotators(self, folder):
        annotation_files = glob.glob(folder + "/**/*.annotation.csv", recursive=True)
        return set([self._extract_annotators(file) for file in annotation_files])
        
    def _get_sensors(self, folder):
        sensor_files = glob.glob(folder + "//**/*.sensor.csv", recursive=True)
        return set([self._extract_sensor_id(file) for file in sensor_files])

    def _extract_date(self, file):
        return re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}', os.path.basename(file).split('.')[2]).group(0)

    def _extract_hour(self, file):
        return re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}-([0-9]{2})', os.path.basename(file).split('.')[2]).group(1)

    def _extract_annotators(self, annotation_file):
        return os.path.basename(annotation_file).split('.')[1].split('-')[0].upper().strip()

    def _extract_sensor_id(self, sensor_file):
        return os.path.basename(sensor_file).split('.')[1].split('-')[0].upper().strip()

    def _get_participants(self):
        return [name for name in os.listdir(self._root) if os.path.isdir(os.path.join(self._root, name)) and not self._excluded_files(name)]

    def _excluded_files(self, name):
        exclude = False
        exclude = exclude or name == '.git'
        return exclude