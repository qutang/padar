import os
import pandas as pd
def main(file, verbose=True, **kwargs):
    path = os.path.abspath(file)
    if verbose:
        print("process: " + path)
    f = open(path, 'rb')
    lines = sum(1 for line in f)
    f.close()
    return pd.DataFrame(data={'path': path, 'lines': lines}, index=[0])