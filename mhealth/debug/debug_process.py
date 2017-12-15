
import os
import pandas as pd
from mhealth.api import M

def count_lines(file, verbose=True, **kwargs):
    path = os.path.abspath(file)
    f = open(path, 'rb')
    lines = sum(1 for line in f)
    f.close()
    # print(str(kwargs['a']) + " - " + path + ": " + str(lines))
    return pd.DataFrame(data={'path': path, 'lines': lines}, index=[0])

def main():
    root = 'F:/data/spades_lab'

    test = M(root).process(rel_pattern = "SPADES_1/MasterSynced/**/*.sensor.csv*", func=count_lines, use_parallel=True, verbose=True, concat=True, a=3)

    print(test)

if __name__ == '__main__':
    main()