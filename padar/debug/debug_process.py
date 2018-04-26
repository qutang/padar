
import os
import pandas as pd
from mhealth.api import M
import mhealth.scripts as scripts

def main():
    root = 'F:/data/spades_lab'
    
    test = M(root).process(rel_pattern = "SPADES_1/MasterSynced/**/*.sensor.csv*", func=scripts.clipper.main, use_parallel=True, verbose=True, start_time="2015-09-24 14:30:00.000", stop_time="2015-09-24 14:40:00.000")

    print(test)

if __name__ == '__main__':
    main()