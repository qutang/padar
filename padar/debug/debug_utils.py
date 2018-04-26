import mhealth.api.utils as utils

def main():
    print(utils.extract_adjacent_file("F:/data/spades_lab/SPADES_33/Derived/preprocessed/2016/03/02/11/ActigraphGT9X-AccelerationCalibrated-NA.TAS1E23150180-AccelerationCalibrated.2016-03-02-00-00-00-000-M0500.sensor.csv", side='prev'))
    print(utils.extract_adjacent_file("F:/data/spades_lab/SPADES_33/Derived/preprocessed/2016/03/02/11/ActigraphGT9X-AccelerationCalibrated-NA.TAS1E23150180-AccelerationCalibrated.2016-03-02-23-00-00-000-M0500.sensor.csv", side='next'))

if __name__ == "__main__":
    main()