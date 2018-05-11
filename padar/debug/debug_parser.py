from padar.api.helpers import parser

if __name__ == '__main__':
	a = parser.parse_location_mapping("F:\data\CamSPADES_Data_original\Lab\CamSPADES_01\OriginalRaw\Actigraph\csv", location_pattern='CAMSpades_01_LAB_([A-Za-z0-9]+) \(.*\)RAW', pid_pattern='(CamSPADES_[0-9]{2})')
	print(a)