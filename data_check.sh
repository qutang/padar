
set -e


echo_in_green () {
    green='\e[0;32m'
    endColor='\e[0m'
    bold='\033[1m'
    endbold='\033[0m'
    echo -e $bold$green$1$endColor$endbold
}

# activate ar virtual environment

echo_in_green "Check virtual environment setting"
if [ "$CONDA_DEFAULT_ENV" = "ar" ]
then
    echo "Virtual environment $CONDA_DEFAULT_ENV is enabled"
else
    echo "Enabling virtual environment $CONDA_DEFAULT_ENV"
    source activate ar
fi

# update padar
echo_in_green "Update data checking dependencies"
read -p 'Update dependencies (y/n)?' update_dependencies
if [ "$update_dependencies" = "y" ]
then 
    echo Update padar
    pip install -e ~/Projects/python/padar > /dev/null
    echo Install/update yad
    sudo add-apt-repository ppa:webupd8team/y-ppa-manager
    sudo apt-get update
    sudo apt-get install yad
else
    echo Skip updating dependencies
fi

# select study folder
echo_in_green "Select root study folder"
root_folder=$(DISPLAY=:0 yad --file-selection --directory)
echo Root folder: $root_folder 

# prompt for pid
echo_in_green "Select participant"
echo What PID you want to run data check [TYPE and ENTER]?
read -p 'PID: ' pid
echo Selected $pid

# prompt for study session
echo_in_green "Name study session"
echo What is current study name [EX. \'CamSPADESInLab\']?
read -p 'Study name: ' study_name
echo Study name is $study_name

echo Create \'Derived\\Summarization\' folder for $pid  
mkdir -p "$root_folder/$pid/Derived/Merged"
mkdir -p "$root_folder/$pid/Derived/Summarization"

# summarize annotation
echo_in_green "Generate annotation summary"
read -p "Skip (y/n)?" skip_annotation_summary
if [ "$skip_annotation_summary" = "y" ]
then
    echo Skip annotation summary
else
    echo Concatenate annotations
    annotation_file=$study_name.annotation.csv
    pad -r $root_folder -p $pid process --pattern MasterSynced/**/*.annotation.csv --par AnnotationConcatenator > $root_folder/$pid/Derived/Merged/$annotation_file
    echo Summarize annotations
    padar describe -o $root_folder/$pid/Derived/Summarization/ $root_folder/$pid/Derived/Merged/$annotation_file
fi

# parse sensor locations
echo_in_green "Parse sensor locations"

read -p "Skip (y/n)?" skip_sensor_location_parsing
if [ "$skip_sensor_location_parsing" = "y" ]
then
    echo Skip sensor location parsing
else
    read -p 'Regexp pattern to extract location in filename: ' location_pattern
    export location_pattern
    echo Location pattern: $location_pattern
    echo Select folder that contains the original Actigraph csv files
    folder=$(DISPLAY=:0 yad --file-selection --directory --filename=$root_folder/$pid)
    padar parse_location -o $root_folder/$pid/ -l $location_pattern -p "($pid)" $folder
    cat $root_folder/$pid/location_mapping.csv | column -t -s,
fi

# summarize sensor (enmo)
echo_in_green "Generate sensor ENMO summary"
read -p "Skip (y/n)?" skip_enmo
if [ "$skip_enmo" = "y" ]
then
    echo Skip enmo
else
    read -p 'Keyword in sensor file: ' sensor_pattern
    echo Sensor file keywrod: $sensor_pattern
    echo Summarize sensors
    read -p 'Input window size (seconds): ' window_size
    sensor_file=$sensor_pattern-enmo.feature.csv

    echo Compute ENMO summary
    pad -r $root_folder -p $pid process --pattern MasterSynced/**/*$sensor_pattern*.sensor.csv --par SensorSummarizer --location_mapping_file $root_folder/$pid/location_mapping.csv --window_size $window_size > $root_folder/$pid/Derived/Summarization/$sensor_file

    echo Generate graph
    padar visualize -o $root_folder/$pid/Derived/Summarization/ $root_folder/$pid/Derived/Summarization/$sensor_file
fi

# summarize sensor (sampling rate)
echo_in_green "Generate sensor sampling rate summary"

read -p "Skip (y/n)?" skip_sampling_rate
if [ "$skip_sampling_rate" = "y" ]
then
    echo Skip summarize sampling rate
else
    read -p 'Keyword in sensor file: ' sensor_pattern
    echo Sensor file keywrod: $sensor_pattern
    echo Summarize sensors
    read -p 'Input window size (seconds): ' window_size
    sensor_file=$sensor_pattern-sr.feature.csv

    echo Compute sampling rate summary
    pad -r $root_folder -p $pid process --pattern MasterSynced/**/*$sensor_pattern*.sensor.csv --par SensorSummarizer --location_mapping_file $root_folder/$pid/location_mapping.csv --method sr --window_size $window_size > $root_folder/$pid/Derived/Summarization/$sensor_file
    echo Generate graph
    padar visualize -o $root_folder/$pid/Derived/Summarization/ $root_folder/$pid/Derived/Summarization/$sensor_file
fi