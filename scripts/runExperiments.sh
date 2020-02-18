#!/bin/bash

if [ "$#" -lt 5 ]; then
  echo "Expecting the following arguments:"
  echo "number_of_runs file_path log_output validation_size number_of_rows key_space_entropy"
fi

RUNS="$1"
INPUTPATH="$2"
LOGOUTPUT="$3"
VALIDATIONSIZE="$4"
NUMBERROWS="$5"
KSE="$6"

#exec ./execExperiments.sh $RUNS "java -jar JsonExplorer.jar $INPUTPATH merge bimax log $LOGOUTPUT val $VALIDATIONSIZE kse $KSE"
exec ./execExperiments.sh $RUNS "spark-submit --master spark://delaware.cse.buffalo.edu:7077 --class BaaziziMain --jars /local/wmspoth/countingtypeinference_2.11-1.0.jar baazizi_2.11-0.1.jar $INPUTPATH merge bimax log $LOGOUTPUT val $VALIDATIONSIZE kse $KSE numberOfRows $NUMBERROWS config spark.conf"
