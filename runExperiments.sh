#!/bin/bash

if [ "$#" -lt 5 ]; then
  echo "Expecting the following arguments:"
  echo "number_of_runs file_path log_output validation_size key_space_entropy"
fi

RUNS="$1"
INPUTPATH="$2"
LOGOUTPUT="$3"
VALIDATIONSIZE="$4"
KSE="$5"

exec ./execExperiments.sh $RUNS "java -jar JsonExplorer.jar $INPUTPATH merge bimax log $LOGOUTPUT val $VALIDATIONSIZE kse $KSE"
