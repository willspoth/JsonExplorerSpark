#!/bin/bash

RUNS="$1"
PROGCMD="${@:2}"

echo ".::running $RUNS times::."
echo ".::as program: $PROGCMD::."

for ((iteration=0;iteration<$RUNS;iteration++)) # passed in by first element, number of times to run program
  do
    for ((train_perc=10;train_perc<100;train_perc+=40))
      do
        SEED="${train_perc}${iteration}"
        echo "${PROGCMD} train ${train_perc}.0 seed $SEED"
      done
  done