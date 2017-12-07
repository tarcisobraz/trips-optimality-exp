#!/bin/bash


FILES=/local/tarciso/masters/experiments/preliminary-exp/preliminary-exp-sample-data/buste-v3a/06_18-24_2017/buste/per_day/*
for f in $FILES
do
    filename=${f##*/} # will drop begin of string upto last occur of `SubStr`
	date=${filename%_*}  # will drop part of string from last occur of `SubStr` to the end
	formatted_date=${date//_/-}
		echo "Processing date $formatted_date ..."
    time spark-submit --driver-memory 4g /local/tarciso/masters/experiments/preliminary-exp/trips-optimality-exp/evaluate-user-trips-decisions.py $formatted_date $formatted_date  /local/tarciso/masters/experiments/preliminary-exp/preliminary-exp-sample-data/buste-v3a/06_18-24_2017/filtered_od/ /local/tarciso/masters/experiments/preliminary-exp/preliminary-exp-sample-data/buste-v3a/06_18-24_2017/buste/per_day/ http://localhost:5601/otp/ /local/tarciso/masters/experiments/preliminary-exp/trips-optimality-exp/
 
done

echo "Done!"

