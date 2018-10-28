#!/bin/bash

python no_sched.py
python plot.py ondemand
python plot.py sched

rm -rf log
mkdir log
mv *.log log

