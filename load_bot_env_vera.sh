#!/bin/bash

module purge
module load anaconda3/2020.07
source /opt/packages/anaconda3/etc/profile.d/conda.sh
conda deactivate
conda activate gw-bot


####################
#Using nohup to run in background

nohup --version

#nohup ./hello.sh 
#Send tos nohup.out and can run below command to verify
#cat nohup.out

#nohup ./hello.sh > myoutput.txt >2&1 &
#Record the ID of this so you can kill it later
#kill 2565
#if you do not save the ID then run this command to get the ID
#ps aux |grep nohup
#or
#ps -ef |grep nohup 
#or
#jobs -l #THIS IS THE BEST ONE!!

####################
#Now how to run the slackbot

#run this once
# chmod +x bot_updated_area.py

cd slackbot/

nohup python -u bot_updated_area.py > botoutput.txt 2> botoutput_err.txt < /dev/null &
echo $! > save_pid.txt

#This saves the pid process as a text file so you can then cancel it!
#Process started on June 9 is 1827243

jobs -l

####################