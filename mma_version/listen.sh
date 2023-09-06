#!/bin/bash

# cleaning from old run
rm -f botoutput_err.txt botoutput.txt GW_FRB_listener.log twistd.log

# Create FRB_XMLs / GW_Avros
#   For now these are actually det within reading_writing.py 
frb_where_to_store="FRB_XMLs"
frb_save_directory="$PWD"/"$( basename "$frb_where_to_store" )"
gw_where_to_store="GW_Avros"
gw_save_directory="$PWD"/"$( basename "$gw_where_to_store" )"
echo -e "\nTemporary files to be saved to \n\t$frb_save_directory \n\t$gw_save_directory"

# Set up Chime (no -n flag so it daemonises)
echo -e "setting up CHIME/FRB twistd comet broker\n"
twistd comet --remote=chimefrb.physics.mcgill.ca --local-ivo=ivo://test_user/test \
    --cmd=./frb_listener.py

sleep 2

# A correctly running twistd broker will have a twistd.pid file during 
#   operation (also how we stop the program

# running the rest of our program
echo -e "GW listener running\n"

nohup python -u bot_general.py > botoutput.txt 2> botoutput_err.txt < /dev/null &
echo "kill _______" > save_pid.txt
echo -e "\t $!" >> save_pid.txt
echo -e "\t $(cat twistd.pid)" >> save_pid.txt
jobs -l


echo -e "\nDon't forget to kill me!"
#This saves the pid process as a text file so you can then cancel it!
#Process started on June 13 is 3165653
#Can check it also with:
	#jobs -l
	#top -u oconnorb


####################