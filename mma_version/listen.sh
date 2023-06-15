#!/bin/bash
# TODO: Add flag option to save written events

set -u

# Create FRB_XMLs / GW_Avros
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
#   operation (also how we stop the program)
if [  -e "twistd.pid" ]
then
    #Ensuring the broker is shut down, wherever the code exits
    #TODO have this close on INT?
    trap "kill $(cat twistd.pid); echo 'FRB listener closing'" EXIT
    echo "FRB listener running"
else
    echo "Your CHIME/FRB broker did not run—ensure IP address of machine is" \
        "registered"
    exit 1
fi

# running the rest of our program
echo -e "GW listener running\n"

python gw_listener.py #"$frb_save_directory"

#Need to comfirm this line print during real run
echo -e "\nGW listener closing"

# Delete FRB_XMLs / GW_Avros?


# how can gw_listener be deamonized like twistd.....