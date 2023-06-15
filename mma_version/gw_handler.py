import sys
import logging
from time import sleep

from hop import Stream
from hop.io import StartPosition
from pprint import pprint
from hop.auth import Auth 

'''
from io import BytesIO
from astropy.coordinates import SkyCoord
from astropy.table import Table
import astropy_healpix as ah
from astropy import units as u
import numpy as np
'''

from slacktalker import slack_bot
from reading_writing import (
    get_file_names,
    read_avro_file,
    write_avro_file,
    read_xml_file,
    alerted_slack,
    remove_avro,
    _clear_avros
    )
from comparing_events import determine_relation

logging.basicConfig(filename='GW_FRB_listener.log',
                    level=logging.INFO,
                    format = "%(asctime)s - %(name)s - %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S%z"
                    )
logger = logging.getLogger("GW_")
logger.handlers.append(logging.StreamHandler(sys.stdout))
    
############################################################

#Significance criteria: frbs also have an `importance` parameter—the only broadcast events >0.9, but recommend 0.98 to avoid false positives
'''
if instance['event']['classification']['BNS'] > 0.5: #and instance['event']['significant'] == True: # && instance['superevent_id'][0] != 'M':
elif instance['event']['classification']['NSBH'] > 0.5:
elif instance['event']['classification']['BBH'] > 0.8 and instance['event']['significant']:
elif instance['event']['classification']['terrestrial'] < 0.05 and instance['event']['significant']:
'''


def compare_to_frbs( message, slackbot ):
    #FRB file names
    files = get_file_names( GW=False )
    for file in files:
        match = determine_relation( message.content[0], read_xml_file(file), slackbot, logger )
        if match:
            # We alerted slack!
            alerted_slack( message.content[0]['superevent_id']+".avro", file )



def deal_with_retraction( content, slackbot ):
    files = get_file_names( GW=True )
    for file in files:
        temp_data = read_avro_file( file )
        # Are we currently storing something that should be removed
        if content["superevent_id"] == temp_data["superevent_id"]:
            logger.info(f"Removing {content['superevent_id']} from saved events...")
            # Did we falsely alert Slack?
            if temp_data["alerted_slack"]:
                message = f"*RETRACTION*: Please note that *superevent_id {content['superevent_id']}* "\
                            "has been retracted; please disregard the previous message."
                slackbot.post_message(title=f"{content['superevent_id']} Retraction", message_text=message)
            #Delete this file
            remove_avro( file )
            logger.info("Done")
            return
    logger.info("Did not delete anything")

def store_file( message ):
    if message.content[0]["alert_type"] =="EARLYWARNING":
        write_avro_file( message, logger )
    else:
        files = get_file_names( GW=True )
        for file in files:
            temp_data = read_avro_file( file )
            if message.content[0]["superevent_id"] == temp_data["superevent_id"]:
                sent = temp_data["alerted_slack"]
                remove_avro( file )
                write_avro_file( message, logger, alerted_slack=sent )
                logger.info(f"UPDATED event {temp_data['superevent_id']}")
                return
        # Reach here if there is no previous version of this event
        logger.info(f"NEW WRITE of event {message.content[0]['superevent_id']}")
        write_avro_file( message, logger )
            

#if __name__ == '__main__':
def main( message ):

    slackbot = slack_bot()

    '''
    auth = Auth("mdm2-6de7a486", "IVNjs29iwjc31npXjYDBlql2GmQTKFfy")                                    
    #start_pos = StartPosition.EARLIEST                                                                  
    start_pos = StartPosition.LATEST                                                                   
    stream = Stream(auth=auth, start_at=start_pos, )#until_eos=True)                                      
    '''

    #TODO: #2 add twistd log to this logger
    logger.info("ADD TWISTD LOGGER TO THIS FILE / FORMAT")
    logger.info("************ Starting new session ************")
    '''
    try:                                                                                                
        logger.info("Type \"control C\" (^C) to end session") 
        with stream.open('kafka://kafka.scimma.org/igwn.gwalert', 'r') as s:
            logger.info("Opened Hop Stream") 

            for message in s:
    '''
    logger.info("--------------------")
    logger.info(f"Received LVK Notice with superevent ID {message.content[0]['superevent_id']}")

    # Schema for data available at https://emfollow.docs.ligo.org/userguide/content.html#kafka-notice-gcn-scimma
    # or simply message.schema
    if message.content[0]['superevent_id'][0] == 'M':
        logger.info("This is a MOCK event, still handling for now")
    else:
        logger.info("This is a REAL event")
    # If this is a retraction, we need to see if we still have the now invalid initial notice
    if message.content[0]["alert_type"] == "RETRACTION":
        logger.info("This is a retraction")
        #Looking for old notice, deleting if found
        deal_with_retraction( message.content[0], slackbot )
    else:
        logger.info("This is not a retraction")
        # Look at current files to see if anything could be updated
        store_file( message )
        # Write to file and compare with stored FRBs
        compare_to_frbs( message, slackbot )


    sleep(0.5)


