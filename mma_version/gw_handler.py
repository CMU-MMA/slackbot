from os import sep as SEP
from time import sleep


from slacktalker import slack_bot
from reading_writing import (
    get_file_names,
    get_sent_files,
    read_avro_file,
    write_avro_file,
    read_xml_file,
    remove_avro,
    remove_fake_avro,
    save_skymap,
    alerted_slack,
    _clear_avros
    )
from comparing_events import determine_relation

import log_setup
logger = log_setup.logger("GW_")
    
############################################################


def compare_to_frbs( message, slackbot ):
    #FRB file names
    files = get_file_names( GW=False )
    for file in files:
        match = determine_relation( message.content[0], read_xml_file(file), slackbot, logger )
        if match:
            # We alerted slack!
            alerted_slack( message.content[0]['superevent_id']+".avro", file, logger )




def deal_with_retraction( content, slackbot ):
    message = f"*RETRACTION*: Please note that *superevent_id {content['superevent_id']}* "\
                 "has been retracted; please disregard the previous message."
    did_nothing = True
    files = get_file_names( GW=True )
    for file in files:
        temp_data = read_avro_file( file )
        # Are we currently storing something that should be removed
        if content["superevent_id"] == temp_data["superevent_id"]:
            logger.info(f"Removing {content['superevent_id']} from saved events...")
            # Did we falsely alert Slack?
            alerted_slack = temp_data["alerted_slack"]
            if alerted_slack:
                slackbot.post_message(title=f"{content['superevent_id']} Retraction", message_text=message)
                remove_fake_avro(file)
            #Delete this file
            remove_avro( file )
            logger.info("Done")
            did_nothing = False
            break
    if did_nothing:
        # dealing with possible retractions within stored data
        files = get_sent_files( GW=True )
        for file in files:
            if file.split(SEP)[-1][:-5] == content['superevent_id']:
                # for file to be stored here it means the event alerted slack
                slackbot.post_message(title=f'{content["superevent_id"]} Retraction', message_text=message)
                remove_fake_avro(file)
                break
        logger.info("Did not delete anything")

def store_file( message ):
    # Always storing skymap, should overwrite if it is an update
    save_skymap( message.content[0] )
    
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
            

def main( message, slackbot ):

    #The filtering of messages is done in the got_general file that calls this,
    #   so we can immediately act on anything sent to this file

    #However frbs also have an `importance` parameter—the only broadcast events >0.9,
    #   but recommend 0.98 to avoid false positives
    #TODO: talk to Mohit about this?

    
    slackbot = slack_bot()

    logger.info("--------------------")
    logger.info(f"Received LVK Notice with superevent ID {message.content[0]['superevent_id']}")

    # Schema for data available at https://emfollow.docs.ligo.org/userguide/content.html#kafka-notice-gcn-scimma
    # or simply message.schema
    if message.content[0]['superevent_id'][0] != 'M':
        # If this is a retraction, we need to see if we still have the now invalid initial notice
        if message.content[0]["alert_type"] == "RETRACTION":
            logger.info("This is a retraction")
            #Looking for old notice, deleting if found
            deal_with_retraction( message.content[0], slackbot )
        else:
            logger.info("This is a new (or updated) event")
            # Look at current files to see if anything could be updated
            store_file( message )
            # Write to file and compare with stored FRBs
            compare_to_frbs( message, slackbot )
    else:
        logger.warning("This is a MOCK event, something is wrong...")


    sleep(0.5)


