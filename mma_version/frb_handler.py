#!/usr/bin/env python
"""
General structure from https://github.com/4pisky/fourpiskytools/blob/master/examples/process_voevent_from_stdin_1.py

Processes a received VOEvent packet.

Accept a VOEvent packet via standard input. Parse it using voeventparse,
then just print the IVORN, Author IVORN and authoring timestamp.

Can be tested at the command line by running (for example):

   cat test_packet.xml | ./frb_listener.py

"""

import sys
import six
import logging

import voeventparse

from slacktalker import slack_bot
from reading_writing import (
    get_file_names,
    get_xml_filename,
    read_xml_file,
    write_xml_file,
    read_avro_file,
    alerted_slack,
    remove_xml,
    _clear_xmls
    )
from comparing_events import determine_relation


logging.basicConfig(filename='GW_FRB_listener.log',
                    level=logging.INFO,
                    format = "%(asctime)s - %(name)s - %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S%z")
logger = logging.getLogger("FRB")
logger.handlers.append(logging.StreamHandler(sys.stdout))



###############################################################################


def compare_to_gws( event, slackbot ):
    #GW file names
    files = get_file_names( GW=True )
    for file in files:
            match = determine_relation( read_avro_file(file), event, slackbot, logger )
            if match:
                # need to note that we sent this event!
                alerted_slack( file, get_xml_filename(event.attrib["ivorn"])+".xml", logger)




def deal_with_retraction( event, slackbot ):
    files = get_file_names( GW=False )
    for file in files:
        temp_file = read_xml_file( file )
        # Are we currently storing something that should be removed
        if event.Citations.EventIVORN == temp_file.attrib["ivorn"]:
            logger.info(f"Removing {event.Citations.EventIVORN} from saved events...")
            # Did we falsely alert Slack?
            if event.Who.Description[-4:0] == "True":
                #Weird formatting of the EventIVORN so it doesn't hyperlink to nothing
                message = f'*RETRACTION*: Please note that `{"ivo://"} {str(event.Citations.EventIVORN)[6:]}` '\
                            'has been retracted; please disregard its previous message.'
                slackbot.post_message(title=f"{event.Citations.EventIVORN} Retraction", message_text=message)
            #Delete this file
            remove_xml( file )
            logger.info("Done")
            return
    logger.info("Did not delete anything")

def deal_with_update( event ):
    files = get_file_names( GW=False )
    for file in files:
        temp_file = read_xml_file( file )
        # Are we currently storing something that should be updated
        if event.element.attrib["ivorn"] == temp_file.attrib["ivorn"]:
            sent = temp_file.attri["alerted_slack"]
            remove_xml( file )
            write_xml_file( event, logger, alerted_slack=sent)
            return

###############################################################################


def main():
    if six.PY2:
        stdin = sys.stdin.read()
    else:
        # Py3:
        stdin = sys.stdin.buffer.read()
    v = voeventparse.loads(stdin)
    handle_voevent(v)
    return 0

def handle_voevent(event):
    slackbot = slack_bot()

    logger.info("--------------------")
    logger.info(f"Received VOEvent with IVORN {event.attrib['ivorn']}")

    if event.attrib["ivorn"][22:36] == "OBS-RETRACTION":
        logger.info("This is a retraction")
        #Looking for old notice, deleting if found
        deal_with_retraction( event, slackbot )
    else:
        logger.info("This is not a retraction")
        if event.attrib["role"] == "utility":
            logger.info("this is an update")
            # Look at current files to see if anything could be updated
            deal_with_update( event )
        else:
            write_xml_file( event, logger )
        # Write to file and compare with stored FRBs
        compare_to_gws( event, slackbot )


    #logger.info("At {0}, received VOEvent with IVORN {1}".format(now, ivorn))
    #logger.info("Authored by {0} at {1}".format(author_ivorn, author_timestamp))

if __name__ == '__main__':
    sys.exit(main())