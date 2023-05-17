############################################################
# Credit for this bot goes to Ved Shah/Gautham Narayan
# https://github.com/uiucsn/GW-Bot
# Written by: Ved Shah (vedgs2@illinois.edu), Gautham Narayan (gsn@illinois.edu) and the UIUCSN team at the Gravity Collective Meeting at UCSC in April 2023
# Credit also to Charlie Kilpatrick (https://github.com/charliekilpatrick/bot)
# Edits/Modified by Brendan O'Connor in May 2023
############################################################
from hop import stream, Stream
from hop.io import StartPosition
from slack import WebClient
from slack_sdk.errors import SlackApiError
from slack_token import SLACK_TOKEN
####
# Brendan needed to add this to fix an error "ssl.SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1056)"
import ssl 
ssl._create_default_https_context = ssl._create_unverified_context
####
from io import BytesIO
from pprint import pprint
from astropy.table import Table
import astropy_healpix as ah
from hop import Stream
from hop.io import StartPosition
import numpy as np
import healpy as hp
from astropy.coordinates import SkyCoord
#from ligo.skymap.moc import uniq2pixarea
############################################################
# Run from environment gw-bot
# conda activate gw-bot
# python3 bot.py 
############################################################
# Uncomment this line to get old alerts. The formatting for these can be rough so be careful.
# This is a way to test the slackbot works if no alerts are currently being sent! But in general, turn it off..... (i.e., comment the line out.)
#stream = Stream(start_at=StartPosition.EARLIEST)
############################################################
# Look into running on spin @ nersc:
# https://www.nersc.gov/systems/spin/
############################################################
#Charlie Kilpatrick's Code for Parsing the Alert (https://github.com/charliekilpatrick/bot):
def most_likely_classification(classification):

    likelihood = 0
    best_class = ''
    for key in classification.keys():
        if classification[key]>likelihood:
            best_class = key
            likelihood = classification[key]

    return(best_class)

#Had trouble installing ligo.skymap in the slackbot environment so commenting out all that stuff for now...
#def area_within_probability(data, cumulative):
#
#    data.sort('PROBDENSITY')
#    data.reverse()
#
#    total_probability = 0.0
#    total_area = 0.0
#    index = 0
#
#    while total_probability < cumulative:
#        area = uniq2pixarea(data['UNIQ'][index])
#        total_probability += data['PROBDENSITY'][index]*area
#        total_area += area
#        index += 1
#
#    # Convert to deg^2
#    total_area = total_area * (180.0/np.pi)**2
#
#    return(total_area)

def parse_notice(record):
    # Only respond to mock events. Real events have GraceDB IDs like
    # S1234567, mock events have GraceDB IDs like M1234567.
    # NOTE NOTE NOTE replace the conditional below with this commented out
    # conditional to only parse real events.
    # if record['superevent_id'][0] != 'S':
    #    return
    
    #if record['superevent_id'][0] != 'M':
    #    return

    if record['alert_type'] == 'RETRACTION':
        print(record['superevent_id'], 'was retracted')
        return

    # Respond only to 'CBC' events. Change 'CBC' to 'Burst' to respond to
    # only unmodeled burst events.
    if record['event']['group'] != 'CBC':
        return

    # Parse sky map
    if 'skymap' in record['event'].keys():
        skymap_bytes = record.get('event', {}).pop('skymap')
    else:
        skymap_bytes = None

    # Initialize map variables
    skymap = None
    ra_deg = None
    dec_deg = None
    ra_hms = None
    dec_dms = None
    dist_mean = None
    dist_std = None
    ninety_percent_area = None
    fifty_percent_area = None

    #Had trouble installing ligo.skymap in the slackbot environment so commenting out all that stuff for now...
    if skymap_bytes:
        # Parse skymap directly and print most probable sky location
        skymap = Table.read(BytesIO(skymap_bytes))
    
        level, ipix = ah.uniq_to_level_ipix(
            skymap[np.argmax(skymap['PROBDENSITY'])]['UNIQ']
        )
        ra, dec = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level),
                                       order='nested')
        coord = SkyCoord(ra, dec)
        dat = coord.to_string(style='hmsdms', sep=':', precision=2)
        ra_hms, dec_dms = dat.split()
        ra_deg = ra.deg
        dec_dec = dec.deg
    
        # Print some information from FITS header
        dist_mean = '%7.2f'%skymap.meta["DISTMEAN"]
        dist_std = '%7.2f'%skymap.meta["DISTSTD"]
        dist_mean = dist_mean.strip()
        dist_std = dist_std.strip()
    #
    #    ninety_percent_area = area_within_probability(skymap, 0.90)
    #    fifty_percent_area = area_within_probability(skymap, 0.50)
    #    ninety_percent_area = '%7.2f'%ninety_percent_area
    #    fifty_percent_area = '%7.2f'%fifty_percent_area

    best_class = most_likely_classification(record['event']['classification'])
    far = 1./record['event']['far'] / (3600.0 * 24 * 365.25)
    far = '%10.4f'%far
    event_id = record['superevent_id']
    inst = record['event']['instruments']
    pipe = record['event']['pipeline']
    time = record['event']['time']
    external = record['external_coinc']
    alert_type = record['alert_type']

    if 'HasMassGap' in record['event']['properties'].keys():
        has_gap = record['event']['properties']['HasMassGap']
    else:
        has_gap = None
    if 'HasNS' in record['event']['properties'].keys():
        has_ns = record['event']['properties']['HasNS']
    else:
        has_ns = None
    if 'HasRemnant' in record['event']['properties'].keys():
        has_remnant = record['event']['properties']['HasRemnant']
    else:
        has_remnant = None

    event_type = 'MOCK'
    if event_id[0]=='S': event_type='REAL'

    kwargs = {
        'event_id': event_id,
        'event_type': event_type,
        'alert_type': alert_type,
        'event_time': time,
        'dist_mean': dist_mean,
        'dist_std': dist_std,
        'ninety_percent_area': ninety_percent_area,
        'fifty_percent_area': fifty_percent_area,
        'has_ns': has_ns,
        'has_remnant': has_remnant,
        'has_gap': has_gap,
        'best_class': best_class,
        'probabilities': record['event']['classification'],
        'far': far,
        'ra_hms': ra_hms,
        'ra_deg': ra_deg,
        'dec_dms': dec_dms,
        'dec_deg': dec_deg,
        'pipe': pipe,
        'external': external,
        #'skymap': skymap, #Commenting out skymap for now as well
        'inst': inst,
    }

    return(kwargs)
    
############################################################


if __name__ == '__main__':

    print("\n\nYour SLACK_TOKEN: "+SLACK_TOKEN+"\n\n")

    with stream.open("kafka://kafka.scimma.org/igwn.gwalert", "r") as s:

        print("\n\nHop Skotch stream open. Creating Slack client...\n\n")
        client = WebClient(token=SLACK_TOKEN)

        for message in s:
            
            # Schema for data available at https://emfollow.docs.ligo.org/userguide/content.html#kafka-notice-gcn-scimma
            data = message.content

            print(f"====================\nIncoming alert of length {len(data)}")

            # Data is a list that can (potentially) have more than 1 element? This is inconsistent with the alert schema
            for instance in data:
                
                # Printing out the alert type and event id to std out
                print(f"{instance['alert_type']}: {instance['superevent_id']}")
                new_channel_name = instance['superevent_id'].lower()

                if instance["alert_type"] != "RETRACTION":

                    try:
                        
                        # Setting some preliminary thresholds so that the channel does not get flooded with bad alerts. Adapt based on needs.
                        # Starting with only significant NS and not mock event as the only threshold.
                        if instance['event']['classification']['BNS'] > 0.5: #and instance['event']['significant'] == True: # && instance['superevent_id'][0] != 'M':

                            print("NSNS")
                            #print(instance)

                            notice = parse_notice(message.content[0])
                            #print(notice)
                            #print('\n\n')
                            #print(notice['event_type'])
                            #print('\n\n')

                            ########

                            #TODO: Whatever processing you want. Make plots, run analysis, classify event, call other api's etc

                            #Auto run gwemopt and download bayestar skymap???
                            #Needs GPS TIME
                            print("\n\n TO DO: Auto run gwemopt and create DECam JSON File \n\n")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            ########

                            if notice is None:

                                # Creating the message text
                                message_text = f"Superevent ID: *{instance['superevent_id']}*\n \
                                Significant detection? {instance['event']['significant']} \n \
                                Group: {instance['event']['group']} \n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Join Related Channel (additional alerts for this event will be sent there): #{instance['superevent_id'].lower()} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "

                            else:

                                print("Notice passes all checks - sending more details:")

                                if notice['has_ns' ]== 1:
                                     has_ns = 'THERE IS A NEUTRON STAR!'
                                else:
                                    has_ns = 'No Neutron Star? Sad...'

                                if notice['has_remnant' ]== 1:
                                    has_rem = 'REMNANT LIKELY!'
                                else:
                                    has_rem = 'No remnant.'

                                if notice['has_gap'] == 1:
                                    has_gap = 'THIS IS A MASS GAP EVENT!'
                                else:
                                    has_gap = 'Not Mass Gap.'

                                if notice['external'] != None:
                                    ext = 'THERE WAS AN EXTERNAL DETECTION!! RAPID RESPONSE REQUIRED!!'
                                    joint_far = 1/notice['external']['time_sky_position_coincidence_far'] / (3600.0 * 24 * 365.25)
                                    ext_details = f"Observatory: {notice['external']['observatory']}, time_difference: {notice['external']['time_difference']} seconds, search:  {notice['external']['search']}, joint FAR:  {joint_far} years"
                                else: 
                                    ext = 'None... :('
                                    ext_details = 'None'

                                #print(has_gap)
                                #print('hi')
                                #print(notice['ra_dec'])
                                #print(notice['event_type'])
                                #print(f"Distance & : {notice['dist_mean']} with error {notice['dist_std']} \n")
                                #print('hi')

                                # If passes CBC cuts and mock cuts then it creates additional outputs:
                                # Creating the message text
                                message_text = f"*Superevent ID: {instance['superevent_id']}* \n \
                                Event Time {notice['event_time']} \n \
                                Notice Time {instance['time_created']} \n \
                                Event Type: {notice['event_type']}\n \
                                Alert Type: {notice['alert_type']}\n \
                                Group: {instance['event']['group']} \n \
                                FAR (years): {notice['far']}\n \
                                Significant detection? *{instance['event']['significant']}* \n \
                                Classification Probabilities: {notice['probabilities']}\n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Most Likely Classification: {notice['best_class']}\n \
                                Has_NS: *{has_ns}* \n \
                                Has_Remnant: {has_rem}\n \
                                Has_Mass_Gap: {has_gap}\n \
                                Distance (Mpc): *{notice['dist_mean']} with error {notice['dist_std']}* \n \
                                Detection pipeline: {notice['pipe']}\n \
                                Detection instruments: {notice['inst']}\n \
                                Any external detection: {ext}\n \
                                External Detection Details: {ext_details} \n \
                                Join Related Channel: #{instance['superevent_id'].lower()} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "   

                                #Likely RA (deg): {notice['ra_deg']} \n \
                                #Likely DEC (deg): {notice['dec_deg']}  \n \

                                print(message_text)


                            # This creates a new slack channel for the alert
                            try:
                                print("Trying to create a new channel...", end='')
                                response = client.conversations_create(name=new_channel_name, token = SLACK_TOKEN)
                                print(response)
                                print("Done")
                            except SlackApiError as e:
                                if e.response["error"] == "name_taken":
                                    print("Done")
                                else:
                                    print("\nCould not create new channel. Error: ", e.response["error"])

                            # # This gets the bot to join the channel
                            # try:
                            #     print("Trying to join new channel...")
                            #     response = client.conversations_join(channel = new_channel_name, token = SLACK_TOKEN)
                            #     print(response)
                            # except SlackApiError as e:
                            #     print("Could not join channel. Error: ", e.response)

                            # This is a message without buttons and stuff. We are assuming #alert-bot-test already exists and the bot is added to it
                            # If it fails, create #alert-bot-test or similar channel and BE SURE to add the slack bot app to that channel or it cannot send a message to it!
                            try:
                                print("Trying to send message to general channel...", end='')
                                response = client.chat_postMessage(channel='#bns-alert', text=message_text)
                                print("Done")
                            except SlackApiError as e:
                                print("\nCould not post message. Error: ", e.response["error"])
                            
                            # This is a message with buttons and stuff to the new channel
                            try:
                                print("Trying to send message to event channel...",end='')
                                response = client.chat_postMessage(
                                                        channel=f"#{new_channel_name}",
                                                        token = SLACK_TOKEN,
                                                        blocks = [  {"type": "section", 
                                                                    "text": {
                                                                                "type": "mrkdwn", 
                                                                                "text": message_text
                                                                                }
                                                                    },
                                                                    {
                                                                        "type": "actions",
                                                                        "block_id": "actions1",
                                                                        "elements": 
                                                                        [
                                                                            {
                                                                                "type": "button",
                                                                                "text": {
                                                                                    "type": "plain_text",
                                                                                    "text": f"Some {instance['superevent_id']} related action"
                                                                                },
                                                                                "value": "cancel",
                                                                                "action_id": "button_1"
                                                                            }
                                                                        ]
                                                                    }
                                                                    
                                                                ]
                                                        )
                                print("Done")
                            except SlackApiError as e:
                                print("\nCould not post message. Error: ", e.response["error"])

                        elif instance['event']['classification']['NSBH'] > 0.5:

                            print("NSBH")
                            #print(instance)

                            notice = parse_notice(message.content[0])
                            #print(notice)
                            #print('\n\n')
                            #print(notice['event_type'])
                            #print('\n\n')

                            ########

                            #TODO: Whatever processing you want. Make plots, run analysis, classify event, call other api's etc

                            #Auto run gwemopt and download bayestar skymap???
                            #Needs GPS TIME
                            print("\n\n TO DO: Auto run gwemopt and create DECam JSON File \n\n")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            ########

                            if notice is None:

                                # Creating the message text
                                message_text = f"Superevent ID: *{instance['superevent_id']}*\n \
                                Significant detection? {instance['event']['significant']} \n \
                                Group: {instance['event']['group']} \n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Join Related Channel (additional alerts for this event will be sent there): #{instance['superevent_id'].lower()} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "

                            else:

                                print("Notice passes all checks - sending more details:")

                                if notice['has_ns' ]== 1:
                                     has_ns = 'THERE IS A NEUTRON STAR!'
                                else:
                                    has_ns = 'No Neutron Star? Sad...'

                                if notice['has_remnant' ]== 1:
                                    has_rem = 'REMNANT LIKELY!'
                                else:
                                    has_rem = 'No remnant.'

                                if notice['has_gap'] == 1:
                                    has_gap = 'THIS IS A MASS GAP EVENT!'
                                else:
                                    has_gap = 'Not Mass Gap.'

                                if notice['external'] != None:
                                    ext = 'THERE WAS AN EXTERNAL DETECTION!! RAPID RESPONSE REQUIRED!!'
                                    joint_far = 1/notice['external']['time_sky_position_coincidence_far'] / (3600.0 * 24 * 365.25)
                                    ext_details = f"Observatory: {notice['external']['observatory']}, time_difference: {notice['external']['time_difference']} seconds, search:  {notice['external']['search']}, joint FAR:  {joint_far} years"
                                else: 
                                    ext = 'None... :('
                                    ext_details = 'None'

                                #print(has_gap)
                                #print('hi')
                                #print(notice['ra_dec'])
                                #print(notice['event_type'])
                                #print(f"Distance & : {notice['dist_mean']} with error {notice['dist_std']} \n")
                                #print('hi')

                                # If passes CBC cuts and mock cuts then it creates additional outputs:
                                # Creating the message text
                                message_text = f"*Superevent ID: {instance['superevent_id']}* \n \
                                Event Time {notice['event_time']} \n \
                                Notice Time {instance['time_created']} \n \
                                Event Type: {notice['event_type']}\n \
                                Alert Type: {notice['alert_type']}\n \
                                Group: {instance['event']['group']} \n \
                                FAR (years): {notice['far']}\n \
                                Significant detection? *{instance['event']['significant']}* \n \
                                Classification Probabilities: {notice['probabilities']}\n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Most Likely Classification: {notice['best_class']}\n \
                                Has_NS: *{has_ns}* \n \
                                Has_Remnant: {has_rem}\n \
                                Has_Mass_Gap: {has_gap}\n \
                                Distance (Mpc): *{notice['dist_mean']} with error {notice['dist_std']}* \n \
                                Detection pipeline: {notice['pipe']}\n \
                                Detection instruments: {notice['inst']}\n \
                                Any external detection: {ext}\n \
                                External Detection Details: {ext_details} \n \
                                Join Related Channel: #{instance['superevent_id'].lower()} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "   

                                #Likely RA (deg): {notice['ra_deg']} \n \
                                #Likely DEC (deg): {notice['dec_deg']}  \n \

                                print(message_text)


                            # This creates a new slack channel for the alert
                            try:
                                print("Trying to create a new channel...", end='')
                                response = client.conversations_create(name=new_channel_name, token = SLACK_TOKEN)
                                print(response)
                                print("Done")
                            except SlackApiError as e:
                                if e.response["error"] == "name_taken":
                                    print("Done")
                                else:
                                    print("\nCould not create new channel. Error: ", e.response["error"])

                            # # This gets the bot to join the channel
                            # try:
                            #     print("Trying to join new channel...")
                            #     response = client.conversations_join(channel = new_channel_name, token = SLACK_TOKEN)
                            #     print(response)
                            # except SlackApiError as e:
                            #     print("Could not join channel. Error: ", e.response)

                            # This is a message without buttons and stuff. We are assuming #alert-bot-test already exists and the bot is added to it
                            # If it fails, create #alert-bot-test or similar channel and BE SURE to add the slack bot app to that channel or it cannot send a message to it!
                            try:
                                print("Trying to send message to general channel...", end='')
                                response = client.chat_postMessage(channel='#nsbh-alert', text=message_text)
                                print("Done")
                            except SlackApiError as e:
                                print("\nCould not post message. Error: ", e.response["error"])
                            
                            # This is a message with buttons and stuff to the new channel
                            try:
                                print("Trying to send message to event channel...",end='')
                                response = client.chat_postMessage(
                                                        channel=f"#{new_channel_name}",
                                                        token = SLACK_TOKEN,
                                                        blocks = [  {"type": "section", 
                                                                    "text": {
                                                                                "type": "mrkdwn", 
                                                                                "text": message_text
                                                                                }
                                                                    },
                                                                    {
                                                                        "type": "actions",
                                                                        "block_id": "actions1",
                                                                        "elements": 
                                                                        [
                                                                            {
                                                                                "type": "button",
                                                                                "text": {
                                                                                    "type": "plain_text",
                                                                                    "text": f"Some {instance['superevent_id']} related action"
                                                                                },
                                                                                "value": "cancel",
                                                                                "action_id": "button_1"
                                                                            }
                                                                        ]
                                                                    }
                                                                    
                                                                ]
                                                        )
                                print("Done")
                            except SlackApiError as e:
                                print("\nCould not post message. Error: ", e.response["error"])


                        elif instance['event']['classification']['BBH'] > 0.8 and instance['event']['significant']:

                            print("BBH")
                            #print(instance)

                            notice = parse_notice(message.content[0])
                            #print(notice)
                            #print('\n\n')
                            #print(notice['event_type'])
                            #print('\n\n')

                            ########

                            #TODO: Whatever processing you want. Make plots, run analysis, classify event, call other api's etc

                            #Auto run gwemopt and download bayestar skymap???
                            #Needs GPS TIME
                            print("\n\n TO DO: Auto run gwemopt and create DECam JSON File \n\n")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            ########

                            if notice is None:

                                # Creating the message text
                                message_text = f"Superevent ID: *{instance['superevent_id']}*\n \
                                Significant detection? {instance['event']['significant']} \n \
                                Group: {instance['event']['group']} \n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Join Related Channel (additional alerts for this event will be sent there): #{instance['superevent_id'].lower()} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "

                            else:

                                print("Notice passes all checks - sending more details:")

                                if notice['has_ns' ]== 1:
                                     has_ns = 'THERE IS A NEUTRON STAR!'
                                else:
                                    has_ns = 'No Neutron Star? Sad...'

                                if notice['has_remnant' ]== 1:
                                    has_rem = 'REMNANT LIKELY!'
                                else:
                                    has_rem = 'No remnant.'

                                if notice['has_gap'] == 1:
                                    has_gap = 'THIS IS A MASS GAP EVENT!'
                                else:
                                    has_gap = 'Not Mass Gap.'

                                if notice['external'] != None:
                                    ext = 'THERE WAS AN EXTERNAL DETECTION!! RAPID RESPONSE REQUIRED!!'
                                    joint_far = 1/notice['external']['time_sky_position_coincidence_far'] / (3600.0 * 24 * 365.25)
                                    ext_details = f"Observatory: {notice['external']['observatory']}, time_difference: {notice['external']['time_difference']} seconds, search:  {notice['external']['search']}, joint FAR:  {joint_far} years"
                                else: 
                                    ext = 'None... :('
                                    ext_details = 'None'

                                #print(has_gap)
                                #print('hi')
                                #print(notice['ra_dec'])
                                #print(notice['event_type'])
                                #print(f"Distance & : {notice['dist_mean']} with error {notice['dist_std']} \n")
                                #print('hi')

                                # If passes CBC cuts and mock cuts then it creates additional outputs:
                                # Creating the message text
                                message_text = f"*Superevent ID: {instance['superevent_id']}* \n \
                                Event Time {notice['event_time']} \n \
                                Notice Time {instance['time_created']} \n \
                                Event Type: {notice['event_type']}\n \
                                Alert Type: {notice['alert_type']}\n \
                                Group: {instance['event']['group']} \n \
                                FAR (years): {notice['far']}\n \
                                Significant detection? *{instance['event']['significant']}* \n \
                                Classification Probabilities: {notice['probabilities']}\n \
                                Most Likely Classification: {notice['best_class']}\n \
                                Has_NS: *{has_ns}* \n \
                                Has_Remnant: {has_rem}\n \
                                Has_Mass_Gap: {has_gap}\n \
                                Distance (Mpc): *{notice['dist_mean']} with error {notice['dist_std']}* \n \
                                Detection pipeline: {notice['pipe']}\n \
                                Detection instruments: {notice['inst']}\n \
                                Any external detection: {ext}\n \
                                External Detection Details: {ext_details} \n \
                                Join Related Channel: #{instance['superevent_id'].lower()} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "   

                                #Likely RA (deg): {notice['ra_deg']} \n \
                                #Likely DEC (deg): {notice['dec_deg']}  \n \

                                print(message_text)

                            # This is a message without buttons and stuff. We are assuming #alert-bot-test already exists and the bot is added to it
                            # If it fails, create #alert-bot-test or similar channel and BE SURE to add the slack bot app to that channel or it cannot send a message to it!
                            # For BBH we are ONLY sending alerts to this channel and NOT creating an individual channel per BBH as that could get unruly...
                            try:
                                print("Trying to send message to general channel...", end='')
                                response = client.chat_postMessage(channel='#bbh-alert', text=message_text)
                                print("Done")
                            except SlackApiError as e:
                                print("\nCould not post message. Error: ", e.response["error"])


                        elif instance['event']['classification']['terrestrial'] < 0.05 and instance['event']['significant']:

                            print("Alert not terrestrial, but significant, doesn't fit our other criteria.")
                            notice = parse_notice(message.content[0])
                            #print(notice)
                            #print('\n\n')
                            #print(notice['event_type'])
                            #print('\n\n')

                            ########

                            #TODO: Whatever processing you want. Make plots, run analysis, classify event, call other api's etc

                            #Auto run gwemopt and download bayestar skymap???
                            #Needs GPS TIME
                            print("\n\n TO DO: Auto run gwemopt and create DECam JSON File \n\n")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            ########

                            if notice is None:

                                # Creating the message text
                                message_text = f"Superevent ID: *{instance['superevent_id']}*\n \
                                Significant detection? {instance['event']['significant']} \n \
                                Group: {instance['event']['group']} \n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Join Related Channel (additional alerts for this event will be sent there): #{instance['superevent_id'].lower()} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "

                            else:

                                print("Notice passes all checks - sending more details:")

                                if notice['has_ns' ]== 1:
                                     has_ns = 'THERE IS A NEUTRON STAR!'
                                else:
                                    has_ns = 'No Neutron Star? Sad...'

                                if notice['has_remnant' ]== 1:
                                    has_rem = 'REMNANT LIKELY!'
                                else:
                                    has_rem = 'No remnant.'

                                if notice['has_gap'] == 1:
                                    has_gap = 'THIS IS A MASS GAP EVENT!'
                                else:
                                    has_gap = 'Not Mass Gap.'

                                if notice['external'] != None:
                                    ext = 'THERE WAS AN EXTERNAL DETECTION!! RAPID RESPONSE REQUIRED!!'
                                    joint_far = 1/notice['external']['time_sky_position_coincidence_far'] / (3600.0 * 24 * 365.25)
                                    ext_details = f"Observatory: {notice['external']['observatory']}, time_difference: {notice['external']['time_difference']} seconds, search:  {notice['external']['search']}, joint FAR:  {joint_far} years"
                                else: 
                                    ext = 'None... :('
                                    ext_details = 'None'

                                #print(has_gap)
                                #print('hi')
                                #print(notice['ra_dec'])
                                #print(notice['event_type'])
                                #print(f"Distance & : {notice['dist_mean']} with error {notice['dist_std']} \n")
                                #print('hi')

                                # If passes CBC cuts and mock cuts then it creates additional outputs:
                                # Creating the message text
                                message_text = f"*Superevent ID: {instance['superevent_id']}* \n \
                                Event Time {notice['event_time']} \n \
                                Notice Time {instance['time_created']} \n \
                                Event Type: {notice['event_type']}\n \
                                Alert Type: {notice['alert_type']}\n \
                                Group: {instance['event']['group']} \n \
                                FAR (years): {notice['far']}\n \
                                Significant detection? *{instance['event']['significant']}* \n \
                                Classification Probabilities: {notice['probabilities']}\n \
                                Most Likely Classification: {notice['best_class']}\n \
                                Has_NS: *{has_ns}* \n \
                                Has_Remnant: {has_rem}\n \
                                Has_Mass_Gap: {has_gap}\n \
                                Distance (Mpc): *{notice['dist_mean']} with error {notice['dist_std']}* \n \
                                Detection pipeline: {notice['pipe']}\n \
                                Detection instruments: {notice['inst']}\n \
                                Any external detection: {ext}\n \
                                External Detection Details: {ext_details} \n \
                                Join Related Channel: #{instance['superevent_id'].lower()} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "   

                                #Likely RA (deg): {notice['ra_deg']} \n \
                                #Likely DEC (deg): {notice['dec_deg']}  \n \

                                print(message_text)

                            # This is a message without buttons and stuff. We are assuming #alert-bot-test already exists and the bot is added to it
                            # If it fails, create #alert-bot-test or similar channel and BE SURE to add the slack bot app to that channel or it cannot send a message to it!
                            # For these types of alerts that fail to pass other criteria but still might be interesting we just send to one channel and not create an individual one...
                            try:
                                print("Trying to send message to general channel...", end='')
                                response = client.chat_postMessage(channel='#alert-bot-test', text=message_text)
                                print("Done")
                            except SlackApiError as e:
                                print("\nCould not post message. Error: ", e.response["error"])

                        else: 

                            print("Ignoring this event - does not fit any of our criteria.")
                
                    except KeyError:
                        print('Bad data formatting...skipping message')         

                # RETRACTION
                else: 

                    """ 
                    This should archives the channel. Current method -> get list of all channels -> find id for channel name -> call archive function
                    Issue - Linear time operation in the number for channels in the workspace. We wan to avoid this. I do not have a good solution yet.
                    One possible idea is to store a hash map from super event id to channel id on our end but that does not work with dummy alerts. It
                    might work engineering run onwards. 
                    """
                    # TODO: Find O(1) method to archive channels. For now I am just sending a message that event was RETRACTED.

                    # try:
                    #     print(f"{instance['superevent_id']} was retracted. Trying to archive related channel id", end = "")
                    #     temp = "#MS230317q".lower()
                    #     channel_id = client.conversations_info(channel=temp, token=SLACK_TOKEN)['channel']['id']
                    #     print(channel_id)
                    #     try:
                    #         response  = client.conversations_archive(channel=temp)
                    #         print("Done")
                    #     except SlackApiError as e:
                    #         print("\nCould not archive channel. Error: ", e.response, response)
                    # except SlackApiError as e:
                    #         print("\nCould not find channel id. Error: ", e.response["error"])

                    try:
                        print(f"Trying to send message to {new_channel_name} channel...", end='')
                        response = client.chat_postMessage(channel=f'#{new_channel_name}', text="This alert was retracted.")
                        print("Done")
                    except SlackApiError as e:
                        print("\nCould post message. Error: ", e.response["error"])

                    

