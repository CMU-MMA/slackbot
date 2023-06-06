############################################################
# Credit for this bot goes to Ved Shah/Gautham Narayan
# https://github.com/uiucsn/GW-Bot
# Written by: Ved Shah (vedgs2@illinois.edu), Gautham Narayan (gsn@illinois.edu) and the UIUCSN team at the Gravity Collective Meeting at UCSC in April 2023
# Credit also to Charlie Kilpatrick (https://github.com/charliekilpatrick/bot)
# Edits/Modified by Brendan O'Connor in June 2023 - adapted from bot_updated.py but adding area cuts
############################################################
from hop import stream, Stream
from hop.io import StartPosition
from hop.auth import Auth
from slack import WebClient
from slack_sdk.errors import SlackApiError
from slack_token import SLACK_TOKEN
from slack_token import hop_username
from slack_token import hop_pw
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
from ligo.skymap.moc import uniq2pixarea
############################################################
# Run from environment gw-bot
# conda activate gw-bot
# python3 bot.py 
############################################################
# Uncomment this line to get old alerts. The formatting for these can be rough so be careful.
# This is a way to test the slackbot works if no alerts are currently being sent! But in general, turn it off..... (i.e., comment the line out.)
#stream = Stream(start_at=StartPosition.EARLIEST)

#Another test using Auth
#auth = Auth(hop_username, hop_pw)
#stream = Stream(start_at=StartPosition.EARLIEST)
#Could just change the start time instead of doing EARLIEST as a way to test specific event sorting...

#Using Auth 
auth = Auth(hop_username, hop_pw)
stream = Stream(auth=auth)

#Default/Original
#stream = Stream()
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

def area_within_probability(data, cumulative):

    data.sort('PROBDENSITY')
    data.reverse()

    total_probability = 0.0
    total_area = 0.0
    index = 0

    while total_probability < cumulative:
        area = uniq2pixarea(data['UNIQ'][index])
        total_probability += data['PROBDENSITY'][index]*area
        total_area += area
        index += 1

    # Convert to deg^2
    total_area = total_area * (180.0/np.pi)**2

    return(total_area)

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

    if record['alert_type'] == 'UPDATE':
        print("This is an update!")
        #print(record['superevent_id'], 'was retracted')
    #    return

    # Respond only to 'CBC' events. Change 'CBC' to 'Burst' to respond to
    # only unmodeled burst events.
    if record['event']['group'] != 'CBC':
        print("Not CBC - Likely Burst Event!")
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
    dist_mean = 0.0
    dist_std = 0.0
    ninety_percent_area = 0.0
    fifty_percent_area = 0.0

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

        logbci = '0.0'
        try:
            logbci  = '%7.2f'%skymap.meta["LOGBCI"]
        except:
            print('log bci not a keyword here')
    
        try:
            ninety_percent_area = area_within_probability(skymap, 0.90)
            fifty_percent_area = area_within_probability(skymap, 0.50)
            ninety_percent_area = '%7.2f'%ninety_percent_area
            fifty_percent_area = '%7.2f'%fifty_percent_area
        except:
            print("reading sky area failed...")

    best_class = most_likely_classification(record['event']['classification'])
    far = 1./record['event']['far'] / (3600.0 * 24 * 365.25)
    if far>100.0:
        far = float(str(int(np.round(far))))
    else:
        far = float('%2.4f'%far)
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
        'area90': ninety_percent_area,
        'area50': fifty_percent_area,
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
        'logbci': logbci,
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


                #if notice not none then can proceed otherwise it breaks for 'update' alerts so can't have notice handling in the initial 'if' statement......
                #Best to handle below then...

                ########
                #Need to think of a way to send the 'special alerts' - the ones that should wake us up etc.
                #If want to use @everyone then need to send to #general channel...
                # For BBH 
                #if notice['area90'] < 200 and and notice['dist_mean']} < 500:
                #   atchannel = '@everyone'

                #For NS
                #if notice['area90'] < 300 and and notice['dist_mean']} < 500:
                #    atchannel = '@everyone'

                #I mean it could be that you only try to wake up the channel for events like this (not #general but event specific)
                #But then need to consider how the waking up works

                #checking whether southern. If max prob is south good guess it is visible from south? BUT %area covered by south is better...
                #and notice['dec_deg'] < 20:
                ########


                if instance["alert_type"] != "RETRACTION":

                    print("Not a retraction...")

                    try:

                        print("Trying...")

                        message_text = None
                        
                        # Setting some preliminary thresholds so that the channel does not get flooded with bad alerts. Adapt based on needs.
                        # Starting with only significant NS and not mock event as the only threshold.
                        if instance['event']['classification']['BNS'] > 0.2 and instance['event']['properties']['HasRemnant'] > 0.015 and instance['event']['classification']['terrestrial'] < 0.2 and instance['event']['significant'] == True and instance['superevent_id'][0] != 'M':

                            print("NSNS")
                            #print(instance)

                            notice = parse_notice(message.content[0])

                            if notice is not None:
                                print("area90 =" + str(notice['area90']))
                            #print(notice)
                            #print('\n\n')
                            #print(notice['event_type'])
                            #print('\n\n')

                            ########

                            #Auto run gwemopt and download bayestar skymap???
                            #Needs GPS TIME - not really as can now just run based on a gracedb name after Robert's code change
                            print("\n\n TO DO: Auto-run gwemopt and auto-create DECam JSON File \n\n")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            ########

                            if notice is None and instance["alert_type"] != "UPDATE":

                                print('notice issue, sending less information')

                                # Creating the message text
                                message_text = f"@Channel \n \
                                Superevent ID: *{instance['superevent_id']}*\n \
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

                            elif instance["alert_type"] == "UPDATE" and float(notice['area90']) < 600:

                                print('This is an update alert, sending less information.')

                                img_link1a = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/Bilby.png"
                                img_link2a = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/Bilby.volume.png"
                                img_link3a = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/Bilby.multiorder.fits"
                                

                                # Creating the message text
                                message_text = f"@Channel \
                                \n\n *This is an update alert. Use bilby skymap because better. * \n\n \
                                Superevent ID: *{instance['superevent_id']}*\n \
                                Significant detection? {instance['event']['significant']} \n \
                                Group: {instance['event']['group']} \n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Bayestar Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Bilby Skymap Image: {img_link1a} \n \
                                Bilby Volume Image: {img_link2a} \n \
                                Bilby Skymap Download Link (Click to download): {img_link3a} \n \
                                Treasure Map Link: {img_link4} \n \
                                "

                            elif instance["alert_type"] != "UPDATE" and float(notice['area90']) < 600:

                                print("Notice passes all checks - sending more details:")

                                if notice['external'] != None:
                                    ext = 'THERE WAS AN EXTERNAL DETECTION!! RAPID RESPONSE REQUIRED!!'
                                    joint_far = 1/notice['external']['time_sky_position_coincidence_far'] / (3600.0 * 24 * 365.25)
                                    ext_details = f"Observatory: {notice['external']['observatory']}, time_difference: {notice['external']['time_difference']} seconds, search:  {notice['external']['search']}, joint FAR: 1 per {joint_far} years"
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
                                message_text = f"@channel \n\
                                *Superevent ID: {instance['superevent_id']}* \n \
                                Event Time: {notice['event_time']} \n \
                                Notice Time: {instance['time_created']} \n \
                                Event Type: {notice['event_type']}\n \
                                Alert Type: {notice['alert_type']}\n \
                                Group: {instance['event']['group']} \n \
                                FAR: 1 per {notice['far']} years \n \
                                log BCI: {notice['logbci']} \n \
                                90% Area: *{notice['area90' ]}* \n \
                                50% Area: *{notice['area50' ]}* \n \
                                Significant detection? *{instance['event']['significant']}* \n \
                                Classification Probabilities: {notice['probabilities']}\n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Most Likely Classification: {notice['best_class']}\n \
                                Has_NS: *{notice['has_ns' ]}* \n \
                                Has_Remnant: *{notice['has_remnant' ]}* \n \
                                Has_Mass_Gap: {notice['has_gap']}\n \
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
                                if message_text is not None:
                                    print("Trying to create a new channel...", end='')
                                    response = client.conversations_create(name=new_channel_name, token = SLACK_TOKEN)
                                    name_taken = False
                                    print(response)
                                    print("Done")
                            except SlackApiError as e:
                                if e.response["error"] == "name_taken":
                                    print("Done")
                                    name_taken = True
                                else:
                                    print("\nCould not create new channel. Error: ", e.response["error"])

                            # This is a message without buttons and stuff. We are assuming #alert-bot-test already exists and the bot is added to it
                            # If it fails, create #alert-bot-test or similar channel and BE SURE to add the slack bot app to that channel or it cannot send a message to it!
                            try:
                                print("Trying to send message to ns channel...", end='')
                                if name_taken == False:
                                    if message_text is not None:
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

                        elif instance['event']['classification']['NSBH'] > 0.2 and instance['event']['properties']['HasRemnant'] > 0.015 and instance['event']['classification']['terrestrial'] < 0.2 and instance['event']['significant'] == True and instance['superevent_id'][0] != 'M':

                            print("NSBH")
                            #print(instance)

                            notice = parse_notice(message.content[0])

                            if notice is not None:
                                print("area90 =" + str(notice['area90']))
                            #print(notice)
                            #print('\n\n')
                            #print(notice['event_type'])
                            #print('\n\n')

                            ########

                            #Auto run gwemopt and download bayestar skymap???
                            #Needs GPS TIME
                            print("\n\n TO DO: Auto run gwemopt and create DECam JSON File \n\n")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            ########

                            if notice is None and instance["alert_type"] != "UPDATE":

                                print('notice issue, sending less information')

                                # Creating the message text
                                message_text = f"@channel \n \
                                Superevent ID: *{instance['superevent_id']}*\n \
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

                            elif instance["alert_type"] == "UPDATE" and float(notice['area90']) < 600:

                                print('This is an update alert, sending less information.')

                                img_link1a = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/Bilby.png"
                                img_link2a = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/Bilby.volume.png"
                                img_link3a = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/Bilby.multiorder.fits"
                                

                                # Creating the message text
                                message_text = f"@channel \
                                \n\n *This is an update alert. Use bilby skymap because better. * \n\n \
                                Superevent ID: *{instance['superevent_id']}*\n \
                                Significant detection? {instance['event']['significant']} \n \
                                Group: {instance['event']['group']} \n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Bayestar Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Bilby Skymap Image: {img_link1a} \n \
                                Bilby Volume Image: {img_link2a} \n \
                                Bilby Skymap Download Link (Click to download): {img_link3a} \n \
                                Treasure Map Link: {img_link4} \n \
                                "

                            elif instance["alert_type"] != "UPDATE" and float(notice['area90']) < 600:

                                print("Notice passes all checks - sending more details:")

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
                                message_text = f"@channel \n \
                                *Superevent ID: {instance['superevent_id']}* \n \
                                Event Time {notice['event_time']} \n \
                                Notice Time {instance['time_created']} \n \
                                Event Type: {notice['event_type']}\n \
                                Alert Type: {notice['alert_type']}\n \
                                Group: {instance['event']['group']} \n \
                                FAR: 1 per {notice['far']} years \n \
                                log BCI: {notice['logbci']} \n \
                                90% Area: *{notice['area90' ]}* \n \
                                50% Area: *{notice['area50' ]}* \n \
                                Significant detection? *{instance['event']['significant']}* \n \
                                Classification Probabilities: {notice['probabilities']}\n \
                                BNS % : {instance['event']['classification']['BNS']}\n \
                                NSBH % : {instance['event']['classification']['NSBH']}\n \
                                Most Likely Classification: {notice['best_class']}\n \
                                Has_NS: *{notice['has_ns' ]}* \n \
                                Has_Remnant: *{notice['has_remnant' ]}* \n \
                                Has_Mass_Gap: {notice['has_gap']}\n \
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
                            if message_text is not None:
                                try:
                                    print("Trying to create a new channel...", end='')
                                    response = client.conversations_create(name=new_channel_name, token = SLACK_TOKEN)
                                    name_taken = False
                                    print(response)
                                    print("Done")
                                except SlackApiError as e:
                                    if e.response["error"] == "name_taken":
                                        name_taken = True
                                        print("Done")
                                    else:
                                        print("\nCould not create new channel. Error: ", e.response["error"])
                                try:
                                    print("Trying to send message to nsbh channel...", end='')
                                    if name_taken == False:
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


                        elif instance['event']['classification']['BBH'] > 0.7 and instance['event']['significant'] == True and instance['superevent_id'][0] != 'M':

                            print("BBH")
                            #print(instance)
                            #print(message.content[0])

                            new_channel_name = '#bbh-alert'


                            print("Parsing the notice")
                            notice = parse_notice(message.content[0])
                            print("Parsed the notice properly")

                            if notice is not None:
                                print("area90 =" + str(notice['area90']))
                            #print(notice)
                            #print('\n\n')
                            #print(notice['event_type'])
                            #print('\n\n')

                            ########

                            #Auto run gwemopt and download bayestar skymap???
                            #Needs GPS TIME
                            print("\n\n TO DO: Auto run gwemopt and create DECam JSON File \n\n")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            ########

                            if notice is None and instance["alert_type"] != "UPDATE":

                                print('notice issue, sending less information')

                                # Creating the message text
                                message_text = f"Superevent ID: *{instance['superevent_id']}*\n \
                                Significant detection? {instance['event']['significant']} \n \
                                Group: {instance['event']['group']} \n \
                                Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Treasure Map Link: {img_link4} \n \
                                "

                            elif instance["alert_type"] == "UPDATE" and float(notice['area90']) < 500:

                                print('This is an update alert, sending less information.')

                                img_link1a = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/Bilby.png"
                                img_link2a = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/Bilby.volume.png"
                                img_link3a = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/Bilby.multiorder.fits"
                                

                                # Creating the message text
                                message_text = f"\n\n *This is an update alert. Use bilby skymap because better. * \n\n \
                                Superevent ID: *{instance['superevent_id']}*\n \
                                Significant detection? {instance['event']['significant']} \n \
                                Group: {instance['event']['group']} \n \
                                Bayestar Skymap Image: {img_link1} \n \
                                Bayestar Volume Image: {img_link2} \n \
                                Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                                Bilby Skymap Image: {img_link1a} \n \
                                Bilby Volume Image: {img_link2a} \n \
                                Bilby Skymap Download Link (Click to download): {img_link3a} \n \
                                Treasure Map Link: {img_link4} \n \
                                "

                            elif instance["alert_type"] != "UPDATE" and float(notice['area90']) < 500:

                                print("Notice passes all checks - sending more details:")

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
                                FAR: 1 per {notice['far']} years \n \
                                log BCI: {notice['logbci']} \n \
                                90% Area: *{notice['area90' ]}* \n \
                                50% Area: *{notice['area50' ]}* \n \
                                Significant detection? *{instance['event']['significant']}* \n \
                                Classification Probabilities: {notice['probabilities']}\n \
                                Most Likely Classification: {notice['best_class']}\n \
                                Has_NS: *{notice['has_ns' ]}* \n \
                                Has_Remnant: *{notice['has_remnant' ]}* \n \
                                Has_Mass_Gap: {notice['has_gap']}\n \
                                Distance (Mpc): *{notice['dist_mean']} with error {notice['dist_std']}* \n \
                                Detection pipeline: {notice['pipe']}\n \
                                Detection instruments: {notice['inst']}\n \
                                Any external detection: {ext}\n \
                                External Detection Details: {ext_details} \n \
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
                            if message_text is not None:
                                try:
                                    print("Trying to send message to bbh channel...", end='')
                                    response = client.chat_postMessage(channel='#bbh-alert', text=message_text)
                                    print("Done")
                                except SlackApiError as e:
                                    print("\nCould not post message. Error: ", e.response["error"])
                                
                        else: 

                            print("Ignoring this event - does not fit any of our criteria.")
                
                    except KeyError:
                        print('Bad data formatting...skipping message')         

                #BURST EVENT
                elif instance['superevent_id'][0] != 'M' and instance['alert_type'] != 'RETRACTION':

                    try:

                        if instance['event']['group'] != 'CBC' and instance['event']['significant'] == True:

                            print("burst")

                            new_channel_name = '#burst-alert'

                            print("TO DO: Area filtering for these events - and creation of similar channel for low significance events that are well localized.")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            # Creating the message text
                            message_text = f" \n\n * THIS IS A BURST EVENT - NO DISTANCE AND NO CLASSIFICATION FOR THESE UNMODELED EVENTS* \n\n \
                            Superevent ID: *{instance['superevent_id']}*\n \
                            Significant detection? {instance['event']['significant']} \n \
                            Group: {instance['event']['group']} \n \
                            Classification Probabilities: {notice['probabilities']}\n \
                            Skymap Image: {img_link1} \n \
                            Bayestar Volume Image: {img_link2} \n \
                            Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                            Treasure Map Link: {img_link4} \n \
                            "

                            try:
                                print("Trying to send message to bbh channel...", end='')
                                response = client.chat_postMessage(channel='#burst-alert', text=message_text)
                                print("Done")
                            except SlackApiError as e:
                                print("\nCould not post message. Error: ", e.response["error"])


                        elif instance['event']['group'] != 'CBC' and instance['event']['significant'] == True and instance['alert_type'] == 'UPDATE':

                            print("This is an update for a burst alert!")

                            new_channel_name = '#burst-alert'

                            print("TO DO: Area filtering for these events - and creation of similar channel for low significance events that are well localized.")

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            # Creating the message text
                            message_text = f" \n\n * THIS IS A BURST EVENT - NO DISTANCE AND NO CLASSIFICATION FOR THESE UNMODELED EVENTS* \n\n \
                            \n\n * THIS IS AN UPDATE ALERT * \n\n \
                            Superevent ID: *{instance['superevent_id']}*\n \
                            Significant detection? {instance['event']['significant']} \n \
                            Group: {instance['event']['group']} \n \
                            Skymap Image: {img_link1} \n \
                            Bayestar Volume Image: {img_link2} \n \
                            Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                            Treasure Map Link: {img_link4} \n \
                            "

                            try:
                                print("Trying to send message to bbh channel...", end='')
                                response = client.chat_postMessage(channel='#burst-alert', text=message_text)
                                print("Done")
                            except SlackApiError as e:
                                print("\nCould not post message. Error: ", e.response["error"])

                    except KeyError:
                        print('Bad data formatting...skipping message')   

                    try: 

                        printprint("Parsing the notice")
                        notice = parse_notice(message.content[0])
                        print("Parsed the notice properly")

                        if float(notice['area90']) < 150 and instance['event']['significant'] != True and instance['event']['classification']['terrestrial'] < 0.4:

                            print("Low Significance Alert")

                            new_channel_name = '#low-sig-alerts'

                            gracedb = f"https://example.org/superevents/{instance['superevent_id']}/view"
                            img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{instance['superevent_id']}/files/bayestar.png"
                            img_link2 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.volume.png"
                            img_link3 = f"https://gracedb.ligo.org/api/superevents/{instance['superevent_id']}/files/bayestar.fits.gz"
                            img_link4 = f"http://treasuremap.space/alerts?graceids={instance['superevent_id']}"

                            print("Notice passes all checks - sending more details:")

                            if notice['external'] != None:
                                ext = 'THERE WAS AN EXTERNAL DETECTION!! RAPID RESPONSE REQUIRED!!'
                                joint_far = 1/notice['external']['time_sky_position_coincidence_far'] / (3600.0 * 24 * 365.25)
                                ext_details = f"Observatory: {notice['external']['observatory']}, time_difference: {notice['external']['time_difference']} seconds, search:  {notice['external']['search']}, joint FAR:  {joint_far} years"
                            else: 
                                ext = 'None... :('
                                ext_details = 'None'

                            message_text = f"*Superevent ID: {instance['superevent_id']}* \n \
                            Event Time {notice['event_time']} \n \
                            Notice Time {instance['time_created']} \n \
                            Event Type: {notice['event_type']}\n \
                            Alert Type: {notice['alert_type']}\n \
                            Group: {instance['event']['group']} \n \
                            FAR: 1 per {notice['far']} years \n \
                            log BCI: {notice['logbci']} \n \
                            90% Area: *{notice['area90' ]}* \n \
                            50% Area: *{notice['area50' ]}* \n \
                            Significant detection? *{instance['event']['significant']}* \n \
                            Classification Probabilities: {notice['probabilities']}\n \
                            Most Likely Classification: {notice['best_class']}\n \
                            Has_NS: *{notice['has_ns' ]}* \n \
                            Has_Remnant: *{notice['has_remnant' ]}* \n \
                            Has_Mass_Gap: {notice['has_gap']}\n \
                            Distance (Mpc): *{notice['dist_mean']} with error {notice['dist_std']}* \n \
                            Detection pipeline: {notice['pipe']}\n \
                            Detection instruments: {notice['inst']}\n \
                            Any external detection: {ext}\n \
                            External Detection Details: {ext_details} \n \
                            Skymap Image: {img_link1} \n \
                            Bayestar Volume Image: {img_link2} \n \
                            Bayestar Skymap Download Link (Click to download): {img_link3} \n \
                            Treasure Map Link: {img_link4} \n \
                            "   

                            print(message_text)

                        try:
                            print("Trying to send message to bbh channel...", end='')
                            response = client.chat_postMessage(channel='#bbh-alert', text=message_text)
                            print("Done")
                        except SlackApiError as e:
                            print("\nCould not post message. Error: ", e.response["error"])
                                
                
                    except KeyError:
                        print('Bad data formatting...skipping message')      


                # RETRACTION
                else: 

                    print("This is a retraction.")

                    try:
                        print(f"Trying to send message to {new_channel_name} channel...", end='')
                        response = client.chat_postMessage(channel=f'#{new_channel_name}', text="This alert was retracted.")
                        print("Done")

                    except SlackApiError as e:
                        print("\nCould post message. Error: ", e.response["error"])

                    