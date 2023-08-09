import os
import sys
from io import BytesIO
from pprint import pprint
from time import sleep

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_token import SLACK_TOKEN

import ssl 
ssl._create_default_https_context = ssl._create_unverified_context

import log_setup
logger = log_setup.logger("slk")

def output( msg, end="\n" ):
    try:
        caller_path = sys._getframe(1).f_globals['__file__']
        caller_file = os.path.split(caller_path)[-1]
        #if called by the gw/frb code (to keep output separate from main code)
        if caller_file == "frb_listener.py" or caller_file == "gw_listener.py":
            logger.info( msg )
        else:
            print( msg, end=end )
    except ValueError or KeyError:
        print(msg, end=end)

class slack_bot:

    def __init__(self):
        self.default_channel = 'gw-frb-listener'

        self.client = WebClient( token=SLACK_TOKEN )
        self.create_new_channel( self.default_channel, setup=True )

    def name_to_id( self, name ):
        response = self.client.conversations_list(types="public_channel, private_channel" )
        for channel_dict in response["channels"]:
            if channel_dict["name"] == name:
                return channel_dict["id"]
        # will raise channel_not_found error if passed to method
        raise SlackApiError("Channel not found", {'ok': False, 'error': 'channel_not_found'})


    def create_new_channel( self, channel_name:str, setup=False ):
        if len(channel_name) == 0:
            output("Invalid Channel name: needs to be a non-empty string)")
            return
        elif channel_name[0] == '#':
            channel_name = channel_name[1:]
            if len(channel_name) == 0:
                output("Invalid Channel name: needs to be a non-empty string)")
                return
        #Create channel
        try:
            if not setup: output(f"Trying to create {channel_name}, a new channel...", end='')
            response = self.client.conversations_create(name = channel_name, token = SLACK_TOKEN)
            if not setup: output("Done")
        except SlackApiError as e:
            if e.response["error"] == "name_taken":
                if not setup: output("Done")
            elif channel_name != channel_name.lower():
                output("Please provide a name with no capital letters")
            else:
                output("\nCould not create new channel. Error: ", e.response["error"])

    def post_short_message( self, message_text, channel_name:str=None, _counter:int=0 ):
        if channel_name is None: channel_name = self.default_channel
        # This is a message without buttons and stuff. We are assuming #alert-bot-test already exists and the bot is added to it
        # If it fails, create #alert-bot-test or similar channel and BE SURE to add the slack bot app to that channel or it cannot send a message to it!
        try:
            output(f"Trying to send message to {channel_name} channel...", end='')
            response = self.client.chat_postMessage(channel=channel_name, text=message_text)
            output("Done")
        except SlackApiError as e:
            if e.response["error"] == 'channel_not_found' and _counter==0:
                self.create_new_channel(channel_name)
                self.post_short_message( channel_name, message_text, _counter=1 )
            else: 
                output("\nCould not post message. Error: ", e.response["error"])


    def post_message( self, title:str, message_text,  channel_name:str=None, _counter:int=0):
        if channel_name is None: channel_name = self.default_channel
        # This is a message with buttons and stuff. 
        # TODO: add buttons and stuff
        try:
            output(f"Trying to send message to {channel_name} event channel...",end='')
            response = self.client.chat_postMessage(
                                    channel=channel_name,
                                    token = SLACK_TOKEN,
                                    text=title,
                                    blocks = [
                                                {
                                                    "type": "section",
                                                    "text": {
                                                        "type": "mrkdwn",
                                                        "text": message_text
                                                    }
                                                }
	                                          ] )
            output("Done")
        except SlackApiError as e:
            if e.response["error"] == 'channel_not_found' and _counter==0:
                self.create_new_channel(channel_name)
                self.post_message( title, message_text, channel_name, _counter=1 )
            else: 
                output("\nCould not post message. Error: ", e.response["error"])

    def post_skymap( self, file_name, ivorn, channel_name:str=None, _counter:int=0 ):
        if channel_name is None: channel_name = self.default_channel
        try:
            output(f"Trying to send skymap to {channel_name} channel...",end='')
            event_name = os.path.split(file_name)[-1][:-4]
            response = self.client.files_upload_v2(
                channel=self.name_to_id( channel_name ),
                file=file_name,
                title="Skymap of possible coincident events",
                initial_comment=f"Skymap showing events {event_name} and {ivorn}",
                )
            output("Done")
        except SlackApiError as e:
            if e.response["error"] == 'channel_not_found' and _counter==0:
                self.create_new_channel(channel_name)
                self.post_skymap( file_name, ivorn, channel_name, _counter=1)
            elif e.response["error"] == 'missing_scope':
                output("\nPlease add the following scope authorization on the slack website: ",e.response["needed"])
            else:
                output("\nCould not post message. Error: ", e.response["error"])


    #Add more: https://slack.dev/python-slack-sdk/web/index.html