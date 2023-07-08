import os
from io import BytesIO
from pprint import pprint
from time import sleep

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_token import SLACK_TOKEN

import ssl 
ssl._create_default_https_context = ssl._create_unverified_context



class slack_bot:

    def __init__(self):
        self.default_channel = 'gw-frb-listener'

        self.client = WebClient( token=SLACK_TOKEN )
        self.create_new_channel( self.default_channel )

    def name_to_id( self, name ):
        response = self.client.conversations_list(types="public_channel, private_channel" )
        for channel_dict in response["channels"]:
            if channel_dict["name"] == name:
                return channel_dict["id"]
        # will raise channel_not_found error if passed to method
        raise SlackApiError("Channel not found", {'ok': False, 'error': 'channel_not_found'})


    def create_new_channel( self, channel_name:str ):
        #Create channel
        try:
            print("Trying to create a new channel...", end='')
            response = self.client.conversations_create(name = channel_name, token = SLACK_TOKEN)
            print("Done")
        except SlackApiError as e:
            if e.response["error"] == "name_taken":
                print("Done")
            else:
                print("\nCould not create new channel. Error: ", e.response["error"])

    def post_short_message( self, channel_name, message_text, verbose:bool=False, _counter:int=0 ):
        # This is a message without buttons and stuff. We are assuming #alert-bot-test already exists and the bot is added to it
        # If it fails, create #alert-bot-test or similar channel and BE SURE to add the slack bot app to that channel or it cannot send a message to it!
        try:
            print("Trying to send message to ns channel...", end='')
            response = self.client.chat_postMessage(channel=self.name_to_id(channel_name), text=message_text)
            if verbose: print(response)
            print("Done")
        except SlackApiError as e:
            if e.response["error"] == 'channel_not_found' and _counter==0:
                self.create_new_channel(channel_name)
                self.post_short_message( channel_name, message_text, verbose, _counter=1 )
            else: 
                print("\nCould not post message. Error: ", e.response["error"])
                            


    def post_message( self, title:str, message_text,  channel_name:str, verbose:bool=False, _counter:int=0):
        #if channel_name is None: channel_name = self.default_channel
        # This is a message with buttons and stuff. 
        # TODO: add buttons and stuff
        try:
            print("Trying to send message to event channel...",end='')
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
                                                                "text": f"Some {title} related action"
                                                            },
                                                            "value": "cancel",
                                                            "action_id": "button_1"
                                                        }
                                                    ]
                                                }
	                                          ] )
            if verbose: print(response)
            print("Done")
        except SlackApiError as e:
            if e.response["error"] == 'channel_not_found' and _counter==0:
                self.create_new_channel(channel_name)
                self.post_message( title, message_text, channel_name, _counter=1 )
            else: 
                print("\nCould not post message. Error: ", e.response["error"])

    def post_skymap( self, file_name, ivorn, channel_name:str=None, _counter:int=0 ):
        if channel_name is None: channel_name = self.default_channel

        try:
            response = self.client.files_upload_v2(
                channel=self.name_to_id( channel_name ),
                file=file_name,
                title="Skymap of possible coincident events",
                initial_comment=f"Skymap showing events {file_name[:-4]} and {ivorn}",
                )
        except SlackApiError as e:
            if e.response["error"] == 'channel_not_found' and _counter==0:
                self.create_new_channel(channel_name)
                self.post_skymap( file_name, ivorn, channel_name, _counter=1)
            elif e.response["error"] == 'missing_scope':
                print("\nPlease add the following scope authorization on the slack website: ",e.response["needed"])
            else:
                print("\nCould not post message. Error: ", e.response["error"])


    #Add more: https://slack.dev/python-slack-sdk/web/index.html