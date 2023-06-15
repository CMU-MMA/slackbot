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
        self._SLACK_TOKEN = SLACK_TOKEN
        self.default_channel = 'gw-frb-listener'

        self.client = WebClient(token=SLACK_TOKEN)


    def create_new_channel( self, channel_name:str, verbose:bool=False):
        #Create channel
        try:
            print("Trying to create a new channel...", end='')
            response = self.client.conversations_create(name = channel_name, token = SLACK_TOKEN)
            if verbose: print(response)
            print("Done")
        except SlackApiError as e:
            if e.response["error"] == "name_taken":
                print("Done")
            else:
                print("\nCould not create new channel. Error: ", e.response["error"])

    def post_short_message( self, channel_name, message_text, verbose:bool=False ):
        # This is a message without buttons and stuff. We are assuming #alert-bot-test already exists and the bot is added to it
        # If it fails, create #alert-bot-test or similar channel and BE SURE to add the slack bot app to that channel or it cannot send a message to it!
        try:
            print("Trying to send message to ns channel...", end='')
            response = self.client.chat_postMessage(channel='#bns-alert', text=message_text)
            print("Done")
        except SlackApiError as e:
            print("\nCould not post message. Error: ", e.response["error"])
                            


    def post_message( self, title:str, message_text,  channel_name:str=None, verbose:bool=False, _counter:int=0):
        if channel_name is None: channel_name = self.default_channel
        # This is a message with buttons and stuff. 
        # TODO: add buttons and stuff
        try:
            '''
            print("Trying to send message to general channel...", end='')
            response = self.client.chat_postMessage(channel=channel_name, text=message_text)
            if verbose: print(response)
            print("Done")'''
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
                                                    "type": "section",
                                                    "text": {
                                                        "type": "mrkdwn",
                                                        "text": "This is a section block with a button."
                                                    },
                                                    "accessory": {
                                                        "type": "button",
                                                        "text": {
                                                            "type": "plain_text",
                                                            "text": "Click Me",
                                                            "emoji": False
                                                        },
                                                        "value": "click_me_123",
                                                        "action_id": "button-action"
                                                    }
                                                }
	                                          ] )
            if verbose: print(response)
            print("Done")
        except SlackApiError as e:
            if e.response["error"] == 'channel_not_found' and _counter==0:
                self.create_new_channel(channel_name)
                self.post_message( title, message_text, channel_name, verbose, _counter=1 )
            else: 
                print("\nCould not post message. Error: ", e.response["error"])


    #Add more: https://slack.dev/python-slack-sdk/web/index.html