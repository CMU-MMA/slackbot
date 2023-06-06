# GW-Bot

Slack alert bot for `LIGO 04` gravitational wave alerts via Scimma's Hopskotch. 

If you are are looking to set up the alert bot within your own workspace, follow the instructions below.

## 1. Getting started in your own workspace:

### 1.1 Set up SCiMMA / HOPSKOTCH listener

* Follow instructions to create credentials and install hop-client https://rtd.igwn.org/projects/userguide/en/v17.1/tutorial/receiving/scimma.html 
And https://github.com/scimma/hop-client/wiki/Tutorial:-using-hop-client-with-the-SCiMMA-Hopskotch-server and https://hop-client.readthedocs.io/en/stable/
* You can download the hopskotch credentials as a .csv to then pass to `hop auth` as the username and password are the credentials and not those used to register on the hopskotch website
* It seems this information from hopskotch for your credentials is only provided once so download it or save it otherwise you will just need to repeat the process. 
* To check your authorizations: `hop auth locate`
* Then need to run `hop subscribe kafka://kafka.scimma.org/igwn.gwalert`
* LVK Alerts content https://emfollow.docs.ligo.org/userguide/content.html

### 1.2 Configure Slack

* Start creating a new app here [https://api.slack.com/apps].
* Click on `Create New App`.
* Choose `From an App Manifest`.
* Choose your workspace. This is where the bot will be installed.
* Use the app manifest below in YAML format. 
* Create the app.
* Navigate to `Features` > `OAuth & Permissions` and scroll down to `OAuth Tokens for Your Workspace`. From here, you can install the app to your workspace. Once you have read through the data permissions, click allow.
* You will now see a `Bot User OAuth Token`. This is what you can use within python to access the slack api. 
* Note: I found that you need `pip3 install slack-sdk` in order for the `import slack` command to work as `pip3 install slackclient` is apparently deprecated…


#### App manifests:
```YAML
display_information:
  name: LIGO-Alert-Bot
features:
  bot_user:
    display_name: LIGO-Alert-Bot
    always_online: false
oauth_config:
  scopes:
    bot:
      - calls:read
      - channels:join
      - channels:manage
      - chat:write
      - chat:write.customize
      - commands
      - files:write
      - groups:write
      - im:write
      - mpim:write
      - channels:read
      - groups:read
settings:
  org_deploy_enabled: false
  socket_mode_enabled: false
  token_rotation_enabled: false

```

### 1.3 Configure Python 

* Create a file named `slack_token.py`. Within this file, store the `Bot User OAuth Token` in a variable called `SLACK_TOKEN`. This token will allow you to interface between python and slack. The file needs only one line which is `SLACK_TOKEN = 'xoxb-xxx-your-token-goes-here'`
* Use the `env.txt` file to recreate the python environment using conda. This can be done using `conda env create --file env.txt`.
* Activate the newly created conda environment and run `python bot_updated.py` and you should seeing the alerts as they come in.
* Note: In order for the alerts to post to slack you need the main channel to already exist and need to have actively added the slackbot app to that channel. 
* Note: The original `bot.py` developed by Ved and Gautham (see acknowledgements) is available through the SCIMMA repo and this fork was edited to add additional capabilities. This file was renamed to `bot_original.py` for record keeping purposes.

## Workflow of the Bot

*TBD*

1. ...
2. ...
3. ...

*TBD*

## Acknowledgements:

This bot was originally created as part of the collaborative efforts of the Gravity collective. If you do use this project in your work, please acknowledge the original code developers Ved Shah (vedgs2@illinois.edu), Gautham Narayan (gsn@illinois.edu), and the UIUCSN team. This repo also makes use of some code developed by Charlie Kilpatrick (https://github.com/charliekilpatrick/bot) for parsing LVK alerts. 
