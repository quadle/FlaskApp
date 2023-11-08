import requests
import json
import pandas as pd
import numpy as np
from atlassian import Jira
from navisharedcode.SQLHelper import SQLHelper 
import requests
import azure.functions as func
import logging 
from navisharedcode.Logger import Logger
from navisharedcode.helperfunctions import pretty_exception
s = SQLHelper('rpttrans')
r = SQLHelper('rpt')
p = SQLHelper('prod')

from requests.auth import HTTPBasicAuth


class ClickUp():
    def __init__(self):
        #self.access_token = self.access_token()
        self.jira = Jira(url="******",username="******",password="******",verify_ssl=False)
        
    def get_task_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_JIRA_TASKS_BINDER]')

        return dict(zip(df['JIRA_KEY'],df['CLICKUP_ID']))

    def get_user_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_USERS]')

        return dict(zip(df['NAME'],df['USER_ID']))

    def update_task(self,payload):
        user = self.get_user_bridge()
        clickup_task_id = self.task.get(payload['issue']['key'])

        #----------------------------------- FIELD --------------------------------------------------
        field = [i['fieldId'] for i in payload['changelog']['items']]

        #----------------------------------- SUBTASK --------------------------------------------------

        if field[0] == 'customfield_10020':
            s = [i['to'] for i in payload['changelog']['items']][0]
            for k in payload['issue']['fields']['issuelinks']:
                if k['type']['name'] == 'Task'and 'outwardIssue' in k.keys():
                    self.jira.update_issue_field(k['outwardIssue']['key'],{'customfield_10020': int(s)})
            pass


        #----------------------------------- RESOLUTION --------------------------------------------------


        if 'resolution' in field:
            field = [i['fieldId'] for i in payload['changelog']['items']][1]
            toString = [i['toString'] for i in payload['changelog']['items']][1]
            if toString == "In Review":
                toString = 'under review'
        #----------------------------------- priority --------------------------------------------------

        elif 'priority' in field:
            priority_dict = {"Low":"Low","Medium":"Normal","High":"high","Highest":"Urgent"}
            field = 'Priority'
            to = [i['toString'] for i in payload['changelog']['items']][0]
            toString = priority_dict.get(to)

        #----------------------------------- sprint --------------------------------------------------
        elif 'customfield' in field:
            field = [i['field'] for i in payload['changelog']['items']][0].lower().replace(" ","_")

        #----------------------------------- status --------------------------------------------------

        else:
            field = [i['fieldId'] for i in payload['changelog']['items']][0]
            toString = [i['toString'] for i in payload['changelog']['items']][0]
            if toString == "In Review":
                toString = 'under review'
        body = {field: toString}
        url = f"https://api.clickup.com/api/v2/task/{clickup_task_id}"
        response = requests.put(url, json=body, headers=self.headers)



    def add_comment(self,payload):
        user = self.get_user_bridge()
        clickup_task_id = self.task.get(payload['issue']['key'])
        url = "https://api.clickup.com/api/v2/task/" + clickup_task_id + "/comment"
        query = {
        "team_id": "****"
        }

        payload = {
        "comment_text": payload['comment']['body'],
        "assignee": user.get(payload['issue']['fields']['assignee']['displayName']),
        "notify_all": False
        }
        response = requests.post(url, json=payload, headers=self.headers, params=query)

        data = response.json()


    def payload_parser(self,payload):
        if payload['webhookEvent'] == 'comment_created':
            self.add_comment(payload)

def main(req: func.HttpRequest) -> func.HttpResponse:
    c = ClickUp()
    r = req.get_json()
    
    logging.info('JIRA UPDATE TO CLICKUP')
    logging.info("JIRA_KEY: "+ f"{r['issue']['key']}")
    logging.info("CLICK_ID: "+ f"{c.task.get(r['issue']['key'])}")
    try:
        if r['issue']['fields']['project']['key']  == 'CUT':
            if r['webhookEvent'] == 'comment_created':
                c.add_comment(r)

            if r['webhookEvent'] == 'jira:issue_updated':
                c.update_task(r)

            return func.HttpResponse('JIRA WEBHOOK WAS SENT TO CLICKUP', status_code=200)
    except Exception as e:
        click_task = c.task.get(r['issue']['key'])
        logger = Logger("clickup_testing_update")
        log_message = {
        "error": pretty_exception(e),
        "event": r['webhookEvent'],

        'jira_key':r['issue']['key'],
        'click_id':click_task}
        logger.insert_log(log_message)
        
        raise e  
    