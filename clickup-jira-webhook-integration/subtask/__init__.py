
import pandas as pd
import requests
import json
from datetime import datetime
import numpy as np
from navisharedcode.SQLHelper import SQLHelper 
import requests
from requests.auth import HTTPBasicAuth
from atlassian import Jira
import logging
import azure.functions as func
import time
s = SQLHelper()
from navisharedcode.Logger import Logger
from navisharedcode.helperfunctions import pretty_exception
logger = Logger("jira_clickup_logs")
class ClickUp():
    def __init__(self):
        #self.access_token = self.access_token()
        self.jira = Jira(url="******",username="******",password="******",verify_ssl=False)
        
    def get_task_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_JIRA_TASKS_BINDER]')

        return dict(zip(df['JIRA_KEY'],df['CLICKUP_ID']))

    def get_subtask(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_JIRA_SUBTASK]')
        return dict(zip(df['JIRA_CHILD'],df['CLICK_PARENT']))

    def get_click_subtask(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_JIRA_SUBTASK]')
        return dict(zip(df['CLICK_CHILD'],df['CLICK_PARENT']))

    def associate(self,click_task,jira_key):
        # try:
        df = s.sql_table_to_df(f"select * from [dbo].[CLICKUP_JIRA_SUBTASK] where CLICK_PARENT = '{click_task}'")
        print(df)
        for parent , child in zip(df['JIRA_PARENT'],df['JIRA_CHILD']):


            link_info =         {
            "type": {"name": "Task" },
            "inwardIssue": { "key": jira_key},
            "outwardIssue": {"key": child}}
            issue_link = self.jira.create_issue_link(link_info)


def main(req: func.HttpRequest) -> func.HttpResponse:

    req = req.get_json()
    df = s.sql_table_to_df(f"select * from [dbo].[CLICKUP_JIRA_SUBTASK]")
    c = ClickUp()
    jira_key = req['issue']['key']
    time.sleep(30)
    click_task = c.get_task_bridge().get(jira_key)

    
    if click_task not in [i for i in df['CLICK_PARENT']]:

        return func.HttpResponse('no successful', status_code=200)
    elif click_task in [i for i in df['CLICK_PARENT']]:
        
        c.associate(click_task,jira_key)
        return func.HttpResponse('relationship successful', status_code=200)
    else:
        return func.HttpResponse('PASS', status_code=100)


        
        
        
