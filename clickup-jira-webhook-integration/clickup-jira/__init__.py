
import pandas as pd
import requests
import json
from datetime import datetime
import numpy as np
import requests
from requests.auth import HTTPBasicAuth
from atlassian import Jira
import logging
import azure.functions as func
import time
logger = Logger("jira_clickup_logs")


class ClickUp():
    def __init__(self):
        self.auth = HTTPBasicAuth("******@email.com", "******")

        self.headers = {
        "Accept": "application/json",
         "Content-Type": "application/json"
        }
        self.jira = Jira(url="******",username="******",password="******",verify_ssl=False)

        self.epic_bridge = self.get_epic_bridge()
        self.list_bridge = self.get_list_bridge()
        self.reporter_bridge = self.get_reporter_bridge()



    def get_reporter_bridge(self):
        df = s.sql_table_to_df('SELECT * FROM [dbo].[C_JIRA_USERS]')
        return dict(zip(df['NAME'],df['ACCOUNT_ID']))

    def get_epic_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_FOLDERS]')
        return dict(zip(df['CLICK_ID'],df['JIRA_KEY']))

    def get_user_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_USERS_BRIDGE]')
        return dict(zip(df['CLICK_ID'],df['JIRA_ID']))

    def get_list_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_LISTS]')
        return dict(zip(df['ID'],df['JIRA_KEY']))

    def get_task_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_JIRA_TASKS_BINDER]')

        return dict(zip(df['CLICKUP_ID'],df['JIRA_KEY']))

    def get_sprint_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_JIRA_SPRINTS]')

        return dict(zip(df['NAME'],df['JIRA_SPRINT']))

    def assign_user_to_jira_task(self,payload,jira_task):
        headers = {
        "Accept": "application/json",
         "Content-Type": "application/json"
        }
        try:
            jira_account_id = self.reporter_bridge.get(payload['assignees'][0]['username'])
            slayload = json.dumps( { "accountId": f"{jira_account_id}"} )

            url = f"https://****.atlassian.net/rest/api/3/issue/{jira_task}/assignee"
            response = requests.request("PUT",url,headers=headers,auth = self.auth,data = slayload)
        except:
            jira_account_id = self.get_user_bridge().get(payload['assignees'][0]['id'])
            slayload = json.dumps( { "accountId": f"{jira_account_id}"} )

            url = f"https://****.atlassian.net/rest/api/3/issue/{jira_task}/assignee"
            response = requests.request("PUT",url,headers=headers,auth = self.auth,data = slayload)
        finally:
            pass

    def custom_field_parse(self,data):
        df_dict = {}
        df_dict[data['id']] = {}
        for custom in data['custom_fields']:
            try:
                df_dict[data['id']][custom['name']] = {}
                if 'value' in custom.keys() and isinstance( custom['value'], list):
                    custom_field_options = {i['id']:i['label'] if 'label' in i.keys() else {i['id']:i['name']} for i in custom['type_config']['options']}
                    df_dict[data['id']][custom['name']] = custom_field_options[custom['value'][0]]

                if 'value' in custom.keys() and isinstance( custom['value'], int):
                    custom_field_options = {i['orderindex']:i['name'] for i in custom['type_config']['options']}
                    df_dict[data['id']][custom['name']] = custom_field_options[custom['value']]
            except:
                pass
        return df_dict       
    def cust(self,re,jira_task):


        for data in re['custom_fields']:
            try:
                if data['name'] == 'Sprint_Jira' and 'value' in data.keys():
                        
                    sprints = self.get_sprint_bridge()
                    sprint_id = sprints.get(data['value'])
                    self.jira.update_issue_field(jira_task,{'customfield_10020': int(sprint_id)})
                elif data['name'] == 'Acceptance Criteria' and 'value' in data.keys():
                    value = data['value'].replace('/n','')
                    self.jira.update_issue_field(jira_task,{'customfield_10045': value})
                elif data['name'] == 'Jira Milestone' and data['value'] == 'true':
                    value = data['value'].replace('/n','')
                    self.jira.update_issue_field(jira_task,{'customfield_10075': int(1)})
                else:
                    pass
            except:
                pass
    def payload_to_jira(self,request,type,info,jiraProject):
        priority_dict = {"low":"Low","normal":"Medium","high":"High","urgent":"Highest"}
        status_dict = {"in progress":"In Progress","to do":"To Do", "under review":"In Review","blocked":"Blocked"}
        epic = self.get_epic_bridge()
        epic_link = epic.get(request['folder']['id'])
        tasks = self.get_task_bridge()
        sprints = self.get_sprint_bridge()
        if type == 'Task':
            try:
                priority = priority_dict.get(request['priority']['priority'])
            except:
                priority = 'Medium'
            try:
                if isinstance(info[request['id']]['Responsible'], str):
                    responsible = info[request['id']]['Responsible']
                else:
                    responsible = ""
            except:
                responsible = ""
            if isinstance(request['points'], int):
                points = request['points']
            else:
                points = 0
            if len(request['assignees']) > 0:
                df = pd.DataFrame(request['assignees'])
                df['TASK_ID'] = request['id']
                s.insert_dataframe_to_db(df, 'CLICKUP_ASSIGNEES')
            #--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            sender = {'project': {'key': jiraProject}, 
                    'issuetype': {'name': 'Task'},
                    'summary': request['name'],
                    'customfield_10024':points,
                    'customfield_10059' : responsible,
                    'priority' : {'name':priority},
                    'customfield_10014': epic_link,
                    'description' : request['description'],
                    'reporter': {'accountId':f"{self.reporter_bridge.get(request['creator']['username'])}"}}

            if not sender['reporter']['accountId'] or sender['reporter']['accountId'] == "None" :
                    del sender['reporter']
                    payload = self.jira.issue_create(fields=sender)

            else:
                    payload = self.jira.issue_create(fields=sender)
            if request['status']['status']:
                if request['status']['status'] == 'to do':
                    pass
                else:
                    self.jira.set_issue_status(payload['key'], status_dict.get(request['status']['status']))
            df = {"CLICKUP_ID":[],"JIRA_KEY":[],"JIRA_ID":[]}
            df['CLICKUP_ID'].append(request['id'])
            df['JIRA_KEY'].append(payload['key'])
            df['JIRA_ID'].append(payload['id'])
            frame = pd.DataFrame(df)
            s.insert_dataframe_to_db(frame, 'CLICKUP_JIRA_TASKS_BINDER')
            
            #--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            self.assign_user_to_jira_task(request,payload['key'])
            try:

                self.jira.update_issue_field(payload['key'],fields={'reporter': {'accountId':f"{self.reporter_bridge.get(request['creator']['username'])}"}})
            except:
                pass
            if request['due_date']:
                dd = int(request['due_date']) / 1000
                self.jira.update_issue_field(payload['key'],fields={'duedate':datetime.fromtimestamp(dd).strftime('%Y-%m-%d %H:%M:%S.%f')})
            if request['start_date']:
                sd = int(request['start_date']) / 1000
                self.jira.update_issue_field(payload['key'],fields={'customfield_10015':datetime.fromtimestamp(sd).strftime('%Y-%m-%d %H:%M:%S.%f')})
            #--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            self.cust(request,payload['id'])

            #--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


            try:
                    link_info =         {
                    "type": {"name": "Task" },
                    "inwardIssue": { "key": self.list_bridge.get(request['list']['id'])},
                    "outwardIssue": {"key": payload['key']}}
                    issue_link = self.jira.create_issue_link(link_info)
            except:
                    pass
            if request['parent']:
                df = {"CLICK_PARENT":[],"CLICK_CHILD":[],"JIRA_PARENT":[],"JIRA_CHILD":[]}
                if tasks.get(request['parent']):
                    link_info =         {
                    "type": {"name": "Task" },
                    "inwardIssue": { "key": tasks.get(request['parent'])},
                    "outwardIssue": {"key": payload['key']}}
                    issue_link = self.jira.create_issue_link(link_info)
                
                    df['CLICK_PARENT'].append(request['parent'])
                    df['CLICK_CHILD'].append(request['id'])
                    df['JIRA_CHILD'].append(payload['key'])
                    df['JIRA_PARENT'].append(tasks.get(request['parent']))
                else:
                    df['CLICK_PARENT'].append(request['parent'])
                    df['CLICK_CHILD'].append(request['id'])
                    df['JIRA_CHILD'].append(payload['key'])
                    df['JIRA_PARENT'].append(np.nan)

                ff = pd.DataFrame(df)
                s.insert_dataframe_to_db(ff, 'CLICKUP_JIRA_SUBTASK')
            else:
                pass


def main(req: func.HttpRequest) -> func.HttpResponse:
    c = ClickUp()
    r = req.get_json()
    try:
        if r['trigger_id'] == '****':
            info = c.custom_field_parse(r['payload'])
            jiraProject = '***'
            ret = c.payload_to_jira(r['payload'],  'Task', info,jiraProject)
            return func.HttpResponse('table successfully updated', status_code=200)
        elif r['trigger_id'] == '****':
            jiraProject = '***'
            info = c.custom_field_parse(r['payload'])
            c.payload_to_jira(r['payload'],  'Task', info,jiraProject)
            return func.HttpResponse('table successfully updated', status_code=200)
    except Exception as e:
        log_message = {
        "error": pretty_exception(e),
        "task_id": r['payload']['id'],
        'webhook_id': r['trigger_id']}
        logger.insert_log(log_message)
        
        raise e  

    
