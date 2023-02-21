
import requests
import json
import pandas as pd
import numpy as np
from navisharedcode.SQLHelper import SQLHelper 
from sqlalchemy import create_engine, event, inspect
import urllib
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from atlassian import Jira
load_dotenv()
from datetime import datetime
from navisharedcode.Logger import Logger
from navisharedcode.helperfunctions import pretty_exception
import re
logger = Logger("jira_clickup_logs")
s = SQLHelper('rpttrans')
r = SQLHelper('rpt')
p = SQLHelper('prod')
class ClickUp():
    def __init__(self,click_id = None):
        self.auth = HTTPBasicAuth("kit@navigatorcre.com", "VaA9Lbjv5PXWeqqFtN3O57FB")

        self.headers = {
        "Accept": "application/json",
         "Content-Type": "application/json"
        }
        self.jira = Jira(url="DAATTTA",username="kit",password="FAKE_PASS",verify_ssl=False)
        self.headers = headers = {"Authorization": "HAHAHA","Content-Type": "application/json",}
        if click_id:
            self.click_task = click_id
            self.jira_task = s.sql_table_to_df(f"select * from [dbo].[SQLQLQL] WHERE data = '{click_id}'")['data'][0]

        self.epic_bridge = self.get_epic_bridge()
        # self.list_bridge = self.get_list_bridge()
        # self.reporter_bridge = self.get_reporter_bridge()
    def get_reporter_bridge(self):
        df = s.sql_table_to_df('SELECT * FROM [dbo].[C_JIRA_USERS]')

        return dict(zip(df['NAME'],df['ACCOUNT_ID']))

    def get_user_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_USERS_BRIDGE]')

        return dict(zip(df['CLICK_ID'],df['JIRA_ID']))

    def get_epic_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_FOLDERS]')

        return dict(zip(df['CLICK_ID'],df['JIRA_KEY']))
    def get_list_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_LISTS]')

        return dict(zip(df['ID'],df['JIRA_KEY']))

    def get_task_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_JIRA_TASKS_BINDER]')

        return dict(zip(df['CLICKUP_ID'],df['JIRA_KEY']))

    def get_sprint_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_JIRA_SPRINTS]')

        return dict(zip(df['NAME'],df['JIRA_SPRINT']))

    def priority(self,data):
        priority_dict = {"low":"Low","normal":"Medium","high":"High","urgent":"Highest"}
        self.jira.update_issue_field(self.jira_task,fields={'priority':{"name":priority_dict.get(data['after']['priority'])}})

    def dates(self,request):
        #dates!

        if request['field'] == 'due_date':
                dd = int(request['after']) / 1000
                self.jira.update_issue_field(self.jira_task,fields={'duedate':datetime.fromtimestamp(dd).strftime('%Y-%m-%d %H:%M:%S.%f')})
        elif request['field'] == 'start_date':
            sd = int(request['after']) / 1000
            self.jira.update_issue_field(self.jira_task,fields={'customfield_10015':datetime.fromtimestamp(sd).strftime('%Y-%m-%d %H:%M:%S.%f')})

    def sprint(self,data):
        sprints = self.get_sprint_bridge()
        sprint_id = sprints.get(data['after'])
        self.jira.update_issue_field(self.jira_task,{'customfield_10020': int(sprint_id)})

    def points(self,data):
        # d = re.search("\\\"\:\\\"(.*)\\\"\},\{\\\"", data['after']).group()


        self.jira.update_issue_field(self.jira_task,{'customfield_10024': int(data['after'])})

    def attachment(self,data):
        d = data['comment']['comment'][0]['attachment']
        for k,v in d.items():
            if k == 'url':
                fi = v.split('/')[-1]

                attachment_response = requests.get(v, headers = self.headers)
                # attachment_ = attachment_response.content.decode('utf-8').splitlines()
                with open(f'{fi}', 'w+b') as f:
                    f.write(attachment_response.content)

                self.jira.add_attachment(self.jira_task, fi)

    def description(self,data):
        # d = re.search("\\\"\:\\\"(.*)\\\"\},\{\\\"", data['after']).group()
        parsed = json.loads(data['after'])
        da = [i['insert'] for i in parsed['ops']]
        value = "".join(da)
        self.jira.update_issue_field(self.jira_task,{'description': value})


    def assign_user_to_jira_task(self,payload):

        jira_account_id = self.get_user_bridge().get(payload['after']['id'])
        slayload = json.dumps( { "accountId": f"{jira_account_id}"} )
        url = f"https://navigatorsrvs.atlassian.net/rest/api/3/issue/{self.jira_task}/assignee"
        response = requests.request("PUT",url,headers=self.headers,auth = self.auth,data = slayload)
        d = 4
    def status(self,data):
        status_dict = {"in progress":"In Progress",
        "to do":"To Do",
        "under review":"In Review",
        "blocked":"Blocked",
        "done":"Done"}
        status_jira = status_dict.get(data['after']['status'])
        self.jira.set_issue_status(self.jira_task,status_jira)
    
    def responsible(self,data):
        af = data['after']
        for i in data['custom_field']['type_config']['options']:
            if i['id'] == af:
                self.jira.update_issue_field(self.jira_task,{'customfield_10059':i['value']})

    def acceptance(self,data):
        value = data['after']
        self.jira.update_issue_field(self.jira_task,{'customfield_10045': value})
        
    def name(self,data):
        # d = re.search("\\\"\:\\\"(.*)\\\"\},\{\\\"", data['after']).group()
        self.jira.update_issue_field(self.jira_task,{'summary': data['after']})

    def delete(self,data):
        self.jira.delete_issue(self.jira_task)

    def milestone(self,data):

        self.jira.update_issue_field(self.jira_task,{"customfield_10070": int(data['after'])})
        
    def director(self,data):
        
        #director will decide what function to call bases on the webhook field
        for di in data['history_items']:
            if di['field'] == 'priority': 
                self.priority(di)

            if di['field'] == 'content': 
                self.description(di)

            if di['field'] == 'status': 
                self.status(di)

            if di['field'] == 'comment':
                if di['comment']['comment'][0]['type'] == 'attachment':
                    self.attachment(di)

            if di['field'] == 'due_date' or di['field'] == 'start_date': 
                self.dates(di)

            if di['field'] == 'points':
                self.points(di)

            if di['field'] == 'assignee_add': 
                self.assign_user_to_jira_task(di)

            if di['field'] == 'custom_field': 
                if di['custom_field']['name'] == "Sprint_Jira":
                    self.sprint(di)

            if di['field'] == 'custom_field': 
                if di['custom_field']['name'] == "Acceptance Criteria":
                    self.acceptance(di)
                    
            if di['field'] == 'custom_field': 
                if di['custom_field']['name'] == "Responsible":
                    self.responsible(di)

            if di['field'] == 'taskDeleted': 
                    self.delete(di)

            if di['field'] == 'custom_type': 
                self.milestone(di)

    def payload_to_jira(self,request,type):
        if type == 'Epic':
            payload = self.jira.issue_create(fields={'project': {'key': 'CUT'},  # add your Project Key here
                    'issuetype': {'name': 'Epic'},  # add your issue Type name
                    'summary': request['name'],
                    'customfield_10011' : request['name']})
            return payload
        if type == 'List':
            payload =  self.jira.issue_create(fields={'project': {'key': 'CUT'}, 
                    'issuetype': {'name': 'Story'}, 
                    'summary': request['name']})
         
            link_info =         {
            "type": {"name": "Story" },
            "inwardIssue": { "key": self.epic_bridge.get(request['folder']['id'])},
            "outwardIssue": {"key": payload['key']}

        }
            issue_link = self.jira.create_issue_link(link_info)

            return payload
    def list_to_jira(self,data):
        response = requests.get(f"https://api.clickup.com/api/v2/list/{data['list_id']}", headers=self.headers).json()
        df_dict = {'ID':[],'FOLDER_ID':[],'FOLDER_NAME':[],'NAME':[],'JIRA_KEY':[]}
        payload = self.payload_to_jira(response, 'List')
        df_dict['ID'].append(response['id'])
        df_dict['FOLDER_ID'].append(response['folder']['id'])
        df_dict['FOLDER_NAME'].append(response['folder']['name'])
        df_dict['NAME'].append(response['name'])

        df_dict['JIRA_KEY'].append(payload['key'])
        df = pd.DataFrame(df_dict)
        s.insert_dataframe_to_db(df, 'CLICKUP_LISTS')


    def folder_to_jira(self,data):
        #testff
        response = requests.get(f"https://api.clickup.com/api/v2/folder/{data['folder_id']}", headers=self.headers).json()
        df_dict = {'CLICK_ID':[],'SPACE_ID':[],'CLICK_NAME':[],'JIRA_KEY':[]}
        payload = self.payload_to_jira(response, 'Epic')
        df_dict['CLICK_ID'].append(response['id'])
        df_dict['SPACE_ID'].append(response['space']['id'])
        df_dict['CLICK_NAME'].append(response['name'])
        df_dict['JIRA_KEY'].append(payload['key'])
        df = pd.DataFrame(df_dict)
        s.insert_dataframe_to_db(df, 'CLICKUP_FOLDERS')
        #### check if lists are in sql
        list_response = requests.get(f"https://api.clickup.com/api/v2/folder/{data['folder_id']}/list", headers=self.headers).json()
        for data in response['lists']:
            if data['id'] not in self.get_list_bridge().keys():
                slayload =  self.jira.issue_create(fields={'project': {'key': 'CUT'}, 
                    'issuetype': {'name': 'Story'}, 
                    'summary': data['name']})
             
                link_info =         {
                                    "type": {"name": "Story" },
                                    "inwardIssue": { "key": payload['key']},
                                    "outwardIssue": {"key": slayload['key']}}
                issue_link = self.jira.create_issue_link(link_info)

    def updateFolder(self,data):
        if data['webhook_id'] == "a84aa817-e65b-4a04-9ef6-1d29d7f4018c":
            epic = self.get_epic_bridge().get(data['folder_id'])
            response = requests.get(f"https://api.clickup.com/api/v2/folder/{data['folder_id']}", headers=self.headers).json()
            self.jira.update_issue_field(epic,{'summary': response['name']})

    def updateList(self,data):
        if data['webhook_id'] == "a84aa817-e65b-4a04-9ef6-1d29d7f4018c":
            story = self.get_list_bridge().get(data['list_id'])
            response = requests.get(f"https://api.clickup.com/api/v2/list/{data['list_id']}", headers=self.headers).json()
            self.jira.update_issue_field(story,{'summary': response['name']})

def main(r):

	r = json.loads(r)
	"""
	task is updated via webhook
	"""
	if r['event'] == "folderUpdated":
		ClickUp().updateFolder(r)
	if r['event'] == "listUpdated":
		ClickUp().updateList(r)

	if r['event'] == 'taskUpdated':
		try:
			if r['task_id'] not in ClickUp().get_task_bridge().keys():
				print(r['task_id'])
			else:
				ClickUp(r['task_id']).director(r)
			# return func.HttpResponse('table successfully updated', status_code=200)
		except Exception as e:
			ev = [i['field'] for i in r['history_items']][0]
			logger = Logger("CLICKUP_UPDATE_LOGS")
			log_message = {
			"error": pretty_exception(e),
			"task_id": r['task_id'],
			'webhook_id': r['webhook_id'],
			'log_type': ev,
			'log_date': datetime.now()}
			logger.insert_log(log_message)
			
			raise e  
	"""
	folder is created via webhook
	"""
	if r['event'] in ['folderCreated','listCreated','taskDeleted']:
		
		try:
			if r['event'] == 'folderCreated':
				ClickUp().folder_to_jira(r)
				# return func.HttpResponse('table successfully updated', status_code=200)

			elif r['event'] == 'listCreated':
				ClickUp().list_to_jira(r)
				# return func.HttpResponse('table successfully updated', status_code=200)

			elif r['event'] == 'taskDeleted':
				ClickUp(r['task_id']).delete(r)
		except Exception as e:
			logger = Logger("CLICKUP_CREATE_LOGS")
			log_message = {
			"error": pretty_exception(e),
			# "task_id": r['list_id'],
			'webhook_id': r['event'],
			'log_date': datetime.today()}
			logger.insert_log(log_message)
		
			raise e  