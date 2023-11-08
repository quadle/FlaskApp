import requests
#data test
import json
import pandas as pd
import numpy as np
import azure.functions as func
from navisharedcode.SQLHelper import SQLHelper 
import requests
from atlassian import Jira
from requests.auth import HTTPBasicAuth
import logging


class ClickUp():
    def __init__(self):
        #self.access_token = self.access_token()
        self.auth = HTTPBasicAuth("******@email.com", "******")

        self.headers = {
        "Accept": "application/json",
         "Content-Type": "application/json"
        }
        self.jira = Jira(url="******",username="******",password="******",verify_ssl=False)

        self.epic_bridge = self.get_epic_bridge()
        self.list_bridge = self.get_list_bridge()
        self.reporter_bridge = self.get_reporter_bridge()



    def access_token():
        url = "https://api.clickup.com/api/v2/oauth/token"

        query = {
        "client_id": "****",
        "client_secret": "****",
        "code": "****"
        }
        response = requests.post(url, params=query)
        data = response.json()
        return data['access_token']


    def get_epic_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_FOLDERS] where space_id = "****"')

        return dict(zip(df['CLICK_ID'],df['JIRA_KEY']))

    def get_epic_bridge_id(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_FOLDERS_REPLICATE] where space_id = "****"')

        return dict(zip(df['CLICK_ID'],df['JIRA_KEY']))

    def get_list_bridge(self):
        df = s.sql_table_to_df('select * from [dbo].[CLICKUP_LISTS]')

        return dict(zip(df['ID'],df['JIRA_KEY']))
        
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

    def build_dataframe(self,list_data,type,custom = None):
        if type == 'Epic':
            df_dict = {'CLICK_ID':[],'SPACE_ID':[],'CLICK_NAME':[],'JIRA_KEY':[]}
            for data in list_data:
                if data['id'] in self.get_epic_bridge().keys():
                    j_key = self.epic_rep.get(data['id'])
                    print(j_key)
                    df_dict['CLICK_ID'].append(data['id'])
                    df_dict['SPACE_ID'].append(data['space']['id'])
                    df_dict['CLICK_NAME'].append(data['name'])
                    # df_dict['JIRA_ID'].append(self.get_epiepic_rep.get(j_key))
                    df_dict['JIRA_KEY'].append(j_key)
                    # df_dict['TASK_COUNT'].append(data['task_count'])
                elif 'Sprint' not in data['name']:
                    payload = self.payload_to_jira(data, 'Epic')
                    df_dict['CLICK_ID'].append(data['id'])
                    df_dict['SPACE_ID'].append(data['space']['id'])
                    df_dict['CLICK_NAME'].append(data['name'])
                    # df_dict['TASK_COUNT'].append(data['task_count'])
                    # df_dict['JIRA_ID'].append(payload['id'])
                    df_dict['JIRA_KEY'].append(payload['key'])
            return pd.DataFrame(df_dict)
        elif type == 'List':
            df_dict = {'ID':[],'FOLDER_ID':[],'FOLDER_NAME':[],'NAME':[],'TASK_COUNT':[],'JIRA_KEY':[]}
            for data in list_data['lists']:
                if data['id'] in self.get_list_bridge().keys():
                    df_dict['ID'].append(data['id'])
                    df_dict['FOLDER_ID'].append(data['folder']['id'])
                    df_dict['FOLDER_NAME'].append(data['folder']['name'])
                    df_dict['NAME'].append(data['name'])
                    df_dict['TASK_COUNT'].append(data['task_count'])
                    df_dict['JIRA_KEY'].append(self.list_bridge.get(str(data['id'])))
                else:
                    if 'Sprint' in data['name']:
                        pass
                    else:
                        payload = self.payload_to_jira(data, 'List')
                        df_dict['ID'].append(data['id'])
                        df_dict['FOLDER_ID'].append(data['folder']['id'])
                        df_dict['FOLDER_NAME'].append(data['folder']['name'])
                        df_dict['NAME'].append(data['name'])
                        df_dict['TASK_COUNT'].append(data['task_count'])
                        df_dict['JIRA_KEY'].append(payload['key'])
                
            return pd.DataFrame(df_dict)



    def get_folders_w_push(self):
        #still need to build push to sql
        url = f"https://api.clickup.com/api/v2/space/****/folder"
        query = {"archived": "false"}
        response = requests.get(url, headers=self.headers, params=query).json()
        data = [i for i in response['folders']]
        df = self.build_dataframe(data,'Epic')
        s.insert_dataframe_to_db(df, 'CLICKUP_FOLDERS',exist_method='replace')
        return data

    def lists(self):
        data_resp = []
        for folder_id in self.get_epic_bridge().keys():  
            response = requests.get(f"https://api.clickup.com/api/v2/folder/{folder_id}/list?archived=false", headers=self.headers).json() 
            df = self.build_dataframe(response, type = 'List')
            data_resp.append(df)
    
        df = pd.concat(data_resp)
        s.insert_dataframe_to_db(df, 'CLICKUP_LISTS',exist_method='replace')

def main(mytimer: func.TimerRequest) -> None:
    logging.info('CLICKUP REFRESH START')
    # c = ClickUp()
    # f = c.get_folders_w_push()
    # l = c.lists()