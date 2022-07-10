#!/usr/bin/env python
# coding: utf-8

# In[46]:


import urllib
# Tableau Rest API to remotely log in 
from tableau_api_lib import TableauServerConnection
# for quering views
from tableau_api_lib.utils import querying
# for accessing the querried viewed as Pandas data frame
from tableau_api_lib.utils import flatten_dict_column, flatten_dict_list_column
from typing import Any, Dict, List, Optional, Union
def replace_special_characters(text): 
    text = urllib.parse.quote(text)
    return text.replace('%3D','=')

def get_encoded_params(param_dict):
    if isinstance(param_dict, dict):
        encoded_dict = {}
        for key in param_dict.keys():
            encoded_dict[key] = replace_special_characters(str(param_dict[key]))
        return encoded_dict
    else:
        raise Exception(f"This function expects a dict but received a {type(param_dict)} instead.")

def biReportList(creds):
    tableau_server_config = {
            'tableau_prod': {
                    'server': 'https://bi.cabadesign.co',  
                    'api_version': '3.11',
                    'username': creds[0],
                    'password': creds[1],
                    'site_name': '',
                    'site_url': ''
            }
    }
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    site_views_df = querying.get_views_dataframe(conn)
    site_views_df = flatten_dict_column(
    site_views_df, keys=['name', 'id'], col_name='project')
    site_views_df = flatten_dict_column(
    site_views_df, keys=['name', 'id'], col_name='workbook')
    conn.sign_out()
    return site_views_df[['workbook_name','name','project_name']]


def checkLogIn(username, password):
    tableau_server_config = {
        'tableau_prod': {
                'server': 'https://bi.cabadesign.co',  
                'api_version': '3.11',

                'username': username,
                'password': password,
                'site_name': '',
                'site_url': ''
        }
    }
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    try:
        conn.sign_out()
        return '<Response [200]>'
    except ConnectionError:
        return '<Response [401]>'

def getIvanFilter(passwtuple):
    my_workbook = 'Fulfillment Reports'
    my_worksheet = 'Initializtions'
    df = tSCRestAPI(passwtuple,my_workbook, my_worksheet)
    return df['Ivan Filter'].dropna().unique().tolist()


def tSCRestAPI(creds: tuple,my_workbook: str, my_worksheet: str, dim1: Optional[str]= None, val1: Optional[str] = None
              ,dim2: Optional[str] = None, val2: Optional[str] = None):

    username=creds[0]
    password=creds[1]
    # Connecting to Tableau Server environment
    tableau_server_config = {
            'tableau_prod': {
                    'server': 'https://bi.cabadesign.co',  
                    'api_version': '3.11',
                    'username': username,
                    'password': password, 
                    'site_name': '',
                    'site_url': ''
            }
    }
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    # print(conn.server_info().json())
    site_views_df = querying.get_views_dataframe(conn)

    #  unpack the workbook details.
    site_views_df = flatten_dict_column(
    site_views_df, keys=['name', 'id'], col_name='workbook')

    # Find the Workbook you want
    relevant_views_df = site_views_df[site_views_df['workbook_name'] == my_workbook]
    
    

    # get the view ID for the target view.
    data_id = relevant_views_df[relevant_views_df['name'] == my_worksheet]['id'].to_list()[0]
    
    # specifiy dimensions to filter
    if (dim2 != None):
        CUSTOM_PARAMS = {
        'dim_1': 'vf_'+dim1+'='+val1,
        'dim_2': 'vf_'+dim2+'='+val2
        }
    if (dim2 == None and dim1 != None):
        CUSTOM_PARAMS = {
        'dim_1': 'vf_'+dim1+'='+val1,
        }
    try:
        param_dict = get_encoded_params(CUSTOM_PARAMS)
        print(param_dict)
    except:
        pass

    # Query the Data in the view filtered on dimensions
    try:
        view_data_raw = querying.get_view_data_dataframe(conn, view_id=data_id,parameter_dict=param_dict)
    except:
        view_data_raw = querying.get_view_data_dataframe(conn, view_id=data_id)

    #renaming convention
    # view_data_raw.rename(columns={'Year of Order Date': 'Year'}, inplace=True)

    conn.sign_out()
    return view_data_raw


