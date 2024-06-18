import azure.functions as func
import logging, os
from datetime import datetime, timezone, timedelta
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import pandas as pd

from sys import path
path.insert(0, 'module')

app = func.FunctionApp()

## custom application modules
from module.bolddesk  import Bolddesk
from module.azure_ad  import AzureAD
from module.netsuite  import Netsuite
from module.warehouse import Warehouse
from module.infosec   import Infosec 
from module.idgov     import save_to_warehouse, add_new_user_to_bd, deactivate_invalid_agent
# from module.google_sheet import GoogleSheet


## disable HTTP Logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


## AD Weekly Jobs
## Schedule: Every Monday at :00 9am
## {second} {minute} {hour} {day} {month} {day-of-week}
@app.function_name(name="timer_update_ad_weekly")
@app.schedule(schedule="0 0 1 * * 1", arg_name="mytimer", run_on_startup=False) 
def timer_update_ad_weekly(mytimer: func.TimerRequest) -> None:
    logging.info('\n===========================================\nTIMER_UPDATE_AD_WEEKLY: triggered.')    

    ###########################
    ## AD and Bolddesk
    ###########################

    ## credential obtained from managed identity or azure login, to access azure SQL and KV
    def_credential = DefaultAzureCredential()

    ## connect to Azure Key Vault securely
    keyVaultName = os.environ["KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    kv_client = SecretClient(vault_url=KVUri, credential=def_credential)

    ## Secret Keys for Azure App Registration
    tenant_id = kv_client.get_secret('azure-tenant-id').value
    client_id = kv_client.get_secret('idgov-app-client-id').value
    client_secret = kv_client.get_secret('idgov-app-client-secret').value

    ## Initialize AD and Warehouse API Module
    ad = AzureAD(tenant_id, client_id, client_secret)
    wh = Warehouse(
        server=os.environ["DB_SERVER"],
        database=os.environ["DB_NAME"],
        credential=def_credential
    )

    ## Update One Drive Usage
    table_name = 'ad_onedrive_usage'
    df = ad.list_one_drive_usage()
    wh.refresh_table_rows(table_name, df, column_name='refresh_date', value=df.refresh_date[0])

    ## completed
    logging.info('\TIMER_UPDATE_AD_WEEKLY: completed.\n===========================================')


## AD and Bolddesk ETL
## Schedule: Hourly at :00 9am-6pm UTC, Monday - Friday
## {second} {minute} {hour} {day} {month} {day-of-week}
@app.function_name(name="timer_update_ad")
@app.schedule(schedule="0 0 1-10 * * 1-5", arg_name="mytimer", run_on_startup=False) 
def timer_update_ad(mytimer: func.TimerRequest) -> None:
    logging.info('\n===========================================\nTIMER_UPDATE_AD_BD: triggered.')    

    ###########################
    ## AD and Bolddesk
    ###########################

    ## credential obtained from managed identity or azure login, to access azure SQL and KV
    def_credential = DefaultAzureCredential()

    ## connect to Azure Key Vault securely
    keyVaultName = os.environ["KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    kv_client = SecretClient(vault_url=KVUri, credential=def_credential)

    ## Secret Keys for Azure App Registration
    tenant_id = kv_client.get_secret('azure-tenant-id').value
    client_id = kv_client.get_secret('idgov-app-client-id').value
    client_secret = kv_client.get_secret('idgov-app-client-secret').value

    # ## Secret Keys for Bolddesk
    # bd_api_key = kv_client.get_secret('bolddesk-nera-it-api-key').value
    # bd_base_url = kv_client.get_secret('bolddesk-nera-it-api-base-url').value

    ## Initialize AD and Bolddesk API Module
    ad = AzureAD(tenant_id, client_id, client_secret)
    # bd = Bolddesk(bd_base_url, bd_api_key)
    
    ## run the jobs
    save_to_warehouse(ad, def_credential)
    # add_new_user_to_bd(ad, bd)
    # deactivate_invalid_agent(ad, bd)

    ## completed
    logging.info('\nTIMER_UPDATE_AD_BD: completed.\n===========================================')


## BD Nera Care ETL 
## Schedule: Hourly at :30 9am-6pm UTC, Monday - Friday
@app.function_name(name="timer_update_bd_care")
@app.schedule(schedule="0 30 1-10 * * 1-5", arg_name="mytimer", run_on_startup=False) 
def timer_update_bd_care(mytimer: func.TimerRequest) -> None:
    logging.info('\n===========================================\nTIMER_UPDATE_NERA_CARE: triggered.')    
    
    ## default credential
    def_credential = DefaultAzureCredential()  

    ## connect to Azure Key Vault securely
    keyVaultName = os.environ["KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    kv_client = SecretClient(vault_url=KVUri, credential=def_credential)

    ## Secret Keys for Bolddesk
    bd_api_key = kv_client.get_secret ('bolddesk-nera-care-api-key').value
    bd_base_url = kv_client.get_secret('bolddesk-nera-care-api-base-url').value

    ## Initialize Bolddesk and Warehouse API Module
    bd = Bolddesk(bd_base_url, bd_api_key)
    wh = Warehouse(
            server=os.environ["DB_SERVER"],
            database=os.environ["DB_NAME"],
            credential=def_credential
    )

    ## Contacts
    df = bd.list_contacts()
    table_name = 'bd_nera_care_contacts'
    wh.erase(table_name)
    wh.append(table_name, df)

    ## Agents
    df = bd.list_agents()
    table_name = 'bd_nera_care_agents'
    wh.erase(table_name)
    wh.append(table_name, df)

    ## Tickets
    df = bd.list_tickets()
    table_name = 'bd_nera_care_tickets'
    wh.erase(table_name)
    wh.append(table_name, df)

    logging.info('\nTIMER_UPDATE_NERA_CARE: completed.\n===========================================')


## BD Helpdesk ETL
## Schedule: Hourly at :15 9am-6pm UTC, Monday - Friday
@app.function_name(name="timer_update_bd_helpdesk")
@app.schedule(schedule="0 20 1-10 * * 1-5", arg_name="mytimer", run_on_startup=False) 
def timer_update_bd_helpdesk(mytimer: func.TimerRequest) -> None:
    logging.info('\n===========================================\nTIMER_UPDATE_IT_HELPDESK: triggered.')

    ## credential obtained from managed identity or azure login, to access azure SQL and KV
    def_credential = DefaultAzureCredential()

    ## connect to Azure Key Vault securely
    keyVaultName = os.environ["KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    kv_client = SecretClient(vault_url=KVUri, credential=def_credential)

    ## Secret Keys for Bolddesk
    bd_api_key  = kv_client.get_secret('bolddesk-nera-it-api-key').value
    bd_base_url = kv_client.get_secret('bolddesk-nera-it-api-base-url').value

    ## Initialize Warehouse and Bolddesk
    bd = Bolddesk(bd_base_url, bd_api_key)
    wh = Warehouse(
        server=os.environ["DB_SERVER"],
        database=os.environ["DB_NAME"],
        credential=def_credential
    )

    ##################
    ### Tickets
    ##################

    ## get tickets
    df = bd.list_tickets()

    ## fix datetime columns
    date_cols = ['cf_last_date_of_service', 'cf_last_day_of_retention', 'resolutionDue', 'createdOn', 'closedOn', 'responseDue', 'lastRepliedOn', 'lastUpdatedOn', 'lastStatusChangedOn',]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

    ## write to SQL
    table_name = 'bd_helpdesk_tickets'
    wh.erase(table_name)
    wh.append(table_name, df)

    logging.info('\nTIMER_UPDATE_IT_HELPDESK: completed.\n===========================================')


## Netsuite ETL
## Schedule: Daily at 7am
@app.function_name(name="timer_update_ns")
@app.schedule(schedule="0 10 1-10 * * 1-5", arg_name="mytimer", run_on_startup=False) 
def timer_update_ns(mytimer: func.TimerRequest) -> None:
    logging.info('\n===========================================\nTIMER_UPDATE_NS: triggered.')    
    
    ## credential obtained from managed identity or azure login, to access azure SQL and KV
    def_credential = DefaultAzureCredential()

    ## connect to Azure Key Vault securely
    keyVaultName = os.environ["KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    kv_client = SecretClient(vault_url=KVUri, credential=def_credential)

    ## Secret Keys for Netsuite
    account_id       = kv_client.get_secret('netsuite-account-id').value
    consumer_key     = kv_client.get_secret('netsuite-consumer-key').value
    consumer_secret  = kv_client.get_secret('netsuite-consumer-secret').value
    token_id         = kv_client.get_secret('netsuite-token-id').value
    token_secret     = kv_client.get_secret('netsuite-token-secret').value
    # gcp_dashboard_bot_key = kv_client.get_secret('gcp-dashboard-bot-key').value

    ## Initialize Netsuite
    ns = Netsuite(account_id, consumer_key, consumer_secret, token_id, token_secret, script=1740)

    ## initialize Warehouse
    wh = Warehouse(
        server=os.environ["DB_SERVER"],
        database=os.environ["DB_NAME"],
        credential=def_credential
    )

    save_list = [
        ('Processing ns_subsidiaries',         'ns_subsidiaries',         'list_subsidiaries'),       
        ('Processing ns_roles',                'ns_roles',                'list_roles'),       
        ('Processing ns_role_record_usage',    'ns_role_record_usage',    'list_role_record_usage'),
        ('Processing ns_role_permissions',     'ns_role_permissions',     'list_role_permissions'),
        ('Processing ns_role_subsidiaries',    'ns_role_subsidiaries',    'list_role_subsidiaries'),
        ('Processing ns_employee_roles',       'ns_employee_roles',       'list_employee_roles'),
        ('Processing ns_employee_license',     'ns_employee_license',     'list_employee_license'),
        ('Processing ns_partner_roles',        'ns_partner_roles',        'list_partner_roles'),
        ('Processing ns_login_audits',         'ns_login_audits',         'list_login_audits'),
        ('Processing ns_login_failure',         'ns_login_failure',       'list_login_failure'),
        ('Processing ns_union_employees_partners','ns_union_employees_partners', 'union_employees_partners'),
        ('Processing ns_approval_matrix',      'ns_approval_matrix',      'list_approval_matrix'),
        ('Processing ns_employee_all',         'ns_employee_all',         'list_employee_all'),
        ('Processing ns_scripts',              'ns_scripts',              'list_scripts'),
        ('Processing ns_client_scripts',              'ns_client_scripts',            'list_client_scripts'),
        ('Processing ns_client_script_deployments',   'ns_client_script_deployments', 'list_client_script_deployments'),
        ('Processing ns_script_logs',             'ns_script_logs', 'list_script_logs')
    ]

    ## run all saving jobs
    for job in save_list:
        logging.info(job[0])
        ## Get Data
        list_func = getattr(ns, job[2])
        df = list_func()
        ## refresh table 
        wh.erase (job[1])
        wh.append(job[1], df)

    ## merge Netsuite Active Employees and AD users (ns_employees_ad_users)
    ## This is a outer join, we are expecting all AD users to be Employees
    table_name = 'ns_employees_ad_users'
    users_df    = wh.get_table('ad_users')\
                    .drop(columns=['id','passwordProfile_forceChangePasswordNextSignInWithMfa','passwordProfile_forceChangePasswordNextSignIn'])
    users_df.columns = [ 'ad_'+c for c in users_df.columns]
    employee_df = ns.list_employees(giveaccess_only=False, refresh=True, active_only=True)
    df = pd.merge(employee_df, users_df, how='outer', left_on='email', right_on='ad_userPrincipalName')
    wh.erase(table_name)
    wh.append(table_name, df)

    ## merge Netsuite Partners and AD users (ns_partners_ad_users)
    ## This is a left join, not all AD users should be partners
    table_name = 'ns_partners_ad_users'
    partners_df = ns.list_partners(giveaccess_only=False, refresh=True)
    df = pd.merge(partners_df, users_df, how='left', left_on='email', right_on='ad_userPrincipalName')
    wh.erase(table_name)
    wh.append(table_name, df)

    logging.info('\nTIMER_UPDATE_NS: completed.\n===========================================')


# ## Infosec ETL
# ## Schedule: Daily at 1am
# @app.function_name(name="timer_update_infosec")
# @app.schedule(schedule="0 40 1 * * 1-5", arg_name="mytimer", run_on_startup=False) 
# def timer_update_infosec(mytimer: func.TimerRequest) -> None:
#     logging.info('\n===========================================\nTIMER_UPDATE_INFOSEC: triggered.')    
#     def_credential = DefaultAzureCredential()  

#     ## connect to Azure Key Vault securely
#     keyVaultName = os.environ["KEY_VAULT_NAME"]
#     KVUri = f"https://{keyVaultName}.vault.azure.net"
#     kv_client = SecretClient(vault_url=KVUri, credential=def_credential)

#     ## Secret Keys for Infosec
#     api_key       = kv_client.get_secret('infosec-api-key').value

#     ## Initialize Module
#     url = 'https://securityiq-eu.infosecinstitute.com/api/v2'
#     ifs = Infosec(url, api_key)

#     ## initialize warehouse
#     wh = Warehouse(
#         server=os.environ["DB_SERVER"],
#         database=os.environ["DB_NAME"],
#         credential=def_credential
#     )

#     save_list = [
#         ('saving ifs_learners',         'ifs_learners',         'list_learners'), 
#         ('saving ifs_campaigns',        'ifs_campaigns',        'list_campaigns'), 
#         ('saving ifs_learner_progress',  'ifs_learner_progress','list_learner_progress') 
#     ]

#     ## run all saving jobs
#     for job in save_list:
#         logging.info(job[0])
#         ## Get Data
#         list_func = getattr(ifs, job[2])
#         df = list_func()
#         ## refresh table 
#         wh.erase (job[1])
#         wh.append(job[1], df)


#     ## completed
#     logging.info('\nTIMER_UPDATE_INFOSEC: completed.\n===========================================')

## Infosec ETL
## Schedule: Daily at 1am
@app.function_name(name="timer_update_infosec")
@app.schedule(schedule="0 40 1 * * 1-5", arg_name="mytimer", run_on_startup=False) 
def timer_update_infosec(mytimer: func.TimerRequest) -> None:
    logging.info('\n===========================================\nTIMER_UPDATE_INFOSEC: triggered.')    
    def_credential = DefaultAzureCredential()  

    ## connect to Azure Key Vault securely
    keyVaultName = os.environ["KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    kv_client = SecretClient(vault_url=KVUri, credential=def_credential)

    ## Secret Keys for Infosec
    api_key       = kv_client.get_secret('infosec-api-key').value

    ## Initialize Module
    url = 'https://securityiq-eu.infosecinstitute.com/api/v2'
    ifs = Infosec(url, api_key)

    ## initialize warehouse
    wh = Warehouse(
        server=os.environ["DB_SERVER"],
        database=os.environ["DB_NAME"],
        credential=def_credential
    )
    ##task name; db table name; method name to get info from infosec
    save_list = [
        ('saving ifs_learners',         'ifs_learners',         'list_learners'), 
        ('saving ifs_campaigns',        'ifs_campaigns',        'list_campaigns'), 
        ('saving ifs_learner_progress',  'ifs_learner_progress','list_learner_progress') 
    ]

        ## run all saving jobs
    for job in save_list:
        logging.info(job[0])
        ## Get Data
        list_func = getattr(ifs, job[2])
        df = list_func()
        ## refresh table 
        wh.erase(job[1])
        wh.append(job[1], df)

    save_git = [
        ('saving git_ifs_learner_progress',  'ifs_learner_progress','list_timeline_events')         
    ]
    for job in save_git:
        logging.info(job[0])
        ## Get Data
        list_func = getattr(ifs, job[2])
        df = list_func()
        ## refresh table 
        wh.append(job[1], df)

    ## completed
    logging.info('\nTIMER_UPDATE_INFOSEC: completed.\n===========================================')