import logging, os
from module.warehouse import Warehouse

def add_new_user_to_bd(ad, bd):
    logging.info('started: add_new_user_to_bd()')
    ## Add New AD User to Bolddesk Agent
    ## New AD User Defiend as
    ## - userPurpose is 'user'
    ## - Member
    ## - accountEnabled
    ## - Not exist in Bolddesk

    ## AD users to be added to Bolddesk
    all_employees_df = ad.list_users().query('userPurpose=="user" and userType=="Member" and accountEnabled==True').set_index('userPrincipalName')
    employees_to_bd_index = ~all_employees_df.index.isin(bd.list_users().index) ## not in bolddesk
    employees_to_bd_df    = all_employees_df.loc[employees_to_bd_index]

    ## Add new agent in Bolddesk as verified
    for emailId, new_user in employees_to_bd_df.iterrows():
        ## map AD fields to Bolddesk fields

        new_agent = {
            'emailId'       : emailId,
            'name'          : new_user.get('displayName'),
            'displayName'   : new_user.get('displayName'),
            'hasAllBrandAccess' : True,
            'brandIds'      : "1",
            'roleIds'       : "1002",
            'ticketAccessScopeId' : 2,
            'isVerified'    : True ## force verified, so that users don't get invitation email from Bolddesk
        }

        logging.info(f'Adding new agent: {emailId}, {new_agent}')
        bd.add_agent(new_agent)

def deactivate_invalid_agent(ad, bd):
    logging.info('started: deactivate_invalid_agent()')
    
    ## Valid Agents are defined as (all condition applied)
    ## - exist in Azure AD 
    ## - Enabled AD Account
    ## - Member Type

    ad_users_df = ad.list_users().query('userPurpose=="user" and userType=="Member" and accountEnabled==True').set_index('userPrincipalName')
    active_agents_df = bd.list_agents().query('status=="Active"').set_index('emailId')
    invalid_agents = ~active_agents_df.index.isin(ad_users_df.index)
    invalid_agents_df = active_agents_df.loc[invalid_agents]

    ## (safety check) proceed only if agents to deactivate is < 10% of total AD users
    if len(invalid_agents_df)/len(ad.users_df) < 0.1:
        for emailId, agent in invalid_agents_df.iterrows():
            bd.deactivate_agent(agent.userId)

def save_to_warehouse(ad, credential):

    wh = Warehouse(
            server=os.environ["DB_SERVER"],
            database=os.environ["DB_NAME"],
            credential=credential
    )

    logging.info('started: save_to_warehouse()')

    ## Define the save jobs
    save_list = [
        ## log_message, table_name, function_name
        ('save_to_warehouse(): saving ad_users',             'ad_users',             'list_users'),
        ('save_to_warehouse(): saving ad_groups',            'ad_groups',            'list_groups'),
        ('save_to_warehouse(): saving ad_groups_umembers',   'ad_groups_members',    'list_groups_umembers'),
        ('save_to_warehouse(): saving ad_groups_gmembers',   'ad_groups_gmembers',   'list_groups_gmembers'),
        ('save_to_warehouse(): saving ad_groups_owners',     'ad_groups_owners',     'list_groups_owners'),
        ('save_to_warehouse(): saving ad_devices_users',     'ad_devices_users',     'list_devices_users'),
        ('save_to_warehouse(): saving ad_users_licenses',    'ad_users_licenses',    'list_users_licenses'),
        ('save_to_warehouse(): saving ad_targets',           'ad_targets',           'list_targets'),
        ('save_to_warehouse(): saving ad_service_principals','ad_service_principals','list_service_principals'),
        ('save_to_warehouse(): saving ad_auth_details',      'ad_auth_details',      'list_auth_details'),
        ('save_to_warehouse(): saving ad_managed_devices',   'ad_managed_devices',   'list_managed_devices')
    ]

    ## run all save jobs
    for job in save_list:
        logging.info(job[0])

        table_name = job[1]
        list_func = getattr(ad, job[2])
        df = list_func()
    
        wh.erase(table_name)
        wh.append(table_name, df)