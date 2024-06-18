import pandas as pd
import requests, logging
from requests.adapters import HTTPAdapter, Retry

logging.info('module.bolddesk: loading...')

class Bolddesk:

    base_url    = ''  ## bolddesk api base url, eg: 'xxx.bolddesk.com/api/v1'
    api_key     = ''  ## bolddesk api key, enabled through Bolddesk user account
    headers     = {}  ## request header that contains api key
    timezones   = []
    users_df    = pd.DataFrame()  ## list of users (contacts and agents), index: emailId, columns: 'userId'
    contacts_df = pd.DataFrame()  ## list of contacts
    agents_df   = pd.DataFrame()  ## list of agents
    tickets_df   = pd.DataFrame()  ## list of agents

    ## initialize headers and retrieve users, timezone, contacts and agents
    def __init__(self, base_url, api_key) -> None:
        logging.info('Bolddesk: initializing ...')
        self.base_url = base_url
        self.api_key  = api_key
        self.headers  = {
                            "x-api-key": self.api_key,
                            "Content-Type": "application/json"
                        }
        self.timezones = self.list_timezones()
    
    ## return request session with retry/backoff
    def get_session(self, total=5, backoff_factor=1) -> dict:
        retries = Retry(total=total, backoff_factor=backoff_factor)
        s = requests.Session()
        s.mount('https://', HTTPAdapter(max_retries=retries))
        return s

    ## Low Level Get All Pages(Multiple Pages Call)
    def get_all(self, url, params={}) -> dict:
        s = self.get_session()
        params['RequiresCounts'] = 'true' ## need this to count number of pages
        params['Page'] = 0
        params['PerPage'] = 100           ## maximize per page return
        result = []
        ## loop till last page
        while (True): 
            try:
                params['Page'] += 1
                response = s.get(self.base_url+url, headers=self.headers, params=params)
                temp = response.json()
                temp_result = temp['result']
                result += temp_result
                ## last page reached, break
                if len(temp_result) <100: break
            except Exception as e:
                logging.info(f'Bolddesk: get_all() - Error: {{e}}')
                result = False
                break

        ## done
        return result

    ## Low Level Get (Single Call)
    def get(self, url, params={}) -> dict:
        s = self.get_session()
        response = s.get(self.base_url+url, headers=self.headers, params=params)
        result = response.json()
        return result

    ## Low Level POST: used by Add Record operations
    def post(self, url, data=None) -> dict:
        s = self.get_session()
        response = s.post(self.base_url+url, headers=self.headers, json=data)
        return response.json()

    ## Low Level PUT: used by Update Record operations
    def put(self, url, data=None) -> dict:
        s = self.get_session()
        response = s.put(self.base_url+url, headers=self.headers, json=data)
        return response.json()

    ## Low Level Patch: used by Block Contact
    def patch(self, url, params) -> dict:
        s = self.get_session()
        response = s.patch(self.base_url+url, headers=self.headers, params=params)
        return response.json()

    ## Update List of Users, called by refresh_agents() and refresh_contacts()
    def refresh_users(self) -> None:
        # self.users_df = pd.concat([self.agents_df,self.contacts_df]).set_index('emailId')
        self.refresh_contacts()
        self.refresh_agents()

    ## Update List of Contacts, update users_df too
    def refresh_contacts(self) -> None:
        self.contacts_df = self.list_contacts(refresh=True)
        self.users_df = pd.concat([self.agents_df,self.contacts_df]).set_index('emailId')
        
    ## Update List of Agents, update users_df too
    def refresh_agents(self) -> None:
        self.agents_df = self.list_agents(refresh=True)
        self.users_df = pd.concat([self.agents_df,self.contacts_df]).set_index('emailId')


    #######################
    ## Users

    ## List All Users (contacts and agents)
    def list_users(self, refresh=False) -> list:    
        logging.info('Bolddesk: list_users()')

        ## return cache if available
        if not (refresh or self.agents_df.empty or self.contacts_df.empty):
            return self.users_df

        self.refresh_contacts()
        self.refresh_agents()
        return self.users_df

    ## Get A Single User
    def get_user(self, userId) -> dict:
        url = f'users/{userId}/userId'
        return self.get(url)

    #######################
    ## Agents

    def get_agent(self, userId) -> dict:
        url = f'agents/{userId}'
        response = self.get(url)
        return response

    ## Update Agent
    def update_agent(self, userId, agent_update) -> dict:
        logging.info(f'Bolddesk: update_agent() - {userId} / {agent_update}')
        ## Post it
        url = f'agents/{userId}'
        return self.put(url, agent_update)

    ## List All Agents
    def list_agents(self, refresh=False) -> pd.DataFrame:
        logging.info('Bolddesk: list_agents()')
        
        ## return cache if available by default
        if not (refresh or self.agents_df.empty):
            return self.agents_df

        url = 'agents'
        df = pd.DataFrame(self.get_all(url))

        ## fix for Bolddesk API always return Capital
        df['emailId'] = df.emailId.str.lower()  

        ## fix date columns
        date_cols =  ['lastModifiedOn','createdOn','lastActivityOn']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)    
        
        ## fix roles
        df['role_names'] = None
        df['role_ids'] = None
        for idx, roles in df.roles.items():
            role_names = [r.get('roleName') for r in roles]
            role_ids =   [str(r.get('roleId'))   for r in roles]
            df.loc[idx, 'role_ids']   = ','.join(role_ids)
            df.loc[idx, 'role_names'] = ','.join(role_names)
        
        df.drop(columns=['roles', 'availabilityStatus','shortCode','colorCode'], inplace=True)

        self.agents_df = df.copy()
        return df

    ## Verify User
    def verify_user(self, userId) -> dict:
        logging.info(f'Bolddesk: verify_user() -> userId: {userId}')
        ## Post it
        url = f'users/verify_manually/{userId}'
        return self.post(url)

    ## Deactive Agent
    def deactivate_agent(self, userId) -> dict:
        logging.info(f'Bolddesk: deactivate_agent() -> userId: {userId}')
        ## PUT it
        url = f'agents/{userId}/deactivate'
        return self.put(url, {})


    ## New Agent
    def add_agent(self, agent) -> dict:
        ## construct the POST data
        new_agent = {
            'name'          : agent.get('name'),
            'emailId'       : agent.get('emailId'),
            'displayName'   : agent.get('displayName'),
            'hasAllBrandAccess' : agent.get('hasAllBrandAccess'),
            'brandIds'      :agent.get('brandIds'),
            'roleIds'       : agent.get('roleIds'),
            'ticketAccessScopeId'      : agent.get('ticketAccessScopeId'),
            'isVerified'    : agent.get('isVerified'),
        }

        ## Post it
        url = 'agents'
        result = self.post(url, new_agent)
        return result

    #######################
    ## Contacts and Groups
    
    ## List All Contact Groups
    def list_contact_groups(self) -> list:
        logging.info('Bolddesk: list_contact_groups()')
        url = 'contact_groups'
        return self.get_all(url)
    
    ## List All Contacts
    def list_contacts(self, refresh=False) -> pd.DataFrame:
        logging.info('Bolddesk: list_contacts()')
        
        ## return cache if available by default
        if not (refresh or self.contacts_df.empty):
            return self.contacts_df
        
        url = 'contacts'
        data = self.get_all(url)
        ## flatten contactCustomFields
        df =  pd.json_normalize(data)
        ## fix column names
        df.columns = [col.replace('contactCustomFields.', '') for col in df.columns]
        df.columns = [col.replace('.', '_') for col in df.columns]
        ## fix for lowercase comparison
        df['emailId'] = df.emailId.str.lower()
        ## fix date columns        
        date_cols =  ['lastModifiedOn','createdOn']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

        df = df.fillna('')
        self.contacts_df = df.copy()
        return df

    ## Add Contact
    def add_contact(self, contact) -> dict:
        ## construct the POST data
        new_contact = {
            'contactName': contact.get('contactName'),
            'emailId'    : contact.get('emailId'),
            'contactDisplayName'   : contact.get('contactDisplayName'),
            'contactMobileNo'      : contact.get('contactMobileNo'),
            'contactJobTitle'      : contact.get('contactJobTitle'),
            'isVerified'           : contact.get('isVerified'),
            'customFields' : {
                'cf_contactCountry'       : contact.get('cf_contactCountry'),
                'cf_contactCity'          : contact.get('cf_contactCity'),
                'cf_contactManagerEmailId': contact.get('cf_contactManagerEmailId')
            }
        }

        ## auto define timezone id from cf_contactCity field
        if contact.get('cf_contactCity'):
            found_tz = next((item for item in self.timezones if contact.get('cf_contactCity') in item.get("description")),None)
            if found_tz:
                timeZoneId = found_tz.get('id')
                new_contact['timeZoneId'] = timeZoneId  ## add to the dict
        
        ## cf_contactManagerEmailId provided, auto define cf_contactManagerUserId
        manager_email_id = contact.get('cf_contactManagerEmailId')
        if manager_email_id:
            ## Manager found in Bolddesk, update custom fields
            if manager_email_id in self.users_df.index:
                new_contact['customFields']['cf_contactManagerUserId'] = int(self.users_df.loc[manager_email_id, 'userId'])  ## add to the dict
            ## reset the manager email if manager not exist in Bolddesk
            else:
                new_contact['customFields']['cf_contactManagerEmailId'] = None            

        ## Post it
        url = 'contacts'
        result = self.post(url, new_contact)
        return result

    ## Update Contact
    def update_contact(self, userId, contact_update) -> dict:
        logging.info(f'Bolddesk: update_contact() - {userId} / {contact_update}')

        ## auto define timezone id from cf_contactCity field
        if 'cf_contactCity' in contact_update.keys():
            timezones = self.list_timezones()
            found_tz = next((item for item in timezones if contact_update.get('cf_contactCity') in item.get("description")),None)
            if found_tz:
                timeZoneId = found_tz.get('id')
                contact_update['timeZoneId'] = int(timeZoneId)  ## add to the dict
        
        ## auto define cf_contactManagerUserId from cf_contactManagerEmailId
        if 'cf_contactManagerEmailId' in contact_update.keys():
            manager_email_id = contact_update.get('cf_contactManagerEmailId')
            ## manager found, update the manager user id
            if manager_email_id in self.users_df.index:
                contact_update['cf_contactManagerUserId'] = int(self.users_df.loc[manager_email_id,'userId'])  ## update manaager user id, fix to int to avoid json decoder error
            ## manager not found, reset both manager email and user id
            else:
                contact_update['cf_contactManagerEmailId'] = None
                contact_update['cf_contactManagerUserId']  = None

        ## reformat to bolddesk api
        data = {'fields': contact_update}

        ## Post it
        url = f'contacts/{userId}'
        return self.put(url, data)

    ## Get A Single Contact
    def get_contact(self, userId) -> dict:
        url = f'contacts/{userId}'
        response = self.get(url)
        return response

    ## Block Contact
    def block_contact(self, userId, markTicketAsSpam=False) -> dict:
        logging.info(f'Bolddesk: blcok_contact() - {userId}')
        url = f'contacts/{userId}/block'
        params = {
            'markTicketAsSpam' : markTicketAsSpam
        }
        result = self.patch(url, params)
        return result

    ## Convert Contact To Agent
    def convert_to_agent(self, userId, data) -> dict:
        logging.info(f'Bolddesk: convert_to_agent() - {userId}')
        url = f'contacts/convert_to_agent/{userId}'
        result = self.put(url, data)
        return result

    ## List All Timezones in Bolddesk        
    def list_timezones(self):
        logging.info('Bolddesk: list_timezones()')
        url = 'locales/timezones'
        return self.get_all(url)
    

    #####################
    ## Permissions
    
    def list_roles(self):
        logging.info('Bolddesk: list_roles()')
        url = 'roles'
        return self.get_all(url)


    #####################
    ## Tickets

    def list_tickets(self, refresh=False) -> pd.DataFrame:
        logging.info('Bolddesk: list_tickets()')
        
        ## return cache if available by default
        if not (refresh or self.tickets_df.empty):
            return self.tickets_df
        
        url = 'tickets'
        data = self.get_all(url)
        df = pd.DataFrame(data)

        ## flatten contactCustomFields
        df =  pd.json_normalize(data)
        df.columns = [col.replace('.', '_') for col in df.columns]
        
        ## fix datetime columns
        date_cols =  ['createdOn', 'closedOn', 'lastStatusChangedOn', 'resolutionDue', 'lastRepliedOn', 'lastUpdatedOn']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

        self.tickets_df = df.copy()
        
        return df

    #####################
    ## Utilities

    ## Get Timezone ID by Country and City
    def get_timezone_id(self, country, city=None) -> int:

        ## convert to lower case, XXXXX force to default timezone
        country = str.lower(country) if country else 'XXXXXXX'
        city    = str.lower(city) if city else 'XXXXXXX'
        if country: country = str.lower(country)
        if city:    city = str.lower(city)
        
        if country in 'malaysia':
            timezone_id = 37
        elif country in 'singapore':
            timezone_id = 37
        elif country in 'philippines':
            timezone_id = 37
        elif country in 'indonesia':
            timezone_id = 7
        elif country in 'thailand':
            timezone_id = 7
        elif country in 'norway':
            timezone_id = 123
        elif country in 'morocco':
            timezone_id = 124
        elif country in 'pakistan':
            timezone_id = 15
        elif country in 'united arab emirates':
            timezone_id = 27
        elif country in 'vietnam':
            timezone_id = 7
        elif country in 'cambodia':
            timezone_id = 7
        elif country in 'india':
            timezone_id = 1
        ## default to SG timezone
        else:
            timezone_id = 37
        return timezone_id

    ## Utility: Flatten nested dict utility. No prefix applied
    def flatten_dict_without_parent_prefix(self, nested_dict):
        items = []
        for key, value in nested_dict.items():
            
            if isinstance(value, dict):
                items.extend(self.flatten_dict_without_parent_prefix(value).items())
            else:
                items.append((key, value))
        return dict(items)
    
    ## Utility: dict1 can have more keys than dict2, and yet return True if all keys in dict2 are similar to dict1
    def compare_dicts(self, dict1, dict2):
        # Check if all keys in dict2 are present in dict1
        for key in dict2.keys():
            if key not in dict1:
                return False

        # Check if values for all common keys are the same
        for key in dict1.keys():
            if key in dict2:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    if not self.compare_dicts(dict1[key], dict2[key]):
                        return False
                elif dict1[key] != dict2[key]:
                    return False

        # If all tests passed, return True
        return True
    
    ## strip down dict by the selected keys
    def filter_dict_by_keys(self, d, keys):
        return {k: v for k, v in d.items() if k in keys}

    ## Return true if all keys/values in dict2 same as dict1. dict1 can have more keys than dict2
    def dict_diff(self, dict1, dict2):
        diff = {}
        for key in dict1.keys() | dict2.keys():
            if dict1.get(key) != dict2.get(key):
                diff[key] = (dict1.get(key), dict2.get(key))
        return diff