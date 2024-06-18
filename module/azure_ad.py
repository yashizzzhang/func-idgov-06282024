from msal import ConfidentialClientApplication
import pandas as pd
import requests, logging
from requests.adapters import HTTPAdapter, Retry

class AzureAD:

    headers = {}
    access_token = ''
    base_url = 'https://graph.microsoft.com/v1.0/'
    users_df = pd.DataFrame()
    groups_df = pd.DataFrame()
    groups_members_df = pd.DataFrame()
    groups_umembers_df = pd.DataFrame()
    groups_gmembers_df = pd.DataFrame()
    groups_owners_df  = pd.DataFrame()
    devices_df  = pd.DataFrame()
    managed_devices_df = pd.DataFrame()
    devices_users_df = pd.DataFrame()
    targets_df   = pd.DataFrame()
    applications_df = pd.DataFrame()

    ## Get access token and set Headers
    
    def __init__(self, tenant_id, client_id, client_secret) -> None:
        logging.info('AzureAD: initializing ...')
        authority_url = f"https://login.microsoftonline.com/{tenant_id}"
        scopes = ["https://graph.microsoft.com/.default"]
        auth_app = ConfidentialClientApplication(
            client_id=client_id, 
            authority=authority_url, 
            client_credential=client_secret
        )

        # Get an access token from Azure AD
        token = auth_app.acquire_token_for_client(scopes=scopes)
        self.access_token = token["access_token"]

        ## Set The Header with New Token
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    ## Low Level API
    ################
    
    ## return request session with retry/backoff
    def get_session(self, total=5, backoff_factor=1) -> dict:
        retries = Retry(total=total, backoff_factor=backoff_factor)
        s = requests.Session()
        s.mount('https://', HTTPAdapter(max_retries=retries))
        return s
    
    def get(self, url, params=None) -> dict:
        s = self.get_session()
        response = s.get(url, headers=self.headers, params=params)
        return response.json()

    ## return json object text(columns), url
    def get_report_info(self, url):
        s = self.get_session()
        response = s.get(url, headers=self.headers)
        return response

    def post(self, url, json=None) -> dict:
        s = self.get_session()
        response = s.post(url, headers=self.headers, json=json)
        return response.json()

    def get_all(self, url, params=None) -> list:
        s=self.get_session()
        result = []
        next_link = url
        # Loop through all pages and retrieve users
        while True:
            response = s.get(next_link, headers=self.headers, params=params)
            data = response.json()
            result += data["value"]
            next_link = data.get("@odata.nextLink")
            params = None  ## second page onwards param is not required
            if not next_link: break

        return (result)

    ## Users Related
    ################

    def list_users(self, refresh=False, include_licenses_plans=False) -> pd.DataFrame:
        logging.info('AzureAD: list_users()')

        ## arrange columns nicely
        all_cols = [
            'id', 'createdDateTime', 'userPrincipalName', 'accountEnabled', 'userType', 'userPurpose', 'displayName', 'department', 'companyName', 'employeeType', 'employeeId', 'jobTitle', 'mobilePhone', 
            'officeLocation', 'city', 'country', 'manager_id', 'manager_displayName','manager_employeeId', 'manager_userPrincipalName',
            'passwordProfile_forceChangePasswordNextSignIn', 'passwordProfile_forceChangePasswordNextSignInWithMfa',
            'assignedLicenses','assignedPlans'
        ]
        
        ## remove last 2 columns (licenses and plans) if not required by caller
        display_cols = all_cols.copy()
        if not include_licenses_plans:
            display_cols = display_cols[:-2]

        ## return cache if available by default
        if (not refresh) and (not self.users_df.empty):
            return self.users_df.loc[:, display_cols]
        
        url = self.base_url + "users"
        params = {
            '$select': 'id,createdDateTime,userType,accountEnabled,assignedLicenses,assignedPlans,passwordProfile,userPrincipalName,mailNickName,displayName,department,companyName,employeeType,employeeId,jobTitle,mobilePhone,city,officeLocation,country,manager,signInSessionsValidFromDateTime', 
            '$expand': 'manager($select=id,displayName,employeeId,userPrincipalName)'
        }
        
        users =  self.get_all(url, params)
        users_df = pd.DataFrame(users)

        ## Flatten Manager
        manager_df  = pd.json_normalize(users_df['manager']) \
                        .filter(items=('id','displayName','employeeId','userPrincipalName'))
        manager_df.columns = ['manager_' + col for col in manager_df.columns]
        users_df.drop(['manager'], axis=1, inplace=True)
        
        ## Flatten passwordProfile
        password_df = pd.json_normalize(users_df['passwordProfile'])\
                        .filter(items=('forceChangePasswordNextSignIn','forceChangePasswordNextSignInWithMfa'))
        password_df.columns = ['passwordProfile_' + col for col in password_df.columns]
        users_df.drop(['passwordProfile'], axis=1, inplace=True)

        ## Combine Flatten Data
        users_df = pd.concat([users_df, password_df, manager_df], axis=1)

        ## Loop all user to find out its userPurpose through mailboxSettings api
        users_df['userPurpose'] = None
        for idx, email in users_df.userPrincipalName.items():
            url = f'{self.base_url}users/{email}/mailboxSettings'
            user_purpose = self.get(url).get('userPurpose')
            users_df.loc[idx, 'userPurpose'] = user_purpose
        
        ## Fix user typo error with trailing empty spaces
        # users_df['userPrincipalName'] = users_df.userPrincipalName.str.lower()
        users_df['city'] = users_df.city.str.strip()
        users_df['country'] = users_df.country.str.strip()
        users_df['jobTitle'] = users_df.jobTitle.str.strip()
        users_df['mobilePhone'] = users_df.mobilePhone.str.strip()
        users_df.replace(pd.NA, None, inplace=True)
        users_df = users_df.loc[:, all_cols]

        ## save cache with full columns
        self.users_df = users_df

        ## return only chosesn columns
        df = users_df.loc[:, display_cols]
        return df.copy()

    def list_auth_details(self) -> pd.DataFrame:
        logging.info('AzureAD: list_authentications()')
        url = f'https://graph.microsoft.com/beta/reports/authenticationMethods/userRegistrationDetails'
        data = self.get_all(url)
        df = pd.DataFrame(data)
        df['method_microsoftAuthenticatorPasswordless'] = False
        df['method_mobilePhone'] = False
        df['method_microsoftAuthenticatorPush'] = False
        df['method_softwareOneTimePasscode'] = False
        df['method_windowsHelloForBusiness'] = False
        df['method_email'] = False
        for idx, user in df.iterrows():
            if 'microsoftAuthenticatorPasswordless' in user.methodsRegistered:
                df.loc[idx, 'method_microsoftAuthenticatorPasswordless'] = True
            if 'mobilePhone' in user.methodsRegistered:
                df.loc[idx, 'method_mobilePhone'] = True
            if 'microsoftAuthenticatorPush' in user.methodsRegistered:
                df.loc[idx, 'method_microsoftAuthenticatorPush'] = True
            if 'softwareOneTimePasscode' in user.methodsRegistered:
                df.loc[idx, 'method_softwareOneTimePasscode'] = True
            if 'windowsHelloForBusiness' in user.methodsRegistered:
                df.loc[idx, 'method_windowsHelloForBusiness'] = True
            if 'email' in user.methodsRegistered:
                df.loc[idx, 'method_email'] = True
            df.loc[idx, 'methodsRegistered'] = ','.join(user.methodsRegistered)
            df.loc[idx, 'systemPreferredAuthenticationMethods'] = ','.join(user.systemPreferredAuthenticationMethods)
        return df

    ## Groups Related
    ################

    def list_groups(self, refresh=False) -> pd.DataFrame:
        logging.info('AzureAD: list_groups()')

        ## return cache if available by default
        if not (refresh or self.groups_df.empty):
            return self.groups_df

        url = f'{self.base_url}groups'        
        params = {
            '$select': 'id,createdDateTime,description,displayName,groupTypes,mail,mailEnabled,securityEnabled,mailNickname,visibility,securityIdentifier'
        }

        data = self.get_all(url, params)
        df = pd.DataFrame(data)

        ## get additional columns from individual Group get API
        df['allowExternalSenders'] = None
        df['hideFromAddressLists'] = None
        df['hideFromOutlookClients'] = None
        for idx, row in df[df.securityEnabled==False].iterrows():
            sub_data = self.get_group(row.id, row.securityEnabled)
            df.loc[idx, 'allowExternalSenders'] = sub_data.get('allowExternalSenders')
            df.loc[idx, 'hideFromAddressLists'] = sub_data.get('hideFromAddressLists')
            df.loc[idx, 'hideFromOutlookClients'] = sub_data.get('hideFromOutlookClients')

        ## fix group_type
        def define_group_type(row):
            if row.groupTypes == 'Unified' and row.mailEnabled and not(pd.isna(row.securityEnabled)):
                return 'M365 Group'
            elif row.groupTypes != 'Unified' and not(row.mailEnabled) and row.securityEnabled:
                return 'Security Group'
            elif row.securityEnabled and row.mailEnabled:
                return 'Mail-enabled Group'
            elif row.mailEnabled and not (row.securityEnabled):
                return 'Distribution Group'
            else:
                return None

        df['groupTypes'] = df.groupTypes.apply(lambda x: 'Unified' if 'Unified' in x else None)
        df['groupType']  = df.apply( define_group_type, axis=1)

        self.groups_df = df
        return df.copy()
    
    def get_group(self, id, securityEnabled) -> dict:
        
        url = f'{self.base_url}groups/{id}'
        
        params = {
            '$select': 'id'
        }

        ## additional parameters only if securityEnabled is False
        if not securityEnabled:
            params['$select'] += ',allowExternalSenders,hideFromAddressLists,hideFromOutlookClients'

        ## get the data
        data = self.get(url,params)

        ## placeholder for non existance data
        if securityEnabled:
            data.update({
                'allowExternalSenders': None,
                'hideFromAddressLists': None,
                'hideFromOutlookClients': None
            })

        return data   

    def list_groups_umembers(self, refresh=False) -> pd.DataFrame:
        logging.info('AzureAD: list_groups_umembers()')
        ## return cache by default if available
        if not (refresh or self.groups_umembers_df.empty):
            return self.groups_umembers_df

        groups_df = self.list_groups(refresh=refresh).set_index('id')
        users_df  = self.list_users(refresh=refresh).set_index('id')

        url = self.base_url + "users"
        params = {
            '$select': 'id',
            '$expand': 'memberOf($select=id)'
        }
        
        users_list =  self.get_all(url, params)
        
        membership = []
        for user in users_list:
            for m in user.get('memberOf'):
                membership += [ {
                    'member_id': user.get('id'),
                    'group_id' : m.get('id')
                }]
        
        df = pd.DataFrame(membership).set_index('group_id')

        ## merge with group to get full columns
        groups_df.columns = [ f'group_{c}' for c in groups_df.columns ]
        df = df.merge(groups_df, left_index=True, right_index=True, how='inner')
        df.index.rename('group_id', inplace=True)
        df.reset_index(inplace=True)

        # merge with users to get full columns
        users_df.columns = [ f'member_{c}' for c in users_df.columns ]
        umember_df = df.set_index('member_id').merge(users_df,left_index=True, right_index=True, how='inner')
        umember_df.reset_index(inplace=True)
        
        ## rearrange columns
        cols = ['group_id','member_id']  + umember_df.columns.to_list()[2:]
        umember_df = umember_df.loc[:, cols]

        ## done
        self.groups_umembers_df = umember_df
        return umember_df.copy()

    def list_groups_gmembers(self, refresh=False) -> pd.DataFrame:
        logging.info('AzureAD: list_groups_gmembers()')        
        ## return cache by default if available
        if not (refresh or self.groups_gmembers_df.empty):
            return self.groups_gmembers_df

        groups_df = self.list_groups(refresh=refresh).set_index('id')

        url = self.base_url + "groups"
        params = {
            '$select': 'id',
            '$expand': 'members($select=id)'
        }
        
        members_list =  self.get_all(url, params)
        
        membership = []
        for member in members_list:
            for m in member.get('members'):
                membership += [ {
                    'group_id': member.get('id'),
                    'member_id': m.get('id')
                }]
        
        df = pd.DataFrame(membership).set_index('group_id')

        ## merge with group to get full columns
        groups_df.columns = [ f'group_{c}' for c in groups_df.columns ]
        df = df.merge(groups_df, left_index=True, right_index=True, how='inner')
        df.index.rename('group_id', inplace=True)
        df.reset_index(inplace=True)

        ## merge with group to get full group column
        groups_df.columns = [ f'member_{c}' for c in groups_df.columns ]
        gmember_df = df.set_index('member_id').merge(groups_df, left_index=True, right_index=True, how='inner')
        # gmember_df = gmember_df.loc[~gmember_df.member_group_mail.isna()] ## remove non group members rows
        gmember_df.reset_index(inplace=True)
        ## rearrange columns
        cols = ['group_id','member_id']  + gmember_df.columns.to_list()[2:]
        gmember_df = gmember_df.loc[:, cols]

        ## done
        self.groups_gmembers_df = gmember_df
        return gmember_df.copy()

    def list_groups_owners(self, refresh=False) -> pd.DataFrame:
        logging.info('AzureAD: list_groups_owners()')
        ## return cache by default if available
        if not (refresh or self.groups_owners_df.empty):
            return self.groups_owners_df

        groups_df = self.list_groups(refresh=refresh).set_index('id')
        users_df  = self.list_users(refresh=refresh).set_index('id')


        url = self.base_url + "groups"
        params = {
            '$select': 'id',
            '$expand': 'owners($select=id)'
        }
        owners_list =  self.get_all(url, params)
        
        ## Construct Owners List
        ownerships = []
        for owner in owners_list:
            for o in owner.get('owners'):
                ownerships += [ {
                    'group_id': owner.get('id'),
                    'owner_id': o.get('id')
                }]

        df = pd.DataFrame(ownerships).set_index('group_id')

        ## merge with group to get full columns
        groups_df.columns = [ f'group_{c}' for c in groups_df.columns ]
        df = df.merge(groups_df, left_index=True, right_index=True, how='inner')
        df.index.rename('group_id', inplace=True)
        df.reset_index(inplace=True)

        # merge with users to get full columns
        users_df.columns = [ f'owner_{c}' for c in users_df.columns ]
        df = df.set_index('owner_id').merge(users_df,left_index=True, right_index=True, how='inner')

        df.reset_index(inplace=True)

        ## rearrange columns
        cols = ['group_id','owner_id']  + df.columns.to_list()[2:]
        df = df.loc[:, cols]

        self.groups_owners_df = df
        return df.copy()
    
    ## Devices Related
    ##################

    def list_devices(self, refresh=False) -> pd.DataFrame:
        logging.info('AzureAD: list_devices()')
        ## return cache if available by default
        if (not refresh) and (not self.devices_df.empty):
            return self.devices_df
        
        url = self.base_url + "devices"
        params = {
            '$select': 'deviceId,accountEnabled,approximateLastSignInDateTime,createdDateTime,displayName,isCompliant,operatingSystem,operatingSystemVersion,profileType,registrationDateTime,trustType',
        }
        
        data =  self.get_all(url, params)
        df = pd.DataFrame(data)

        self.devices_df = df
        return df.copy()

    def list_managed_devices(self, refresh=False) -> pd.DataFrame:
        logging.info('AzureAD: list_managed_devices()')
        ## return cache if available by default
        if (not refresh) and (not self.managed_devices_df.empty):
            return self.managed_devices_df
        
        url = self.base_url + "deviceManagement/managedDevices"
        
        data =  self.get_all(url)
        df = pd.DataFrame(data)

        self.managed_devices_df = df
        return df.copy()

    def list_devices_users(self, refresh=False) -> pd.DataFrame:
        logging.info('AzureAD: list_devices_users')        
        users_df = self.list_users(refresh=refresh).set_index('id')
        users_df.columns = ['user_' + c for c in users_df.columns]

        devices_df = self.list_devices(refresh=refresh).set_index('deviceId')
        devices_df.columns = ['device_' + c for c in devices_df.columns]

        ## return cache if available by default
        if (not refresh) and (not self.devices_users_df.empty):
            return self.devices_users_df
        
        url = self.base_url + "devices"
        params = {
            '$select': 'deviceId',
            '$expand': 'registeredUsers($select=id)'
        }
        
        devices_list =  self.get_all(url, params)
        
        registered_users = []
        for device in devices_list:
            for u in device.get('registeredUsers'):
                registered_users += [ {
                    'device_id': device.get('deviceId'),
                    'user_id': u.get('id')
                }]
        
        ## merge with users and devices to get full columns
        df = pd.DataFrame(registered_users).set_index('device_id')
        df = df.merge(devices_df,how='left', left_index=True, right_index=True)\
               .merge(users_df, how='left', left_on='user_id', right_index=True)
      
        df.reset_index(inplace=True)
        return df.copy()
    
    ## List Targets : Users and Devices Combined


    def list_targets(self, refresh=False) -> pd.DataFrame: 
        logging.info('AzureAD: list_targets()')

        if (not refresh) and (not self.targets_df.empty):
            return self.targets_df

        df1   = self.list_users().loc[:, ('id','displayName', 'department', 'country' )]
        df1.columns = ['target_id','target_displayName', 'user_department', 'user_country']
        df1['target_type'] = 'user'
        
        df2 = self.list_devices_users().loc[:, ('device_id', 'device_displayName', 'user_department', 'user_country')]
        df2.columns = ['target_id','target_displayName', 'user_department', 'user_country']
        df2['target_type'] = 'device'

        self.targets_df = pd.concat((df1, df2), axis=0, ignore_index=True)
        return self.targets_df.copy()

    ## List Applications

    def list_service_principals(self, refresh=False) -> pd.DataFrame: 
        logging.info('AzureAD: list_pplications()')

        if (not refresh) and (not self.applications_df.empty):
            return self.applications_df

        url = self.base_url + "servicePrincipals"
        params = {
            '$select': 'id,appId,createdDateTime,accountEnabled,displayName,homepage,notes,preferredSingleSignOnMode,signInAudience,servicePrincipalType,appRoleAssignmentRequired,oauth2PermissionScopes,keyCredentials,passwordCredentials',
            '$expand': 'owners($select=id)'
        }
        
        data =  self.get_all(url, params)
        df = pd.DataFrame(data)
        df['owners_count'] = df.owners.apply( lambda x: len(x))
        df['permissions_count'] = df.oauth2PermissionScopes.apply( lambda x: len(x))
        df['passwords_count'] = df.passwordCredentials.apply( lambda x: len(x))
        df['keys_count'] = df.keyCredentials.apply( lambda x: len(x))
        cols = ['id','appId', 'createdDateTime','accountEnabled','displayName','homepage','notes','preferredSingleSignOnMode','servicePrincipalType','owners_count','permissions_count','passwords_count','keys_count']
        df = df[cols]
        self.applications_df = df
        return df.copy()


    ## Licenses

    def list_users_licenses(self, refresh=False) -> pd.DataFrame:
        logging.info('AzureAD: list_users_licenses')
        users_df = self.list_users(refresh=refresh,include_licenses_plans=True).set_index('id')
        microsoft_df = pd.read_csv('module/microsoft_products.csv', encoding='cp1252').loc[:, ('GUID','Product_Display_Name','String_Id')].drop_duplicates().set_index('GUID')

        users_licenses = []
        for user_id, user in users_df.iterrows():
            for l in user.assignedLicenses:
                users_licenses += [{
                    'user_id': user_id,
                    'skuId': l.get('skuId')
                }]

        users_licenses_df = pd.DataFrame(users_licenses)

        ## merge with license data and full user detail
        df = users_licenses_df.merge(users_df, how='left', left_on='user_id', right_index=True).iloc[:, :-2] \
             .merge(microsoft_df,how='left', left_on='skuId', right_index=True) 
        
        return df
    
    ## Reports

    def list_one_drive_usage(self) -> pd.DataFrame:
        url = f"https://graph.microsoft.com/v1.0/reports/getOneDriveUsageAccountDetail(period='D7')"
        info = self.get_report_info(url)
        df = pd.read_csv(info.url)
        
        col_rename = {
            'Report Refresh Date': 'refresh_date',
            'Owner Display Name': 'owner_name',
            'Is Deleted': 'is_deleted',
            'Last Activity Date': 'last_activity',
            'File Count': 'file_count',
            'Active File Count': 'active_file_count',
            'Owner Principal Name': 'owner_upn',
            'Storage Used (Byte)': 'storage_used'
        }

        df = df.rename( columns = col_rename) \
                .loc[:, col_rename.values()]
        
        ## Fix date columns
        df['last_activity'] = pd.to_datetime(df.last_activity).dt.tz_localize(None)
        df['refresh_date'] = pd.to_datetime(df.refresh_date).dt.tz_localize(None)
        
        return df
