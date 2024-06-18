import requests, logging, math, random, time, urllib, hmac, hashlib, base64
from sqlalchemy import create_engine, MetaData, Table, func, text
from math import floor 
import pandas as pd
from requests.adapters import HTTPAdapter, Retry
from requests_oauthlib import OAuth1Session
from oauthlib import oauth1

# import sys
# sys.path.append('./module')

class Netsuite:

    ## security related
    api_url = ''
    consumer_key = ''
    consumer_secret = ''
    token_id     = ''
    token_secret = ''
    signature_method = 'HMAC-SHA256'
    version = '1.0'
    deploy = 1
    script = 1740
    standard_params = {}

    ## data related
    employees_df = pd.DataFrame()
    partners_df = pd.DataFrame()
    subsidiaries_df = pd.DataFrame()
    roles_df = pd.DataFrame()
    role_permissions_df = pd.DataFrame()
    employee_roles_df = pd.DataFrame()
    employee_permissions_df = pd.DataFrame()
    partner_roles_df = pd.DataFrame()
    partner_permissions_df = pd.DataFrame()
    role_subsidiaries_df = pd.DataFrame()
    approval_matrix_df = pd.DataFrame()
    employee_license_df = pd.DataFrame()
    role_usage_df = pd.DataFrame()
    scripts_df    = pd.DataFrame()

    ## Initialize
    def __init__(self, account_id, consumer_key, consumer_secret, token_id, token_secret, signature_method='HMAC-SHA256', version='1.0', script=1740, deploy=1) -> None:
        logging.info('Netsuite: initializing ...')
        self.account_id = account_id
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.token_id = token_id
        self.token_secret = token_secret
        self.signature_method = signature_method
        self.version = version
        self.script = script
        self.deploy = deploy
        self.api_url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
        self.standard_params = {
            'script': script,
            'deploy': deploy
        }

    ############################
    ## Low Level Data Retrieval
    ############################

    def post(self, body):

        client = OAuth1Session(
            client_secret=self.consumer_secret,
            client_key=self.consumer_key,
            resource_owner_key=self.token_id,
            resource_owner_secret=self.token_secret,
            realm=self.account_id,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256
        )

        params = self.standard_params
        headers = {
            "Prefer": "transient",
            "Content-Type": "application/json"
        }

        response = client.post(url=self.api_url, json=body, headers=headers, params=self.standard_params)
        return response.json()
    
    ## Low Level Query All Pages(Multiple Pages Call)
    def query_all(self, action='queryRun', query=None,  ss_id=None, page_szie=10000):

        body= {
            "action":   action,
            "query":    query,
            "id":       ss_id,
            "page":     0,
            "pageSize": page_szie
        }

        result = []
        ## loop till last page
        while (True): 
            try:
                temp = self.post(body)
                temp_data = temp['data']
                result += temp_data
                body['page'] += 1
                if body['page'] >= temp['totalPages']:
                    break
            except Exception as e:
                logging.info(f'Netsuite: query_all() - Error: {e}')
                result = False
                break

        ## done
        return result
    

    ############################
    ## Utilities
    ############################

    def perm_to_name(self, level):
        if level==1: return "View"
        elif level==2: return "Create"
        elif level==3: return "Edit" 
        else: return "Full"

    ############################
    ## High Level Data Return
    ############################

    ## List All Active Roles
    def list_roles(self, refresh=False):
        logging.info('Netsuite: list_roles()')

        if (not refresh) and (not self.roles_df.empty):
            return self.roles_df.copy()

        query = """ 
            SELECT
                id,
                name,
                scriptid,
                isinactive,
                issalesrole,
                iswebserviceonlyrole,
                employeerestriction,
                subsidiaryviewingallowed,
                subsidiaryoption,
                effectivesubsidiaries,
                coreadminpermission,
                employeeselectionunrestricted,
                centertype
            FROM
                role
            WHERE
                role.isinactive = 'F'
        """
        data = self.query_all(query=query)
        df = pd.DataFrame(data)

        ## Employee Roles with User Assigned
        query = """ 
            SELECT DISTINCT
                EmployeeRolesForSearch.role as role_id
            FROM
                Employee
                LEFT JOIN EmployeeRolesForSearch ON EmployeeRolesForSearch.entity  = Employee.id
            WHERE
                Employee.giveaccess = 'T'
        """
        data = self.query_all(query=query)
        employee_roles = pd.DataFrame(data).role_id

        ## Partner Roles with User Assigned
        query=""" 
            SELECT DISTINCT
                role_id 
            FROM 
                (   SELECT
                        Partner.id as partner_id,
                        LoginAudit.Role as role_id,
                    ROW_NUMBER() OVER ( PARTITION BY Partner.id ORDER BY LoginAudit.Date desc) as row_num
                    FROM 
                        Partner
                    JOIN 
                        LoginAudit ON Partner.id = LoginAudit.user
                    WHERE
                        Partner.giveaccess = 'T' and LoginAudit.role IS NOT NULL
                ) as A
            WHERE
                A.row_num =1
        """
        data = self.query_all(query=query)
        partner_roles = pd.DataFrame(data).role_id

        ## we want to know if any user assigned to this role
        active_role_ids = pd.concat([employee_roles, partner_roles])
        # active_role_ids = list(set(self.list_employee_roles().role_id.to_list() + self.list_partner_roles().role_id.to_list()))
        df['with_user_assigned'] = df.id.isin(active_role_ids)

        ## Merge with Role Restriction
        restrict_df = self.list_role_restrictions()
        restrict_df.columns = [ 'restrict_' + x for x in restrict_df.columns]
        restrict_df['with_restriction'] = True
        df = pd.merge(df,restrict_df,left_on='id', right_index=True, how='left')
        df['with_restriction'] = df.with_restriction == True
        self.roles_df = df
        return df.copy()

    ## List All Active Roles and Its Permissions 
    def list_role_permissions(self, refresh=False):
        logging.info('Netsuite: list_role_permissions()')

        if (not refresh) and (not self.role_permissions_df.empty):
            return self.role_permissions_df.copy()

        query = """ 
        SELECT
            RolePermissions.role,
            RolePermissions.permkey as perm_key,
            RolePermissions.name as perm_name,
            RolePermissions.permlevel as perm_level,
            RolePermissions.restriction as perm_restriction,
            case 
                when RolePermissions.permlevel = 1 Then 'View' 
                when RolePermissions.permlevel = 2 Then 'Create' 
                when RolePermissions.permlevel = 3 Then 'Edit' 
                else 'Full' 
            end as perm_level_name
        FROM
            RolePermissions
        """
        data = self.query_all(query=query)
        df = pd.DataFrame(data)

        ## merge with roles to get role columns
        roles_df = self.list_roles()
        roles_df.columns = [  'role_' + c for c in roles_df.columns]
        df = pd.merge(roles_df, df, how='left',left_on='role_id', right_on='role')
        df.drop(columns=['role'], inplace=True)
        
        ## join to get actual usage
        role_usage_df = self.list_role_record_usage().loc[:, ['role_id','recordtype_name','perm_level_max']]
        role_usage_df.columns = ['role_id','perm_name','perm_level_max']
        df = pd.merge(df, role_usage_df, on=['role_id', 'perm_name'], how='left')

        ## save cache
        self.role_permissions_df = df

        return df.copy()

    ## List All Active Subsidiaries
    def list_subsidiaries(self, refresh=False):
        logging.info('Netsuite: list_subsidiaries()')

        if (not refresh) and (not self.subsidiaries_df.empty):
            return self.subsidiaries_df.copy()
        
        query = """ 
        SELECT 
            Subsidiary.id, 
            Subsidiary.name,
            Subsidiary.parent,
            Subsidiary.tranprefix, 
            Subsidiary.traninternalprefix, 
            Subsidiary.lastmodifieddate,
            Subsidiary.isinactive,
            BUILTIN.DF(Subsidiary.country) AS country,
            BUILTIN.DF(Subsidiary.currency) AS currency
        FROM 
            Subsidiary
        WHERE
            Subsidiary.isinactive = 'F'
        """
        data = self.query_all(query=query)
        df = pd.DataFrame(data)
        df['lastmodifieddate'] = pd.to_datetime(df.lastmodifieddate, format="%d/%m/%Y")

        ## save cache
        self.subsidiaries_df = df

        return df.copy()

    ## List All Employees with switch on giveaccess
    def list_employees(self, giveaccess_only=False, refresh=False, active_only=True):
        logging.info('Netsuite: list_employees()')

        if (not refresh) and (not self.employees_df.empty):
            return self.employees_df.copy()
        
        # where = "WHERE Employee.isinactive = 'F' and Employee.giveaccess = 'T'" if giveaccess_only else "WHERE Employee.isinactive = 'F'"
        
        if giveaccess_only==True and active_only==True:
            where = f"WHERE Employee.giveaccess = 'T' OR Employee.isinactive='F'"
        elif giveaccess_only==True and active_only==False:
            where = f"WHERE Employee.giveaccess = 'T' OR Employee.isinactive='T'"
        elif giveaccess_only==False and active_only==True:
            where = f"WHERE Employee.giveaccess = 'T' OR Employee.isinactive='F'"
        elif giveaccess_only==False and active_only==False:
            where = f"WHERE Employee.giveaccess = 'T' OR Employee.isinactive='F'"

        # active_clause1 = "and Employee.isinactive='F'" if active_only else ""
        # active_clause2 = "WHERE Employee.isinactive='F' OR Employee.giveaccess = 'T'" if active_only else ""
        # where = f"WHERE Employee.giveaccess = 'T' {active_clause1}" if giveaccess_only else active_clause2

        query = f"""
        SELECT
            Employee.ID AS id,
            Employee.custentity_nera_global_employee_id as global_empid,
            Employee.email,
            Employee.entityid,
            Employee.giveaccess,
            Employee.firstname,
            Employee.middlename,
            Employee.lastname,
            Employee.title,
            Employee.issalesrep,
            Employee.issupportrep,
            Employee.isjobresource,
            Employee.isjobmanager,
            Employee.isinactive,
            Employee.dateCreated,,
            Employee.lastmodifieddate,
            BUILTIN.DF(Employee.Class) AS regco_name,
            Subsidiary.name AS subsidiary_name,
            BUILTIN.DF(Subsidiary.country) AS subsidiary_country,
            Supervisor.email AS supervisor_email,
            Department.fullName AS department
        FROM
            Employee
	        LEFT OUTER JOIN Department ON Employee.department = Department.id            
	        LEFT OUTER JOIN Subsidiary ON Employee.subsidiary = Subsidiary.id
            LEFT OUTER JOIN Employee AS Supervisor ON Supervisor.id = Employee.supervisor
        {where}
        """

        data = self.query_all(query=query)
        df = pd.DataFrame(data)
        df['datecreated'] = pd.to_datetime(df.datecreated, format="%d/%m/%Y")
        df['lastmodifieddate'] = pd.to_datetime(df.lastmodifieddate, format="%d/%m/%Y")
        
        ## Fix Netsuite Data Error
        df['entityid'] = df.entityid.str.replace('  ',' ')
        df['entityid'] = df.entityid.str.replace('   ',' ')

        ## Standarize Case
        df['email'] = df.email.str.lower()
        
        ## save cache
        self.employees_df = df
        
        return df.copy()

    ## List All Employees (both active and inactive)
    def list_employee_all(self):
        logging.info('Netsuite: list_employee_all()')
        df = self.list_employees(giveaccess_only=False, refresh=True, active_only=False)
        return df

    ## List All Active Partners or Given Access with switch on giveaccess
    def list_partners(self, giveaccess_only=False, refresh=False):
        logging.info('Netsuite: list_partners()')

        if (not refresh) and (not self.partners_df.empty):
            return self.partners_df.copy()

        where = "WHERE Partner.giveaccess = 'T'" if giveaccess_only else "WHERE Partner.isinactive = 'F'"

        query = f"""
        SELECT
            Partner.id,
            Partner.email,
            Partner.partnercode,
            Partner.entityid,
            Partner.giveaccess as partner_license,
            Partner.firstname,
            Partner.middlename,
            Partner.lastname,
            Partner.title,
            Partner.isperson,
            Partner.isinactive,
            Partner.subpartnerlogin,
            Partner.companyname,
            Partner.datecreated,
            Partner.lastmodifieddate,
            Subsidiary.name AS subsidiary_name,
            BUILTIN.DF(Subsidiary.country) AS subsidiary_country,
            Supervisor.email AS supervisor_email,
            BUILTIN.DF(Partner.department) AS department
        FROM
            Partner
            LEFT OUTER JOIN PartnerSubsidiaryRelationship ON PartnerSubsidiaryRelationship.entity= Partner.id
            LEFT OUTER JOIN Subsidiary ON PartnerSubsidiaryRelationship.subsidiary = subsidiary.id
            LEFT OUTER JOIN Partner AS Supervisor ON Supervisor.ID = Partner.parent
            {where}
        """
        data = self.query_all(query=query)
        df = pd.DataFrame(data)
        df['datecreated'] = pd.to_datetime(df.datecreated, format="%d/%m/%Y")
        df['lastmodifieddate'] = pd.to_datetime(df.lastmodifieddate, format="%d/%m/%Y")

        ## Standarize Case
        df['email'] = df.email.str.lower()

        ## save cache
        self.partners_df = df

        return df.copy()
    
    ## Union All Active Partners and Employees with Given Access Only
    def union_employees_partners(self):

        p = self.list_partners(giveaccess_only=True, refresh=True)
        p['license_type'] = 'Partner'
        e = self.list_employee_license()
        # e = self.list_employees(giveaccess_only=True, refresh=True)
        # e['source'] = 'employee'
        df = pd.concat((e,p)).loc[:, ('id', 'email', 'entityid', 'subsidiary_name', 'subsidiary_country', 'department', 'license_type')]
        return df

    ## List Given Access Partners Roles
    def list_partner_roles(self, refresh=False):
        logging.info('Netsuite: list_partner_roles()')
        if (not refresh) and (not self.partner_roles_df.empty):
            return self.partner_roles_df.copy()
                
        query=""" 
        SELECT 
            partner_id, 
            role_id 
        FROM 
            (   SELECT
                    Partner.id as partner_id,
                    LoginAudit.Role as role_id,
                ROW_NUMBER() OVER ( PARTITION BY Partner.id ORDER BY LoginAudit.Date desc) as row_num
                FROM 
                    Partner
                JOIN 
                    LoginAudit ON Partner.id = LoginAudit.user
                WHERE
                    Partner.giveaccess = 'T' and LoginAudit.role IS NOT NULL
            ) as A
        WHERE
            A.row_num =1
        """
        data = self.query_all(query=query)
        df = pd.DataFrame(data)

        ## merge with roles to get role columns
        roles_df = self.list_roles()
        roles_df.columns = [  'role_' + c for c in roles_df.columns]
        
        ## Partners - Roles Join
        partners_df = self.list_partners(giveaccess_only=True, refresh=True)
        df = pd.merge(partners_df, df, how='inner', left_on='id', right_on='partner_id')\
               .merge(roles_df, how='inner',left_on='role_id', right_on='role_id')\
               .drop(columns=['partner_id'])

        self.partner_roles_df = df
        return df.copy()

    ## List Given Access Employees Roles
    def list_employee_roles(self, refresh=False):
        logging.info('Netsuite: list_employee_roles()')

        if (not refresh) and (not self.employee_roles_df.empty):
            return self.employee_roles_df.copy()
        
        query = """ 
            SELECT 
                EmployeeRolesForSearch .role,
                Employee.id as entity
            FROM
                Employee
                LEFT JOIN EmployeeRolesForSearch ON EmployeeRolesForSearch.entity  = Employee.id
            WHERE
                Employee.giveaccess = 'T'
        """
        data = self.query_all(query=query)
        df = pd.DataFrame(data)

        ## merge with roles to get role columns
        roles_df = self.list_roles()
        roles_df.columns = [  'role_' + c for c in roles_df.columns]
        
        ## Employees - Roles Join
        employees_df = self.list_employees(giveaccess_only=True, refresh=True)
        df = pd.merge(employees_df, df, how='inner', left_on='id', right_on='entity')\
               .merge(roles_df, how='inner',left_on='role', right_on='role_id')\
               .drop(columns=['role','entity'])

        ## save cache
        self.employee_roles_df = df

        return df.copy()        
    
    ## List Employee License (Given Access, regarless active or not)
    def list_employee_license(self, refresh=False):
    
        if (not refresh) and (not self.employee_license_df.empty):
            return self.employee_license_df.copy()
        
        df = self.list_employee_roles()  # employees given access only, regardless Active or not
        df['license_type'] = df.role_centertype.apply(lambda x: 'Employee' if x=='EMPLOYEE' else 'Full')
        cols = ['id','email','entityid','giveaccess','firstname','middlename','lastname','title','issalesrep','issupportrep','isjobresource','isjobmanager','datecreated','lastmodifieddate','isinactive','regco_name','subsidiary_name','subsidiary_country','supervisor_email','department','license_type']
        df = df[cols].drop_duplicates()
        
        ## save cache
        self.employee_license_df = df

        return df.copy()

    ## Join All Active Roles and Its Effective Subsidiaries
    def list_role_subsidiaries(self, refresh=False):
        if (not refresh) and (not self.role_subsidiaries_df.empty):
            return self.role_subsidiaries_df.copy()
        
        subsidiaries_df = self.list_subsidiaries()
        subsidiaries_df.columns = [ 'subsi_' + c for c in subsidiaries_df.columns]

        roles_df = self.list_roles()
        roles_df.columns = [ 'role_' + c for c in roles_df.columns]

        non_na = roles_df.role_effectivesubsidiaries.notna()
        roles_df = roles_df[non_na]
        records = []
        for idx, row in roles_df.iterrows():
                subsidiaries = row.role_effectivesubsidiaries.split(', ')
                for s in subsidiaries:
                        record = {
                                'role_id': row.role_id,
                                'subsi_id': int(s)
                        }
                        records = records + [record]

        role_subsi_map = pd.DataFrame(records)

        df = pd.merge(role_subsi_map, roles_df, how='left', left_on='role_id', right_on='role_id')\
               .merge(subsidiaries_df, how='left', left_on='subsi_id', right_on='subsi_id')
        
        self.role_subsidiaries_df = df
        return df.copy()
    
    ## List All Active Approval Matrix
    def list_approval_matrix(self, refresh=False):
        if (not refresh) and (not self.approval_matrix_df.empty):
            return self.approval_matrix_df.copy()
        

        employees_df = self.list_employees().set_index('id')

        query=""" 
        SELECT
            RecordList.id   AS list_id,
            RecordList.name AS list_name,
            Matrix.recordid AS record_id,
            Matrix.name     AS record_name,
            Matrix.custrecord_nra_desc                 AS description,
            Matrix.custrecord_nra_record_approver_l1       AS approver_l1,
            Matrix.custrecord_nra_record_approver_l1_email AS approver_l1_email,
            Matrix.custrecord_nra_record_approver_l2       AS approver_l2,
            Matrix.custrecord_nra_record_approver_l2_email AS approver_l2_email,
            Matrix.custrecord_nra_record_approver_l3       AS approver_l3,
            Matrix.custrecord_nra_record_approver_l3_email AS approver_l3_email,
            Matrix.custrecord_nra_record_approver_l4       AS approver_l4,
            Matrix.custrecord_nra_record_approver_l4_email AS approver_l4_email,
            Matrix.custrecord_nra_record_approver_l5       AS approver_l5,
            Matrix.custrecord_nra_record_approver_l5 AS approver_l5_email,
            Matrix.custrecord_nra_record_approver_l6       AS approver_l6,
            Matrix.custrecord_nra_record_approver_l6 AS approver_l6_email
        FROM
            CUSTOMLIST_NERA_APPROVAL_RECORD_LIST RecordList
        LEFT JOIN
            CUSTOMRECORD_NERA_APPROVAL_MATRIX Matrix ON RecordList.recordId = Matrix.custrecord_nra_nera_approval_record
        WHERE
            Matrix.isinactive = 'F'
        """
        data = self.query_all(query=query)
        approval_matrix_df = pd.DataFrame(data)

        approval_matrix_df['approver_l1_names'] = ''
        approval_matrix_df['approver_l2_names'] = ''
        approval_matrix_df['approver_l3_names'] = ''
        approval_matrix_df['approver_l1_email_names'] = ''
        approval_matrix_df['approver_l2_email_names'] = ''
        approval_matrix_df['approver_l3_email_names'] = ''

        for idx, row in approval_matrix_df.iterrows():

            ## L1 Approvers Renaming
            if row.approver_l1:
                approver_list = [int(x) for x in row.approver_l1.split(',')]
                
                approver_names=[]
                for id in approver_list:
                    try: 
                        approver_names = approver_names + [employees_df.loc[id, 'entityid']]
                    except:
                        approver_names = approver_names + [f'Invalid({str(id)})']
                l1_names = ', '.join(approver_names)
                approval_matrix_df.loc[idx, 'approver_l1_names'] = l1_names

            ## L1_Email Approvers Renaming
            if row.approver_l1_email:
                approver_list = [int(x) for x in row.approver_l1_email.split(',')]
                
                approver_names=[]
                for id in approver_list:
                    try: 
                        approver_names = approver_names + [employees_df.loc[id, 'entityid']]
                    except:
                        approver_names = approver_names + [f'Invalid({str(id)})']
                l1_names = ', '.join(approver_names)
                approval_matrix_df.loc[idx, 'approver_l1_email_names'] = l1_names

            ## L2 Approvers Renaming
            if row.approver_l2:
                approver_list = [int(x) for x in row.approver_l2.split(',')]
                
                approver_names=[]
                for id in approver_list:
                    try: 
                        approver_names = approver_names + [employees_df.loc[id, 'entityid']]
                    except:
                        approver_names = approver_names + [f'Invalid({str(id)})']
                l1_names = ', '.join(approver_names)
                approval_matrix_df.loc[idx, 'approver_l2_names'] = l1_names

            ## L2_Email Approvers Renaming
            if row.approver_l2_email:
                approver_list = [int(x) for x in row.approver_l2_email.split(',')]
                
                approver_names=[]
                for id in approver_list:
                    try: 
                        approver_names = approver_names + [employees_df.loc[id, 'entityid']]
                    except:
                        approver_names = approver_names + [f'Invalid({str(id)})']
                l1_names = ', '.join(approver_names)
                approval_matrix_df.loc[idx, 'approver_l2_email_names'] = l1_names        
            ## L3 Approvers Renaming
            if row.approver_l3:
                approver_list = [int(x) for x in row.approver_l3.split(',')]
                
                approver_names=[]
                for id in approver_list:
                    try: 
                        approver_names = approver_names + [employees_df.loc[id, 'entityid']]
                    except:
                        approver_names = approver_names + [f'Invalid({str(id)})']
                l1_names = ', '.join(approver_names)
                approval_matrix_df.loc[idx, 'approver_l3_names'] = l1_names

            ## L3_Email Approvers Renaming
            if row.approver_l3_email:
                approver_list = [int(x) for x in row.approver_l3_email.split(',')]
                
                approver_names=[]
                for id in approver_list:
                    try: 
                        approver_names = approver_names + [employees_df.loc[id, 'entityid']]
                    except:
                        approver_names = approver_names + [f'Invalid({str(id)})']
                l1_names = ', '.join(approver_names)
                approval_matrix_df.loc[idx, 'approver_l3_email_names'] = l1_names

        cols = ['list_id','list_name','record_id','record_name','description','approver_l1_names','approver_l1_email_names','approver_l2_names','approver_l2_email_names','approver_l3_names','approver_l3_email_names']
        approval_matrix_df = approval_matrix_df[cols]

        self.approval_matrix_df = approval_matrix_df
        return approval_matrix_df.copy()
        
    ## List All Daily Successful/Failure Logins
    def list_login_audits(self, last_n_days=60):

        query = f""" 
            SELECT 
                user_id, 
                date, 
                COUNT(date) as login_count
            FROM (SELECT 
                LoginAudit.user AS user_id,
                TRUNC(LoginAudit.Date) as date
            FROM
                LoginAudit
            WHERE
                LoginAudit.Date >= SYSDATE - {last_n_days} and status='Success'
            )
            GROUP BY
                user_id, date
        """

        data = self.query_all(query=query)
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df.date, format="%d/%m/%Y")

        return df
    
    ## List all Daily Failure Logins
    def list_login_failure(self, last_n_days=60):

        query = f""" 
            SELECT
                TO_CHAR ( LoginAudit.date, 'YYYY-MM-DD HH:MI:SS' ) AS datetime,
                TO_CHAR ( LoginAudit.date, 'YYYY-MM-DD' ) AS date,
                LoginAudit.user as user_id,
                BUILTIN.DF(LoginAudit.user) as entitiId,
                LoginAudit.detail,
                LoginAudit.ipAddress,
                LoginAudit.requestUri,
                LoginAudit.userAgent
            FROM
                LoginAudit
            WHERE 
                LoginAudit.Date >= SYSDATE - {last_n_days} and status='Failure'
            ORDER BY
                LoginAudit.date DESC
        """

        data = self.query_all(query=query)
        df = pd.DataFrame(data)
        df['date'] =     pd.to_datetime(df['date'],     format="%Y-%m-%d").dt.date
        df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%d %H:%M:%S")
        return df

    ## List Custom Records Recordtypes Definition
    def list_custom_records_definition(self):
        query = f""" 
        SELECT
            CustomRecordType.allowAttachments,
            CustomRecordType.allowInlineDeleting,
            CustomRecordType.allowInlineEditing,
            CustomRecordType.allowQuickSearch,
            CustomRecordType.description,
            CustomRecordType.enableMailMerge,
            CustomRecordType.isInactive,
            CustomRecordType.includeName,
            CustomRecordType.internalId,
            CustomRecordType.isOrdered,
            CustomRecordType.lastModifiedDate,
            CustomRecordType.name,
            CustomRecordType.noPermissionRequired,
            CustomRecordType.owner,
            CustomRecordType.scriptId,
            CustomRecordType.showNotes,
            CustomRecordType.usePermissions
        FROM
            CustomRecordType
        """

        data = self.query_all(query=query)
        df = pd.DataFrame(data)

        return df
    
    ## List Custom List Definition
    def list_custom_list_definition(self):

        query = f""" 
        SELECT
            CustomList.description,
            CustomList.isInactive,
            CustomList.internalId,
            CustomList.isOrdered,
            CustomList.lastModifiedDate,
            CustomList.name,
            CustomList.owner,
            CustomList.scriptId
        FROM
            CustomList
        """
        data = self.query_all(query=query)
        df = pd.DataFrame(data)
        return df

    ## List All Standard and Custom Recordtypes Definition
    def list_all_records_definition(self):

        logging.info('Netsuite: list_all_records_definition()')

        ## internal record types read from CSV
        # internal_df = pd.read_csv('ns_internal_recordtypes.csv')
        internal_df = pd.read_csv('module/ns_internal_recordtypes.csv')
        internal_df['recordtype_type'] = 'Standard'
        
        ## custom record types read from Netsuite
        customrec_df = self.list_custom_records_definition().loc[:, ['internalid','name']]
        customrec_df['recordtype_type'] = 'Custom Record'

        customlist_df = self.list_custom_list_definition().loc[:, ['internalid','name']]
        customlist_df['recordtype_type'] = 'Custom List'
        
        ## Union Them
        df = pd.concat([internal_df, customrec_df, customlist_df])
        df.columns = ['internalid', 'recordtype_name', 'recordtype_type']
        return df
    
    ## List the maximum permission used for all roles
    def list_role_record_usage(self, refresh=False, ndays=180):
        logging.info('Netsuite: list_role_record_usage()')

        if (not refresh) and (not self.role_usage_df.empty):
            return self.role_usage_df.copy()

        q = f''' 
            SELECT
                SystemNote.role as role_id, 
                SystemNote.recordTypeId as recordtypeid, 
                case 
                    when MAX(SystemNote.type) = 1 Then 'View' 
                    when MAX(SystemNote.type) = 2 Then 'Create' 
                    when MAX(SystemNote.type) = 3 Then 'Edit' 
                    else 'Full' 
                end as perm_level_max
            FROM
                SystemNote
            WHERE
                SystemNote.date >= SYSDATE - {ndays} AND
                SystemNote.field NOT LIKE '%MEDIA%' AND
                SystemNote.role IS NOT NULL
            GROUP BY
                SystemNote.role, SystemNote.recordTypeId
        '''
        result = self.query_all(query=q)
        df1 = pd.DataFrame(result)
        
        ## join to get recordtype definition
        df2 = self.list_all_records_definition()
        df3 = pd.merge(df1, df2, left_on='recordtypeid', right_on='internalid', how='left')
        df3.drop(columns='internalid', inplace=True)
        
        ## join to get role definition
        df4 = self.list_roles().loc[:, ['id','name']]
        df4.columns = ['role_id','role_name']
        self.role_usage_df = pd.merge(df4, df3, on='role_id', how='right')
        
        return self.role_usage_df.copy()
    
    ## List Role Restrictions (index is role id)
    def list_role_restrictions(self):
        logging.info('Netsuite: list_role_restrictions()')
        q = ''' 
            SELECT
                roleRestrictions.viewingAllowed,
                roleRestrictions.itemsRestricted,
                roleRestrictions.restriction,
                roleRestrictions.role,
                roleRestrictions.segment
            FROM
                roleRestrictions
            '''
        result = self.query_all(query=q)
        df = pd.DataFrame(result)

        ## encode restriction description
        df = df.replace(-101, 'class').replace(-102,'department').replace(-103,'location')
        ## encode restriction parameters
        for idx, row in df.iterrows():
            value = '' 
            if row.viewingallowed == 'T':
                value =  'view=T'
            if row.itemsrestricted == 'T':
                value =   value + ', items=T' if value else '(items=T)'
            df.loc[idx, 'restriction'] = row.restriction + ' (' + value + ')' if value else row.restriction
            
        ## convert to pivot
        df = df.pivot(index='role', columns='segment',values='restriction')
        return df

    ## List All Script Records
    def list_client_scripts(self):
        logging.info('Netsuite: list_client_scripts()')

        q = ''' 
            SELECT
                clientScript.id,
                clientScript.scriptid,
                clientScript.apiversion,
                clientScript.isinactive,
                clientScript.scripttype,
                clientScript.description,
                clientScript.name,
                clientScript.owner,
                clientScript.fieldchangedfunction,
                clientScript.lineinitfunction,
                clientScript.notifyadmins,
                clientScript.notifyuser,
                clientScript.notifyemails,
                clientScript.notifygroup,
                clientScript.notifyowner,
                clientScript.pageinitfunction,
                clientScript.postsourcingfunction,
                clientScript.recalcfunction,
                clientScript.saverecordfunction,
                clientScript.scriptfile,
                clientScript.validatedeletefunction,
                clientScript.validatefieldfunction,
                clientScript.validateinsertfunction,
                clientScript.validatelinefunction
            FROM
                clientScript
            '''
        result = self.query_all(query=q)
        s = pd.DataFrame(result)
        
        ## combine with employees (owners)
        e =  self.list_employee_all()
        e.columns = [ 'owner_' + n for n in e.columns]
        df = pd.merge( s,e, left_on='owner', right_on='owner_id', how='left')
        df.drop(columns='owner', inplace=True)

        ## combine with number of deployments
        d = self.list_client_script_deployments()
        g = d.groupby('script').size()
        g.name = 'deploy_count'
        df = pd.merge(df, g, left_on='id', right_index=True, how='left')
        
        return df
    
    ## List Client Script Deployments
    def list_client_script_deployments(self):
        logging.info('Netsuite: list_client_script_deployments()')
        q = ''' 
            SELECT
                clientScriptDeployment.id,
                clientScriptDeployment.script,
                clientScriptDeployment.scriptid as deploy_name,
                clientScriptDeployment.recordtype,
                clientScriptDeployment.allemployees,
                clientScriptDeployment.alllocalizationcontexts,
                clientScriptDeployment.allpartners,
                clientScriptDeployment.allroles,
                clientScriptDeployment.audience,
                clientScriptDeployment.hascodeaccess,
                clientScriptDeployment.isdeployed,
                clientScriptDeployment.deploymentid,
                clientScriptDeployment.eventtype,
                clientScriptDeployment.loglevel,
                clientScriptDeployment.primarykey,
                clientScriptDeployment.status,
                clientScriptDeployment.istask,
                clientScriptDeployment.version
            FROM
                clientScriptDeployment
        '''

        result = self.query_all(query=q)
        df = pd.DataFrame(result)

        # ## merge of script info
        # s = self.list_client_scripts()
        # s.columns = [ 'cs_' + n for n in s.columns]
        # df = pd.merge(d, s, left_on='script', right_on='cs_id')
        # df.drop(columns='script', inplace=True)
        return df
    
    ## List Script logs
    def list_script_logs(self):
        logging.info('Netsuite: list_script_logs()')
        q = ''' 
            SELECT
                ScriptNote.internalId,
                ScriptNote.date,
     	        ScriptNote.type as log_type,
                ScriptNote.scriptType as script_id,
                script.scripttype as script_type,
                script.name as script_name,
                ScriptNote.title,
                ScriptNote.detail
            FROM
                ScriptNote
                LEFT OUTER JOIN script ON ScriptNote.scriptType = script.id		
            WHERE 
                type IN ('ERROR','SYSTEM')
            ORDER BY date asc
        '''
        result = self.query_all(query=q)
        df = pd.DataFrame(result)
        df['date'] = pd.to_datetime(df.date, format="%d/%m/%Y")

        return df

    ## List All Scripts (active and inactive)
    def list_scripts(self, refresh=False):
        logging.info('Netsuite: list_scripts()')

        if (not refresh) and (not self.scripts_df.empty):
            return self.scripts_df.copy()
                
        q = ''' 
            SELECT
                script.id,
                script.scriptid,
                script.scripttype,
                script.isinactive,
                script.name,
                script.owner,
                script.deploymentmodel,
                script.description,
                script.apiversion,
                script.afterinstallfunction,
                script.aftersubmitfunction,
                script.afterupdatefunction,
                script.beforeinstallfunction,
                script.beforeloadfunction,
                script.beforesubmitfunction,
                script.beforeuninstallfunction,
                script.beforeupdatefunction,
                script.class,
                script.complexfunction,
                script.deletefunction,
                script.typedocumentationfile,
                script.fieldchangedfunction,
                script.defaultfunction,
                script.getfunction,
                script.getinputdatafunction,
                script.dfolderlastupdate,
                script.lineinitfunction,
                script.returnrecordtype,
                script.mapfunction,
                script.notifyadmins,
                script.notifyuser,
                script.notifyemails,
                script.notifygroup,
                script.notifyowner,
                script.pageinitfunction,
                script.parametrizedfunction,
                script.portlettype,
                script.postfunction,
                script.postsourcingfunction,
                script.putfunction,
                script.recalcfunction,
                script.reducefunction,
                script.returntype,
                script.saverecordfunction,
                script.scriptfile,
                script.simplefunction,
                script.summarizefunction,
                script.validatedeletefunction,
                script.validatefieldfunction,
                script.validateinsertfunction,
                script.validatelinefunction
            FROM
                script
        '''
        result = self.query_all(query=q)
        df = pd.DataFrame(result)

        ## merge with employee records to get owner detail
        e = self.list_employee_all().loc[:,['id','entityid','email','firstname','lastname','isinactive','title']]
        e.columns = [ 'owner_' + x for x in e.columns]
        df = pd.merge( df, e, how='left', left_on='owner', right_on='owner_id')        
        df.drop(columns='owner_id', inplace=True)
        self.scripts_df = df
        return df.copy()