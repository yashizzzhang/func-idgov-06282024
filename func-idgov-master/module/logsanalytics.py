import logging
from datetime import datetime, timedelta
from azure.monitor.query import LogsQueryClient
import pandas as pd
import numpy as np

class LogsAnalytics:

    client    = None
    logs_id   = ''

    def __init__(self, logs_id, credential) -> None:
        logging.info('LogAanalytics: initializing...')
        self.client = LogsQueryClient(credential)
        self.logs_id = logs_id

    ## default timeout is 5m
    def query_table(self, query, time_span, server_timeout=300) -> pd.DataFrame:
        
        logging.info('LogAanalytics: query_table()')
        ## query and process response
        response = self.client.query_workspace(self.logs_id, query, timespan=time_span, server_timeout=server_timeout)

        ## query and return
        data = response.tables
        for table in data:   ## always one table returned only
            df  = pd.DataFrame(table.rows, columns=table.columns)
        return df


    def list_signins(self, time_span=timedelta(days=30), server_timeout=300) -> pd.DataFrame:
        logging.info('LogAanalytics: list_signin()')
        query="""
        SigninLogs
        | project TimeGenerated, Id, UserId, AppDisplayName, ResultType, IPAddress, LocationDetails, DeviceDetail
        | extend FailureOrSuccess = iff(ResultType in ("0", "50125", "50140"), "Success", "Failure")
        | extend Country=tostring(LocationDetails.countryOrRegion)
        | extend City=tostring(LocationDetails.city)
        | extend DeviceId=tostring(DeviceDetail.deviceId)
        | project-away  DeviceDetail, LocationDetails
        """

        df = self.query_table(query, time_span, server_timeout)

        # standardize null values to None
        df = df.fillna(np.nan).replace([np.nan], [None]).drop_duplicates()

        # Fix DateTime so that it is compatible with SQL datetime field type
        df['TimeGenerated'] = pd.to_datetime(df.TimeGenerated).dt.tz_localize(None) 
        
        return df
    

    def list_audits_alerts(self, time_span=timedelta(days=30), server_timeout=300) -> pd.DataFrame:
        query = r'''
            AuditLogs
            | extend initiatedby_userid  = tostring(InitiatedBy.user.id)          
            | extend initiatedby_upn     = tostring(InitiatedBy.user.userPrincipalName)
            | extend initiatedby_name    = tostring(InitiatedBy.user.displayName)
            | extend initiatedby_ip      = tostring(InitiatedBy.user.ipAddress)
            | mv-expand AdditionalDetail = AdditionalDetails
            | extend additional_detail = strcat(AdditionalDetail.key, ": ", AdditionalDetail.value)
            | mv-expand Targets = TargetResources
            | extend target_id          = tostring(Targets["id"])
            | extend target_displayName = tostring(Targets["displayName"])
            | extend target_type        = tostring(Targets["type"])
            | extend target_upn         = tostring(Targets["userPrincipalName"])
            | extend target_modifiedProperties = Targets["modifiedProperties"]
            | mv-apply tp = target_modifiedProperties on (
                extend modified_properties = strcat(tp['displayName'], ": ", tp['newValue'],'\n')
                | summarize modified_properties = replace_strings(strcat_array(make_list(modified_properties), '/'),dynamic(['"',']','[','/']),dynamic(['','','','']))
            )
            | where InitiatedBy  has "user"  and target_id != "" and target_id  != initiatedby_userid
            | project TimeGenerated, Category, AADOperationType, ActivityDisplayName, Result, initiatedby_userid, initiatedby_upn, initiatedby_name, initiatedby_ip, target_id, target_displayName, target_upn, target_type, additional_detail, modified_properties
            '''
        df = self.query_table(query, time_span)
        
        # standardize null values to None
        df = df.fillna(np.nan).replace([np.nan], [None]).drop_duplicates()
        
        # Fix DateTime so that it is compatible with SQL datetime field type
        df['TimeGenerated'] = pd.to_datetime(df.TimeGenerated).dt.tz_localize(None) 
        
        return df
    
    ## Query for Realtime Alerts Purpose
    def list_audits(self, time_span=timedelta(days=30), server_timeout=300) -> pd.DataFrame:
        logging.info('LogAanalytics: list_audits()')
        query = """ 
        AuditLogs
        | extend Initiatedby_userId           = tostring(InitiatedBy.user.id)          
        | extend Initiatedby_userPrincipalName= tostring(InitiatedBy.user.userPrincipalName)
        | extend Initiatedby_ipaddress        = tostring(InitiatedBy.user.ipAddress)
        | extend InitiatedBy_servicePrincipalId = tostring(InitiatedBy.app.servicePrincipalId)
        | extend InitiatedBy_appDisplayName   = tostring(InitiatedBy.app.displayName)
        | mv-expand Targets = TargetResources
        | extend target_id          = tostring(Targets["id"])
        | extend target_displayName = tostring(Targets["displayName"])
        | extend target_type        = tostring(Targets["type"])
        | extend target_modifiedProperties        = Targets["modifiedProperties"]
        | project Id, TimeGenerated, AADOperationType, ActivityDisplayName, Category, 
                Result, ResultDescription, Identity,
                Initiatedby_userId, Initiatedby_userPrincipalName, Initiatedby_ipaddress, InitiatedBy_servicePrincipalId, InitiatedBy_appDisplayName,
                LoggedByService, target_id, target_displayName, target_type, target_modifiedProperties, AdditionalDetails
        """ 

        df = self.query_table(query, time_span)
        
        # standardize null values to None
        df = df.fillna(np.nan).replace([np.nan], [None]).drop_duplicates()
        
        # Fix DateTime so that it is compatible with SQL datetime field type
        df['TimeGenerated'] = pd.to_datetime(df.TimeGenerated).dt.tz_localize(None) 
        
        return df
    

    ### Generic Query
    #################
    def list(self, query=None, time_span=timedelta(days=30), server_timeout=300) -> pd.DataFrame:
        logging.info('LogAanalytics: list()')
        df = self.query_table(query, time_span)
        
        # standardize null values to None
        df = df.fillna(np.nan).replace([np.nan], [None]).drop_duplicates()
        
        # Fix DateTime so that it is compatible with SQL datetime field type
        if 'TimeGenerated' in df.columns:
            df['TimeGenerated'] = pd.to_datetime(df.TimeGenerated).dt.tz_localize(None) 
        
        return df