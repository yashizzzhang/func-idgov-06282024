import pandas as pd
import requests, logging
from requests.adapters import HTTPAdapter, Retry


logging.info('module.infosec: loading...')

class Infosec:

    base_url    = ''  ## api base url, eg: 'https://securityiq-eu.infosecinstitute.com/api/v2'
    api_key     = ''  ## api key, enabled through user account
    headers     = {}  ## request header that contains api key
    learners_df = pd.DataFrame()
    campaigns_df        = pd.DataFrame()

    ## initialize headers
    def __init__(self, base_url, api_key) -> None:
        logging.info('Infosec: initializing ...')
        self.base_url = base_url
        self.api_key  = api_key
        self.headers = { 
                            'Authorization' : f'Bearer {api_key}',
                             "Content-Type": "application/json"
                        }

    ## return request session with retry/backoff
    def get_session(self, total=5, backoff_factor=1) -> dict:
        retries = Retry(total=total, backoff_factor=backoff_factor)
        s = requests.Session()
        s.mount('https://', HTTPAdapter(max_retries=retries))
        return s    

    ## Low Level Get All Pages(Multiple Pages Call)
    def get_all(self, url, params={}) -> dict:
        s = self.get_session()
        params['page']  = 0       ## start from 1
        params['limit'] = 100     ## maximize per page return
        result = []
        ## loop till last page
        while (True): 
            try:
                params['page'] += 1
                response = s.get(self.base_url+url, headers=self.headers, params=params)
                temp = response.json()
                temp_result = temp['data']
                result += temp_result
                ## last page reached, break
                if len(temp_result) <100: break
            except Exception as e:
                logging.info(f'Infosec: get_all() - Error: {{e}}')
                result = False
                break

        ## done
        return result
    
    ## Return List of Learners
    def list_learners(self, refresh=False) ->pd.DataFrame:
        logging.info('Infosec: list_learners()')
        if (not refresh) and (not self.learners_df.empty):
            return self.learners_df.copy()
        url = '/learners'
        data = self.get_all(url=url)
        df = pd.DataFrame(data)
        df['modified'] = pd.to_datetime(df.modified).dt.tz_localize(None)

        self.learners_df = df
        return df.copy()
    
    ## Return List of Learner Status For A Specific Campagin/Run
    def list_campaignRunLearners(self, campaign=None, run=None) -> pd.DataFrame:
        logging.info('Infosec: list_campaignRunLearners()')
        url = f'/campaigns/{campaign}/runs/{run}/learners'
        data = self.get_all(url=url)
        df = pd.DataFrame(data)
        df['completed_on'] = pd.to_datetime(df.completed_on).dt.tz_localize(None)
        return df

    ## Return list of learner progress for all runs of single campaign
    def list_campaignRunsLearners(self, campaign=None, runs=[]) -> pd.DataFrame:
        logging.info('Infosec: list_campaignRunsLearners()')
        df = pd.DataFrame()
        for run in runs:
            temp_df = self.list_campaignRunLearners(campaign=campaign, run=run)
            temp_df['campaign_id'] = campaign
            temp_df['run_id']      = run
            df = pd.concat([df,temp_df], axis=0)
                
        
        cols = ['learner_id', 'email', 'first_name', 'last_name', 'campaign_id', 'run_id', 'status', 'completed_on']
        
        ## Rows found, reformat header
        if len(df):
            df.rename(columns = { 'id': 'learner_id'}, inplace=True)
            learners_df = self.list_learners()
            df = df.merge(learners_df, how='left', left_on='learner_id', right_on='id')
            df = df[cols]
        
        ## No row found
        else:
            df = pd.DataFrame(columns=cols)
        return df
    
    ## List ALl Campaings
    def list_campaigns(self, refresh=False) -> pd.DataFrame:
        logging.info('Infosec: list_campaigns()')
        if (not refresh) and (not self.campaigns_df.empty):
            return self.campaigns_df.copy()        
        url = '/campaigns'
        data = self.get_all(url=url)
        df = pd.DataFrame(data)
        self.campaigns_df = df
        return df.copy()

    ## List All Runs For A Single Campaign
    def list_campaign_runs(self, campaign=None) -> dict:
        logging.info('Infosec: list_campaign_runs()')
        url = f'/campaigns/{campaign}/runs'
        data = self.get_all(url=url)
        return data

    ## List All Awareness Campaigns, and All associated Runs
    def list_awareness_campaigns_runs(self) -> [dict]:
        logging.info('Infosec: list_awareness_campaigns_runs()')
        campaigns_df = self.list_campaigns().query('type=="awareness"')
        
        data = []
        for idx, campaign in campaigns_df.iterrows():
            data = data + [{
                'campaign' : campaign.to_dict(),
                'runs': self.list_campaign_runs(campaign=campaign.id)
            }] 
        return data
    
    ## Return Learner Progress Of Awareness Campaigns
    def list_learner_progress(self) -> pd.DataFrame():
        df = pd.DataFrame()
        cam_runs     = self.list_awareness_campaigns_runs()
        for cr in cam_runs:
            run_ids = [r.get('id') for r in cr.get('runs')]
            campaign_id = cr.get('campaign').get('id')
            temp_df = self.list_campaignRunsLearners(campaign = campaign_id, runs = run_ids)
            df = pd.concat([df, temp_df], axis=0)
        return df
    

    ## List All Runs For A Single Campaign
    # List All Runs For A Single Campaign
    def list_timeline_events(self, limit=1000) -> pd.DataFrame:
        logging.info('Infosec: list_timeline_events()')
        url = f'/timeline-events?limit={limit}'
        records = self.get_all(url=url)
        target_campaign_id = 'GiT'
        results = [record for record in records if record.get('campaign_id') == target_campaign_id]
        df = pd.DataFrame(results)

        df.rename(columns={
            'learner_id': 'learner_id',
            'campaign_id': 'campaign_id',
            'campaign_run_id': 'run_id',
        }, inplace=True)

        df['email'] = None
        df['first_name'] = None
        df['last_name'] = None
        df['status'] = None
        df['completed_on'] = None
        
        # Reorder the columns to match the desired output
        df = df[['learner_id', 'email', 'first_name', 'last_name', 'campaign_id', 'run_id', 'status', 'completed_on']]
        
        return df