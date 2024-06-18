import pandas as pd
import pygsheets, logging, math
import datetime as dt

class GoogleSheet:
    
    pgsc = None

    ## Init
    def __init__(self, secret_key) -> None:
        logging.info(f'GoogleSheet: Initializing')

        SCOPES = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/documents.readonly',
            "https://mail.google.com/"
        ]
        
        ## initialize global variables
        self.pgsc = pygsheets.authorize(service_account_json = secret_key,  scopes = SCOPES )
