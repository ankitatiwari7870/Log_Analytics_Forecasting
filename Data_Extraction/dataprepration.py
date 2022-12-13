import pandas as pd
import numpy as np
from urllib import parse
import logging
logging.basicConfig(level=logging.DEBUG)

class data_prepration:

    def __init__(self,merged_data):
        self._mdata = merged_data

 

    def duplicate_removal(self):

        mdata1 = self._mdata
        logging.debug('Data duplication removal task started')
        m_data1 = mdata1.drop_duplicates()
        logging.debug('Data duplication Removed')
        return m_data1

    

    def date_cleaning(self,data):
        logging.debug('Data timestamp cleaning started')
        return data["timestamp [UTC]"].apply(lambda x  : ":".join(x.replace("T"," ").split("z")))

    def url_parser(self,url):
        try:
            resource=parse.urlsplit(url)
            return resource.netloc
        except AttributeError:
            pass

    def server_splitter(self):
        df=self.duplicate_removal()
        logging.debug('Data url cleaning started')
        df['url_resgrp_req']=df['url_x'].apply(lambda x:(self.url_parser(x)))
        df['url_resgrp_page']=df['url_y'].apply(lambda x:(self.url_parser(x)))
        logging.debug('Data cleaning completed')
        return df


    def preprocessing(self):
        df = self.server_splitter()
        # creating server columns for both req and page_view.
        # df['url_resgrp_req']=df['url_x'].apply(lambda x:(self.url_parser(x)))
        # df['url_resgrp_page']=df['url_y'].apply(lambda x:(self.url_parser(x)))
        df["time"] = pd.to_datetime(self.date_cleaning(df))
        df['Time']=df['time'].dt.time
        df['Time']=df['Time'].apply(lambda x : x.strftime('%H:%M:%S'))
        df['date']=df['time'].dt.date
        logging.debug('Data Time cleaning completed')
        # df['date']=df['date'].apply(lambda x : x.strftime('%Y-%m-%d'))
        # df['datetime']=df['date']+ ' ' + df['Time']
        df.drop(columns=['time'],inplace=True)
        logging.debug(f'shape of Final processed Data is {df.shape}')
        return df
       













        

    