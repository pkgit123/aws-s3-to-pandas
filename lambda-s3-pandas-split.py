# ==============================================================================
# Filename:     deltaneutral_s3_shuffle_ticker
# Date:         Sep 23, 2020
# Description:  Retrieve option data S3.  Unzip files stored by date.
#               Shuffle data by UnderlyingSymbol (ticker).
#               Save to different S3 location.  
#
# ==============================================================================
'''
References:
* https://stackoverflow.com/questions/30818341/how-to-read-a-csv-file-from-an-s3-bucket-using-pandas-in-python
* https://www.geeksforgeeks.org/split-large-pandas-dataframe-into-list-of-smaller-dataframes/
'''

# standard AWS Lambda python packages
import boto3
from io import StringIO

# lambda layers
import pandas as pd

def read_s3_csv_to_pandas(str_s3_bucket, str_s3_key):
    '''
    Read CSV file from S3, into pandas dataframe.
    
    Input:
        str_s3_bucket - str, name of S3 bucket
            For example, 'conifers'
        str_s3_key - str, name of CSV file in S3 
            For example, 'unzip_daily_files/options_20200923.csv'
    '''
    
    
    # instantiate boto3-S3 client
    s3_client = boto3.client('s3')
    
    # read CSV from S3 to pandas
    csv_obj = s3_client.get_object(Bucket=str_s3_bucket, Key=str_s3_key)
    csv_body = csv_obj['Body']
    csv_string = csv_body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    
    print("Address of CSV file")
    print(f"s3://{str_s3_bucket}/{str_s3_key}")
    print("Shape of dataframe")
    print(df.shape)
    print(df.columns)
    print(df.head())
    print()
    
    print("Group dataframe into tickers.")
    print("Result is list of tuples length 2.")
    print("Each tuple: first item is ticker, second item is dataframe")
    
    # split dataframe into list of tuple-dataframes, tuple length 2
    # each tuple has ticker (str) and dataframe
    ls_tp_df_ticker = list(df.groupby('UnderlyingSymbol'))
    print("Length of list of tuples with dataframes: ", len(ls_tp_df_ticker))
    
    return ls_tp_df_ticker
    
    
def upload_csv_s3_shuffle_by_ticker(
        str_s3_bucket_up, 
        str_s3_key_up,
        str_partial_filename_up, 
        ls_tp_df_ticker):
    '''
    Write multiple CSV files to S3 from list of tuple-dataframes.
    '''
    
    # instantiate boto3-S3 resource
    s3_resource = boto3.resource('s3')
    
    ### only loop through 5 ... loop through all when done ### 
    for each_tuple in ls_tp_df_ticker:
        
        # unpack each tuple: ticker and dataframe
        (str_ticker, df_ticker) = each_tuple
        
        # create s3 object key
        s3_object_key_up = f"{str_s3_key_up}{str_ticker}/{str_partial_filename_up}.csv"
        
        try:
            # Write dataframe to string buffer
            csv_buffer = StringIO()
            df_ticker.to_csv(csv_buffer, index=False)
            
            # upload string buffer using s3 resource
            s3_resource.Object(str_s3_bucket_up, s3_object_key_up).put(Body=csv_buffer.getvalue())
            
        except:
            print(f"Error writing ticker: {str_ticker}")
    

def lambda_handler(event, context):
    
    # Variables to download from S3
    str_s3_bucket_down = "conifers"
    str_s3_key_down = 'unzip_daily_files/options_20200923.csv'
    
    # grab partial filename for upload
    # 'unzip_daily_files/options_20200923.csv'  => 'options_20200923'
    str_partial_filename_up = str_s3_key_down.replace('.', '/').split('/')[1]
    
    # Variables to upload to S3
    str_s3_bucket_up = "conifers"
    str_s3_key_up = 'shuffle_by_ticker/'
    
    # read CSV to list of tuple-dataframes
    ls_tp_df_ticker = read_s3_csv_to_pandas(str_s3_bucket_down, str_s3_key_down)
    
    # upload CSV to S3
    upload_csv_s3_shuffle_by_ticker(
        str_s3_bucket_up, 
        str_s3_key_up,
        str_partial_filename_up,
        ls_tp_df_ticker
        )
    
    return 
