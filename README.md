# aws-s3-to-pandas

Pre-processing code for longitudinal study of options data by ticker.  Uses Python and AWS Lambda.  

AWS Lambda code to split-and-shuffle options data (end-of-day) by ticker.

***Steps:***
1. Download CSV files from AWS S3.  
1. Read CSV into pandas dataframe
1. Split the dataframe into list-of-tuples (str_ticker, df_by_ticker)
1. Upload each tuple-dataframe up to AWS S3 by ticker.  
