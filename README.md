# aws-s3-to-pandas
Playbook for AWS Lambda, AWS S3, and pandas.  

Pre-processing code for longitudinal study of options data by ticker.  Uses Python and AWS Lambda.  

AWS Lambda code to split-and-shuffle options data (end-of-day) by ticker.  Learned that pandas groupby objects can be convert into list of tuples (key, dataframe).  

Similar concept as split-map-shuffle-reduce paradigm used in "big data" analysis.  

***Steps:***
1. Download CSV files from AWS S3  
1. Read CSV into pandas dataframe
1. Split the dataframe into list-of-tuples (str_ticker, df_by_ticker)
1. Upload each tuple-dataframe up to AWS S3 by ticker 

***Further research:***
1. Extend groupby into Type (call/put)
1. Extend groupby into Expiration Date
1. Extend groupby into Strike Price
1. Look for opportunities to "map" after "split"
1. Look for opportunities to "reduce" after "shuffle"
