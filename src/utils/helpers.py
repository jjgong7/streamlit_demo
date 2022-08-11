'''
Helper functions
'''
import pandas as pd
import os
import boto3
import pymongo
import streamlit as st
from dotenv import load_dotenv
from utils.config import DB_NAME, VIEW, OUTPUT_LOCATION
from utils.athena_query import get_query_df

# https://docs.streamlit.io/library/advanced-features/caching


@st.cache(allow_output_mutation=True)  # (suppress_st_warning=True)
def get_query():
    '''
    Run the fetch Athena query only once
    '''

    # Load environment variables
    load_dotenv()

    # Create boto3 Session
    session = boto3.Session(aws_access_key_id=os.getenv('aws_access_key_id'),
                            aws_secret_access_key=os.getenv(
                                'aws_secret_access_key'),
                            region_name='us-east-1')

    # Build Query
    query = f'''
                SELECT * FROM {DB_NAME}.{VIEW}
                '''

    # Retrieve Query:
    query_df = get_query_df(query, session, OUTPUT_LOCATION)
    df = query_df.copy()
    df['reason'] = ""

    return df

# @st.cache


def get_pulse_mongo(db, collection):
    '''
    Retrieve data from MongoDB
    '''
    # Load environment
    load_dotenv()

    ##### MONGODB #####
    # Retrieve MongoDB credentials
    mongo_user, mongo_pw = os.getenv(
        'mongo_db_user'), os.getenv('mongo_db_password')

    # Connect to Pulse Analytics MongoDB
    client = pymongo.MongoClient("mongodb+srv://wave-ik4h2.mongodb.net/test",
                                 username=mongo_user,
                                 password=mongo_pw,
                                 authSource='admin',
                                 replicaSet='wave-shard-0',
                                 readPreference='primary',
                                 appname='MongoDB%20Compass%20Community',
                                 ssl=True)

    # Retrieve MongoDB Database
    mydb = client[db]
    # Retrieve MongoDB Collections for Providers
    #coll = mydb.providers
    coll = getattr(mydb, collection)

    # Retrieve all products
    data_all = list(coll.find())
    # Convert list of dictionary to pandas dataframe
    prov_df = pd.DataFrame(data_all)

    return prov_df


def mongo_upload(df_list, db_name, coll_name, **kwargs):
    """
    Uploads data to MongoDB based on DB and Collection
    """
    # Load environment
    load_dotenv()

    ##### MONGODB #####
    # Retrieve MongoDB credentials
    mongo_user, mongo_pw = os.getenv(
        'mongo_db_user'), os.getenv('mongo_db_password')

    # Connect to Pulse Analytics MongoDB
    client = pymongo.MongoClient("mongodb+srv://wave-ik4h2.mongodb.net/test",
                                 username=mongo_user,
                                 password=mongo_pw,
                                 authSource='admin',
                                 replicaSet='wave-shard-0',
                                 readPreference='primary',
                                 appname='MongoDB%20Compass%20Community',
                                 ssl=True)

    # Retrieve time for now
    now = pd.Timestamp.now()
    now = pd.Timestamp(now.year, now.month, now.day)

    # Retrieve parameters arguments
    date = kwargs.get('date', now)

    print(f"Uploading to Mongo database: {db_name}",
          f" and collection name: {coll_name}")

    # Retrieve MongoDB Database
    mydb = client[db_name]
    # Create MongoDB Collection
    coll = getattr(mydb, coll_name)
    # Drop MongoDB collection if it exists
    # if coll.estimated_document_count() !=0:
    #     print("Collection {} was dropped".format(coll))
    #     coll.drop()

    # Insert data into collections
    print("Inserting data into collections...")
    coll.insert_many(df_list)

    print("Adding last updated date to documents...")
    # Add last updated time to collection
    coll.update_many({}, {'$set': {'followers_updated': date}})

    print("Collection has been updated successfully!")


def clear_mongo(db_name, coll_name):
    """
    Clear MongoDB Collection
    """
    # Load environment
    load_dotenv()

    ##### MONGODB #####
    # Retrieve MongoDB credentials
    mongo_user, mongo_pw = os.getenv(
        'mongo_db_user'), os.getenv('mongo_db_password')

    # Connect to Pulse Analytics MongoDB
    client = pymongo.MongoClient("mongodb+srv://wave-ik4h2.mongodb.net/test",
                                 username=mongo_user,
                                 password=mongo_pw,
                                 authSource='admin',
                                 replicaSet='wave-shard-0',
                                 readPreference='primary',
                                 appname='MongoDB%20Compass%20Community',
                                 ssl=True)

    # Retrieve MongoDB Database
    mydb = client[db_name]
    # Create MongoDB Collection
    coll = getattr(mydb, coll_name)
    # Drop MongoDB collection if it exists
    if coll.estimated_document_count() != 0:
        print("Collection {} was dropped".format(coll))
        coll.drop()

    print("Collection has been cleared!")
