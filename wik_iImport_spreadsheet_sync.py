## THE SCRIPT
import pandas as pd
from pymongo import MongoClient, ReadPreference
import etlw as et
from utils import get_config_field
from gspread_pandas import Spread


def get_mongo_db_object(prod=True):
    if prod:
        db_url = 'prod_db_url'
    else:
        db_url = 'dev_db_url'
    MONGO_DB_NAME = get_config_field('MONGODB', 'db_name')
    MONGO_DB_URL = get_config_field('MONGODB', db_url)
    client = MongoClient(MONGO_DB_URL, read_preference=ReadPreference.SECONDARY)
    db = client[MONGO_DB_NAME]
    return db


def update_pages_in_db(completeness_values, truth_value, tags_collection):
    ids = completeness_values[completeness_values == truth_value].index.tolist()
    update_results = tags_collection.update_many(filter={"_id": {"$in": ids}},
                                                 update={"$set": {'lesswrongWikiImportCompleted': truth_value}})

    return update_results


def run_spreadsheet_sync():

    # Get Mongo
    db = get_mongo_db_object(prod=False)
    tags_collection = db['tags']

    # Get Spreadsheet
    spreadsheet_user = get_config_field('GSHEETS', 'user')
    s = Spread(user=spreadsheet_user, spread='Old LessWrong Wiki Import', sheet='Imported/Merged Pages')

    df = s.sheet_to_df().iloc[3:]
    completed = df['Looks Good! / Completed!'] != ''

    # Perform Update
    a = update_pages_in_db(completed, True, tags_collection)
    b = update_pages_in_db(completed, False, tags_collection)

if __name__ == '__main__':
    run_spreadsheet_sync()
