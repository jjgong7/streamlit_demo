# # if __name__=="__main__":
# #     print("hey")
import sys
import os
import boto3
import awswrangler as wr
import streamlit as st
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
# from utils.config import DB_NAME, TB_NAME, VIEW, OUTPUT_LOCATION
# from utils.athena_query import get_query_df, get_query
from utils.helpers import get_query, mongo_upload, get_pulse_mongo, clear_mongo
from utils.config import MONGO_DB, MONGO_COLLECTION

# Retrieve query
df_query = get_query()
df = df_query.copy()

# Retrieve Mongo
# df_mongo = get_pulse_mongo(MONGO_DB, MONGO_COLLECTION)
# dfm = df_mongo.copy()
dfm_new = get_pulse_mongo(MONGO_DB, MONGO_COLLECTION)

# Examples

# Unique Providers
uniq_providers = list(df['name'].unique())

#Display the grid
st.header("NPI Registry Provider --> Affiliates")
st.write('S3 Data')
# df_loc = pd.read_csv('data/locations.csv')

providers_choice = st.sidebar.multiselect(
    'Choose providers:',
    uniq_providers,
    default = ['compass oncology']
)

# Filter dataframe based off of providers sidebar
df = df[df['name'].isin(providers_choice)]

# https://streamlit-aggrid.readthedocs.io/en/docs/GridOptionsBuilder.html
options_builder = GridOptionsBuilder.from_dataframe(df)
options_builder.configure_column('reason', editable=True)
options_builder.configure_selection('multiple', use_checkbox=True)
options_builder.configure_pagination(paginationPageSize=30)
grid_options = options_builder.build()

# Generate Grid
grid_return = AgGrid(df, grid_options, fit_columns_on_grid_load=True,
update_mode = GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED)

# new_df = grid_return['data']
selected_rows = grid_return['selected_rows']

# st.write('You selected new:')
# st.write(new_df)

# Select Rows
df_selected = pd.DataFrame(selected_rows)
list_dict = df_selected.to_dict('records')

st.subheader("Incorrect provider --> affiliates")
st.write('TDG Selection')
if df_selected.empty:
    st.write('No rows have been selected')
else:
    st.write(pd.DataFrame(selected_rows))

# Sidebar Update data
if st.sidebar.button('Update MongoDB'):
    if len(list_dict)>0:
        # Next steps: make sure none are duplicated. Otherwise allow removal
        mongo_upload(list_dict, db_name=MONGO_DB, coll_name=MONGO_COLLECTION)
        dfm_new = get_pulse_mongo(MONGO_DB, MONGO_COLLECTION)
    else:
        st.write('No rows have been selected')

# Sidebar Update data
if st.sidebar.button(f'Clear MongoDB'):
    clear_mongo(MONGO_DB, MONGO_COLLECTION)
    dfm_new = get_pulse_mongo(MONGO_DB, MONGO_COLLECTION)

st.subheader(f"Incorrect Affiliates in MongoDB")
st.write('Pulse MongoDB')
st.write(f'Collection: {MONGO_DB}.{MONGO_COLLECTION}')
mongo_grid = AgGrid(dfm_new)



# Current incorrect provider-->affiliates




# st.table(new_df)

# Next steps:
# 1. Add or remove rows: https://www.ag-grid.com/javascript-data-grid/data-update-transactions/
# 1. Incorporate Google Sheets API
# https://share.streamlit.io/streamlit/example-app-bug-report/main
# https://github.com/streamlit/example-app-bug-report/blob/main/streamlit_app.py
