# # if __name__=="__main__":
# #     print("hey")


# import streamlit as st
# import pandas as pd

# st.write("Location Table:")
# df_loc = pd.read_csv('data/locations.csv')

# options = st.multiselect('What locations do you want to search up?',
# list(df_loc['locations_imbruvica_cll'].unique()))


# st.write('You selected:')
# df_options=pd.DataFrame(options,  columns=['locations'])
# st.table(df_options)