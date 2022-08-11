'''
Module to hard-code multiple variables and params
'''
DB_NAME = 'npi_registry'
VIEW = 'distinct_slug_orgs'
TB_NAME = 'distinct_slug_orgs'
OUTPUT_LOCATION = 's3://pulse-analytics-data-1/aws-athena-queries/aws-athena-queries/'

# MongoDB Configs
MONGO_DB = 'pulse-dev'
MONGO_COLLECTION = 'incorrectProviderAffiliates'


# # S3 Path
# PATH = "s3://pulse-analytics-data-1/source=novel_drugs/"

# # Athena
# # S3 Config
# DB_NAME = 'attributes'
# TB_NAME = 'novel_drugs'
# DESC = 'Indicates if drug is novel'
# PARAM = {
#     "source": "Novel Drugs",
#     "class": "novel_drugs"
# }
# COMMENTS = {
# 'uuid' : 'Vega ID product',
# 'novel_drug': 'True or False if novel drug'
# }
