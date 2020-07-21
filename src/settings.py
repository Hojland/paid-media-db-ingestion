import os

# RESOURCES_PATH = 'resources/'
# UPDATE_FREQUENCY = 60 * 60 * 6 # deprecated. Controlled in Jenkins

MARIADB_CONFIG = {
    "user": os.environ["MARIADB_USR"],
    "psw": os.environ["MARIADB_PSW"],
    "host": "cubus.cxxwabvgrdub.eu-central-1.rds.amazonaws.com",
    "port": 3306,
    "db": "input",
}


## GOOGLE
SCOPE = u'https://www.googleapis.com/auth/adwords'
GOOGLE_CLIENT_CUSTOMER_ID = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
YOUSEE_CUSTOMER_ID = '298-878-5433'