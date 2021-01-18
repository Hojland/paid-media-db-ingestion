import google_ads
import google_campaign_manager
import facebook

import settings
from utils import sql_utils
import sql

if __name__ == "__main__":
    google_ads.main()
    google_campaign_manager.main()
    facebook.main()

    mariadb_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name='output', db_type='mysql')
    res = mariadb_engine.execute(sql.COLLECTED_VIEW)