import pandas as pd
from sqlalchemy import create_engine,text
import oracledb
# from CommonUtilities.utilities import sales_data_from_Linux_server
from Configuration.etlconfig import *
import logging

# Logging configution
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="a" ,
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)


oracle_engine = create_engine(
    f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/?service_name={ORACLE_SERVICE}"
)


# MySQL engine
mysql_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

class DataLoading:

    def load_fact_sales_table(self):
        logger.info("Loading the fact_sales table started...")
        try:
            query = text("""insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date)
                            select sd.sales_id,sd.product_id,sd.store_id,sd.quantity,sd.sales_amount,sd.sale_date from intermediate_sales_with_details as sd""")
            with mysql_engine.connect() as conn:
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Loading the fact_sales table completed...")
        except Exception as e:
            logger.error("Error while Loading the fact_sales table",e,exc_info=True)

    def load_fact_inventory_table(self):
        logger.info("Loading the fact_inventory table started...")
        try:
            query = text("""insert into fact_inventory(product_id,store_id,quantity_on_hand,last_updated)
                            select product_id,store_id,quantity_on_hand,last_updated from staging_inventory""")
            with mysql_engine.connect() as conn:
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Loading the fact_inventory table completed...")
        except Exception as e:
            logger.error("Error while Loading the fact_inventory table",e,exc_info=True)

    def load_monthly_sales_summary_table(self):
        logger.info("Loading the monthly_sales_summary table started...")
        try:
            query = text("""insert into monthly_sales_summary(product_id,month,year,total_sales)
                            select product_id,month,year,total_sales from intermediate_monthly_sales_summary_source""")
            with mysql_engine.connect() as conn:
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Loading the monthly_sales_summary table completed...")
        except Exception as e:
            logger.error("Error while Loading the monthly_sales_summary table",e,exc_info=True)


    def load_inventory_level_by_stores_table(self):
        logger.info("Loading the inventory_level_by_stores table started...")
        try:
            query = text("""insert into inventory_levels_by_store(store_id,total_inventory)
                            select store_id,total_inventory from intermediate_aggregated_inventory_level""")
            with mysql_engine.connect() as conn:
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Loading the inventory_level_by_stores table completed...")
        except Exception as e:
            logger.error("Error while Loading the inventory_level_by_stores table",e,exc_info=True)


if __name__ == "__main__":
    dl = DataLoading()
    dl.load_fact_sales_table()
    dl.load_fact_inventory_table()
    dl.load_monthly_sales_summary_table()
    dl.load_inventory_level_by_stores_table()
    print("success from load file")


