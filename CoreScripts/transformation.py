import pandas as pd
from sqlalchemy import create_engine
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


# database connection
# Oracle engine
oracle_engine = create_engine(
    f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/?service_name={ORACLE_SERVICE}"
)
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")


class DataTransformation:

    def transform_filter_sales_data(self):
        logger.info("filter transfromation started...")
        try:
            query = """select * from staging_sales where sale_date >= '2024-09-10'"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("intermediate_filtered_sales",
                      mysql_engine, index=False,if_exists="append")
            logger.info("filter transfromation completed...")
        except Exception as e:
            logger.error("Error while filter transformation",e,exc_info=True)

    def transform_router_sales_data_High_region(self):
        logger.info("router transformation for High region started...")
        try:
            query = """select * from intermediate_filtered_sales where region='High'"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("intermediate_high_sales", mysql_engine, index=False,if_exists="append")
            logger.info("router transformation for High region completed...")
        except Exception as e:
            logger.error("Error while router transformation for High region",e,exc_info=True)

    def transform_router_sales_data_Low_region(self):
        logger.info("router transformation for Low region started...")
        try:
            query = """select * from intermediate_filtered_sales where region='Low'"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("intermediate_low_sales", mysql_engine, index=False,if_exists="append")
            logger.info("router transformation for Low region completed...")
        except Exception as e:
            logger.error("Error while router transformation for Low region",e,exc_info=True)

    def transform_aggregator_sales_data(self):
        logger.info("Agrregator transformation for Sales data started...")
        try:
            query = """select product_id,year(sale_date) as year ,month(sale_date) as month,sum(quantity*price) as total_sales from intermediate_filtered_sales
                        group by product_id,year(sale_date) ,month(sale_date)"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("intermediate_monthly_sales_summary_source", mysql_engine, index=False,if_exists="append")
            logger.info("Agrregator transformation for Sales data completed...")
        except Exception as e:
            logger.error("Error while Agrregator transformation for Sales data",e,exc_info=True)

    def transform_Joiner_sales_product_stores(self):
        logger.info("transform_Joiner_sales_product_stores started...")
        try:
            query = """select fs.sales_id,fs.quantity,fs.price,fs.quantity*fs.price as sales_amount,fs.sale_date,p.product_id,p.product_name,
                        s.store_id,s.store_name
                        from intermediate_filtered_sales as fs
                        inner join staging_product as p on fs.product_id = p.product_id
                        inner join staging_stores as s on fs.store_id = s.store_id"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("intermediate_sales_with_details", mysql_engine, index=False,if_exists="append")
            logger.info("transform_Joiner_sales_product_stores completed...")
        except Exception as e:
            logger.error("Error while transform_Joiner_sales_product_stores",e,exc_info=True)

    def transform_aggregator_inventory_level(self):
        logger.info("transform_aggregator_inventory_level started...")
        try:
            query = """select store_id,sum(quantity_on_hand) as total_inventory from staging_inventory group by store_id"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("intermediate_aggregated_inventory_level", mysql_engine, index=False,if_exists="append")
            logger.info("transform_aggregator_inventory_level completed...")
        except Exception as e:
            logger.error("Error while transform_aggregator_inventory_level",e,exc_info=True)


if __name__ == "__main__":
    dt = DataTransformation()
    dt.transform_filter_sales_data()
    dt.transform_router_sales_data_High_region()
    dt.transform_router_sales_data_Low_region()
    dt.transform_aggregator_sales_data()
    dt.transform_Joiner_sales_product_stores()
    dt.transform_aggregator_inventory_level()
    print("success from transformation")


