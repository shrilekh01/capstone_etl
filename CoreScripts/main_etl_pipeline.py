"""
ETL Pipeline Orchestrator
This script orchestrates the entire ETL pipeline by calling extraction, transformation, and loading modules in sequence.
"""

import logging
import sys
from datetime import datetime

# Import the ETL modules
from CoreScripts.extraction import DataExtraction
from CoreScripts.transformation import DataTransformation
from CoreScripts.load import DataLoading

# Logging configuration
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="a",
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL Pipeline Orchestrator"""

    def __init__(self):
        self.extraction = DataExtraction()
        self.transformation = DataTransformation()
        self.loading = DataLoading()
        self.pipeline_start_time = None
        self.pipeline_end_time = None

    def run_extraction(self):
        """Run all extraction tasks"""
        logger.info("=" * 80)
        logger.info("EXTRACTION PHASE STARTED")
        logger.info("=" * 80)

        try:
            self.extraction.extract_sales_data()
            self.extraction.extract_product_data_and_load_stage()
            self.extraction.extract_inventory_data_and_load_stage()
            self.extraction.extract_supplier_data_and_load_stage()
            self.extraction.extract_stores_data_and_load_stage()

            logger.info("EXTRACTION PHASE COMPLETED SUCCESSFULLY")
            print("✓ Extraction phase completed")
            return True

        except Exception as e:
            logger.error(f"EXTRACTION PHASE FAILED: {e}", exc_info=True)
            print(f"✗ Extraction phase failed: {e}")
            return False

    def run_transformation(self):
        """Run all transformation tasks"""
        logger.info("=" * 80)
        logger.info("TRANSFORMATION PHASE STARTED")
        logger.info("=" * 80)

        try:
            self.transformation.transform_filter_sales_data()
            self.transformation.transform_router_sales_data_High_region()
            self.transformation.transform_router_sales_data_Low_region()
            self.transformation.transform_aggregator_sales_data()
            self.transformation.transform_Joiner_sales_product_stores()
            self.transformation.transform_aggregator_inventory_level()

            logger.info("TRANSFORMATION PHASE COMPLETED SUCCESSFULLY")
            print("✓ Transformation phase completed")
            return True

        except Exception as e:
            logger.error(f"TRANSFORMATION PHASE FAILED: {e}", exc_info=True)
            print(f"✗ Transformation phase failed: {e}")
            return False

    def run_loading(self):
        """Run all loading tasks"""
        logger.info("=" * 80)
        logger.info("LOADING PHASE STARTED")
        logger.info("=" * 80)

        try:
            self.loading.load_fact_sales_table()
            self.loading.load_fact_inventory_table()
            self.loading.load_monthly_sales_summary_table()
            self.loading.load_inventory_level_by_stores_table()

            logger.info("LOADING PHASE COMPLETED SUCCESSFULLY")
            print("✓ Loading phase completed")
            return True

        except Exception as e:
            logger.error(f"LOADING PHASE FAILED: {e}", exc_info=True)
            print(f"✗ Loading phase failed: {e}")
            return False

    def run_pipeline(self):
        """Run the complete ETL pipeline"""
        self.pipeline_start_time = datetime.now()

        logger.info("=" * 80)
        logger.info(f"ETL PIPELINE STARTED AT {self.pipeline_start_time}")
        logger.info("=" * 80)
        print("\n" + "=" * 60)
        print("ETL PIPELINE EXECUTION STARTED")
        print("=" * 60 + "\n")

        # Run Extraction
        if not self.run_extraction():
            logger.error("Pipeline stopped due to extraction failure")
            print("\n✗ Pipeline execution failed at extraction phase")
            sys.exit(1)

        # Run Transformation
        if not self.run_transformation():
            logger.error("Pipeline stopped due to transformation failure")
            print("\n✗ Pipeline execution failed at transformation phase")
            sys.exit(1)

        # Run Loading
        if not self.run_loading():
            logger.error("Pipeline stopped due to loading failure")
            print("\n✗ Pipeline execution failed at loading phase")
            sys.exit(1)

        # Pipeline completed successfully
        self.pipeline_end_time = datetime.now()
        duration = self.pipeline_end_time - self.pipeline_start_time

        logger.info("=" * 80)
        logger.info(f"ETL PIPELINE COMPLETED SUCCESSFULLY AT {self.pipeline_end_time}")
        logger.info(f"TOTAL DURATION: {duration}")
        logger.info("=" * 80)

        print("\n" + "=" * 60)
        print("ETL PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        print(f"Start Time: {self.pipeline_start_time}")
        print(f"End Time: {self.pipeline_end_time}")
        print(f"Duration: {duration}")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    pipeline = ETLPipeline()

    try:
        pipeline.run_pipeline()
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        print("\n⚠ Pipeline execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in pipeline: {e}", exc_info=True)
        print(f"\n✗ Pipeline execution failed: {e}")
        sys.exit(1)