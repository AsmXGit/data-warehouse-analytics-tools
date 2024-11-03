import os
import logging
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
from retrying import retry

# Set up logging with a timestamp format and log level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class BigQueryValidator:
    def __init__(self, project_id, dataset_id):
        # Ensure environment variables are set
        if not project_id or not dataset_id:
            raise EnvironmentError("Environment variables GCP_PROJECT_ID and BQ_DATASET_ID are required.")

        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def validate_table(self, table_id):
        """Validates the integrity of a specific table in BigQuery."""
        try:
            # Get the table reference
            table_ref = self.client.dataset(self.dataset_id).table(table_id)
            table = self.client.get_table(table_ref)

            # Check the row count
            row_count = table.num_rows
            logging.info(f'Validating table: {table_id} with {row_count} rows.')

            # Perform more checks here
            self.check_null_values(table_id)
            self.check_schema(table_id)

            logging.info(f'Table {table_id} validated successfully.')

        except NotFound:
            logging.error(f'Table {table_id} not found in dataset {self.dataset_id}.')
        except GoogleCloudError as e:
            logging.error(f'Google Cloud error during validation of table {table_id}: {str(e)}')
            raise
        except Exception as e:
            logging.error(f'Unexpected error during validation of table {table_id}: {str(e)}')
            raise

    def check_null_values(self, table_id):
        """Check for null values in the table."""
        query = f"""
            SELECT COUNT(*) AS null_value_count
            FROM `{self.dataset_id}.{table_id}`
            WHERE column_name IS NULL
        """
        try:
            result = self.client.query(query).result()
            null_count = [row['null_value_count'] for row in result][0]
            if null_count > 0:
                logging.warning(f'Table {table_id} has {null_count} null values in column_name.')
            else:
                logging.info(f'Table {table_id} has no null values in column_name.')
        except GoogleCloudError as e:
            logging.error(f'Error checking null values for table {table_id}: {str(e)}')
            raise

    def check_schema(self, table_id):
        """Check if the schema matches expected schema."""
        expected_schema = [
            bigquery.SchemaField("column_name", "STRING"),
            # Add other expected fields as required
        ]

        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_id)
            table = self.client.get_table(table_ref)

            if table.schema != expected_schema:
                logging.warning(f'Table {table_id} schema does not match expected schema.')
            else:
                logging.info(f'Table {table_id} schema is valid.')
        except GoogleCloudError as e:
            logging.error(f'Error checking schema for table {table_id}: {str(e)}')
            raise

    def validate_all_tables(self):
        """Validate all tables in the dataset."""
        try:
            tables = self.client.list_tables(self.dataset_id)
            for table in tables:
                self.validate_table(table.table_id)
        except GoogleCloudError as e:
            logging.error(f'Error listing tables in dataset {self.dataset_id}: {str(e)}')
            raise

if __name__ == "__main__":
    # Fetch environment variables and validate them
    PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    DATASET_ID = os.getenv('BQ_DATASET_ID')

    if not PROJECT_ID or not DATASET_ID:
        logging.error("GCP_PROJECT_ID and BQ_DATASET_ID environment variables must be set.")
        raise SystemExit("Environment variables missing.")

    # Create an instance of the validator
    validator = BigQueryValidator(PROJECT_ID, DATASET_ID)

    # Validate all tables in the dataset
    try:
        validator.validate_all_tables()
        logging.info("All tables validated successfully.")
    except Exception as e:
        logging.error(f"Validation process encountered an error: {str(e)}")
        raise
