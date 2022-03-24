import pandas as pd
import boto3
import os
import logging
from io import BytesIO
import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import URL

class Redshift:
    def __init__(
        self,
        redshift_url: str,
        database_name: str,
        redshift_username: str,
        redshift_password: str
        ) -> None:
        """Initialise the Redshift class, and create SQLAlchemy Engine
        Args:
            redshift_url (str): The full url of Redshift: <clusterid>.xxxxxx.<aws-region>.redshift.amazonaws.com
            database_name (str): The database in Redshift you wish to load to
            redshift_username (str): The Redshift username you wish to authenticate with
            redshift_password (str): The Redshift password you wish to authenticate with
        """
        self.logger = logging.getLogger("Redshift")
        self.redshift_dsn = URL.create(
            drivername="redshift+psycopg2",
            host=redshift_url,
            port=5439,
            database=database_name,
            username=redshift_username,
            password=redshift_password
        )
        self.logger.info(f"Created redshift DSN:\n{self.redshift_dsn}")

        self.redshift_engine = create_engine(self.redshift_dsn)

        self.logger.info("Created engine")
    

    def verify_connection(self) -> bool:
        """Connects to DB and returns True if connection is good and False if not
        Returns:
            bool: True or False representing connection state
        """
        try:
            self.redshift_engine.connect()
            return True
        except sqlalchemy.exc.SQLAlchemyError as err:
            return False

    
    def load_df_to_table(self, dataframe: pd.DataFrame, table_name: str):
        """Loads a dataframe to the Redshift cluster table specified
        Args:
            dataframe (pandas.DataFrame): A pandas dataframe containing all the formatted data
            table_name (str): The name of the table the data is being loaded to
        """
        self.logger.info(f"Loading a dataframe of {dataframe.shape[0]} rows to table {table_name}")
        with self.redshift_engine.connect() as redshift_db:
            dataframe.to_sql(
                name=table_name,
                con=redshift_db,
                index=False,
                if_exists="append"
            )
        self.logger.info("Dataframe loaded successfully")

    
    def load_query_to_df(self, query: str) -> pd.DataFrame:
        """Executes a query and loads the results into DF
        Args:
            query (str): The query to execute
        Returns:
            pandas.DataFrame: The dataframe object containing the query results
        """
        self.logger.info(f"Loading Query: {query}")
        with self.redshift_engine.connect() as redshift_db:
            df = pd.read_sql(
                sql=query,
                con=redshift_db
            )
        return df


def download_csv_from_S3_Bucket(bucket_name: str,csv_file :str):
    """Downloads a csv file from an s3 bucket and saves in your local directory
    Args:
        bucket_name(str): The name of the S3 Bucket that you want to download from
        csv_file(str): The name of the csv file that you want to download
     """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    if not os.path.exists(csv_file):
        bucket.download_file(csv_file,csv_file)


def uploading_csv_to_S3_Bucket(df: pd.DataFrame,csv_file: str,bucket_name: str):
    """Uploads a dataframe to an s3 bucket 
    Args:
        df: The dataframe object containing the cleaned data
        csv_file(str): The name of the csv file that you want to upload
        bucket_name(str): The name of the S3 Bucket that you want to download from
     """
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, encoding= 'UTF-8',mode='wb',index=False)
    csv_buffer.seek(0)
    s3_client = boto3.client('s3')
    s3_client.upload_fileobj(csv_buffer,bucket_name,csv_file)
