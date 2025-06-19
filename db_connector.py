# utils/db.py
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
from job_logger import logging

load_dotenv()

def get_snowpark_session():
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }

    logging.info("Creating Snowpark session with externalbrowser auth")
    try:
        session = Session.builder.configs(connection_parameters).create()
        logging.info(f"Connected to Snowflake as {session.get_current_user()}")
        return session
    except Exception as e:
        logging.exception("Failed to connect to Snowflake")
        raise

if __name__ == "__main__":
    get_snowpark_session()

