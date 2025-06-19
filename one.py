from pathlib import Path
from datetime import datetime
from time import time
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
from tabulate import tabulate

# ------------------ Logging Setup ------------------ #
def setup_logging():
    LOG_FILE = Path(__file__).parent / "run_log.log"
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

    logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

def log_start():
    logging.info("=" * 80)
    logging.info("Job started.")

def log_success(rows_inserted: int, total_damage: float, duration: float):
    logging.info(f"Job succeeded | Rows inserted: {rows_inserted} | Total damage cost: ${total_damage:,.2f}")
    logging.info(f"[SUMMARY] Job completed in {duration:.2f} seconds")

def log_error(error: Exception):
    logging.error(f"[ERROR] Job failed | {str(error)}", exc_info=True)

# ------------------ Snowflake Connection ------------------ #
def get_snowpark_session():
    load_dotenv()
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }

    logging.info("[SESSION] Creating Snowpark session with externalbrowser auth")
    try:
        session = Session.builder.configs(connection_parameters).create()
        logging.info(f"[SESSION] Connected to Snowflake as {session.get_current_user()}")
        return session
    except Exception as e:
        logging.exception("[SESSION] Failed to connect to Snowflake")
        raise

# ------------------ SQL Execution ------------------ #
def execute_sql_statements(session):
    sql_path = Path(__file__).parent / "query.sql"
    with sql_path.open() as f:
        query = f.read()

    statements = [
        stmt.strip()
        for stmt in query.split(";")
        if stmt.strip()
        and not stmt.strip().startswith("--")
        and not stmt.strip().startswith("/*")
        and not stmt.strip().endswith("*/")
    ]

    for stmt in statements:
        if stmt.lower().startswith(("use ", "alter session")):
            logging.info(f"[SKIP] Unsupported statement: {stmt}")
            continue

        preview = stmt.replace("\n", " ")[:100]
        logging.info(f"[SQL] Executing SQL: {preview}...")
        session.sql(stmt).collect()

def fetch_alert_summary(cursor):
    cursor.execute("""
        SELECT COUNT(*) 
        FROM EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K 
        WHERE email_notified = FALSE AND total_damage_cost >= 1000
    """)
    rows_inserted = cursor.fetchone()[0]

    cursor.execute("""
        SELECT SUM(total_damage_cost) 
        FROM EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K 
        WHERE email_notified = FALSE AND total_damage_cost >= 1000
    """)
    total_damage_cost = cursor.fetchone()[0] or 0.0

    return rows_inserted, total_damage_cost

# ------------------ Email Alert Logic ------------------ #
def send_email_alert(session):
    results = session.sql("""
        SELECT * 
        FROM EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K 
        WHERE email_notified = FALSE AND total_damage_cost >= 1000
    """).collect()

    raw_headers = results[0].as_dict().keys()
    headers = [h for h in raw_headers if h.lower() != "email_notified"]

    rows_html = ""
    for row in results:
        row_dict = row.as_dict()
        rows_html += "<tr>" + "".join(f"<td>{row_dict[h]}</td>" for h in headers) + "</tr>"

    html_body = f"""
        <html>
            <body>
                <p><b>Inbound Damages Over $1000 Detected</b></p>
                <p>Total Rows Inserted: {len(results)}<br>
                Total Damage Cost: ${sum([float(row.as_dict().get('TOTAL_DAMAGE_COST', 0)) for row in results]):,.2f}</p>
                <table border="1" cellpadding="5" cellspacing="0">
                    <thead>
                        <tr>{"".join(f"<th>{h}</th>" for h in headers)}</tr>
                    </thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </body>
        </html>
    """

    recipients = ["valusuri@chewy.com"]  

    message = MIMEMultipart()
    message["From"] = "valusuri@chewy.com"
    message["To"] = ", ".join(recipients)
    message["Subject"] = "Inbound Damages Over $1K - New Instances Detected"
    message.attach(MIMEText(html_body, "html"))

    server = smtplib.SMTP("smtp.chewymail.com", 25)
    server.sendmail(message["From"], recipients, message.as_string())
    server.quit()

    logging.info("[EMAIL] Email alert successfully sent.")

    session.sql("""
        UPDATE EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K 
        SET email_notified = TRUE 
        WHERE email_notified = FALSE AND total_damage_cost >= 1000
    """).collect()
    logging.info("[EMAIL] Updated rows as notified.")

    return results

def log_inserted_rows(results):
    try:
        log_fields = ["WH_ID", "PO_NUMBER", "ITEM_NUMBER", "TOTAL_UNITS", "TOTAL_DAMAGE_COST"]
        table_rows = [[row.as_dict().get(f) for f in log_fields] for row in results]
        table_str = tabulate(table_rows, headers=log_fields, tablefmt="github")
        logging.info("[ROWS] Inserted rows:\n" + table_str)
    except:
        logging.warning("[ROWS] Failed to log inserted rows.")

# ------------------ Main Job ------------------ #
def run_job():
    setup_logging()
    start_time = time()
    log_start()

    try:
        session = get_snowpark_session()
        cursor = session._conn._cursor
        cursor.execute("BEGIN")

        execute_sql_statements(session)

        cursor.execute("COMMIT")
        logging.info("[SQL] Transaction committed.")

        rows_inserted, total_damage_cost = fetch_alert_summary(cursor)

        if rows_inserted > 0:
            logging.info("[EMAIL] Preparing alert for new rows...")
            try:
                results = send_email_alert(session)
                cursor.execute("COMMIT")
            except Exception as e:
                logging.exception("[EMAIL] Failed to send email or update flag")
                log_inserted_rows(results)

        duration = time() - start_time
        log_success(rows_inserted, total_damage_cost, duration)

        logging.info("[SESSION] Closing Snowflake session")
        session.close()

    except Exception as e:
        try:
            cursor.execute("ROLLBACK")
            logging.info("[SQL] Transaction rolled back due to error.")
        except:
            logging.warning("[SQL] Failed to rollback transaction.")
        log_error(e)

if __name__ == "__main__":
    run_job()
