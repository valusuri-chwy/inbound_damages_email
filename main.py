from pathlib import Path
import job_logger
from db_connector import get_snowpark_session
from snowflake.snowpark.exceptions import SnowparkSQLException

# Email libraries
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def run_job():
    
    job_logger.log_start()

    try:
        session = get_snowpark_session()
        cursor = session._conn._cursor
        cursor.execute("BEGIN")

        sql_path = Path("query.sql")
        with sql_path.open() as f:
            query = f.read()

        rows_inserted = 0
        total_damage_cost = 0.0

        # Clean SQL statements
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
                job_logger.logging.info(f"⚠️ Skipping unsupported statement: {stmt}")
                continue

            preview = stmt.replace("\n", " ")[:100]
            job_logger.logging.info(f"➡️ Executing SQL: {preview}...")

            if stmt.lower().startswith("insert"):
                try:
                        logging.info(f"statement: {stmt}")
                        session.sql(stmt).collect()
                        query_id = cursor.sfqid
                        logging.info(f"ℹ️ Query ID: {query_id}")

                        # ✅ More reliable row count check using table content
                        cursor.execute("""
                            SELECT COUNT(*) 
                            FROM EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K 
                            WHERE start_tran_date_time >= current_timestamp - interval '5 minutes'
                        """)
                        rows_inserted = cursor.fetchone()[0]

                except Exception as e:
                        logging.warning("⚠️ Could not fetch inserted row count from table.")
                        logging.exception("Exception while counting inserted rows:")
                        rows_inserted = 0

                try:
                    damage_result = session.sql("SELECT SUM(total_damage_cost) FROM ib_damages").collect()
                    total_damage_cost = damage_result[0][0] or 0.0
                except SnowparkSQLException:
                    total_damage_cost = 0.0


            else:
                session.sql(stmt).collect()

        if rows_inserted > 0:
            cursor.execute("COMMIT")
            job_logger.logging.info("✅ Transaction committed.")
        else:
            cursor.execute("ROLLBACK")
            job_logger.logging.info("ℹ️ No new rows inserted, transaction rolled back.")

        job_logger.log_success(rows_inserted, total_damage_cost)

        # ✅ Email alert if rows inserted
        if rows_inserted >= 1:
            job_logger.logging.info("📬 Preparing email alert for new rows...")

            try:
                results = session.sql("""
                    SELECT *
                    FROM EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K
                    WHERE start_tran_date_time >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
                """).collect()

                if results:
                    headers = results[0].as_dict().keys()
                    rows_html = ""
                    for row in results:
                        row_dict = row.as_dict()
                        rows_html += "<tr>" + "".join(f"<td>{row_dict[h]}</td>" for h in headers) + "</tr>"

                    html_body = f"""
                        <html>
                            <body>
                                <p><b>🚨 Inbound Damages Over $1000 Detected</b></p>
                                <p>Total Rows Inserted: {rows_inserted}<br>
                                Total Damage Cost: ${total_damage_cost:,.2f}</p>
                                <table border="1" cellpadding="5" cellspacing="0">
                                    <thead>
                                        <tr>{"".join(f"<th>{h}</th>" for h in headers)}</tr>
                                    </thead>
                                    <tbody>{rows_html}</tbody>
                                </table>
                            </body>
                        </html>
                    """

                    recipients = [
                        # "dl-ib_vc_specialists@chewy.com",
                        # "dl-fc_ib_seniors@chewy.com"
                        "valusuri@chewy.com", "rortiz5@chewy.com"
                    ]

                    message = MIMEMultipart()
                    message["From"] = "valusuri@chewy.com"
                    message["To"] = ", ".join(recipients)
                    message["Subject"] = "🚨 Inbound Damages Over $1K - New Records Detected"
                    message.attach(MIMEText(html_body, "html"))

                    server = smtplib.SMTP("smtp.chewymail.com", 25)
                    server.sendmail(message["From"], recipients, message.as_string())
                    server.quit()

                    job_logger.logging.info("✅ Email alert successfully sent.")
                else:
                    job_logger.logging.info("ℹ️ No new rows matched for email content.")
            except Exception as e:
                job_logger.logging.error(f"❌ Failed to send email alert: {e}")

    except Exception as e:
        try:
            cursor.execute("ROLLBACK")
            job_logger.logging.info("❌ Transaction rolled back due to error.")
        except:
            job_logger.logging.warning("⚠️ Failed to rollback transaction.")
        job_logger.log_error(e)

if __name__ == "__main__":
    run_job()
