import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

def get_emails(process_name="Test Inbound Damages"):
    return ["valusuri@chewy.com"]  # Use your own for testing

def send_email(subject: str, html_body: str, process_name="Test Inbound Damages"):
    recipients = get_emails(process_name)

    message = MIMEMultipart()
    message["From"] = "valusuri@chewy.com"  # Must match your domain
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject

    # Attach HTML body
    message.attach(MIMEText(html_body, "html"))

    try:
        server = smtplib.SMTP("smtp.chewymail.com", 25)
        server.sendmail(message["From"], recipients, message.as_string())
        server.quit()
        print("✅ Test email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

if __name__ == "__main__":
    # Simulate recent rows
    df = pd.DataFrame({
        "wh_id": ["FC01"],
        "po_number": ["PO123456"],
        "item_number": ["ITEM789"],
        "financial_calendar_reporting_year": ["2025"],
        "financial_calendar_reporting_period": ["05"],
        "start_tran_date": ["2025-05-26"],
        "start_tran_date_time": ["2025-05-26 11:30:00"],
        "total_units": [50],
        "total_damage_cost": [1337.42]
    })

    # Build HTML table
    headers = df.columns
    rows_html = ""
    for _, row in df.iterrows():
        rows_html += "<tr>" + "".join(f"<td>{row[col]}</td>" for col in headers) + "</tr>"

    html_body = f"""
        <html>
            <body>
                <p><b>🚨 Test: Inbound Damages Over $1000</b></p>
                <p>This is a test alert for review.</p>
                <table border="1" cellpadding="5" cellspacing="0">
                    <thead><tr>{"".join(f"<th>{col}</th>" for col in headers)}</tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </body>
        </html>
    """

    send_email(
        subject="🚨 TEST EMAIL: Inbound Damages > $1000",
        html_body=html_body,
        process_name="Test Inbound Damages"
    )
