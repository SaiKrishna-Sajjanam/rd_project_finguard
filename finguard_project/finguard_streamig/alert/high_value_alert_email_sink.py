import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from pyspark import pipelines as dp

# Gmail account (sender address and app password retrieved from the Databricks secret scope)
# Sender email id: sk.nscw@gmail.com  (stored in secret 'gmail_sender_email', admin-updatable)
EMAIL_FROM = dbutils.secrets.get(
    scope="finguard-scope",
    key="gmail_sender_email"
)

gmail_api_key = dbutils.secrets.get(
    scope="finguard-scope",
    key="gmail_api_key"
)


def send_alert_emails(rows):
    """Send one email per alert row in the batch, reusing a single SMTP session."""
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL_FROM, gmail_api_key)

        for row in rows:
            if row.customer_email is None:
                continue

            subject = f"High Value Transaction Alert - {row.transaction_id}"

            body = f"""
            <html>
            <body>
                <h2>High Value Transaction Alert</h2>
                <p>Dear {row.customer_name},</p>
                <p>A transaction was performed with a value higher than your configured limit.</p>
                <table cellpadding="6" cellspacing="0" border="1">
                    <tr><td><b>Transaction ID</b></td><td>{row.transaction_id}</td></tr>
                    <tr><td><b>Amount</b></td><td>{row.transaction_amount} {row.currency}</td></tr>
                    <tr><td><b>Configured Limit</b></td><td>{row.transaction_limit} {row.currency}</td></tr>
                    <tr><td><b>Merchant</b></td><td>{row.merchant_name} ({row.merchant_category})</td></tr>
                    <tr><td><b>Transaction Type</b></td><td>{row.transaction_type}</td></tr>
                    <tr><td><b>Location</b></td><td>{row.city}, {row.country} (International: {row.is_international})</td></tr>
                    <tr><td><b>Transaction Time</b></td><td>{row.transaction_timestamp}</td></tr>
                    <tr><td><b>Status</b></td><td>{row.status}</td></tr>
                </table>
                <p>If you did not perform this transaction, please contact support immediately.</p>
            </body>
            </html>
            """

            msg = MIMEMultipart()
            msg["From"] = EMAIL_FROM
            msg["To"] = row.customer_email
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "html"))

            try:
                server.send_message(msg)
                print(f"Email sent successfully for alert {row.alert_id} to {row.customer_email}")
            except Exception as e:
                print(f"Error sending email for alert {row.alert_id}:", e)


@dp.foreach_batch_sink(
    name="high_value_transactions_email_sink"
)
def high_value_transactions_email_sink(df, batch_id):
    rows = df.collect()
    if not rows:
        return

    send_alert_emails(rows)


@dp.append_flow(
    target="high_value_transactions_email_sink"
    ,name="high_value_transactions_email_flow"
    ,comment="Stream new high value transaction alerts and send them as emails"
)
def high_value_transactions_email_flow():
    return spark.readStream.table("finguard.gold.high_value_transactions_alert")