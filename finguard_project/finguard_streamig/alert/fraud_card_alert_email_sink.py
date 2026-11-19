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


def send_fraud_alert_emails(rows):
    """Send one email per fraud alert row in the batch, reusing a single SMTP session."""
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL_FROM, gmail_api_key)

        for row in rows:
            if row.customer_email is None:
                continue

            masked_card_number = f"****-****-****-{str(row.card_number)[-4:]}"

            subject = f"Fraud Alert - Suspicious Transaction {row.transaction_id}"

            body = f"""
            <html>
            <body>
                <h2>Fraud Alert - Suspicious Transaction Detected</h2>
                <p>Dear {row.customer_name},</p>
                <p>A transaction was performed with your card that matches our fraud watchlist. Please review the details below.</p>
                <table cellpadding="6" cellspacing="0" border="1">
                    <tr><td><b>Alert ID</b></td><td>{row.alert_id}</td></tr>
                    <tr><td><b>Transaction ID</b></td><td>{row.transaction_id}</td></tr>
                    <tr><td><b>Amount</b></td><td>{row.amount} {row.currency}</td></tr>
                    <tr><td><b>Card Number</b></td><td>{masked_card_number}</td></tr>
                    <tr><td><b>Merchant</b></td><td>{row.merchant_name} ({row.merchant_category})</td></tr>
                    <tr><td><b>Transaction Type</b></td><td>{row.transaction_type}</td></tr>
                    <tr><td><b>Payment Channel</b></td><td>{row.payment_channel}</td></tr>
                    <tr><td><b>Device ID</b></td><td>{row.device_id}</td></tr>
                    <tr><td><b>Location</b></td><td>{row.transaction_city}, {row.transaction_country} (International: {row.is_international})</td></tr>
                    <tr><td><b>Transaction Time</b></td><td>{row.transaction_timestamp}</td></tr>
                    <tr><td><b>Status</b></td><td>{row.transaction_status}</td></tr>
                </table>
                <h3>Fraud Watchlist Details</h3>
                <table cellpadding="6" cellspacing="0" border="1">
                    <tr><td><b>Watch Type</b></td><td>{row.watch_type}</td></tr>
                    <tr><td><b>Risk Level</b></td><td>{row.risk_level}</td></tr>
                    <tr><td><b>Action</b></td><td>{row.action}</td></tr>
                    <tr><td><b>Reason</b></td><td>{row.reason_code} - {row.reason_description}</td></tr>
                    <tr><td><b>Reported By</b></td><td>{row.reported_by} ({row.reported_source})</td></tr>
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
                print(f"Email sent successfully for fraud alert {row.alert_id} to {row.customer_email}")
            except Exception as e:
                print(f"Error sending email for fraud alert {row.alert_id}:", e)


@dp.foreach_batch_sink(
    name="fraud_card_alert_email_sink"
)
def fraud_card_alert_email_sink(df, batch_id):
    rows = df.collect()
    if not rows:
        return

    send_fraud_alert_emails(rows)


@dp.append_flow(
    target="fraud_card_alert_email_sink"
    ,name="fraud_card_alert_email_flow"
    ,comment="Stream new fraud card alerts and send them as emails"
)
def fraud_card_alert_email_flow():
    return spark.readStream.table("finguard.gold.fraud_card_alert")