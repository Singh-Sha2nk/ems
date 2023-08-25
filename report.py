from influxdb_client import InfluxDBClient
import csv
import smtplib
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# InfluxDB configuration
token = "yq-ltfF4RCiLbSBpTF0E48b_6rNwsN8mqEway_5Q5nctEdRAXFDaFiJr_WUL9kpAVGBuEZVI8jbNfzFlWX7vSg=="
org = "JBM"
bucket = "EMS"

# Define the time range from today at 7 am to 9 am
now = datetime.datetime.utcnow()
start_time = now.replace(hour=7, minute=0, second=0, microsecond=0)
end_time = now.replace(hour=9, minute=18, second=0, microsecond=0)

# Create InfluxDB client
client = InfluxDBClient(url="http://localhost:8086", token=token)

window_period = "5m"
# Build Flux query
query = (
    f'from(bucket: "{bucket}") '
    f'|> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()}) '
    f'|> filter(fn: (r) => r["_measurement"] == "NEEL_1_EMS") '
    f'|> filter(fn: (r) => r["_field"] == "kWh_ABS223") '
    f'|> aggregateWindow(every: {window_period}, fn: last, createEmpty: false) '
    f'|> yield(name: "last")'
)

# Execute query
result = client.query_api().query(org=org, query=query)
records = list(result)


# Email configuration
sender_email = "thirdeyeaialerts@gmail.com"
sender_password = "Ybmibdqshiybnmgjy"
receiver_email = "shashankbdsingh@gmail.com"

# Connect to InfluxDB and execute query
client = InfluxDBClient(url="http://localhost:8086", token=token)
result = client.query_api().query(org=org, query=query)
records = list(result)

# Generate CSV report
csv_file = 'report.csv'
with open(csv_file, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(records[0].records[0].keys())
    for record in records[0].records:
        csvwriter.writerow(record.values())

# Create email message
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = "InfluxDB Report"

body = "Please find attached the InfluxDB report."
msg.attach(MIMEText(body, 'plain'))

# Attach CSV report to email
with open(csv_file, 'rb') as f:
    part = MIMEApplication(f.read(), Name="report.csv")
part['Content-Disposition'] = f'attachment; filename="{csv_file}"'
msg.attach(part)

# Send email
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(sender_email, sender_password)
server.sendmail(sender_email, receiver_email, msg.as_string())
server.quit()

print("Report generated and sent successfully.")
