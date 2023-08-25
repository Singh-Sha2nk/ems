import pymongo
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime

# MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["JBMEMS"]
collection = db["EMSDB23"]

# Define the time range
start_time = datetime.strptime("2023-08-16T15:04:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
end_time = datetime.strptime("2023-08-16T15:07:30.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")

# Fetch data within the time range

query = {
    "time": {"$gte": start_time, "$lte": end_time},
    "kWh_ABS223": {"$exists": True}
}
data = collection.find(query)
print(data)

pdf_file = "report.pdf"

def generate_pdf(data):
    c = canvas.Canvas(pdf_file, pagesize=letter)
    c.drawString(100, 750, "EMS REPORT")
    
    y = 700
    for item in data:
        c.drawString(100, y, f"Time: {item['time']}")
        c.drawString(100, y - 20, f"Machine ID: {item['machineId']}")
        c.drawString(100, y - 40, f"Plant ID: {item['plantId']}")
        c.drawString(100, y - 60, f"Line ID: {item['lineId']}")
        c.drawString(100, y - 80, f"Company ID: {item['companyId']}")
        c.drawString(100, y - 100, f"kWh_ABS223: {item['kWh_ABS223']}")
        y -= 120
        print(item)  # Print each document to the terminal
    
    c.save()

# Email sending
def send_email(subject, body, attachment_path):
    from_email = "thirdeyeaialerts@gmail.com"
    to_email = "shashankbdsingh@gmail.com"
    password = "bmibdqshiybnmgjy"
    
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body, 'plain'))
    
    with open(attachment_path, "rb") as f:
        attach = MIMEApplication(f.read(), _subtype="pdf")
        attach.add_header('Content-Disposition', 'attachment', filename=attachment_path)
        msg.attach(attach)
    
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_email, password)
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()
# ... (your existing code)

# Main script
if __name__ == "__main__":
    print("Fetching data from MongoDB...")
    print(data)
    for item in data:
     print(item)

    generate_pdf(data)
    print("Generating PDF...")
    
    subject = "EMS Report"
    body = "Please find attached the EMS report generated."
    attachment_path = pdf_file
    
    send_email(subject, body, attachment_path)
    print("Email sent with report attached.")
