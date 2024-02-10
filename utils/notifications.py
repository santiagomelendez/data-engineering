import base64, os
import smtplib, ssl
from email.mime.text import MIMEText
from email.message import EmailMessage
from requests import HTTPError

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

sender_email = os.getenv('SENDER_EMAIL')
receiver_email = os.getenv('RECEIVER_EMAIL')
smtp_host = os.getenv('SMTP_HOST')
smtp_port = os.getenv('SMTP_PORT')

credential_path = os.getenv('CREDENTIALS_PATH')

google_credentials_file = f'{credential_path}/google_credentials.json'
google_token_file = f'{credential_path}/google_token.json'

SCOPES = [
        "https://www.googleapis.com/auth/gmail.send",
    ]


def _create_gmail_credentials():
    flow = InstalledAppFlow.from_client_secrets_file(google_credentials_file, scopes=SCOPES)
    creds = flow.run_local_server(port=0)
    return creds
   

def _get_gmail_credentials():
    creds = None
    if os.path.exists(google_token_file):
        creds = Credentials.from_authorized_user_file(google_token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
           creds = _create_gmail_credentials()
    with open(google_token_file, 'w') as token_file:
       token_file.write(creds.to_json())
    return creds


def send_email_from_gmail(body, subject):
    msg = EmailMessage()
    msg.set_content(body)
    msg['To'] = receiver_email
    msg['Subject'] = subject
    print(f'THIS IS THE MESSAGE TO SEND: {msg}')
    create_message = {'raw': base64.urlsafe_b64encode(msg.as_bytes()).decode()}

    creds = _get_gmail_credentials()
    with build('gmail', 'v1', credentials=creds) as service:
        try:
            message = (service.users().messages().send(userId="me", body=create_message).execute())
            print(f'sent message to {message} Message Id: {message["id"]}')
        except HTTPError as error:
            print(f'An error occurred: {error}')
            message = None