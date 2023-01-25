import win32com.client as wincl
import datetime
import smtplib
import email
import base64
import pathlib
from os.path import basename
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import logging

logger = logging.getLogger(__name__)


class EmailModule():
    def __init__(self, config):
        self.server = smtplib.SMTP(config['smtp-server'], config["smtp-port"])

    def send_email(self, config, filename, test=False):
        self.server.ehlo()
        self.server.starttls()
        self.server.login(config['email-username'], config['email-password'])
        message = self.write_email(config, filename, test)
        receiver_list = config["receiver_email"] + \
            config["cc_email"] + config["bcc_email"]
        self.server.sendmail(config["sender_email"],
                             receiver_list, message.as_string())
        self.server.close()

    def write_email(self, config, filename, test):
        cur_date = datetime.datetime.now().strftime("%d-%b-%Y")
        cur_time = datetime.datetime.now().strftime("%d-%b-%Y, %H:%M:%S")
        message = MIMEMultipart()
        message["From"] = config["sender_email"]
        if test:
            message["To"] = ", ".join(config["receiver_test"])
            message["Cc"] = ", ".join(config["cc_test"])
            message["Bcc"] = ", ".join(config["bcc_test"])
        else:
            message["To"] = ", ".join(config["receiver_email"])
            message["Cc"] = ", ".join(config["cc_email"])
            message["Bcc"] = ", ".join(config["bcc_email"])
        print("Sending email to: " +
              message["To"] + ", cc: " + message["Cc"] + ", bcc: " + message["Bcc"])

        message["Subject"] = "SAN/OOL Vessels Description 3 - " + cur_date

        body = "This email contains the SAN/OOL Vessels Description 3 Report. SQL pull executed at " + cur_time
        message.attach(MIMEText(body, "plain"))

        with open(filename, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(filename)
            )
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(
            filename)
        message.attach(part)

        return message


class GmailModule():
    """
    Contains code that allows user to log into an external email account and send a customizable email.
    """

    def __init__(self, client, userId=None):
        self.client = client

        if userId is None:
            self_profile = self.getProfile()
            self.userId = self_profile["emailAddress"]

        else:
            self.userId = userId

    def get_email_details(self, CONFIG, args):
        if not args.testing:

            receiver = CONFIG["receiver-email"]
            cc = CONFIG["cc-email"]
            bcc = [self.userId]

        else:

            receiver = [self.userId]
            cc = [self.userId]
            bcc = [self.userId]

        return receiver, cc, bcc

    def getProfile(self):
        """
        Returns:
            dict of the profile of the user
        """
        response = self.client.gmail_service.users(
        ).getProfile(userId="me").execute()
        return response

    def send(self, body, media_body=None, media_mime_type=None):
        """
        Send a message.

        Args:
            body: dict, a message created by new_message
            media_body:
           media_mime_type:

        Returns:
            response
        """
        return self.client.gmail_service.users().messages().send(userId=self.userId, body=body,
                                                                 media_body=media_body, media_mime_type=media_mime_type).execute()

    def new_message(self, to, subject, plain, cc=[], bcc=[], html=None, from_alias=None, files=None):
        """
        Create a new message.

        Args:
            to: str/list, email adress
            cc: list, list of cc email addresses
            bcc: list, list of bcc email addresses
            subject: str, the subject header
            plain: str, the message in plain text
            html: str, the message as html

        Returns:
            dict
        """
        if not files:
            files = []
        if isinstance(files, str):
            files = [files]

        msg = MIMEMultipart()
        if isinstance(to, str):
            msg["to"] = to
        elif isinstance(to, list):
            msg["to"] = ", ".join(to)
        else:
            msg["to"] = str(to)

        msg["cc"] = ", ".join(cc)
        msg["bcc"] = ", ".join(bcc)

        if not from_alias:
            msg["from"] = self.userId
        else:
            msg["from"] = from_alias
        msg["subject"] = subject
        plain = MIMEText(plain, "plain")
        msg.attach(plain)

        if html:
            html = MIMEText(html, "html")
            msg.attach(html)

        for filename in files:
            with open(filename, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=basename(filename)
                )
            part["Content-Disposition"] = f"attachment; filename={basename(filename)}"
            msg.attach(part)

        return {"raw": base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")}
