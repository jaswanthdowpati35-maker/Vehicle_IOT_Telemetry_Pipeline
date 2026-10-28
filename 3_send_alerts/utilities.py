
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
from dotenv import load_dotenv
import os

load_dotenv()


def send_whatsapp(to_number, message):
    """Send a WhatsApp message using Twilio."""

    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    whatsapp_number = os.getenv("TWILIO_WHATSAPP_NUMBER")

    # Validate environment variables
    if not account_sid or not auth_token or not whatsapp_number:
        print("Error: Missing Twilio credentials in .env")
        return False

    # Remove spaces from phone numbers
    to_number = to_number.replace(" ", "").replace("-", "")
    whatsapp_number = whatsapp_number.replace(" ", "").replace("-", "")

    # Add WhatsApp prefix if it isn't already present
    if whatsapp_number.startswith("whatsapp:"):
        sender = whatsapp_number
    else:
        sender = "whatsapp:" + whatsapp_number

    if to_number.startswith("whatsapp:"):
        recipient = to_number
    else:
        recipient = "whatsapp:" + to_number

    client = Client(account_sid, auth_token)

    try:
        result = client.messages.create(
            body=message,
            from_=sender,
            to=recipient
        )

        print(f"Message submitted successfully! SID: {result.sid}")
        print(f"Message status: {result.status}")
        return True

    except TwilioRestException as e:
        print(f"Twilio error code: {e.code}")
        print(f"HTTP status: {e.status}")
        print(f"Error details: {e.msg}")
        return False


def main():
    recipient = "+12345678"
    message = "Test alert message from Fleet Pulse"

    send_whatsapp(recipient, message)


if __name__ == "__main__":
    main()
