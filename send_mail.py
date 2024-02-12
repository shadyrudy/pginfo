import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_mail(
    subject,
    body,
    to_email,
    from_email="your_email@example.com",
    password="your_password",
):
    """
    Sends a plain text email with the given subject and body to the specified recipient.

    Args:
        subject (str): Email subject.
        body (str): Email body.
        to_email (str): Email address of the recipient.
        from_email (str): Email address of the sender.
        password (str): Password for the sender's email address.
    """
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    try:
        server = smtplib.SMTP(
            "smtp.example.com", 587
        )  # Use your SMTP server and port 587 or 25
        server.starttls()

        # Depending on your email setup, a password may not be required 1
        server.login(from_email, password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Sends a plain text email to the specified recipient. The sender's email address and password are required."
    )
    parser.add_argument("subject", help="Email subject")
    parser.add_argument("body", help="Email body")
    parser.add_argument("to_email", help="Recipient email address")
    args = parser.parse_args()

    send_mail(args.subject, args.body, args.to_email)
