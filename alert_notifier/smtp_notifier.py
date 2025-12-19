import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(to_email, subject, body):
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT"))
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    sender = os.getenv("SMTP_FROM")
    use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        server = smtplib.SMTP(host, port, timeout=10)
        if use_tls:
            server.starttls()

        server.login(username, password)
        server.send_message(msg)
        server.quit()

        print(f"[EMAIL] Inviata a {to_email}", flush=True)
        return True

    except smtplib.SMTPRecipientsRefused:
        print(f"[EMAIL] Destinatario inesistente: {to_email}", flush=True)

    except smtplib.SMTPAuthenticationError:
        print("[EMAIL] Errore di autenticazione SMTP", flush=True)

    except smtplib.SMTPConnectError:
        print("[EMAIL] Connessione SMTP fallita", flush=True)

    except smtplib.SMTPException as e:
        print(f"[EMAIL] Errore SMTP generico: {e}", flush=True)

    except Exception as e:
        print(f"[EMAIL] Errore imprevisto: {e}", flush=True)

    return False
