
from email.message import Message
from email.header import decode_header


def email_subject(message: Message) -> str:
    subject = message.get('subject')
    decoded = decode_header(subject)[0]
    decoded = decoded[0] if decoded[1] is None else decoded[0].decode()
    return decoded
