import re
import logging

from threading import Thread
from queue import Queue

from collections import defaultdict

from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import Future

from imapclient import IMAPClient, SEEN
from email import message_from_bytes
from email.utils import parseaddr
from email.message import EmailMessage
from email.policy import default as email_default_policy
import time

from test_helper.webexactivation.util import email_subject
from test_helper.webexactivation.activation_flow import activate, ActivationContext, ActivationResult

from typing import Callable, Optional, Dict

log = logging.getLogger(__name__)

# Activation callback is called with the email address of a user and activation result
ActivationCallback = Callable[[ActivationResult], bool]


class MonitorInbox(Thread):
    """
    Thread to monitor inbox. pass uid of new messages to Activator (different thread) via callback
    """

    def __init__(self, imap_server: str, imap_user: str, imap_pass: str,
                 activator: "WebexActivator",
                 name: str = None) -> None:
        name = name or 'MonitorInbox'
        super().__init__(name=name, daemon=True)
        self._imap_server = imap_server
        self._imap_user = imap_user
        self._imap_pass = imap_pass
        self._activator = activator
        self._stop_requested: bool = False

    def run(self):
        # create IMAP connection
        use_idle = False
        with IMAPClient(host=self._imap_server, use_uid=True) as client:
            log.debug('Logging in..')
            client.login(username=self._imap_user, password=self._imap_pass)
            client.select_folder('INBOX')
            scheduled_messages = set()
            loops_after_stop_requested = 3
            while loops_after_stop_requested:
                if self._stop_requested:
                    loops_after_stop_requested -= 1
                # check all unseen messages
                log.debug('Check UNSEEN messages')
                try:
                    unseen_messages = client.search(criteria='UNSEEN')
                except Exception as e:
                    log.warning(f'Got exception while searching for unseen messages: {e}')
                else:
                    log.debug(f'Working on {len(unseen_messages)} unseen messages: {unseen_messages} ')
                    unseen_messages.reverse()
                    for unseen_message in unseen_messages:
                        if unseen_message in scheduled_messages:
                            log.debug(f'Already scheduled processing for message {unseen_message}')
                            continue
                        scheduled_messages.add(unseen_message)
                        self._activator.schedule_processing(uid=unseen_message)

                if use_idle:
                    # Start IDLE mode and wait for messages
                    log.debug('IMAP IDLE mode')
                    client.idle()

                    while not self._stop_requested:
                        # Wait for up to 30 seconds for an IDLE response
                        log.debug('Waiting 30 seconds for idle')
                        responses = client.idle_check(timeout=30)
                        log.debug(f'IDLE responses: {responses}')
                        # is there an EXISTS somewhere
                        new_messages = next((True for r in responses if r[1] == b'EXISTS'), False)
                        if new_messages:
                            break
                    log.debug('IMAP IDLE done')
                    client.idle_done()
                else:
                    log.debug('Sleep 10s before checking UNSEEN again')
                    try:
                        client.noop()
                    except Exception as e:
                        log.warning(f'Got exception on noop(): {e}')
                    time.sleep(10)
                    try:
                        client.noop()
                    except Exception as e:
                        log.warning(f'Got exception on noop(): {e}')
            log.debug('stop monitoring inbox')

    def close(self):
        self._stop_requested = True


class WebexActivator(Thread):
    WORKERS = 10

    def __init__(self, imap_server: str, imap_user: str, imap_pass: str, user_password: str,
                 activation_callback: ActivationCallback = None,
                 name=None):
        assert imap_server and imap_user and imap_pass
        name = name or 'WebexActivator'

        super().__init__(name=name, daemon=True)
        self._imap_server = imap_server
        self._imap_user = imap_user
        self._imap_pass = imap_pass
        self._user_password = user_password
        self._activation_callback = activation_callback
        self._pool = ThreadPoolExecutor(max_workers=self.WORKERS, thread_name_prefix='WebexUser')
        self._inbox_monitor = MonitorInbox(imap_server=imap_server, imap_user=imap_user, imap_pass=imap_pass,
                                           activator=self)
        self._processing_queue = Queue(maxsize=10)
        self._stop_requested = False
        # for each user we only want to act on an activation email with a given subject once
        self._activation_tracking: Dict[str, set] = defaultdict(set)
        self._imap_client = None

    def close(self):
        self._stop_requested = True
        self._inbox_monitor.close()

    def join(self, timeout: Optional[float] = ...) -> None:
        self._inbox_monitor.join()
        self._pool.shutdown(wait=True)
        super().join()

    def start(self) -> None:
        super().start()
        self._inbox_monitor.start()

    def run(self):

        def assert_client():
            if self._imap_client is None:
                self._imap_client = IMAPClient(host=self._imap_server, use_uid=True)
                log.debug('logging in..')
                self._imap_client.login(username=self._imap_user, password=self._imap_pass)
                self._imap_client.select_folder('inbox')

        def close_client():
            if self._imap_client is None:
                return
            try:
                self._imap_client.logout()
            except Exception:
                try:
                    self._imap_client.shutdown()
                except Exception:
                    pass
            self._imap_client = None

        while not (self._stop_requested and self._processing_queue.empty()):
            command, uid = self._processing_queue.get()
            log.debug(f'command from queue: {command}:{uid}')
            for i in range(3):
                try:
                    assert_client()
                    if command == 'start':
                        self.process_message(client=self._imap_client, uid=uid)
                    elif command == 'done':
                        # mark message as read
                        self.mark_as_read(client=self._imap_client, uid=uid)
                    self._processing_queue.task_done()
                except SystemError:
                    close_client()
                    if i == 2:
                        raise
                    continue
                else:
                    break
        close_client()

        log.debug('waiting for thread pool to shut down')
        self._pool.shutdown(wait=True)
        log.debug('thread pool shut down')

    def schedule_processing(self, uid: int) -> None:
        log.debug(f'schedule processing: start:{uid}')
        self._processing_queue.put(item=('start', uid))

    def schedule_done(self, uid: int) -> None:
        log.debug(f'schedule processing: done:{uid}')
        self._processing_queue.put(item=('done', uid))

    def process_message(self, client: IMAPClient, uid: int) -> None:
        log.debug(f'process message {uid}')
        client.noop()
        fetched = client.fetch(messages=[uid], data='BODY.PEEK[]')
        message_data = fetched.get(uid)
        if message_data is None:
            log.warning(f'could not get message {uid}')
            return
        message: EmailMessage = message_from_bytes(message_data[b'BODY[]'], policy=email_default_policy)
        from_address = parseaddr(message.get('from'))[1]
        if not from_address.endswith('@webex.com'):
            log.debug(f'skipping message from {from_address}/{message.get("date")}: '
                      f'"{email_subject(message)}"')
            return
        sent_to = parseaddr(message['to'])
        email = sent_to[1]
        log.debug(f'found message for {email} from {from_address}/{message.get("date")}: '
                  f'"{email_subject(message)}"')
        body: EmailMessage = message.get_body(preferencelist=('plain',))
        if not body:
            log.error(f'Email from {from_address} at {message.get("date")} for {email}, subject "{email_subject(message)}" has no body!')
            return
        content = body.get_content()
        assert content
        # find activation url
        webex_urls = [
            # r'https://teams.webex.com/activate\?\S+',
            r'https://web.webex.com/activate?\S+',
            r'https://idbroker[a-z\-]*.webex.com(:\d+)?/idb/setPassword?\S+',
            r'http://tracking\-us.webex.com/\S+']
        for i, url in enumerate(webex_urls):
            m: re.match = re.search(url, content)
            if m:
                break

        if not m:
            log.warning('activation url not found')
            return
        if i > 1:
            log.debug(f'{m[0]} is not a Webex activation url')
            return
        activation_url = m[0]
        log.debug(f'activation url {activation_url}')
        subject = email_subject(message=message)
        if subject in self._activation_tracking[email]:
            log.warning(f'Duplicate activation email for {email}, uid: {uid}, subject: {subject}')
            # ignore this message
            self.mark_as_read(client=client, uid=uid)
            return
        self._activation_tracking[email].add(subject)

        log.debug(f'scheduling activation for {message["to"]}')
        context = ActivationContext(uid=uid, email=sent_to[1], password=self._user_password, url=activation_url)
        activation_future = self._pool.submit(activate, context=context)
        activation_future.add_done_callback(self.activation_done)

    @staticmethod
    def mark_as_read(client: IMAPClient, uid: int):
        client.add_flags(messages=[uid], flags=[SEEN])
        log.debug(f'marked message {uid} as read')

    def activation_done(self, future: Future):
        activation_result: ActivationResult = future.result()
        log.debug(
            f'activation done: {activation_result.context.email}, success: {activation_result.success}, '
            f'{activation_result.text}, {activation_result}')
        self.schedule_done(uid=activation_result.context.uid)
        if self._activation_callback and self._activation_callback(activation_result):
            log.debug(f'activation_done: callback returned True -> initiating activator shutdown')
            self.close()
