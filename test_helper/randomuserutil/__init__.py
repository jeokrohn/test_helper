"""
Utility to get new random users for creation in a Webex org and automatic activation of these users
"""
import asyncio
import re
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from re import Pattern
from typing import List, Optional, Dict, Union

from wxc_sdk.as_api import AsWebexSimpleApi
from wxc_sdk.people import Person

from test_helper import webexactivation
from test_helper.randomuser import RandomUserApi, User

__all__ = ['RandomUserUtil']


@dataclass(init=False)
class RandomUserUtil:
    _email_to_uuid_re: Pattern
    _uid_to_email: str
    _api: AsWebexSimpleApi

    def __init__(self, api: AsWebexSimpleApi, gmail_address: str):
        m = re.match(r'(?P<user>\w+)@gmail.com', gmail_address)
        if not m:
            raise ValueError('email address has to be a valid gmail address: user@gmail.com')
        user = m.group('user')
        # regular expression to match email addresses and extract the uid
        self._email_to_uuid_re = re.compile(f'{user}\+(?P<uid>\w+)@gmail\.com')
        self._uid_to_email = user + '+{uid}@gmail.com'
        self._api = api

    def email_to_uid(self, mail: str) -> Optional[str]:
        """
        Extract UID from an email address
        :param mail:
        :return:
        """
        m = self._email_to_uuid_re.match(mail)
        if m:
            return m.group('uid')
        return None

    def uid_to_email(self, uid: str) -> str:
        """
        convert UID to email address
        :param uid:
        :return:
        """
        email = self._uid_to_email.format(uid=uid)
        return email

    async def get_new_users(self, number_of_users: int) -> List[User]:
        """
        Get a list of new random users
        :param number_of_users:
        :return:
        """
        new_users = []
        # get existing users and existing display names
        existing_users = await self._api.people.list()
        existing_uids = [uid
                         for user in existing_users
                         if (uid := self.email_to_uid(user.emails[0])) is not None]
        existing_display_names = set(u.display_name for u in existing_users)

        while len(new_users) < number_of_users:
            async with RandomUserApi() as random_user:
                # get new users
                new_batch = await random_user.users(results=number_of_users - len(new_users),
                                                    inc='name',
                                                    nat='us')

            # only consider random users with not existing display names
            for user in new_batch:
                display_name = user.display_name
                if display_name not in existing_display_names:
                    new_users.append(user)
                    existing_display_names.add(display_name)
                else:
                    pass
                # if
            # for
        # while

        # creating uids for new users
        random_user.set_uid(new_users, existing_uids=existing_uids)

        # set the email addresses
        for user in new_users:
            user.email = self.uid_to_email(uid=user.uid)
        return new_users

    @staticmethod
    async def activate_users(users: List[Union[Person, str]], imap_server: str,
                             imap_user: str, imap_pass: str, user_password: str,
                             output=print):
        """
        Activate a number of users
        :param users: list of users. Either a WebexPerson or an email address
        :param imap_server
        :param imap_user
        :param imap_pass
        :param user_password
        :param output
        """

        if isinstance(users[0], str):
            emails = users
        else:
            emails = [u.emails[0] for u in users]

        # only activate users where the email address is a sub address of the main gmail address
        m = re.match(r'(?P<user>\w+)@gmail.com', imap_user)
        if not m:
            raise ValueError('email address has to be a valid gmail address: user@gmail.com')
        user = m.group('user')

        # match something like foo+uid@gmail.com
        email_re = re.compile(f'{user}\+\w+@gmail.com')

        # only activate users with matching email addresses
        emails = [email for email in emails
                  if email_re.match(email)]
        if not emails:
            # nothing to do
            return

        # now we want to activate all users
        loop = asyncio.get_running_loop()

        # create a dictionary for the activation counts for each user. We expect two events for each user
        results: Dict[str, List[webexactivation.ActivationResult]] = {email: list() for email in emails}

        # noinspection DuplicatedCode
        def user_activated(act_result: webexactivation.ActivationResult) -> bool:
            """
            activation callback. Is called for every successful activation
            :param act_result:
            :return: True -> stop activator
            """
            # activation callback
            # this is called in the context of a different thread
            email = act_result.context.email
            if results.get(email) is None:
                output(f'Got activation callback for email not in this batch: {email}')
                return False

            results[email].append(act_result)
            activations = len(results[email])
            output(f'user_activated: {act_result.context.email}:{activations}')
            users_done = [u for u, v in results.items() if len(v) >= 2]
            users_none = [u for u, v in results.items() if len(v) == 0]
            users_half = [u for u, v in results.items() if len(v) == 1]
            users_done.sort()
            users_none.sort()
            users_half.sort()
            output(f'user_activated: {len(users_done)} users done')
            output(f'user_activated: {len(users_half)} users half activated')
            output(f'user_activated: {len(users_none)} users w/o activation')

            # we are done if we got two or more activation callbacks for each user to be activated
            if all(len(v) >= 2 for v in results.values()):
                output('user_activated: got all callbacks: closing activator')
                return True
            return False

        def activation():
            """
            This runs in its own thread: the actual activation flow
            """
            activator = webexactivation.WebexActivator(imap_server=imap_server,
                                                       imap_user=imap_user,
                                                       imap_pass=imap_pass,
                                                       user_password=user_password,
                                                       activation_callback=user_activated)
            activator.start()
            activator.join()

        with ThreadPoolExecutor(max_workers=1) as pool:
            await loop.run_in_executor(pool, activation)
