import logging
from dataclasses import dataclass
from requests import Session
from urllib.parse import urlparse, parse_qs
from base64 import b64encode
from hashlib import sha256
from time import sleep

from typing import Tuple, Dict, Optional

log = logging.getLogger(__name__)


@dataclass
class ActivationContext:
    uid: int
    email: str
    url: str
    password: str


@dataclass
class ActivationResult:
    context: ActivationContext
    success: bool
    text: str


class UserActivation:
    CLIENT_ID = 'C96d389d632c96d038d8f404c35904b5108988bd6d601d4b47f4eec88a569d5db'
    CLIENT_SECRET = 'b11c3e96a0d51f66ff9686220b74e2c0f6c6c7636bba98b71ca5dbbf5d6896d6'
    SCOPE = "webexsquare:get_conversation Identity:SCIM spark:kms spark:people_read spark:rooms_read " \
            "spark:rooms_write spark:memberships_read spark:memberships_write spark:messages_read spark:messages_write"

    def __init__(self, context: ActivationContext):
        self._context = context
        self.debug(f'Starting activation flow')
        self._session = Session()
        self._service_links: Dict[str, str] = dict()
        self._user_id: Optional[str] = None
        self._user_entities: Dict[str, str] = dict()
        self._access_token: Optional[str] = None
        self._user_access_token: Optional[str] = None
        self.service_catalog(mode='DEFAULT_BY_PROXIMITY')
        hash = sha256()
        hash.update(self._context.email.lower().encode())
        self.service_catalog(emailhash=hash.hexdigest())

    @property
    def u2c_service_link(self):
        link = self._service_links.get('u2c', 'https://u2c.wbx2.com/u2c/api/v1')
        return link

    def request(self, method, url, **kwargs):
        from requests import ConnectionError
        connection_errors = 0
        retry_502 = 0
        while True:
            try:
                r = self._session.request(method, url, **kwargs)
            except ConnectionError:
                connection_errors += 1
                if connection_errors <= 3:
                    self.warning(f'ConnectionError on {method} on {url}, retry {connection_errors}/3')
                    continue
                else:
                    raise
            if r.status_code == 502:
                # sometimes we just need to retry
                retry_502 += 1
                if retry_502 <= 3:
                    self.warning(f'502 on {method} on {url}, retry {retry_502}/3')
                    continue
            if r.status_code == 429:
                retry_after = r.headers.get('Retry-After')
                wait = retry_after
                if wait is None:
                    wait = 120
                else:
                    wait = int(wait)
                wait = min(wait, 120)
                self.debug(f'got 429, retry after {retry_after} on {method} on {url}, waiting {wait} seconds')
                sleep(wait)
                continue
            break

        return r

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def patch(self, url, **kwargs):
        return self.request('PATCH', url, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self._session:
            self._session.close()
            self._session = None

    def debug(self, message: str):
        log.debug(f'({self._context.email}) {message}')

    def warning(self, message: str):
        log.warning(f'({self._context.email}) {message}')

    def error(self, message: str):
        log.error(f'({self._context.email}) {message}')

    @property
    def idbroker_url(self):
        return self._service_links.get('idbroker', 'https://idbroker.webex.com')

    @property
    def basic_auth(self):
        auth = b64encode(f'{self.CLIENT_ID}:{self.CLIENT_SECRET}'.encode()).decode()
        return f'Basic {auth}'

    @property
    def bearer_auth(self):
        return f'Bearer {self.access_token}'

    @property
    def user_bearer_auth(self):
        if self._user_access_token is None:
            self.error(f'User access token not present')
            raise KeyError('User access token not present')
        return f'Bearer {self._user_access_token}'  #

    @property
    def access_token(self) -> str:
        if self._access_token is None:
            self.debug('access token not set: need to get access_token')
            self._access_token = self._get_access_token()
        return self._access_token

    @property
    def user_identity_url(self) -> Optional[str]:
        url = self._user_entities.get('identityUrl')
        if url is None:
            self.warning('user identity URL not set!')
            url = 'https://identity.webex.com'
        return url

    @property
    def atlas_url(self):
        return self._service_links.get('atlas', 'https://atlas-a.wbx2.com/admin/api/v1')

    def _get_access_token(self) -> str:
        self.debug('Getting access token')
        url = f'{self.idbroker_url}/idb/oauth2/v1/access_token'
        headers = {'Authorization': f'{self.basic_auth}'}
        data = {
            'grant_type': 'client_credentials',
            'scope': 'webexsquare:admin webexsquare:get_conversation Identity:SCIM',
            'self_contained_token': 'true'
        }
        r = self.post(url=url, headers=headers, data=data)
        r.raise_for_status()
        self.debug('Got access token')
        data = r.json()
        return data['access_token']

    def service_catalog(self, **params):
        params.update({'format': 'hostmap'})
        self.debug(f'get service catalog: {", ".join(f"{k}={v}" for k, v in params.items())}')
        url = f'{self.u2c_service_link}/limited/catalog'
        r = self.get(url=url, params=params)
        r.raise_for_status()
        data = r.json()
        self._service_links = data['serviceLinks']
        return r.json()

    def invoke(self) -> Tuple[int, dict]:
        self.debug(f'invoke')
        url = f'{self.idbroker_url}/idb/token/v1/actions/UserActivation/invoke'
        basic_auth = b64encode(f'{self.CLIENT_ID}:{self.CLIENT_SECRET}'.encode()).decode()
        headers = {
            'Referer': self._context.url,
            'Authorization': f'{self.basic_auth}'

        }
        parsed_url = urlparse(self._context.url)
        query = parse_qs(parsed_url.query)
        token = query.get('t', [None])[0]
        assert token
        data = {'verificationToken': token,
                'scope': self.SCOPE}
        r = self.post(url=url, json=data, headers=headers)
        data = r.json()

        self.debug(f'invoke response: {r.status_code}: {data}')
        # fetch user access token from the response
        self._user_access_token = data.get('tokenData', {}).get('access_token')
        return r.status_code, data

    def activations(self, supress_email=False) -> dict:
        self.debug('Call activations')
        url = f'{self.atlas_url}/users/activations'
        headers = {'Authorization': f'{self.bearer_auth}'}
        data = {
            'email': self._context.email.lower(),
            'reqId': 'WEBCLIENT'
        }
        if supress_email:
            data['supressEmail'] = 'true'
        r = self.post(url=url, json=data, headers=headers)
        r.raise_for_status()
        data = r.json()
        self._user_id = data.get('id')
        self._user_entities = data.get('userEntities', dict())
        self.debug(f'activations result: {data}')
        return data

    def resend_invite(self) -> Tuple[bool, bool]:
        data = self.activations()
        return data['verificationEmailTriggered'], data['hasPassword']

    def set_password(self, password: str) -> Tuple[int, dict]:
        self.debug('Setting password')
        url = f'{self.user_identity_url}/identity/scim/v1/Users/{self._user_id}'
        data = {"schemas": ["urn:scim:schemas:core:1.0", "urn:scim:schemas:extension:cisco:commonidentity:1.0"],
                "password": password}
        headers = {'Authorization': self.user_bearer_auth}
        r = self.patch(url=url, json=data, headers=headers)
        if r.status_code == 200:
            data = r.json()
        else:
            data = {}
        self.debug(f'Set password result: {r.status_code}/{data}')
        return r.status_code, data


def activate(context: ActivationContext) -> ActivationResult:
    with UserActivation(context=context) as activation:
        status_code, data = activation.invoke()
        if status_code in {400, 404}:
            code = int(data['error']['message'][0]['code'])
            if code == 100133:
                # activation code is invalid
                # resend invitation
                email_triggered, has_password = activation.resend_invite()
                if has_password:
                    return ActivationResult(context=context, success=True,
                                            text='Activation token already used and password already set')
                if email_triggered:
                    return ActivationResult(context=context, success=False,
                                            text='Activation link expired, new verification email triggered')
                else:
                    return ActivationResult(context=context, success=False,
                                            text=f'Activation link expired, failed to trigger verification email. has '
                                                 f'password: {has_password}')
            elif code == 200004:
                # user null not found
                return ActivationResult(context=context, success=False,
                                        text=data['error']['message'][0]['description'])
            else:
                raise NotImplementedError(f'unknown status code {code}: {data}')
            pass
        elif status_code == 200:
            activation_result = activation.activations()
            if not activation_result['hasPassword']:
                # try to set password
                status_code, data = activation.set_password(context.password)
                if status_code != 200:
                    return ActivationResult(context=context, success=False,
                                            text=f'Failed to set password. {status_code}:{data}')
        else:
            raise NotImplementedError(f'unhandled HTTP status: {status_code}: {data}')

    return ActivationResult(context=context, success=True, text='Account activated')
