"""
Random user API wrapper
Using 'https://randomuser.me/api/'
"""
import unicodedata
from collections import defaultdict
from typing import Optional, List, Union, Dict

import pydantic
from aiohttp import ClientSession
from pydantic import BaseModel

__all__ = ['Name', 'Street', 'Coordinates', 'Timezone', 'Location', 'Login', 'DOB', 'Registered', 'ID', 'Picture',
           'User', 'RandomUserApi']


class ResultInfo(BaseModel):
    seed: str
    results: int
    page: int
    version: str


class Name(BaseModel):
    title: str
    first: str
    last: str


class Street(BaseModel):
    number: int
    name: str


class Coordinates(BaseModel):
    latitude: float
    longitude: float


class Timezone(BaseModel):
    offset: str
    description: str


class Location(BaseModel):
    street: Street
    city: str
    state: str
    country: str
    postcode: Union[str, int]
    coordinates: Coordinates
    timezone: Timezone


class Login(BaseModel):
    uuid: str
    username: str
    password: str
    salt: str
    md5: str
    sha1: str
    sha256: str


class DOB(BaseModel):
    date: str
    age: int


class Registered(BaseModel):
    date: str
    age: int


class ID(BaseModel):
    name: str
    value: Optional[str] = None


class Picture(BaseModel):
    large: str
    medium: str
    thumbnail: str


class User(BaseModel):
    uid: Optional[str] = None
    gender: Optional[str] = None
    name: Optional[Name] = None
    location: Optional[Location] = None
    email: Optional[str] = None
    login: Optional[Login] = None
    dob: Optional[DOB] = None
    registered: Optional[Registered] = None
    phone: Optional[str] = None
    cell: Optional[str] = None
    id: Optional[ID] = None
    picture: Optional[Picture] = None
    nat: Optional[str] = None

    @property
    def display_name(self) -> str:
        return f'{self.name.first} {self.name.last}'

    @property
    def email_id(self) -> str:
        def normalize(s: str) -> str:
            # normalize unicode data to avoid umlauts etc.
            r = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode().lower()

            # make sure that there are no spaces
            r = r.replace(' ', '')
            return r

        first = normalize(self.name.first)
        last = normalize(self.name.last)
        return f'{first[:max(1, 8 - len(last))]}{last[:7]}'


class ApiResult(BaseModel):
    results: List[User]
    info: Optional[ResultInfo] = None


def validate(var: Union[None, str, List[str]],
             allowed: List[str],
             var_name: str,
             upper: bool = False) -> Union[None, str]:
    if var is None:
        return var
    if isinstance(var, str):
        var = [var]
    if upper:
        var = [v.upper() for v in var]
    if any(i not in allowed for i in var):
        raise KeyError(f'{var_name} has to be one of {", ".join(allowed)}')
    return ','.join(var)


class RandomUserApi:
    BASE = 'https://randomuser.me/api/'
    ALLOWED_INC_DEC = ['gender', 'name', 'location', 'email', 'login', 'registered', 'dob', 'phone', 'cell', 'id',
                       'picture', 'nat']
    ALLOWED_NATS = ['AU', 'BR', 'CA', 'CH', 'DE', 'DK', 'ES', 'FI', 'FR', 'GB', 'IE', 'IR', 'NO', 'NL', 'NZ', 'TR',
                    'US']

    def __init__(self):
        self.session = ClientSession()

    async def close(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def users(self, results=1, gender=None, nat: List[str] = None, inc: List[str] = None,
                    exc: List[str] = None) -> List[User]:
        """
        Obtain a list of random users from the API
        :param results:
        :param gender:
        :param nat:
        :param inc:
        :param exc:
        :return:
        """
        nat = validate(nat, self.ALLOWED_NATS, 'nat', upper=True)
        inc = validate(inc, self.ALLOWED_INC_DEC, 'inc')
        exc = validate(exc, self.ALLOWED_INC_DEC, 'exc')
        params = {k: v for k, v in locals().items() if k != 'self' and v is not None}

        url = f'{self.BASE}'
        async with self.session.get(url, params=params) as r:
            data = await r.json()
        try:
            result = ApiResult.model_validate(data)
        except pydantic.ValidationError as e:
            raise
        return result.results

    @staticmethod
    def set_uid(users: List[User], existing_uids: List[str] = None) -> None:
        """
        Set uids for each user
        uid: first 8 characters of  concatenation of first character of first name and last name. If that is not
        unique a number is added at the end
        :param existing_uids: list of already existing UIDs
        :param users: list of users. UIDs for these users are set
        :return: None
        """
        uid_dict: Dict[str, int] = defaultdict(int)
        if existing_uids:
            for uid in (u[:8] for u in existing_uids):
                uid_dict[uid] += 1
        for user in users:
            uid = user.email_id
            u = uid_dict[uid]
            uid_dict[uid] += 1
            if u:
                uid = f'{uid}{u}'
            user.uid = uid
        return
