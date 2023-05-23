"""
Helper to create random WxC locations in US
GEO coding using APIs available at https://geocodify.com
NANP helpers using resources from https://www.nationalnanpa.com/
"""
import logging
import os.path
import random
from csv import DictReader
from dataclasses import dataclass, field
from io import StringIO
from typing import Literal, List, Optional, Generator, Any, Dict

from aiohttp import TCPConnector, ClientSession
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pydantic import BaseModel as PydanticBase, Field, root_validator, validator, ValidationError
from pydantic.errors import PydanticValueError

log = logging.getLogger(__name__)

__all__ = ['RandomLocation', 'GeoCodeResponseFeature', 'GeoCodifyResponse', 'NpaInfo', 'Address']

API_KEY = 'GEOCODIFY_API_KEY'
ENV_FILE = 'geocodify.env'

Layer = Literal[
    'venue', 'street', 'country', 'macroregion', 'region', 'county', 'localadmin', 'locality', 'borough',
    'neighbourhood', 'continent', 'empire', 'dependency', 'macrocounty', 'macrohood', 'microhood', 'disputed',
    'postalcode', 'ocean', 'marinearea', 'address']


class BaseModel(PydanticBase):
    @classmethod
    def parse_obj(cls, obj: Any):
        try:
            return super().parse_obj(obj)
        except ValidationError as e:
            log.error(f'failed to parse: {obj}')
            raise e


class ZipRecord(BaseModel):
    zip_code: str = Field(alias='zip5')
    record_type: Optional[str] = Field(alias='recordType')

    def __repr__(self):
        return self.zip_code

    @property
    def is_po_box(self) -> bool:
        return self.record_type and self.record_type == 'PO BOX'

    @property
    def is_unique(self) -> bool:
        return self.record_type and self.record_type == 'UNQIUE'

    @property
    def is_regular(self) -> bool:
        return not (self.is_po_box or self.is_unique)


class ZipResult(BaseModel):
    status: str = Field(alias='resultStatus')
    city: Optional[str]
    state: Optional[str]
    zip_list: List[ZipRecord] = Field(alias='zipList', default_factory=list)

    @property
    def regular_zips(self) -> Generator[ZipRecord, None, None]:
        """
        Generator for regular zip records
        :return:
        """
        return (record for record in self.zip_list
                if record.is_regular)

    @property
    def success(self) -> bool:
        return self.status == 'SUCCESS'


class NpaInfo(BaseModel):
    class Config:
        extra = 'ignore'

    npa: str = Field(alias='NPA_ID')
    type_of_code: str
    assignable: bool = Field(alias='ASSIGNABLE')
    reserved: bool = Field(alias='RESERVED')
    assigned: bool = Field(alias='ASSIGNED')
    in_service: bool = Field(alias='IN_SERVICE')
    country: str = Field(alias='COUNTRY')


class NpaCity(BaseModel):

    @validator('city', 'county', pre=True)
    def to_title(cls, v):
        return v.title()

    npa: str = Field(alias='NPA')
    city: str = Field(alias='City')
    county: str = Field(alias='County')
    state: str = Field(alias='State')


class GeoCodeResponseMeta(BaseModel):
    code: int


class GeoCodePointValueError(PydanticValueError):
    code = 'failed to validate'
    msg_template = 'exception in validator: {err}'


class GeoCodePoint(BaseModel):
    lat: float
    lon: float

    @root_validator(pre=True)
    def lat_lon(cls, values):
        """
        allow lat, lon or coordinates(list of two floats)
        :param values:
        :return:
        """
        if values.get('lat') and values.get('lon'):
            return values
        try:
            # convert from a list of two floats in "coordinates"
            if values['type'] != 'Point':
                raise GeoCodePointValueError(err=f'wrong type, expected "Point"')
            values = {'lat': values['coordinates'][1],
                      'lon': values['coordinates'][0]}
        except Exception as e:
            raise GeoCodePointValueError(err=e)
        return values


class FeatureProperties(BaseModel):
    feature_id: str = Field(alias='id')
    gid: str
    layer: Layer
    source: str
    source_id: str
    name: str
    housenumber: Optional[str]
    street: Optional[str]
    postalcode: Optional[str]
    postalcode_gid: Optional[str]

    confidence: float
    match_type: Optional[Literal['fallback', 'exact']]  # not part of reverse response
    distance: Optional[float]
    accuracy: str
    country: Optional[str]
    country_gid: Optional[str]
    country_a: Optional[str]
    region: Optional[str]
    region_gid: Optional[str]
    region_a: Optional[str]
    macrocounty: Optional[str]
    macrocounty_gid: Optional[str]
    county: Optional[str]
    county_gid: Optional[str]
    county_a: Optional[str]
    localadmin: Optional[str]
    localadmin_gid: Optional[str]
    locality: Optional[str]
    locality_gid: Optional[str]
    locality_a: Optional[str]
    borough: Optional[str]
    borough_gid: Optional[str]
    neighbourhood: Optional[str]
    neighbourhood_gid: Optional[str]
    continent: str
    continent_gid: str
    label: str
    addendum: Optional[dict]


class BoundingBox(BaseModel):

    @classmethod
    def root_validator(cls, key: str, values):
        """
        root validator for models having a BoundingBox attribute
        create two points from list of four floats
        :param key:
        :param values:
        :return:
        """
        if (floats := values.get(key)) is not None:
            values[key] = {'p1': {'lon': floats[0], 'lat': floats[1]},
                           'p2': {'lon': floats[2], 'lat': floats[3]}}
        return values

    p1: GeoCodePoint
    p2: GeoCodePoint

    def grid(self, steps: int) -> Generator[GeoCodePoint, None, None]:
        """
        Generator for grid points within the boundary
        :param steps: number of lat/lon steps for grid points
        :return:
        """
        lat_diff = self.p2.lat - self.p1.lat
        lon_diff = self.p2.lon - self.p1.lon
        steps += 1
        for lat_step in range(1, steps):
            for lon_step in range(1, steps):
                lat = self.p1.lat + lat_diff / steps * lat_step
                lon = self.p1.lon + lon_diff / steps * lon_step
                yield GeoCodePoint(lat=lat, lon=lon)


class GeoCodeResponseFeature(BaseModel):
    feature_type: Literal['Feature'] = Field(alias='type')
    point: GeoCodePoint = Field(alias='geometry')
    properties: FeatureProperties
    bounding_box: Optional[BoundingBox] = Field(alias='bbox')

    @root_validator(pre=True)
    def bbox_from_list(cls, values):
        """
        bounding_box from list of floats
        :param values:
        :return:
        """
        values = BoundingBox.root_validator('bbox', values)
        return values

    def __repr__(self):
        return f'Feature({self})'

    def __str__(self):
        return f'{self.properties.match_type}, {self.properties.layer}, {self.properties.label}'

    @property
    def google_maps_url(self) -> str:
        return f'https://www.google.com/maps/place/{self.point.lat},{self.point.lon}'

    @property
    def address(self) -> 'Address':
        return Address.from_geo_code_response_feature(self)


class GeoCodeResponseResponse(BaseModel):
    geocoding: Any
    response_type: Literal['FeatureCollection'] = Field(alias='type')
    features: List[GeoCodeResponseFeature]
    bounding_box: BoundingBox = Field(alias='bbox')

    @root_validator(pre=True)
    def bbox_from_list(cls, values):
        """
        bounding_box from list of floats
        :param values:
        :return:
        """
        values = BoundingBox.root_validator('bbox', values)
        return values

    @property
    def locality(self) -> Optional[GeoCodeResponseFeature]:
        """
        the 1st 'locality' feature
        :return:
        """
        return next((feature
                     for feature in self.features
                     if feature.properties.layer == 'locality'), None)

    @property
    def exact(self) -> Optional[GeoCodeResponseFeature]:
        """
        The 1st exact match feature
        :return:
        """
        return next((feature
                     for feature in self.features
                     if feature.properties.match_type == 'exact'), None)

    @property
    def addresses(self) -> Generator[GeoCodeResponseFeature, None, None]:
        """
        'address' layer features
        :return:
        """
        return (feature for feature in self.features
                if feature.properties.layer == 'address')


class GeoCodifyResponse(BaseModel):
    meta: GeoCodeResponseMeta
    response: GeoCodeResponseResponse


@dataclass
class Address:
    city: str
    address1: str
    address2: str
    zip_or_postal_code: str
    state_or_province: str
    state_or_province_abbr: str
    country: str
    countr_abbr: str
    geo_location: GeoCodePoint
    npa: Optional[str] = field(default=None)

    @property
    def google_maps_url(self) -> str:
        return f'https://www.google.com/maps/place/{self.geo_location.lat},{self.geo_location.lon}'

    @classmethod
    def from_geo_code_response_feature(cls, feature: GeoCodeResponseFeature) -> 'Address':
        return cls(city=feature.properties.locality,
                   address1=feature.properties.name,
                   address2='',
                   zip_or_postal_code=feature.properties.postalcode,
                   state_or_province=feature.properties.region,
                   state_or_province_abbr=feature.properties.region_a,
                   country=feature.properties.country,
                   countr_abbr=feature.properties.country_a,
                   geo_location=feature.point)

    def __repr__(self):
        return f'{self.address1}, {self.city}, {self.state_or_province_abbr} {self.zip_or_postal_code} USA'


# Response for random address calls:
# Address instance
RandomAddressResponse = Optional[Address]


@dataclass(init=False)
class RandomLocation:
    """
    Helper class for random address creation
    """
    _api_key: str
    _session: ClientSession
    _base_url = 'https://api.geocodify.com/v2'

    def __init__(self, *, api_key: str = None):
        self._api_key = None
        self._session = None
        # determine API key for geocodify API
        api_key = api_key or os.getenv(API_KEY)
        env_path = os.path.join(os.getcwd(), ENV_FILE)
        if api_key is None:
            # load geocodify.env from current dir
            load_dotenv(env_path)
            api_key = os.getenv(API_KEY)
        if api_key is None:
            raise KeyError(
                f'Geocodify API key needs to be specified by passing to __init__, in {API_KEY} environment variable, '
                f'or in {env_path}')
        self._api_key = api_key
        self._session = ClientSession(raise_for_status=True, connector_owner=True,
                                      connector=TCPConnector(force_close=True,
                                                             enable_cleanup_closed=True))

    async def __aenter__(self):
        return self

    async def close(self):
        if self._session:
            await self._session.close()
        self._session = None

    async def _geo_get(self, *, url: str, params: dict, **kwargs) -> GeoCodifyResponse:
        """
        get on geocodify API requires the API key as parameter
        :param url:
        :param params:
        :param kwargs:
        :return:
        """
        params['api_key'] = self._api_key
        async with self._session.get(url=url, params=params, **kwargs) as r:
            data = await r.json()
        return GeoCodifyResponse.parse_obj(data)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def get_zips(self, *, city: str, state: str) -> ZipResult:
        """
        Get list of zip codes for a given city from USPS API
        :param city:
        :param state:
        :return:
        """
        url = 'https://tools.usps.com/tools/app/ziplookup/zipByCityState'
        data = {'city': city,
                'state': state}
        headers = {
            'origin': 'https://tools.usps.com',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/97.0.4692.71 Safari/537.36'
        }
        async with self._session.post(url=url, data=data, headers=headers) as r:
            data = await r.json()
        result = ZipResult.parse_obj(data)
        log.debug(f'get_zips({city}, {state}): {result.status} {len(result.zip_list)} -> '
                  f'{", ".join(f"{r.zip_code}" for r in result.zip_list)}')
        return result

    async def geocode(self, *, search: str) -> GeoCodifyResponse:
        """
        Geocode a search request
        :param search:
        :return:
        """
        url = f'{self._base_url}/geocode'
        params = {'q': search}
        data = await self._geo_get(url=url, params=params)
        result = GeoCodifyResponse.parse_obj(data)
        return result

    async def reverse(self, *, lat: float, lon: float) -> GeoCodifyResponse:
        params = {'lat': lat,
                  'lng': lon}
        url = f'{self._base_url}/reverse'
        data = await self._geo_get(url=url, params=params)
        result = GeoCodifyResponse.parse_obj(data)
        return result

    async def load_npa_data(self) -> List[NpaInfo]:
        """
        Load and parse NPA info from NANPA website

        :return:
        """
        url = 'https://www.nationalnanpa.com/nanp1/npa_report.csv'
        async with self._session.get(url=url) as r:
            csv = await r.text()
        csv_io = StringIO(csv)

        # skip header
        next(csv_io)

        reader = DictReader(csv_io)
        result = list()
        for row in reader:
            # Weirdness... There is a row with None as key?
            row.pop(None, None)
            npa_info = NpaInfo.parse_obj(row)
            result.append(npa_info)
        return result

    async def npa_cities(self, *, npa: str) -> List[NpaCity]:
        """
        Get cities for given NPA from NANPA website

        :param npa:
        :return:
        """
        url = 'https://www.nationalnanpa.com/enas/displayNpaCityReport.do'
        params = {'city': '',
                  'stateAbbr': '',
                  'npaId': npa}
        async with self._session.post(url=url, params=params) as r:
            body = await r.text()
        soup = BeautifulSoup(body, 'html.parser')

        # city information is in table rows
        rows = soup.find_all('tr')

        # get list of lists of columns
        rows = list(map(lambda row: row.find_all('td'), rows))
        row_iter = iter(rows)

        # headers are in the 1st row with 4 columns
        header_row = next(row for row in row_iter if len(row) == 4)
        columns = list(map(lambda td: td.text.strip(), header_row))

        # city info is in the next rows with 4 columns
        # row with something different than 4 columns terminates the list of cities
        result = list()
        for row in row_iter:
            if len(row) != 4:
                # Done
                break
            # extract values from columns
            values = map(lambda td: td.text.strip(), row)

            # create dict and parse into object
            data = {k: v for k, v in zip(columns, values)}
            result.append(NpaCity.parse_obj(data))
        log.debug(f'npa_cities({npa}): {len(result)} -> {", ".join(f"{r.city}" for r in result)}')
        return result

    async def zip_random_address(self, *, zip_code: str, state: str, default_city: str = None) -> RandomAddressResponse:
        """
        random address for given zip code
        :param zip_code:
        :param state:
        :param default_city:
        :return:
        """
        # get location for zip code
        zip_search = f'{state} {zip_code}, USA'
        geocoded = await self.geocode(search=zip_search)
        exact = geocoded.response.exact
        if exact is None:
            log.debug(f'No exact match found for {zip_search}')
            return None

        """
        Alternative code: look at multiple points
        # use grid of four points within ZIP code bounding box
        tasks = [self.reverse(lat=point.lat, lon=point.lon)
                 for point in exact.bounding_box.grid(steps=2)]
        # noinspection PyTypeChecker
        reverse_results = await asyncio.gather(*tasks)
        reverse_results: List[GeoCodifyResponse]
        addresses = chain.from_iterable(r.response.addresses for r in reverse_results)
        """

        # reverse lookup on geo location of zip code should give us some addresses
        reverse = await self.reverse(lat=exact.point.lat, lon=exact.point.lon)
        addresses = [Address.from_geo_code_response_feature(a) for a in reverse.response.addresses]

        log.debug(f'{len(addresses)} addresses for zip {zip_code}: {addresses}')

        # if the zip code in any of the addresses we got from geo coding is unknown
        # -> we assume that zip code we are searching addresses for
        missing_zip = 0
        for address in addresses:
            if not address.zip_or_postal_code or address.zip_or_postal_code == '00000':
                address.zip_or_postal_code = zip_code
                missing_zip += 1
        if missing_zip:
            log.debug(f'{missing_zip} missing zip codes set to default {zip_code}')

        if default_city:
            # set city to default if city is missing in any addresses
            missing_city = 0
            for address in addresses:
                if not address.city:
                    address.city = default_city
                    missing_city += 1
            if missing_city:
                log.debug(f'{missing_city} misssing cities set to default {default_city}')

        # if the addresses are in the wrong state then that's kind of fishy
        wrong_state = sum(address.state_or_province_abbr != state for address in addresses)
        if wrong_state:
            log.debug(f'{wrong_state} addresses in wrong state')

        # only consider valid addresses in correct state
        addresses = [address
                     for address in addresses
                     if address.city and address.address1 and address.state_or_province_abbr and
                     address.zip_or_postal_code and address.state_or_province_abbr == state]
        log.debug(f'{len(addresses)} valid addresses for zip {zip_code}: {addresses}')

        if not addresses:
            log.debug(f'No addresses for reverse lookup on location of {zip_search}')
            return None

        # return a random address
        result = random.choice(addresses)
        log.debug(f'zip_random_address({zip_code}, {state}): {result}')
        return result

    async def city_random_address(self, *, city: str, state: str) -> RandomAddressResponse:
        """
        random address for given city
        :param city:
        :param state:
        :return:
        """
        # get zip codes for city
        zip_result = await self.get_zips(city=city, state=state)
        zips_to_try = [zip_record.zip_code for zip_record in zip_result.regular_zips]
        if not zips_to_try:
            log.debug(f'No zip codes for {city}, {state}')
            return None

        # iterate through ZIPs in random order
        random.shuffle(zips_to_try)
        for zip_code in zips_to_try:
            address = await self.zip_random_address(zip_code=zip_code, state=zip_result.state, default_city=city)
            if address:
                return address
            log.debug(f'No address for {zip_code}, {zip_result.state}')
        # for zip_code ...
        return None

    async def npa_random_address(self, *, npa: str) -> RandomAddressResponse:
        """
        random address for npa
        :param npa:
        :return:
        """
        cities = await self.npa_cities(npa=npa)
        if not cities:
            log.debug(f'No cities found for NPA {npa}')
            return None

        # try cities in random order
        random.shuffle(cities)
        for city in cities:
            address = await self.city_random_address(city=city.city, state=city.state)
            if address:
                address.npa = npa
                return address
            log.debug(f'no address for {city.city}, {city.state}')
        # for city ...
        return None

    async def random_address(self) -> Address:
        """
        Get a random address in the US
        """
        npa_data = await self.load_npa_data()
        us_npas = [npa.npa
                   for npa in npa_data
                   if npa.country == 'US' and npa.in_service]
        while True:
            npa = random.choice(us_npas)
            address = await self.npa_random_address(npa=npa)
            if address is not None:
                break
        return address
