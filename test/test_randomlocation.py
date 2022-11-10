"""
Tests on package nanpa
"""
import asyncio
import logging
import random
import webbrowser
from functools import wraps
from unittest import TestCase, skip

from test_helper.randomlocation import RandomLocation
from test_helper.randomlocation.geo_nominatim import GeoNominatim
from testbase import time_it, GeoCodifyEnv


def sync2async(func):
    @wraps(func)
    def async_run(*args, **kwargs):
        asyncio.run(func(*args, **kwargs))
        pass

    return async_run


class TestLoadNpa(TestCase):

    def test_001_load_npa_data(self):
        """
        Load NPA data
        :return:
        """

        async def test():
            async with RandomLocation() as util:
                with time_it('Getting NPA info'):
                    info = await util.load_npa_data()
            print(f'Got info for {len(info)} NPAs')
            print(f'{sum(npa.in_service and npa.country == "US" for npa in info)} NPAs in service in the US')
            return

        asyncio.run(test())

    def test_002_cities_212(self):
        """
        get cities for NPA 212
        :return:
        """

        async def test():
            async with RandomLocation() as util:
                with time_it('Getting cities for NPA 212'):
                    cities = await util.npa_cities(npa='212')
            print('\n'.join(f'{city.city}, {city.state}' for city in cities))
            return

        asyncio.run(test())

    def test_003_cities_all_active_NPA(self):
        """
        get cities for all active US NPAs
        :return:
        """

        async def test():
            async with RandomLocation() as util:
                with time_it('Getting NPA info'):
                    npa_info = await util.load_npa_data()

                # only in service NPAs in the US
                npa_info = [npa for npa in npa_info
                            if npa.in_service and npa.country == 'US']

                tasks = [util.npa_cities(npa=npa.npa)
                         for npa in npa_info]
                with time_it(f'Getting cities for {len(npa_info)} NPAs'):
                    cities_list = await asyncio.gather(*tasks)

            for npa, cities in zip(npa_info, cities_list):
                print(f'{npa.npa} ({len(cities):2}): {"; ".join(f"{city.city}, {city.state}" for city in cities)}')

            print()
            print(f'Total: {sum(len(cities) for cities in cities_list)} cities')
            return

        asyncio.run(test())


class GeoCodifyBase(GeoCodifyEnv):

    def setUp(self) -> None:
        super().setUp()
        console_handler = logging.StreamHandler()
        self.random_location_console_handler = console_handler
        logger = logging.getLogger('randomlocation')
        logger.addHandler(console_handler)
        self.random_location_logger_level = logger.level
        logger.setLevel(logging.DEBUG)

    def tearDown(self) -> None:
        logger = logging.getLogger('randomlocation')
        logger.removeHandler(self.random_location_console_handler)
        logger.setLevel(self.random_location_logger_level)
        super().tearDown()


class TestInit(GeoCodifyBase):
    """
    Test __init__
    """

    def test_001(self):
        async def test():
            api = RandomLocation()
            pass

        asyncio.run(test())


class TestGeocode(GeoCodifyBase):
    """
    Test geocode(
    """

    def test_001(self):
        async def test():
            async with RandomLocation() as api:
                data = await api.geocode(search='City: Darmstadt, Germany')

        asyncio.run(test())

    def test_002(self):
        async def test():
            async with RandomLocation() as api:
                data = await api.geocode(search='New York, NY, USA')

        asyncio.run(test())

    def test_003(self):
        async def test():
            async with RandomLocation() as api:
                data = await api.geocode(search='NC 27709, USA')

        asyncio.run(test())

    def test_004(self):
        async def test():
            async with RandomLocation() as api:
                data = await api.geocode(search='NY 10280, USA')

        asyncio.run(test())

    def test_005(self):
        async def test():
            async with RandomLocation() as api:
                data = await api.geocode(search='64291, Germany')

        asyncio.run(test())


class TestReverse(GeoCodifyBase):
    def test_001(self):
        async def test():
            async with RandomLocation() as api:
                # 8.649440308833686, 49.871469651928095,
                # [8.649194, 49.87138, 8.649639, 49.87156]
                data = await api.reverse(lon=8.649440308833686, lat=49.871469651928095)

        asyncio.run(test())


class GetAddressInCity(GeoCodifyBase):
    def test_001_New_York(self):
        async def test():
            async with RandomLocation() as api:
                geocoded = await api.geocode(search='New York, NY, USA')
                locality = geocoded.response.locality
                self.assertIsNotNone(locality, 'No locality found')
                reverse = await api.reverse(lon=locality.point.lon,
                                            lat=locality.point.lat)
                addresses = list(reverse.response.addresses)
                foo = 1

            pass

        asyncio.run(test())


class TestZIP(GeoCodifyBase):
    def test_001_new_york(self):
        async def test():
            async with RandomLocation() as zip_api:
                zip_codes = await zip_api.get_zips(city='New York', state='NY')
            pass

        asyncio.run(test())

    def test_002_manhattan(self):
        async def test():
            async with RandomLocation() as zip_api:
                zip_codes = await zip_api.get_zips(city='Manhattan', state='NY')
            # looking up Manhattan gets us list of zip codes but looking up "Manhattan" is not acceptable
            # hence the list of "regular" zip codes should be empty
            # .. and "NOT ACCEPTABLE" shows up in the record type
            self.assertFalse(not list(zip_codes.regular_zips), 'No regular ZIP codes found')
            self.assertTrue(all(
                not zip_code.is_po_box and not zip_code.is_unique and 'NOT ACCEPTABLE' in zip_code.record_type for
                zip_code in zip_codes.zip_list))

        asyncio.run(test())


class RandomLocationWithZip(GeoCodifyBase):

    def test_001_npa_219(self):
        async def test():
            async with RandomLocation() as api:
                result = await api.npa_random_address(npa='219')
            print(result)
            self.assertIsNotNone(result)

        asyncio.run(test())

    def test_002_npa_808(self):
        async def test():
            async with RandomLocation() as api:
                result = await api.npa_random_address(npa='808')
            print(result)
            self.assertIsNotNone(result)

        asyncio.run(test())

    def test_002_npa_820(self):
        async def test():
            async with RandomLocation() as api:
                result = await api.npa_random_address(npa='820')
            print(result)
            self.assertIsNotNone(result)

        asyncio.run(test())

    def test_004_random_npa(self):
        async def test():
            async with RandomLocation() as api:
                npa_data = await api.load_npa_data()
                # only assigned NPAs in US
                npa_data = [npa for npa in npa_data
                            if npa.in_service and npa.country == 'US']
                npa = random.choice(npa_data)
                print(f'Getting random address for npa {npa.npa}')
                result = await api.npa_random_address(npa=npa.npa)
            print(result)
            self.assertIsNotNone(result)
            webbrowser.open_new_tab(url=result.google_maps_url)

        logging.basicConfig(level=logging.DEBUG)
        asyncio.run(test())

    @skip('Violates rate limits')
    def test_005_random_address_all_npas(self):
        async def test():
            async with RandomLocation() as api:
                npa_data = await api.load_npa_data()
                # only assigned NPAs in US
                npa_data = [npa for npa in npa_data
                            if npa.in_service and npa.country == 'US']
                tasks = [api.npa_random_address(npa=npa.npa)
                         for npa in npa_data]
                with time_it(f'getting random addresses for {len(npa_data)} NPAs'):
                    results = await asyncio.gather(*tasks)
            for npa, address in zip(npa_data, results):
                print(f'{npa}: {address}')

        asyncio.run(test())

    def test_003_pearl_city(self):
        async def test():
            async with RandomLocation() as api:
                result = await api.city_random_address(city='Pearl City', state='HI')
            print(result)
            self.assertIsNotNone(result)

        asyncio.run(test())


class TestGeoNominatim(GeoCodifyBase):
    def test(self):
        async def run():
            async with GeoNominatim() as geo:
                address = await geo.adddress_for_zip(country='US',
                                                     zip_code='20002')
                print(address.address)
                webbrowser.open_new_tab(f'https://www.google.com/maps/place/{address.latitude},{address.longitude}')
            async with RandomLocation() as api:
                r_address = await api.zip_random_address(zip_code='20002', state='DC')
                print(r_address)
                webbrowser.open_new_tab(r_address.google_maps_url)

        asyncio.run(run())

    @sync2async
    async def test_npa_and_address(self):
        async def address_in_city(*, city: str, state: str):
            geocoded = await geo._nominatim.geocode(query={'city': city, 'state': state},
                                                    country_codes='us')
            foo = 1
            reverse_geo = await geo._nominatim.reverse((geocoded.latitude, geocoded.longitude))
            return reverse_geo

        async with GeoNominatim() as geo:
            async with RandomLocation() as api:
                # pick random NPA
                npa_data = await api.load_npa_data()
                npa_data = [npa for npa in npa_data
                            if npa.in_service]
                random.shuffle(npa_data)
                npas = iter(npa_data)
                npa = next(npas)
                print(f'NPA: {npa.npa}')

                # get cities for NPA
                cities = await api.npa_cities(npa=npa.npa)
                print('cities:')
                print('\n'.join(f'  {city.city}, {city.state}' for city in cities))

                # for each city try to get an address
                addresses = [await address_in_city(city=city.city, state=city.state) for city in cities]
                # tasks = [address_in_city(city=city.city, state=city.state) for city in cities]
                # addresses = await asyncio.gather(*tasks)
                print('addresses:')
                print('\n'.join(f'  {a}' for a in addresses))
