from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from geopy.adapters import AioHTTPAdapter
from geopy.geocoders import Nominatim


class GeoAbc(ABC):
    @abstractmethod
    async def adddress_for_zip(self, *, country: str, zip_code: str):
        ...


@dataclass(init=False)
class GeoNominatim(GeoAbc):
    _nominatim: Optional[Nominatim] = None

    def __init__(self):
        self._nominatim = Nominatim(
            user_agent='geo_nominatim',
            adapter_factory=AioHTTPAdapter)
        foo = 1

    async def __aenter__(self):
        await self._nominatim.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._nominatim.__aexit__(*args, **kwargs)

    async def adddress_for_zip(self, *, country: str, zip_code: str):
        result = await self._nominatim.geocode(query={'country': country,
                                                      'postalcode': zip_code},
                                               addressdetails=True)
        lat = result.latitude
        lon = result.longitude
        # bb = result.raw['boundingbox']
        # lat = random.uniform(float(bb[0]), float(bb[1]))
        # lon = random.uniform(float(bb[2]), float(bb[3]))
        geo_reverse = await self._nominatim.reverse(query=(lat, lon), exactly_one=True,
                                                    zoom=18,
                                                    addressdetails=True)
        return geo_reverse
