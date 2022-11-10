import os
from contextlib import contextmanager
from time import perf_counter
from unittest import TestCase

from dotenv import load_dotenv


@contextmanager
def time_it(action: str = None):
    action = action or 'operation'
    start = perf_counter()
    try:
        yield
    finally:
        stop = perf_counter()
        print(f'{action} took {(stop - start) * 1000:.3f} ms')


class GeoLocationEnv(TestCase):
    """
    Mixin to load the ENV file required for the geolocation API
    """
    @classmethod
    def setUpClass(cls) -> None:
        env_path = os.path.abspath(os.path.join(os.getcwd(), '..', 'ipgeolocation.env'))
        load_dotenv(env_path)
        super().setUpClass()


class GeoCodifyEnv(GeoLocationEnv):

    @classmethod
    def setUpClass(cls) -> None:
        env_path = os.path.abspath(os.path.join(os.getcwd(), '..', 'geocodify.env'))
        load_dotenv(env_path)
        super().setUpClass()

