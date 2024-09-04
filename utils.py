import os

import yaml
from temporalio.client import TLSConfig


def get_temporal_connection(env: str):
    address, tls, namespace = "localhost:7233", False, "default"
    return address, tls, namespace
