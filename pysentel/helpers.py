# coding: utf-8
"""
Helpers for pysentel.
"""
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDataIngest:
    """
    Module for handling ingests to InfluxDB-bucket.
    """

    def __init__(self,
                 url: str,
                 org: str,
                 bucket: str,
                 token: str):
        """
        Initialization constructor - start our DB-connection.
        :param url: Define url for the InfluxDB connection. Required.
        :param org: Define InfluxDB organization. Required.
        :param bucket: Define InfluxDB bucket to ingest into. Required.
        :param token: Define authentication-token with write-access. Required.
        """
        self.org = org
        self.bucket = bucket
        self.url = url
        self.token = token
        self.connection = self._establish_connection()
        self.client = self._write_definitions()

    def __del__(self):
        """ Destructor for closing object correctly. """
        self._close_connection()

    def _establish_connection(self):
        """ Create connection to InfluxDB """
        return InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org)

    def _write_definitions(self):
        """ Define write options for write_api """
        return self.connection.write_api(write_options=SYNCHRONOUS)

    def _close_connection(self):
        """ Closing connection to InfluxDB """
        return self.client.close()

    def write_point(self):
        """ Write datapoint """
        # TODO: Receive datapoint from call
        # TODO: Create serializer for setting measurement information correctly
        point = Point('measurement').tag('tab_name', 'tag_value').field(
            'field_name', 'value')
        return self.client.write(
            bucket=self.bucket,
            org=self.org,
            record=point)
