import unittest
from unittest import TestCase, mock
from influxdb import DataFrameClient, InfluxDBClient


from influxtool import  (
    InfluxMain,
    InfluxAnalyser,
)


class TestInfluxdbMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.host, self.port = "localhost", 8086
        self.user, self.password = "", ""
        self.dbname = "TestDB"
        self.influxdb_client = InfluxDBClient(self.host, self.port, self.user, self.password, self.dbname)

    def test_influxdb_connection(self):
        self.assertTrue(self.influxdb_client.query("SHOW DATABASES").get_points())
        self.influxdb_client.close()


if __name__ == '__main__':
    unittest.main()
