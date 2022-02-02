import unittest
from unittest import TestCase, mock
from influxdb import DataFrameClient, InfluxDBClient


from influxtool import (
    InfluxMain,
    InfluxAnalyser,
    logger,
)


class TestInfluxMainMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.host, self.port = "localhost", 8086
        self.user, self.password = "", ""
        self.dbname = "TestDB"
        self.InfluxMain = InfluxMain(
            self.host, self.port, self.user, self.password, self.dbname
        )

    def test_influxdb_connection(self):
        self.assertTrue(self.InfluxMain.influxdb_client.query("SHOW DATABASES"))
        self.InfluxMain.influxdb_client.close()
        self.InfluxMain.close_connection()

    def test_database_creation(self):
        with self.assertLogs(logger) as log:
            testdb = "TempTestDB"
            self.InfluxMain.__create_database__(testdb)
        self.assertIn(
            f"DB is ok : '{testdb}'",
            log.output[0],
        )
        self.InfluxMain.influxdb_client.query(f"DROP DATABASE {testdb}")
        self.InfluxMain.close_connection()
        self.InfluxMain.influxdb_client.close()
        logger.info(f"Dropping '{testdb}'")


if __name__ == "__main__":
    unittest.main()
