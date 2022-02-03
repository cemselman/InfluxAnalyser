import unittest
from unittest import TestCase, mock
from influxdb import DataFrameClient, InfluxDBClient
import pandas as pd


from influxtool import (
    InfluxMain,
    InfluxAnalyser,
    logger,
)


class TestInfluxMainMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.host, self.port = "localhost", 8086
        self.user, self.password = "", ""
        self.dbname = "TempTestDB"
        self.InfluxMain = InfluxMain(
            self.host, self.port, self.user, self.password, self.dbname
        )

    def test_influxdb_connection(self):
        self.assertTrue(self.InfluxMain.influxdb_client.query("SHOW DATABASES"))
        self.InfluxMain.influxdb_client.close()
        self.InfluxMain.close_connection()

    def test_database_creation(self):
        with self.assertLogs(logger) as log:
            testdb = "TempNewTestDB"
            self.InfluxMain.__create_database__(testdb)
        self.assertIn(
            f"DB is ok : '{testdb}'",
            log.output[0],
        )
        self.InfluxMain.influxdb_client.query(f"DROP DATABASE {testdb}")
        self.InfluxMain.close_connection()
        self.InfluxMain.influxdb_client.close()
        logger.info(f"Dropping '{testdb}'")

    def test_influxdb_connection_close(self):
        with self.assertLogs(logger) as log:
            self.InfluxMain.close_connection()
        self.assertIn(
            f"Successfully closed db connection",
            log.output[0],
        )

    def test_insert_data(self):
        testmeasurement = "TempTestMeasurement"

        # Test data : Dataframe with Python Dataframe
        df = pd.DataFrame(
            columns=["Name", "City", "Market_Type", "Par_Val", "Core_Val"]
        )
        dfrow = {
            "Name": "Meril",
            "City": "Istanbul",
            "Market_Type": "marmar",
            "Par_Val": "23443234",
            "Core_Val": "7567567",
            "Start_Time": "2021-01-01 00:10:33",
        }
        df = df.append(dfrow, ignore_index=True)
        df["Index_Time"] = pd.to_datetime(df["Start_Time"])
        df.set_index(
            "Index_Time", inplace=True
        )  # Setting index time for influxdb measurement

        tag_columns = [
            "Name",
            "City",
            "Market_Type",
        ]  # Define tag column names. Fields come with data.

        with self.assertLogs(logger) as log:
            self.InfluxMain.insert_data(df, testmeasurement, tag_columns)
        self.assertIn(
            f"Successfully inserted to database",
            log.output[0],
        )
        self.InfluxMain.influxdb_client.query(f"DROP DATABASE {self.dbname}")
        self.InfluxMain.close_connection()
        self.InfluxMain.influxdb_client.close()


if __name__ == "__main__":
    unittest.main()
