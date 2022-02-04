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

        # START : Test data : Dataframe with Python Dataframe
        self.df = pd.DataFrame(
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
        self.df = self.df.append(dfrow, ignore_index=True)
        self.df["Index_Time"] = pd.to_datetime(self.df["Start_Time"])
        self.df.set_index(
            "Index_Time", inplace=True
        )  # Setting index time for influxdb measurement

        self.tag_columns = [
            "Name",
            "City",
            "Market_Type",
        ]  # Define tag column names. Fields come with data.
        # END : Test data : Dataframe with Python Dataframe

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
        with self.assertLogs(logger) as log:
            self.InfluxMain.insert_data(self.df, testmeasurement, self.tag_columns)
        self.assertIn(
            f"Successfully inserted to database",
            log.output[0],
        )
        self.InfluxMain.influxdb_client.query(f"DROP DATABASE {self.dbname}")
        self.InfluxMain.close_connection()
        self.InfluxMain.influxdb_client.close()

    def test_drop_measurement(self):
        testmeasurement = "TempTestMeasurement"
        self.InfluxMain.insert_data(self.df, testmeasurement, self.tag_columns)
        with self.assertLogs(logger) as log:
            self.InfluxMain.drop_measurement(testmeasurement)
        self.assertIn(
            f"Measurement Dropped.",
            log.output[0],
        )
        self.InfluxMain.influxdb_client.query(f"DROP DATABASE {self.dbname}")
        self.InfluxMain.close_connection()
        self.InfluxMain.influxdb_client.close()

    def test__write_to_database__(self):
        testmeasurement = "TempTestMeasurement"
        with self.assertLogs(logger) as log:
            self.InfluxMain.__write_to_database__(
                self.df, testmeasurement, self.tag_columns, protocol="line"
            )
        self.assertIn(
            f"Successfully inserted to database",
            log.output[0],
        )
        self.InfluxMain.influxdb_client.query(f"DROP DATABASE {self.dbname}")
        self.InfluxMain.close_connection()
        self.InfluxMain.influxdb_client.close()


class TestInfluxAnalyserMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.host, self.port = "localhost", 8086
        self.user, self.password = "", ""
        self.dbname = "TempTestDB"
        self.InfluxAnalyser = InfluxAnalyser(
            self.host, self.port, self.user, self.password, self.dbname
        )

        # START : Test data : Dataframe with Python Dataframe
        self.df = pd.DataFrame(
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
        self.df = self.df.append(dfrow, ignore_index=True)
        self.df["Index_Time"] = pd.to_datetime(self.df["Start_Time"])
        self.df.set_index(
            "Index_Time", inplace=True
        )  # Setting index time for influxdb measurement

        self.tag_columns = [
            "Name",
            "City",
            "Market_Type",
        ]  # Define tag column names. Fields come with data.
        # END : Test data : Dataframe with Python Dataframe

    def test_influxdb_connection_close(self):
        with self.assertLogs(logger) as log:
            self.InfluxAnalyser.close_connection()
        self.assertIn(
            f"Successfully closed db connection",
            log.output[0],
        )

    def test_get_databases(self):
        with self.assertLogs(logger) as log:
            self.InfluxAnalyser.get_databases(True)
        self.assertIn(
            f"Databases list successfully received",
            log.output[0],
        )
        self.InfluxAnalyser.close_connection()

    def test_show_measurements(self):
        with self.assertLogs(logger) as log:
            self.InfluxAnalyser.show_measurements()
        self.assertIn(
            f"Databases list successfully received",
            log.output[0],
        )
        self.InfluxAnalyser.close_connection()


if __name__ == "__main__":
    unittest.main()
