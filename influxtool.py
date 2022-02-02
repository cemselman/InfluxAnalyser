from influxdb import DataFrameClient, InfluxDBClient
from pandas import DataFrame
import pandas as pd
import numpy as np
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class InfluxMain:
    def __init__(self, host, port, user, password, dbname):
        self.host, self.port = host, port
        self.user, self.password = user, password
        self.dbname = dbname
        self.client = DataFrameClient(host, port, user, password, dbname)
        self.influxdb_client = InfluxDBClient(host, port, user, password, dbname)
        self.__create_database__(dbname)  # Creates if db does not exist

    def __create_database__(self, db):
        try:
            print(f"Creating '{db}' if '{db}' does not exists.")
            self.client.create_database(db)
            logger.info(f"DB is ok : '{db}'")
        except Exception as e:
            print(e)

    def close_connection(self):
        self.client.close()
        logger.info(f"Successfully closed db connection")

    def insert_data(self, data, measurement, tag_columns):
        self.__write_to_database__(data, measurement, tag_columns)

    def drop_measurement(self, measurement):
        print("Dropping measurement: " + measurement)
        self.client.drop_measurement(measurement)

    def __write_to_database__(self, data, measurement, tag_columns, protocol="line"):
        try:
            print("Create Measurement: " + measurement)
            self.client.write_points(
                data,
                measurement,
                tag_columns=tag_columns,
                protocol=protocol,
                batch_size=10000,
            )
            print("Done!")
            logger.info(f"Successfully inserted to database")
        except Exception as e:
            traceback.print_exc()


class InfluxAnalyser:
    def __init__(self, host, port, user, password, dbname):
        self.host, self.port = host, port
        self.user, self.password = user, password
        self.dbname = dbname
        self.influxdb_client = InfluxDBClient(host, port, user, password, dbname)

    def close_connection(self):
        self.influxdb_client.close()
        logger.info(f"Successfully closed db connection")

    def get_databases(self, print_to_screen):
        try:
            df_databases = DataFrame(
                self.influxdb_client.query("SHOW DATABASES").get_points()
            )
            if print_to_screen == True:
                print("\n| INFLUX DATABASES |\n")
                for i in range(len(df_databases)):
                    print("DB-" + str(i + 1), "> ", df_databases["name"].loc[i])
            return df_databases
        except Exception as e:
            print(e)

    def show_measurements(self):
        try:
            df_databases = self.get_databases(False)
            for i in range(len(df_databases)):
                db_name = df_databases["name"].loc[i]
                print("\nDATABASE : " + db_name + "\n")
                df_measurements = DataFrame(
                    self.influxdb_client.query(
                        "show measurements on " + db_name
                    ).get_points()
                )
                print("Measurements >")
                print(df_measurements)
        except Exception as e:
            print(e)

    def migrate_measurement(self, source, target, influx, tag_columns, influx_index):
        select = "select * from " + source
        df = DataFrame(self.influxdb_client.query(select).get_points())
        df["Index_Time"] = pd.to_datetime(df[influx_index])
        df.set_index("Index_Time", inplace=True)
        influx.insert_data(df, target, tag_columns)
        logger.info(f"Successfully completed the migration")


if __name__ == "__main__":

    host, port = "localhost", 8086
    user, password = "", ""
    database = "TestDB"
    measurement = "TestMeasurement"

    influx = InfluxMain(
        host, port, user, password, database
    )  # Create DB if not exists, initiate connection object
    # influx.drop_measurement(measurement)

    # Test data : Dataframe with Python Dataframe
    df = pd.DataFrame(columns=["Name", "City", "Market_Type", "Par_Val", "Core_Val"])
    dfrow = {
        "Name": "Mert",
        "City": "ist",
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
    influx.insert_data(
        df, measurement, tag_columns
    )  # Create measurement if not exists, and add data

    analyser = InfluxAnalyser(host, port, user, password, database)
    analyser.get_databases(True)
    analyser.show_measurements()
    source, target, dbname = "TestMeasurement", "NewMeasurement", "TestDB"
    influx_index = "Start_Time"  # Influx DB table index time
    analyser.migrate_measurement(
        source, target, influx, tag_columns, influx_index
    )  # Read from source, write to target
    analyser.close_connection()

    del analyser
    del influx
