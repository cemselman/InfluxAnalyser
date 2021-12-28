from influxdb import DataFrameClient, InfluxDBClient
from pandas import DataFrame

class InfluxAnalyser:

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.influxdb_client = InfluxDBClient(host, port, user, password)

    def get_databases(self, print_to_screen):
        try:
            df_databases = DataFrame(self.influxdb_client.query("SHOW DATABASES").get_points())
            if print_to_screen == True:
                print("################\nINFLUX DATABASES\n################\n")
                for i in range(len(df_databases)) : print("DB-" + str(i + 1), "> ", df_databases['name'].loc[i])
            return df_databases
        except Exception as e:
            print(e)

    def show_measurements(self):
        try:
            df_databases = self.get_databases(False)
            for i in range(len(df_databases)):
                db_name = df_databases['name'].loc[i]
                print("\nDATABASE : " + db_name + "\n")
                df_measurements = DataFrame(self.influxdb_client.query("show measurements on " + db_name).get_points())
                print("Measurements >")
                print(df_measurements)
        except Exception as e:
            print(e)

if __name__ == "__main__":

    host = "localhost"
    port = 8086
    user = ""
    password = ""

    analyser = InfluxAnalyser(host, port, user, password)
    analyser.get_databases(True)
    analyser.show_measurements()

