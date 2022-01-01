from influxdb import DataFrameClient, InfluxDBClient
from pandas import DataFrame
import pandas as pd
import numpy as np

class InfluxMain:

    def __init__(self, host, port, user, password, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.client = DataFrameClient(host, port, user, password, dbname)
        self.__create_database__(dbname) #Creates if db does not exist
        
    def __create_database__(self, db):
        try:
            print("Creating DB: " + db)
            self.client.create_database(db)
        except Exception as e:
            print(e)

    def insert_data(self, data, measurement):
        tag_columns=['Name', 'City', 'Market_Type'] #Define tag column names
        self.__write_to_database__(data, measurement, tag_columns)
            
    def drop_measurement(self, measurement):
        print("Dropping measurement: " + measurement)
        self.client.drop_measurement(measurement)

    def __write_to_database__(self, data, measurement, tag_columns, protocol="line"):
        try:
            print("Create Measurement: " + measurement)            
            #fieldlist = ['Par_Val', 'Core_Val', 'Start_Time'] comes with data, we are not defining in write_points. 
            self.client.write_points(data, measurement, tag_columns=tag_columns, protocol = protocol, batch_size=10000)
            print("Done!")            
        except Exception as e:
            traceback.print_exc()

class InfluxAnalyser:

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.influxdb_client = InfluxDBClient(host, port, user, password)
        
    def close_connection(self):
        self.influxdb_client.close()

    def get_databases(self, print_to_screen):
        try:
            df_databases = DataFrame(self.influxdb_client.query("SHOW DATABASES").get_points())
            if print_to_screen == True:
                print("\n| INFLUX DATABASES |\n")
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
    database = "TestDB"
    measurement = "TestMeasurement"
    
    #Test data - Dataframe with Python Dataframe
    df = pd.DataFrame(columns=['Name','City','Market_Type','Par_Val','Core_Val'])
    dfrow = {'Name':'Mert', 'City':'ist','Market_Type':'marmar','Par_Val':'23443234','Core_Val':'7567567', 'Start_Time':'2021-01-01 00:10:33'}
    df = df.append(dfrow, ignore_index = True)    
    df['Index_Time']=pd.to_datetime(df['Start_Time'])
    df.set_index('Index_Time',inplace=True) 
    
    influxdb_obj = InfluxMain(host, port, user, password, database) #Create DB
    influxdb_obj.drop_measurement(measurement) #Delete measurement
    influxdb_obj.insert_data(df, measurement) #Create measurement and add data
    del influxdb_obj

    analyser = InfluxAnalyser(host, port, user, password)
    analyser.get_databases(True)
    analyser.show_measurements()
    analyser.close_connection()
    del analyser
    
    