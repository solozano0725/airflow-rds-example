import psycopg2
import pandas as pd
from psycopg2 import OperationalError

class Connection:
    
    def __init__(self, db_name, db_user, db_password, db_host, db_port):
        self.connection = None
        try:
            self.connection = psycopg2.connect(
                database=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
            )
            self.cursor = self.connection.cursor()
            print("Connection to PostgreSQL DB successful")
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        return self.connection
        
    def get_cursor(self):
        return self.cursor()

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.cursor.close()
            self.connection.commit()
            return True
        except Error as e:
            print(f"The error '{e}' occurred")

    def read_query(self, query):
        r = None
        try:
            self.cursor.execute(query)
            r = self.cursor.fetchall()
            self.cursor.close()
            return r
        except Error as e:
            print(f"The error '{e}' occurred")

    def read_query_df(self, select_query, column_names):
        try:
            self.cursor.execute(select_query)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.cursor.close()
            return 1
        df = pd.DataFrame(self.cursor.fetchall(), columns=column_names)
        self.cursor.close()
        return df