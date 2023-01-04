import sys
import pyodbc
import configparser
import pandas as pd
import datetime

class QSCOREDB():
    parser = configparser.ConfigParser()
    parser.read('qscore.ini')
    SQL_driver = parser['myserv']['SQL_driver']
    Server = parser['myserv']['Server']
    Database = parser['myserv']['Database']
    uid = parser['myserv']['uid']
    pwd = parser['myserv']['pwd']
    
    def __init__(self):
        con_string = f'Driver={self.SQL_driver};Server={self.Server};Database={self.Database};UID={self.uid};pwd={self.pwd}'
        # con_string = f'Driver={self.SQL_driver};Server={self.Server};Database={self.Database};Trusted_Connection=yes;'
        try:
            self.conn = pyodbc.connect(con_string)
        except Exception as e:
            print(e)
            print('Task is terminate')
            sys.exit()
        else:
            self.cursor = self.conn.cursor()
            print('Conection successfully','(',datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),')')
    
    ########################################################################
    def create_qscore_tbl(self):
        create_qscore_sql = """CREATE TABLE qscore (
            ID int NOT NULL IDENTITY(1,1),
            vendor varchar(15), 
            material varchar(15), 
            year int,
            month int,
            q_score decimal(10,2),
            MA decimal(10,2)
            )
            """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(create_qscore_sql)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('create_qscore_sql error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            print('create_qscore_sql successful','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            self.cursor.commit()
            self.cursor.close()
            
    def insert_qscore_tbl(self,data):
        insert_qscore_sql = """INSERT INTO qscore (
            vendor, 
            material, 
            year, 
            month, 
            q_score,
            MA
            ) 
            VALUES (?,?,?,?,?,?)
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(insert_qscore_sql, data)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('insert_qscore_tbl error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()
            
    def get_qscore_tbl(self):
        get_qscore_sql = """SELECT * from qscore
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(get_qscore_sql)
            data = self.cursor.fetchall()
            data = pd.DataFrame((tuple(t) for t in data))
            data = data.rename(columns={
                0:'ID',
                1:'vendor',
                2:'material',
                3:'year',
                4:'month',
                5:'q_score',
                6:'MA',
                })
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('get_qscore_tbl error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()
            return  data
        
    def truncate_qscore_tbl(self):
        truncate_qscore_sql = """TRUNCATE TABLE qscore"""
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(truncate_qscore_sql)
        except Exception as ex:
            print(ex)
            print('truncate_qscore error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()
            
    ########################################################################
    def create_bestqscore_tbl(self):
        create_bestqscore_sql = """CREATE TABLE bestqscore (
            ID int NOT NULL IDENTITY(1,1),
            vendor varchar(15), 
            material varchar(15), 
            year int,
            month int,
            q_score decimal(10,2),
            MA decimal(10,2)
            )
            """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(create_bestqscore_sql)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('create_bestqscore_sql error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            print('create_bestqscore_sql successful','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            self.cursor.commit()
            self.cursor.close()
            
    def insert_bestqscore_tbl(self,data):
        insert_bestqscore_sql = """INSERT INTO bestqscore (
            vendor, 
            material, 
            year, 
            month, 
            q_score,
            MA
            ) 
            VALUES (?,?,?,?,?,?)
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(insert_bestqscore_sql, data)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('insert_bestqscore_tbl error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()
            
    def get_bestqscore_tbl(self):
        get_bestqscore_sql = """SELECT * from bestqscore
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(get_bestqscore_sql)
            data = self.cursor.fetchall()
            data = pd.DataFrame((tuple(t) for t in data))
            data = data.rename(columns={
                0:'ID',
                1:'vendor',
                2:'material',
                3:'year',
                4:'month',
                5:'q_score',
                6:'MA'
                })
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('get_bestqscore_tbl error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()
            return  data
        
    def truncate_bestqscore_tbl(self):
        truncate_bestqscore_sql = """TRUNCATE TABLE bestqscore"""
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(truncate_bestqscore_sql)
        except Exception as ex:
            print(ex)
            print('truncate_bestqscore error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()