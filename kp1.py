# -*- coding: utf-8 -*-
import psycopg2
from psycopg2 import sql
import csv
import numpy as np
import os
import logging
import time


def get_float_positions(cursor):
    float_query = """SELECT ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'zno' AND data_type='double precision' ORDER BY ordinal_position"""
    cursor.execute(float_query)
    float_positions = []
    for tup in cursor.fetchall():
        float_positions += [tup[0] - 1]
    return float_positions


def get_column_names(cursor):
    columns=[]
    col_query="""SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'zno' ORDER BY ordinal_position;"""
    cursor.execute(col_query)
    col_names = (cursor.fetchall())
    for tup in col_names:
        columns += [tup[0]]
    return columns

def float_preparations(data, floats):
    for lst in data:
        for num in floats:
            try:
                lst[num] = float(lst[num])
            except ValueError:
                lst[num] = float(lst[num].replace(',', '.'))
            except TypeError:
                lst[num] = None


def prepare_data_for_insert(csv_reader, year, float_positions):
    data = [i + [year] for i in csv_reader]
    data=np.array([None if j=='null' else j for x in data for j in x])
    data=np.reshape(data, (len(csv_reader), 127))
    float_preparations(data, float_positions)
    return data



def write_in_file(dict_history, regions):
    if os.path.exists('history_average.csv'):
        os.remove('history_average.csv')
    with open('history_average.csv', mode='w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(["Регіони", "2019", "2020"])
        for i in regions:
            writer.writerow([i, dict_history[i][0], dict_history[i][1]])


query="""
    CREATE TABLE IF NOT EXISTS zno (
        out_id VARCHAR PRIMARY KEY,
        birth VARCHAR,
        sex VARCHAR,
        region VARCHAR,
        area VARCHAR,
        tername VARCHAR,
        reg_type VARCHAR,
        ter_type VARCHAR,
        class_profile VARCHAR,
        class_lang VARCHAR,
        EOName VARCHAR,
        EOType VARCHAR,
        EOReg VARCHAR,
        EOArea VARCHAR,
        EOTer VARCHAR,
        EOParent VARCHAR,
        ukr_test VARCHAR,
        ukr_test_stat VARCHAR,
        ukr_100 FLOAT,
        ukr_12 FLOAT,
        ukr_ball FLOAT,
        ukr_adapt FLOAT,
        ukrPTName VARCHAR,
        ukrPTReg VARCHAR,
        ukrPTArea VARCHAR,
        ukrPTTer VARCHAR,
        hist_test VARCHAR,
        hist_lang VARCHAR,
        hist_test_stat VARCHAR,
        hist_100 FLOAT,
        hist_12 FLOAT,
        hist_ball FLOAT,
        histPTName VARCHAR,
        histPTReg VARCHAR,
        histPTArea VARCHAR,
        histPTTer VARCHAR,
        math_test VARCHAR,
        math_lang VARCHAR,
        math_test_stat VARCHAR,
        math_100 FLOAT,
        math_12 FLOAT,
        math_ball FLOAT,
        mathPTName VARCHAR,
        mathPTReg VARCHAR,
        mathPTArea VARCHAR,
        mathPTTer VARCHAR,
        phys_test VARCHAR,
        phys_lang VARCHAR,
        phys_test_stat VARCHAR,
        phys_100 FLOAT,
        phys_12 FLOAT,
        phys_ball FLOAT,
        physPTName VARCHAR,
        physPTReg VARCHAR,
        physPTArea VARCHAR,
        physPTTer VARCHAR,
        chem_test VARCHAR,
        chem_lang VARCHAR,
        chem_test_stat VARCHAR,
        chem_100 FLOAT,
        chem_12 FLOAT,
        chem_ball FLOAT,
        chemPTName VARCHAR,
        chemPTReg VARCHAR,
        chemPTArea VARCHAR,
        chemPTTer VARCHAR,
        bio_test VARCHAR,
        bio_lang VARCHAR,
        bio_test_stat VARCHAR,
        bio_100 FLOAT,
        bio_12 FLOAT,
        bio_ball FLOAT,
        bioPTName VARCHAR,
        bioPTReg VARCHAR,
        bioPTArea VARCHAR,
        bioPTTer VARCHAR,
        geo_test VARCHAR,
        geo_lang VARCHAR,
        geo_test_stat VARCHAR,
        geo_100 FLOAT,
        geo_12 FLOAT,
        geo_ball FLOAT,
        geoPTName VARCHAR,
        geoPTReg VARCHAR,
        geoPTArea VARCHAR,
        geoPTTer VARCHAR,
        eng_test VARCHAR,
        eng_test_stat VARCHAR,
        eng_100 FLOAT,
        eng_12 FLOAT,
        eng_dpa VARCHAR,
        eng_ball FLOAT,
        engPTName VARCHAR,
        engPTReg VARCHAR,
        engPTArea VARCHAR,
        engPTTer VARCHAR,
        fra_test VARCHAR,
        fra_test_stat VARCHAR,
        fra_100 FLOAT,
        fra_12 FLOAT,
        fra_dpa VARCHAR,
        fra_ball FLOAT,
        fraPTName VARCHAR,
        fraPTReg VARCHAR,
        fraPTArea VARCHAR,
        fraPTTer VARCHAR,
        deu_test VARCHAR,
        deu_test_stat VARCHAR,
        deu_100 FLOAT,
        deu_12 FLOAT,
        deu_dpa VARCHAR,
        deu_ball FLOAT,
        deuPTName VARCHAR,
        deuPTReg VARCHAR,
        deuPTArea VARCHAR,
        deuPTTer VARCHAR,
        spa_test VARCHAR,
        spa_test_stat VARCHAR,
        spa_100 FLOAT,
        spa_12 FLOAT,
        spa_dpa VARCHAR,
        spa_ball FLOAT,
        spaPTName VARCHAR,
        spaPTReg VARCHAR,
        spaPTArea VARCHAR,
        spaPTTer VARCHAR,
        year VARCHAR)
        """

logger = logging.getLogger(__name__)
logging.basicConfig(filename="log_time.txt",format="%(asctime)s *** %(message)s",level=logging.INFO)
logger.info("Start of program")
def ReconectingDataBase(func):
    def wrapper():
        try:
            func()
        except psycopg2.errors.AdminShutdown:
            time.sleep(1)
            print("Enter docker-compose up in terminal, then enter DataBase information")
            wrapper()
    return wrapper()
@ReconectingDataBase
def main():
    dbname=input('Enter DataBase name: ')
    #dbname='test'
    #user='postgres'
    #password='postgres'
    #host='localhost'
    user=input('Enter username: ')
    password=input('Enter password: ')
    host=input("Enter host: ")
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    cursor=conn.cursor()
    cursor.execute(query)
    conn.commit()

    columns=get_column_names(cursor)
    float_positions=get_float_positions(cursor)

    query2=sql.SQL("INSERT INTO zno ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder()*len(columns)))

    logger.info("Start of reading from file")
    with open('Odata2019File.csv') as csv_file:
        csv_reader=list(csv.reader(csv_file, delimiter=';'))[1:]
    with open('Odata2020File.csv') as csv_file:
        csv_reader2=list(csv.reader(csv_file, delimiter=';'))[1:]
    logger.info("End of reading from file")

    data2019=prepare_data_for_insert(csv_reader, 2019, float_positions)
    data2020=prepare_data_for_insert(csv_reader2, 2020, float_positions)
    data=np.vstack((data2019, data2020))
    cursor.execute("""SELECT COUNT(*) FROM zno""")
    num=cursor.fetchall()[0][0]
    if num<data.shape[0]:
        logger.info("Inserting data into table")
        counter=num
        try:
            for i in data:
                cursor.execute(query2, i)
                counter+=1
                if counter%100==0:
                    conn.commit()
                    print(counter)
                    with open('commits.csv', 'a+', newline='') as write_obj:
                        # Create a writer object from csv module
                        csv_writer = csv.writer(write_obj)
                        # Add contents of list as last row in the csv file
                        csv_writer.writerow([counter])
        except psycopg2.errors.UniqueViolation:
            with open('commits.csv') as csv_file:
                last_commit = int(list(csv.reader(csv_file, delimiter=';'))[-1][0])
            print(last_commit)
            cursor.execute("""rollback;""")
            for i in data[last_commit+1:]:
                cursor.execute(query2, i)
                counter+=1
                if counter%100==0:
                    conn.commit()
                    print(counter)
                    with open('commits.csv', 'a+', newline='') as write_obj:
                        # Create a writer object from csv module
                        csv_writer = csv.writer(write_obj)
                        # Add contents of list as last row in the csv file
                        csv_writer.writerow([counter])

        logger.info("Data inserted")

    query_2019="""SELECT year, AVG(hist_100), region FROM zno
                WHERE hist_test_stat='Зараховано'
                GROUP BY region, year
                ORDER BY year;
                """
    cursor.execute(query_2019)
    history_query=np.array(cursor.fetchall())
    regions = set(history_query[:, 2])
    dict_history={}
    for i in regions:
        marks=np.where(history_query==i)[0]
        dict_history[i]=[history_query[marks[0]][1], history_query[marks[1]][1]]

    write_in_file(dict_history, regions)
    cursor.close()
    conn.commit()
    conn.close()
    logger.info("End of program")


if __name__=="__main__":
    try:
        main()
    except TypeError:
        pass
