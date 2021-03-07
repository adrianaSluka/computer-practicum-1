# -*- coding: utf-8 -*-
import psycopg2
from psycopg2 import sql
import csv
from statistics import mean
import numpy as np
import os
import logging
import time

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
        ukr_100 VARCHAR,
        ukr_12 VARCHAR,
        ukr_ball VARCHAR,
        ukr_adapt VARCHAR,
        ukrPTName VARCHAR,
        ukrPTReg VARCHAR,
        ukrPTArea VARCHAR,
        ukrPTTer VARCHAR,
        hist_test VARCHAR,
        hist_lang VARCHAR,
        hist_test_stat VARCHAR,
        hist_100 VARCHAR,
        hist_12 VARCHAR,
        hist_ball VARCHAR,
        histPTName VARCHAR,
        histPTReg VARCHAR,
        histPTArea VARCHAR,
        histPTTer VARCHAR,
        math_test VARCHAR,
        math_lang VARCHAR,
        math_test_stat VARCHAR,
        math_100 VARCHAR,
        math_12 VARCHAR,
        math_ball VARCHAR,
        mathPTName VARCHAR,
        mathPTReg VARCHAR,
        mathPTArea VARCHAR,
        mathPTTer VARCHAR,
        phys_test VARCHAR,
        phys_lang VARCHAR,
        phys_test_stat VARCHAR,
        phys_100 VARCHAR,
        phys_12 VARCHAR,
        phys_ball VARCHAR,
        physPTName VARCHAR,
        physPTReg VARCHAR,
        physPTArea VARCHAR,
        physPTTer VARCHAR,
        chem_test VARCHAR,
        chem_lang VARCHAR,
        chem_test_stat VARCHAR,
        chem_100 VARCHAR,
        chem_12 VARCHAR,
        chem_ball VARCHAR,
        chemPTName VARCHAR,
        chemPTReg VARCHAR,
        chemPTArea VARCHAR,
        chemPTTer VARCHAR,
        bio_test VARCHAR,
        bio_lang VARCHAR,
        bio_test_stat VARCHAR,
        bio_100 VARCHAR,
        bio_12 VARCHAR,
        bio_ball VARCHAR,
        bioPTName VARCHAR,
        bioPTReg VARCHAR,
        bioPTArea VARCHAR,
        bioPTTer VARCHAR,
        geo_test VARCHAR,
        geo_lang VARCHAR,
        geo_test_stat VARCHAR,
        geo_100 VARCHAR,
        geo_12 VARCHAR,
        geo_ball VARCHAR,
        geoPTName VARCHAR,
        geoPTReg VARCHAR,
        geoPTArea VARCHAR,
        geoPTTer VARCHAR,
        eng_test VARCHAR,
        eng_test_stat VARCHAR,
        eng_100 VARCHAR,
        eng_12 VARCHAR,
        eng_dpa VARCHAR,
        eng_ball VARCHAR,
        engPTName VARCHAR,
        engPTReg VARCHAR,
        engPTArea VARCHAR,
        engPTTer VARCHAR,
        fra_test VARCHAR,
        fra_test_stat VARCHAR,
        fra_100 VARCHAR,
        fra_12 VARCHAR,
        fra_dpa VARCHAR,
        fra_ball VARCHAR,
        fraPTName VARCHAR,
        fraPTReg VARCHAR,
        fraPTArea VARCHAR,
        fraPTTer VARCHAR,
        deu_test VARCHAR,
        deu_test_stat VARCHAR,
        deu_100 VARCHAR,
        deu_12 VARCHAR,
        deu_dpa VARCHAR,
        deu_ball VARCHAR,
        deuPTName VARCHAR,
        deuPTReg VARCHAR,
        deuPTArea VARCHAR,
        deuPTTer VARCHAR,
        spa_test VARCHAR,
        spa_test_stat VARCHAR,
        spa_100 VARCHAR,
        spa_12 VARCHAR,
        spa_dpa VARCHAR,
        spa_ball VARCHAR,
        spaPTName VARCHAR,
        spaPTReg VARCHAR,
        spaPTArea VARCHAR,
        spaPTTer VARCHAR,
        year VARCHAR)
        """

logger = logging.getLogger(__name__)
logging.basicConfig(filename="log_time.txt",format="%(asctime)s *** %(message)s",level=logging.INFO)
logger.info("Start")
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
    user=input('Enter username: ')
    password=input('Enter password: ')
    host=input("Enter host: ")
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    cursor=conn.cursor()
    cursor.execute(query)
    columns=[]
    col_query="""SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'zno';"""
    cursor.execute(col_query)
    col_names = (cursor.fetchall())
    for tup in col_names:
        columns +=[tup[0]]

    query2=sql.SQL("INSERT INTO zno ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder()*len(columns)))

    with open('Odata2019File.csv') as csv_file:
        csv_reader=list(csv.reader(csv_file, delimiter=';'))[1:]

    with open('Odata2020File.csv') as csv_file:
        csv_reader2=list(csv.reader(csv_file, delimiter=';'))[1:]

    data2019=[i+['2019'] for i in csv_reader]
    data2020=[i+['2020'] for i in csv_reader2]

    query_test="""SELECT COUNT(*) FROM ZNO"""
    cursor.execute(query_test)
    num=cursor.fetchall()[0][0]
    if num==0:
        for i in data2019:
            cursor.execute(query2, i)
        for i in data2020:
            cursor.execute(query2, i)

    query_2019="""SELECT hist_100, region FROM zno
                WHERE hist_test_stat='Зараховано' AND year='2019'"""
    query_2020="""SELECT hist_100, region FROM zno
                WHERE hist_test_stat='Зараховано' AND year='2020'"""

    cursor.execute(query_2019)
    hist_2019=np.array(cursor.fetchall())
    cursor.execute(query_2020)
    hist_2020=np.array(cursor.fetchall())
    regions=set(hist_2019[:, 1])
    dict_2019={}
    for i in hist_2019:
        if i[1] in dict_2019:
            dict_2019[i[1]].append(float(i[0].replace(',','.')))
        else:
            dict_2019[i[1]]=[float(i[0].replace(',','.'))]
    dict_2020={}
    for i in hist_2020:
        if i[1] in dict_2020:
            dict_2020[i[1]].append(float(i[0].replace(',','.')))
        else:
            dict_2020[i[1]]=[float(i[0].replace(',','.'))]
    dict_mean_2019 = {key:[mean(value)] for (key,value) in dict_2019.items()}
    dict_mean_2020={key:mean(value) for (key,value) in dict_2020.items()}
    for i in regions:
        dict_mean_2019[i].append(dict_mean_2020[i])
    print(dict_mean_2019)
    if os.path.exists('history_average.csv'):
        os.remove('history_average.csv')
    with open('history_average.csv', mode='w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(["Регіони", "2019", "2020"])
        for i in regions:
            writer.writerow([i, dict_mean_2019[i][0], dict_mean_2019[i][1]])
    cursor.close()
    conn.commit()
    conn.close()
    logger.info("end")


if __name__=="__main__":
    try:
        main()
    except TypeError:
        pass
