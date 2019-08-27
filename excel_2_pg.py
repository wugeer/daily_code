"""
author:xiehongwang
description:提供一个文件夹路径，将该文件夹下的所有Excel先转为CSV文件，并将CSV通过spark导入hive中
last_modified:2019-08-04 
version:1.0 
improtion:遍历文件夹，传入要写入的hive表名
"""
from pyspark import SparkConf,SparkContext
import os,sys,time
import xlrd
import xlwt
import shutil
import csv
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import codecs
import re
xlrd.Book.encoding = "utf-8"
dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
PG_DRIVER = 'org.postgresql.Driver'

class spark_etl_2_hive(object):
    """ 
    遍历工程下的所有文件，不含文件夹，将Excel文件转为CSV，并将CSV文件导入hive中
    导入数据库中的同一个表
    """
    def __init__(self, dir_name, target_table):
        self.dir_name = dir_name
        self.target_table = target_table
        self.csv_dir = os.path.join(self.dir_name, 'csv')
        # if os.path.exists():
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)
        self.run()
    def run(self,):
        for file_name in os.scandir(self.dir_name):
            if file_name.name.endswith('.xlsx') or file_name.name.endswith('.xls'):
                self.excel_2_csv(os.path.join(self.dir_name, file_name))
            elif file_name.endswith('.csv'):
                # 移动CSV文件到指定目录
                shutil.move(os.path.join(self.dir_name, file_name), os.path.join(self.csv_dir, file_name))
                # self.csv_2_hive(file_name)
        # for file_name in os.scandir(self.csv_dir):
        #     # 对这个文件夹下所有的CSV导入库中
        #     self.csv_2_hive(os.path.join(self.csv_dir, file_name.name))
    def excel_2_csv(self, file_name, start_row=-1):
        workbook = xlrd.open_workbook(file_name)
        all_worksheets = workbook.sheet_names()
        dir_name,file_name = os.path.split(file_name)
        for worksheet_name in all_worksheets:
                worksheet = workbook.sheet_by_name(worksheet_name)
                # with open(os.path.join(self.csv_dir,worksheet_name+'.xlsx'), 'w', encoding='utf_8_sig' ,newline='') as your_csv_file:
                # for row in range(worksheet.nrows):
                #     for col in range(worksheet.ncols):
                # codecs.open('temp.csv', 'w', 'utf_8_sig')
                with open(os.path.join(self.csv_dir,worksheet_name+'.csv'), 'w', encoding='utf-8' , newline='') as your_csv_file:
                    csv_writer = csv.writer(your_csv_file)
                    for rownum in range(worksheet.nrows):
                            row_container = [re.sub('[,，]',' ', str(entry)) for idx, entry in enumerate(worksheet.row_values(rownum)) if idx>start_row]
                            print(row_container)
                            if row_container:
                                csv_writer.writerow(row_container)
    def csv_2_hive(self, file_name, mode='append'):
        df = spark.read.csv(file_name, header=True, inferSchema=False)
        # target_table = 'fresh_ods.ods_{0}'.format(file_name.lower())
        start = time.time()
        print('starting 写入{0}.......................................'.format(file_name))
        df.write.saveAsTable(self.target_table, mode = mode)
        print("cost {0} seconds".format(time.time()-start))
        print('end 写入{0}............................................'.format(file_name))

class spark_etl_2_pg(object):
    """ 
    遍历工程下的所有文件，不含文件夹，将Excel文件转为CSV，并将CSV文件导入hive中
    导入数据库中的同一个表
    """
    def __init__(self, dir_name, target_table, driver, db_url):
        spark_conf = SparkConf()
        spark_conf.set('driver-memory', '3g')
        spark_conf.set('executor-memory', '8g')
        spark_conf.set("spark.sql.shuffle.partitions", '1')
        spark_conf.set("spark.jars",os.path.join(dir_name,'spark-excel_2.11-0.8.3.jar'))
        spark_conf.set("spark.driver.extraClassPath", dir_name+"\postgresql-42.1.1.jar")
        # spark_conf.set("spark.jars",os.path.join(dirname,'spark-hadoopoffice-ds_2.11-1.3.1.jar'))
        self.spark = SparkSession.builder.appName("Pyspark-excel").master('local[*]').config(conf=spark_conf).getOrCreate()
        self.dir_name = dir_name
        self.target_table = target_table
        self.driver = driver
        self.db_url = db_url
        # self.spark = spark
        self.csv_dir = os.path.join(self.dir_name, 'csv')
        # if os.path.exists():
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)
        self.run()
    def run(self,):
        for file_name in os.scandir(self.dir_name):
            if file_name.name.endswith('.xlsx') or file_name.name.endswith('.xls'):
                self.excel_2_csv(os.path.join(self.dir_name, file_name), start_row=0)
            elif file_name.name.endswith('.csv'):
                shutil.move(os.path.join(self.dir_name, file_name.name), os.path.join(self.csv_dir, file_name.name))
        # for file_name in os.scandir(self.csv_dir):
        #     # 对这个文件夹下所有的CSV导入库中
        #     self.excel_2_pg(os.path.join(self.csv_dir, file_name.name))
    def excel_2_csv(self, file_name, start_row=0, start_column=1, time_col=[1], skip_sheet = []):
        """
        能解决Excel中时间类型
        传入参数：
            file_name 文件名称
            start_row 0代表第一行
            start_column 0代表第一列   
            time_col 列表形式，元素为行号，例如：[1,2]代表第2,3列是时间类型需要特殊处理
            skip_sheet 列表形式，元素为sheet页的名称
        返回值:空
        """
        workbook = xlrd.open_workbook(file_name)
        all_worksheets = workbook.sheet_names()
        dir_name,file_name = os.path.split(file_name)
        for worksheet_name in all_worksheets:
            if worksheet_name in skip_sheet:
                continue
            worksheet = workbook.sheet_by_name(worksheet_name)
            with open(os.path.join(self.csv_dir,file_name[:-5]+'_'+worksheet_name+'.csv'), 'w', encoding='utf-8' ,newline='') as your_csv_file:
                csv_writer = csv.writer(your_csv_file)
                for row in range(start_row, worksheet.nrows):
                    row_container=[]
                    # for idx, entry in enumerate(worksheet.row_values(rownum)):
                    for col in range(start_column, worksheet.ncols):
                        entry = worksheet.cell(row,col).value
                        # if col>start_column:
                        if row==0 or col not in time_col:
                            row_container.append(re.sub('\s+','', str(entry)))
                        else:
                            row_container.append(datetime.datetime(*xlrd.xldate_as_tuple(entry, workbook.datemode)))
                    # row_container = [re.sub('\s+','', str(entry)) if idx>1 else datetime.datetime(*xlrd.xldate_as_tuple(entry, workbook.datemode)) for idx, entry in enumerate(worksheet.row_values(rownum)) if idx>start_row]
                    # datetime.datetime(*xlrd.xldate_as_tuple(a1, book.datemode))
                    if row_container:
                        csv_writer.writerow(row_container)

    def excel_2_pg(self, file_name, mode='append'):
        df = self.spark.read.csv(file_name, header=True, inferSchema=False)
        start = time.time()
        print('starting 写入{0}.......................................'.format(file_name))
        df.write.jdbc(mode=mode, url=self.db_url, table=self.target_table, properties={"driver": self.driver})
        print("cost {0} seconds".format(time.time()-start))
        print('end 写入{0}............................................'.format(file_name))

if __name__ =="__main__":
    # obj = spark_etl_2_pg()
    # spark_conf = SparkConf()
    # spark_conf.set('driver-memory', '3g')
    # spark_conf.set('executor-memory', '8g')
    # spark_conf.set("spark.sql.shuffle.partitions", '1')
    conn = {'DB_HOST':'192.168.16.236', 'DB_NAME':'tiku', 'DB_PORT':'5432', 'DB_USER':'tiku', 'DB_PASSWORD':'gv7ULMNLGVuBIUn0'}
    # spark = SparkSession.builder.appName("Pyspark-excel").master('local[*]').config(conf=spark_conf)\
    #     .config("spark.driver.extraClassPath", dirname+"\postgresql-42.1.1.jar").getOrCreate()
    db_url = "jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?user={DB_USER}&password={DB_PASSWORD}".format(**conn)
    print("获取到数据连接信息如下所示:\n"+db_url)
    pg_driver = 'org.postgresql.Driver'
    dir_name = r"""C:\Users\xhw\Desktop\temp"""
    obj = spark_etl_2_pg(dir_name=dir_name, target_table='tiku.test', driver=pg_driver,db_url=db_url)
    # obj.excel_2_csv()
    # file_name = r"""D:\gongsi_project\菲儿雪\warehouse\linezone_fresh_wh_1\Hadoop\choice.xls"""
    # excel_file = pd.read_excel(file_name, sheet_name=None, header=0, dtype='str')
    # # 我们可以dbtable参数是传入一个SQL将查询出来的结果作为表返回
    # # .option("dbtable", "(select token, fequence from idf_feq where token in (" + token + ")) as tmp")
    # # sql_server_url = """jdbc:sqlserver://{ip}:{port};DatabaseName={DatabaseName};username={user};password={password}""".format(**conn)
    # for sheet_name, df_pd in excel_file.items():
    #     print(f'工作簿的名字为：{sheet_name}')
    #     if (sheet_name != '选择题'):
    #         continue
    #     # 不要第一列
    #     # df_pd.drop(df_pd.columns[0], axis=1, inplace=True)
    #     # 替换null值，否则转spark报错：Can not merge type <class 'pyspark.sql.types.DoubleType'> and <class 'pyspar
    #     df_pd = df_pd.replace(np.NaN, '')
    #     df = spark.createDataFrame(df_pd)#,schema=mySchema
    #     pg_table = """tiku.choice"""
    #     print("开始写入数据....")
    #     ticks_first = time.time()
    #     df.write.jdbc(mode="overwrite", url=db_url, table=pg_table, properties={"driver": pg_driver})
    #     ticks_last = time.time()
    #     print(f"已写入表：{sheet_name}"+"用时"+str(ticks_last-ticks_first)+'s')
