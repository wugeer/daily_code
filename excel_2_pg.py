"""
author:xiehongwang
description:提供一个文件夹路径，将该文件夹下的所有Excel先转为CSV文件，并将CSV通过spark导入hive中
last_modified:2019-08-04 
version:1.0 
improtion:遍历文件夹，传入要写入的hive表名
"""
import os,sys
import xlrd
import xlwt
import shutil
import csv
import datetime
import glob
import codecs
import re
from config import *
xlrd.Book.encoding = "utf-8"
# dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
# PG_DRIVER = 'org.postgresql.Driver'

import pandas as pd
import numpy as np

class file_process(object):
    """
    将Excel中的每一个sheet转为csv文件，并将csv文件弄到专门的文件夹下
    """
    def __init__(self, dir_name):
        """
        遍历该文件夹，考虑文件夹下子文件夹
        入参:要导入数据所在的文件夹
        """
        self.csv_dir = os.path.join(dir_name, 'csv')
        # self.dir_name = dir_name
        # 如果不存在csv目录则创建
        # print(1000000000000)
        print(self.csv_dir)
        os.makedirs(self.csv_dir, exist_ok=True)
        self.my_run(dir_name)
    @get_cost_time_log(level='info')
    def my_run(self, dir_name):
        """
        遍历文件夹下所有文件，如果是Excel则转为csv，如果是csv直接移动到指定的文件夹
        """
        for entry in os.scandir(dir_name):
            if entry.is_dir() and entry.name!='csv':
                self.my_run(os.path.join(dir_name, entry.name))
            if entry.name.endswith('.xlsx') or entry.name.endswith('.xls'):
                self.excel_2_csv(file_name=os.path.join(dir_name, entry.name))
            elif entry.name.endswith('.csv'):
                # 移动CSV文件到指定目录
                shutil.move(os.path.join(dir_name, entry.name), os.path.join(self.csv_dir, entry.name))
    def excel_2_csv(self, file_name, start_row=excel_config.START_ROW, start_column=excel_config.START_COLUMN, time_col=excel_config.TIME_COLUMN, skip_sheet=excel_config.SKIP_SHEET):
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
        # 遍历所有sheet页
        for worksheet_name in all_worksheets:
            # 如果sheet页是跳过的sheet页，则继续循环
            if worksheet_name in skip_sheet:
                continue
            # print(worksheet_name)
            worksheet = workbook.sheet_by_name(worksheet_name)
            with open(os.path.join(self.csv_dir,os.path.splitext(file_name)[0]+'_'+worksheet_name+'.csv'), 'w', encoding='utf-8' ,newline='') as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=csv_config.DELIMITER)
                # 遍历sheet页中每一个元素，从指定行列开始，并对时间列就行处理
                for row in range(start_row, worksheet.nrows):
                    row_container=[]
                    for col in range(start_column, worksheet.ncols):
                        entry = worksheet.cell(row,col).value
                        # print(entry)
                        # if col>start_column:
                        if row==0 or col not in time_col:
                            # 剔除每一个元素中多余的空格，主要是有些元素中有换行符等，导致生成的csv有问题
                            row_container.append(str(re.sub(r'\s+','', str(entry))))
                        else:
                            # 处理时间类型的列
                            row_container.append(datetime.datetime(*xlrd.xldate_as_tuple(entry, workbook.datemode)))
                    if row_container:
                        # 跳过空行
                        csv_writer.writerow(row_container)


class csv_or_excel_2_pg(file_process):
    """ 
    遍历工程下的所有文件，不含文件夹，将Excel文件转为CSV，并将CSV文件导入hive中
    导入数据库中的同一个表
    """
    def __init__(self, dir_name, target_table):
        super(csv_or_excel_2_pg, self).__init__(dir_name)
        # super().run(dir_name)
        self.spark = SparkInit().get_spark()
        # self.dir_name = dir_name
        self.target_table = target_table
        self.run()

    @get_cost_time_log(level='info')
    def run(self,):
        for file_name in os.scandir(self.csv_dir):
            # 对这个文件夹下所有的CSV导入库中
            self.csv_2_pg(file_name=os.path.join(self.csv_dir, file_name.name),target_table=self.target_table)

    @get_cost_time_log(level='info')
    def csv_2_pg(self, file_name, target_table, mode='append', header=csv_config.HEADER, inferSchema=csv_config.INFERSCHEMA):
        """
        默认是有标题头的,csv导入pg
        """
        # df = self.spark.read.csv(file_name, header=header, inferSchema=inferSchema,delimiter='\t')
        df = self.spark.read.format("csv").option("header", header) \
                                    .option("inferSchema", inferSchema) \
                                    .option("delimiter", csv_config.DELIMITER) \
                                    .load(file_name)
        # print(df.show())
        db_url = pg_config.get_pg_conn()
        # print(target_table)
        df.write.jdbc(mode=mode, url=db_url, table=target_table, properties={"driver": pg_config.PG_DRIVER})

class csv_or_excel_2_hive(file_process):
    """ 
    遍历工程下的所有文件，不含文件夹，将Excel文件转为CSV，并将CSV文件导入hive中
    导入数据库中的同一个表
    """
    def __init__(self, dir_name, target_table):
        super(csv_or_excel_2_hive, self).__init__(dir_name)
        super().run(dir_name)
        self.spark = SparkInit().get_spark()
        # self.dir_name = dir_name
        self.target_table = target_table
        self.run()
    def run(self,):
        for file_name in os.scandir(self.csv_dir):
            # 对这个文件夹下所有的CSV导入库中
            self.csv_2_hive(file_name=os.path.join(self.csv_dir, file_name.name), target_table=self.target_table)

    def csv_2_hive(self, file_name, target_table, mode='append', header=csv_config.HEADER, inferSchema=csv_config.INFERSCHEMA):
        """
        默认是有标题头的,csv中应该是有字段名，如果没有则应该替换
        """
        # df = self.spark.read.csv(file_name, header=header, inferSchema=inferSchema)
        df = self.spark.read.format("csv").option("header", header) \
                                    .option("inferSchema", inferSchema) \
                                    .option("delimiter", csv_config.DELIMITER) \
                                    .load(file_name)
        df.write.saveAsTable(target_table, mode = mode)


class other_2_hive(object):
    def __init__(self, ):
        self.spark = SparkInit().get_spark()
    @get_cost_time_log(level='info')
    def pg_write_hive(self, query, target_table, mode='append'):
        """
        source_table 可以带schema表名，也可以是一段SQL得到的临时表比如说仅选择某一天的数据
        例如：ods.ods_sale,select * from ods.ods_sale where sale_date='2019-01-01'
        target_table 带schema的表名
        mode 写到hive中表的方式，默认是追加
        """
        db_url = pg_config.get_pg_conn()
        df = self.spark.read.format("jdbc").options(url=db_url, dbtable=query, properties={"driver": pg_config.PG_DRIVER}).load()
        # print(df.show())
        df.write.saveAsTable(target_table, mode = mode)
    
    @get_cost_time_log(level='info')
    def sqlserver_write_hive(self, query, target_table, mode='append'):
        """
        source_table 可以带schema表名，也可以是一段SQL得到的临时表比如说仅选择某一天的数据
        例如：ods.ods_sale,select * from ods.ods_sale where sale_date='2019-01-01'
        target_table 带schema的表名
        mode 写到hive中表的方式，默认是追加
        """
        # source_table = "(SELECT 题目, 答案, 选项 FROM fds.ds) tmp"
        db_url = sqlserver_config.get_sqlserver_conn()
        df = self.spark.read.format("jdbc").options(url=db_url, dbtable=query, properties={"driver": sqlserver_config.SQLSERVER_DRIVER}).load()
        # print(df.show())
        df.write.saveAsTable(target_table, mode = mode)
    
    @get_cost_time_log(level='info')
    def oracle_write_hive(self, query, target_table, mode='append'):
        """
        未测试
        source_table 可以带schema表名，也可以是一段SQL得到的临时表比如说仅选择某一天的数据
        例如：ods.ods_sale,select * from ods.ods_sale where sale_date='2019-01-01'
        target_table 带schema的表名
        mode 写到hive中表的方式，默认是追加
        """
        db_con = oracle_config.ORACLE_CONN
        df = self.spark.read.format("jdbc")\
            .option("url", """jdbc:oracle:thin:@//{DB_HOST}:{DB_PORT}/{DB_NAME}""".format(db_con['DB_HOST'],db_con['DB_PORT'], db_con['DB_NAME']))\
            .option("dbtable", query)\
            .option("user", db_con['DB_USER'])\
            .option("password", db_con['DB_PASSWORD']) \
            .option("driver", oracle_config.ORACLE_DRIVER)\
            .load()
        # print(df.show())
        df.write.saveAsTable(target_table, mode = mode)

    @get_cost_time_log(level='info')
    def hana_write_hive(self, query, target_table, mode='append'):
        """
        未测试
        source_table 可以带schema表名，也可以是一段SQL得到的临时表比如说仅选择某一天的数据
        例如：ods.ods_sale,select * from ods.ods_sale where sale_date='2019-01-01'
        target_table 带schema的表名
        mode 写到hive中表的方式，默认是追加
        """
        db_con = hana_config.HANA_CONN
        df= self.spark.read.jdbc(url="jdbc:sap:{DB_HOST}:{DB_PORT}/{DB_NAME}".format(db_con['DB_HOST'],db_con['DB_PORT'], db_con['DB_NAME']),\
                dbtable=query, properties={"user": db_con['DB_USER'], "password": db_con['DB_PASSWORD']})
        # print(df.show())
        df.write.saveAsTable(target_table, mode = mode)
    
    @get_cost_time_log(level='info')
    def mysql_write_hive(self, query, target_table, mode='append'):
        """
        未测试
        source_table 可以带schema表名，也可以是一段SQL得到的临时表比如说仅选择某一天的数据
        例如：ods.ods_sale,select * from ods.ods_sale where sale_date='2019-01-01'
        target_table 带schema的表名
        mode 写到hive中表的方式，默认是追加
        例子：
        .option("driver", "com.mysql.jdbc.Driver")
        .option("url", "jdbc:mysql://ip:3306")
        .option("dbtable", "db.user_test")
        .option("user", "test")
        .option("password", "123456")
        .option("fetchsize", "3")
        .load()
        """
        db_con = mysql_config.MYSQL_CONN
        df= self.spark.read.spark.read.format("jdbc")\
                .option("driver", mysql_config.MYSQL_DRIVER)\
                .option("url", "jdbc:mysql://{DB_HOST}:{DB_PORT}".format(db_con['DB_HOST'],db_con['DB_PORT']))\
                .option("dbtable", query)\
                .option("user", db_con['DB_USER'])\
                .option("password", db_con['DB_PASSWORD'])\
                .load()
        # print(df.show())
        df.write.saveAsTable(target_table, mode = mode)

class hive_2_other(object):
    def __init__(self, ):
        self.spark = SparkInit().get_spark()
    
    @get_cost_time_log(level='info')
    def hive_write_pg(self, query, target_table, mode='append'):
        """
        query 基于spark SQL获取临时表，简单的比如说查询某个表某天或者全部数据，或者复杂点的，多个表进行关联得到的临时表
        例如：select * from ods.ods_sale where sale_date='2019-01-01' 或者 select * from ods.ods_sale
        target_table 要插入的带schema的表名
        mode 写到pg中表的方式，默认是追加
        """
        df = self.spark.sql(query)
        # print(df.show())
        db_conn = pg_config.PG_CONN
        target_url = "jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}".format(DB_HOST=db_conn['DB_HOST'], DB_PORT=db_conn['DB_PORT'], DB_NAME=db_conn['DB_NAME'])
        properties = {
            "user": db_conn['DB_USER'],
            "password": db_conn['DB_PASSWORD'],
            "driver": pg_config.PG_DRIVER
        }
        df.write.jdbc(url=target_url, table=target_table, mode=mode, properties=properties)

    @get_cost_time_log(level='info')
    def hive_write_sqlserver(self, query, target_table, mode='append'):
        """
        query 基于spark SQL获取临时表，简单的比如说查询某个表某天或者全部数据，或者复杂点的，多个表进行关联得到的临时表
        例如：select * from ods.ods_sale where sale_date='2019-01-01' 或者 select * from ods.ods_sale
        target_table 要插入的带schema的表名
        mode 写到sqlserver中表的方式，默认是追加
        """
        df = self.spark.sql(query)
        # print(df.show())
        db_conn = sqlserver_config.SQLSERVER_CONN
        target_url = """jdbc:sqlserver://{DB_HOST}:{DB_PORT};DatabaseName={DB_NAME};username={DB_USER};password={DB_PASSWORD}""".format(**db_conn)
        properties = {
            "driver": sqlserver_config.SQLSERVER_DRIVER
        }
        df.write.jdbc(url=target_url, table=target_table, mode=mode, properties=properties)
    
    @get_cost_time_log(level='info')
    def hive_write_oracle(self, query, target_table, mode='append'):
        """
        未测试
        query 基于spark SQL获取临时表，简单的比如说查询某个表某天或者全部数据，或者复杂点的，多个表进行关联得到的临时表
        例如：select * from ods.ods_sale where sale_date='2019-01-01' 或者 select * from ods.ods_sale
        target_table 要插入的带schema的表名
        mode 写到oracle中表的方式，默认是追加
        """
        df = self.spark.sql(query)
        # print(df.show())
        db_conn = oracle_config.ORACLE_CONN
        target_url = """jdbc:oracle:thin:@//{DB_HOST}:{DB_PORT}/{DB_NAME}""".format(db_conn['DB_HOST'],db_conn['DB_PORT'], db_conn['DB_NAME'])
        properties = {
            "user": db_conn['DB_USER'],
            "password": db_conn['DB_PASSWORD'],
            "driver": oracle_config.ORACLE_DRIVER
        }
        df.write.jdbc(url=target_url, table=target_table, mode=mode, properties=properties)

    @get_cost_time_log(level='info')
    def hive_write_hana(self, query, target_table, mode='append'):
        """
        未测试
        query 基于spark SQL获取临时表，简单的比如说查询某个表某天或者全部数据，或者复杂点的，多个表进行关联得到的临时表
        例如：select * from ods.ods_sale where sale_date='2019-01-01' 或者 select * from ods.ods_sale
        target_table 要插入的带schema的表名
        mode 写到hana中表的方式，默认是追加
        """
        df = self.spark.sql(query)
        # print(df.show())
        db_conn = hana_config.HANA_CONN
        target_url = "jdbc:sap:{DB_HOST}:{DB_PORT}/{DB_NAME}".format(DB_HOST=db_conn['DB_HOST'],DB_PORT=db_conn['DB_PORT'],DB_NAME=db_conn['DB_NAME'])
        properties = {
            "user": db_conn['DB_USER'],
            "password": db_conn['DB_PASSWORD'],
            "driver": hana_config.HANA_DRIVER
        }
        df.write.jdbc(url=target_url, table=target_table, mode=mode, properties=properties)

    @get_cost_time_log(level='info')
    def mysql_write_hive(self, query, target_table, mode='append'):
        """
        未测试
        query 基于spark SQL获取临时表，简单的比如说查询某个表某天或者全部数据，或者复杂点的，多个表进行关联得到的临时表
        例如：select * from ods.ods_sale where sale_date='2019-01-01' 或者 select * from ods.ods_sale
        target_table 要插入的带schema的表名
        mode 写到mysql中表的方式，默认是追加
        """
        df = self.spark.sql(query)
        # print(df.show())
        db_conn =  mysql_config.MYSQL_CONN
        target_url = "jdbc:mysql://{DB_HOST}:{DB_PORT}".format(db_conn['DB_HOST'],db_conn['DB_PORT'])
        properties = {
            "user": db_conn['DB_USER'],
            "password": db_conn['DB_PASSWORD'],
            "driver": mysql_config.MYSQL_DRIVER
        }
        df.write.jdbc(url=target_url, table=target_table, mode=mode, properties=properties)
    
if __name__ =="__main__":
    dir_name = r"""C:\Users\xhw\Desktop"""
    obj = csv_or_excel_2_pg(dir_name, 'tiku.xhw')
    # obj.csv_2_pg(file_name,'tiku.xhw')
    # obj.pg_write_hive('ds', 'ds')
    # dir_name = r"""C:\Users\xhw\Desktop\temp"""
    # obj = csv_or_excel_2_pg(dir_name, 'tiku.test')
    # obj = csv_or_excel_2_pg(dir_name, target_table='tiku.test')
    # obj = other_and_hive()
    # obj.sqlserver_write_hive('dbo.RMSEIB', 'dashk')
    # obj = spark_etl_2_pg()
    # spark_conf = SparkConf()
    # spark_conf.set('driver-memory', '3g')
    # spark_conf.set('executor-memory', '8g')
    # spark_conf.set("spark.sql.shuffle.partitions", '1')
    # conn = {'DB_HOST':'192.168.16.236', 'DB_NAME':'tiku', 'DB_PORT':'5432', 'DB_USER':'tiku', 'DB_PASSWORD':'gv7ULMNLGVuBIUn0'}
    # spark = SparkSession.builder.appName("Pyspark-excel").master('local[*]').config(conf=spark_conf)\
    #     .config("spark.driver.extraClassPath",os.path.join('D:\code', "postgresql-42.1.1.jar")).getOrCreate()
    # db_url = "jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?user={DB_USER}&password={DB_PASSWORD}".format(**conn)
    # # print("获取到数据连接信息如下所示:\n"+db_url)
    # pg_driver = 'org.postgresql.Driver'
    # # dir_name = r"""C:\Users\xhw\Desktop\temp"""
    # # obj = csv_or_excel_2_pg(dir_name=dir_name, target_table='tiku.test', driver=pg_driver,db_url=db_url)
    # # obj.excel_2_csv()
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
