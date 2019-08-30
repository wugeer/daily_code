"""
配置类：含pg，sql server, oracle等驱动和连接信息
"""
import time 
import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf

class pg_config(object):
    """
    pg连接设置，主要是驱动类,url串
    """
    PG_DRIVER = 'org.postgresql.Driver'
    PG_CONN = {'DB_HOST':'192.168.16.236', 'DB_NAME':'tiku', 'DB_PORT':'5432', 'DB_USER':'tiku', 'DB_PASSWORD':'gv7ULMNLGVuBIUn0'}
    def __init__(self,):
        pass
    @staticmethod
    def get_pg_conn():
        return "jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?user={DB_USER}&password={DB_PASSWORD}".format(**pg_config.PG_CONN)

class sqlserver_config(object):
    """
    SQLserver连接设置，主要是驱动类和url串
    """
    SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    SQLSERVER_CONN = {'DB_HOST':'218.65.45.87', 'DB_NAME':'FEX', 'DB_PORT':'5320', 'DB_USER':'sa', 'DB_PASSWORD':'lanzhong2019'}
    def __init__(self,):
        pass
    @staticmethod
    def get_sqlserver_conn():
        return """jdbc:sqlserver://{DB_HOST}:{DB_PORT};DatabaseName={DB_NAME};username={DB_USER};password={DB_PASSWORD}""".format(**sqlserver_config.SQLSERVER_CONN)

class oracle_config(object):
    """
    oracle 连接设置，主要是驱动类和连接参数，由于没找到含用户名和密码的整个url串，所以就不设置函数直接返回完整版的url串，而是用连接参数分别指定相应的参数
    """
    ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver"
    ORACLE_CONN = {'DB_HOST':'218.65.45.87', 'DB_NAME':'FEX', 'DB_PORT':'5320', 'DB_USER':'sa', 'DB_PASSWORD':'lanzhong2019'}

class hana_config(object):
    """
    hana 连接设置，主要是驱动类和连接参数，由于没找到含用户名和密码的整个url串，所以就不设置函数直接返回完整版的url串，而是用连接参数分别指定相应的参数
    """
    HANA_DRIVER = "com.sap.db.jdbc.Driver"   
    HANA_CONN = {'DB_HOST':'要连接的hana库ip', 'DB_NAME':'数据库名', 'DB_PORT':'hana端口', 'DB_USER':'用户名', 'DB_PASSWORD':'密码'}

class mysql_config(object):
    """
    mysql 连接设置，主要是驱动类和连接参数，由于没找到含用户名和密码的整个url串，所以就不设置函数直接返回完整版的url串，而是用连接参数分别指定相应的参数
    """
    MYSQL_DRIVER = "com.mysql.jdbc.Driver"
    MYSQL_CONN = {'DB_HOST':'要连接的MySQL库ip', 'DB_NAME':'此处可忽略', 'DB_PORT':'MySQL端口', 'DB_USER':'用户名', 'DB_PASSWORD':'密码'}


class excel_config(object):
    """
    这个是Excel配置类，主要是从哪一行哪一列开始，时间列的列号，跳过的sheet页名字等
    start_row 0代表第一行
    start_column 0代表第一列   
    time_col 列表形式，元素为行号，例如：[1,2]代表第2,3列是时间类型需要特殊处理
    skip_sheet 列表形式，元素为sheet页的名称
    """
    START_ROW=0
    START_COLUMN=1
    TIME_COLUMN=[1]
    SKIP_SHEET = []

class csv_config(object):
    """
    header=True, inferSchema=False
    """
    HEADER=True
    INFERSCHEMA=False
    DELIMITER=','

def get_cost_time_log(level):
    """
    打印函数执行时间的装饰器
    """
    def decorator(func):
	    def wrapper(*args, **kwargs):
		    start = time.time()
		    print('[{log_level}]: function {func_name} start at  {local_time}'.format(log_level=level,local_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),\
                func_name=func.__name__))
		    func(*args, **kwargs)
		    local_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		    print('[{log_level}]: function {func_name} end at  {local_time} cost {cost_time} seconds!'.format(log_level=level,local_time=local_time,\
                func_name=func.__name__, cost_time=time.time()-start))
	    return wrapper
    return decorator

class SparkInit(object):
    """
    spark配置和spark实例
    """
    def __init__(self,):
        # app的名称
        app_name = "PySpark-etl"
        # config 配置
        spark_conf = SparkConf()
        # 允许操作分区表
        spark_conf.set("hive.exec.dynamic.partition.mode", 'nonstrict')
        # 目的是解决报错：Detected cartesian product for INNER join between logical plans
        spark_conf.set("spark.sql.crossJoin.enabled", 'true')
        # 目的是解决小文件过多问题
        spark_conf.set("spark.sql.shuffle.partitions", '1')
        spark_conf.set('driver-memory', '1g')
        spark_conf.set('executor-memory', '3g')
        spark_conf.set("spark.sql.shuffle.partitions", '10')
        self.spark = SparkSession \
            .builder.appName(app_name) \
            .master('local[*]') \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()

    def get_spark(self,):
        return self.spark


    