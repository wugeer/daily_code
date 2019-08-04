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
import shutil
from pyspark.sql import SparkSession
dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
spark_conf = SparkConf()
spark_conf.set('driver-memory', 2g)
spark_conf.set('executor-memory', 4g)
spark_conf.set("spark.sql.shuffle.partitions", '1')
spark = SparkSession.builder.appName("Pyspark-excel").master('local[*]').config(conf=spark_conf).getOrCreate()

# 使用 spark-Excel D:\gongsi_project\菲儿雪\20190716\览众-菲尔雪20190716\RMSEAA-UTF8.csv "/user/hive/warehouse/"
# spark_conf.set("spark.jars",os.path.join(dirname,'spark-excel_2.11-0.8.3.jar'))
# dirname = "/user/hive/warehouse/"
class spark_etl(object):
    """ 
    遍历工程下的所有文件，不含文件夹，将Excel文件转为CSV，并将CSV文件导入hive中
    """
    def __init__(self, dir_name):
        self.dir_name = dir_name
        self.csv_dir = os.path.join(self.dir_name, 'csv')
        # if os.path.exists():
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)
        self.run()
    def run(self,):
        for file_name in os.listdir(self.dir_name):
            if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                self.excel_2_csv(os.path.join(self.dir_name, file_name))
            elif file_name.endswith('.csv'):
                # 移动CSV文件到指定目录
                # shutil.move(os.path.join(self.dir_name, file_name), os.path.join(self.csv_dir, file_name))
                self.csv_2_hive(file_name)
        for file_name in os.listdir(self.csv_dir):
            self.csv_2_hive(file_name)
    def excel_2_csv(self, file_name):
        workbook = xlrd.open_workbook(file_name)
        all_worksheets = workbook.sheet_names()
        dir_name,file_name = os.path.split(file_name)
        for worksheet_name in all_worksheets:
                worksheet = workbook.sheet_by_name(worksheet_name)
                with open(os.path.join(self.csv_dir,worksheet_name+'.csv'), 'w', encoding='utf-8' ,newline='') as your_csv_file:
                    csv_writer = csv.writer(your_csv_file)
                    for rownum in range(worksheet.nrows):
                            row_container = [entry for entry in worksheet.row_values(rownum)]
                            if row_container:
                            csv_writer.writerow(row_container)
    def csv_2_hive(self, file_name):
        df = spark.read.csv(file_name, header=True, inferSchema=False)
        target_table = 'fresh_ods.ods_{0}'.format(file_name.lower())
        start = time.time()
        print('starting 写入{0}.......................................'.format(file_name))
        df.write.saveAsTable(target_table, mode ='overwrite')
        print("cost {0} seconds".format(time.time()-start))
        print('end 写入{0}............................................'.format(file_name))


# i=0
# for file in os.listdir(dirname):
#     if file.endswith('.csv'):
#         print(file)
#         file_name = file[:6]
#         f = "file:///home/fresh_etl/ods/{0}.csv".format(file_name)
#         df = spark.read.csv(f, header=True, inferSchema=False)

#         # te = """ETB000, ETB001, ETB024, ETB002, ETB003, ETB004, ETB005, ETB006, ETB007, ETB008, ETB009, ETB010, ETB011, ETB012, ETB013, ETB014, ETB016, ETB017, ETB018, ETB019, ETB020, ETB021, ETB022, ETB023, ETB025, ETB026, ETB027, ETB960, ETB961, ETB962, ETB963, ETB964, ETB965, ETB980, ETB981, ETB982, ETB983, ETB984, ETB985, ETB901, ETB902, ETB903, ETB904, ETB905, ETB028, ETB029, ETB030, ETB031, ETB032, ETB033
#         # """
#         # li = [item.strip().lower() for item in te.split(",")]

#         # for item in range(len(li)):
#         #     # d['_c{0}'.format(item)]=li[item][:6].lower()
#         #     df = df.withColumnRenamed('_c{0}'.format(item), li[item])
#         #     # df = df.withColumn(li[item].strip().lower(), df[li[item].strip().lower()].cast("string"))
#         # # # df = df.rename(columns=d)
#         # # df = df.select()
#         # print(df.dtypes)
#         target_table = 'fresh_ods.ods_{0}'.format(file_name.lower())
#         # df = spark.read.format("com.crealytics.spark.excel")\
#         #     .option('location', xlsx)\
#         #     .option("sheetname", "sheet1")\
#         #     .option("addColorColumns", "false")\
#         #     .option("useHeader","true")\
#         #     .option("treatEmptyValuesAsNulls","false")\
#         #     .option("inferSchema","false")\
#         #     .option("maxRowsInMemory", 20)\
#         #     .option("excerptSize", 10)\
#         #     .load() 
#         # df.show(1000)
#         # df.printSchema()
#         # conn = {'ip':'218.65.45.87', 'port':'5320', 'DatabaseName':'FEX', 'user':'sa', 'password':'lanzhong2019'}
#         # url = """jdbc:sqlserver://{ip}:{port};DatabaseName={DatabaseName};username={user};password={password}""".format(**conn)
#         #  接下来就是写库了
#         start = time.time()
#         print('starting 写入{0}.......................................'.format(file_name))
#         df.write.saveAsTable(target_table, mode ='overwrite')
#         print("cost {0} seconds".format(time.time()-start))
#         print('end 写入{0}............................................'.format(file_name))
#         i += 1

if __name__=="__main__":
    dir_name = project_name #这里是你要处理的项目的文件夹
    obj = spark_etl(dir_name)