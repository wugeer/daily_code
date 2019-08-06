"""
-- ================================================
-- Project: 菲儿雪
-- Description: 拉链表
-- Target table: {p_edw_schema}.dw_surrogate_key
-- Source table:
-- Path:
-- Remark:没有考虑门店换编码的情况，仅将新的门店商品编码插入
-- Version: v1.0 create
-- Author: xiehongwang
-- Create date: 2019-08-06
-- ================================================
"""
import os
import sys
import datetime
sys.path.append("..")
from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    lz_spark = SparkInit(file_name)
    sql = """
    select 
        b.eaa001 code,
        java_method('java.util.UUID', 'randomUUID') uuid
    from fresh_ods.ods_rmseaa b 
    left join fresh_edw.dw_surrogate_key c 
        on b.eaa001=c.code
    where c.code is null and b.eaa001 not like '%&%'
    union all 
    select 
        b.dea001 code,
        java_method('java.util.UUID', 'randomUUID') uuid
    from fresh_ods.ods_tpadea b 
    left join fresh_edw.dw_surrogate_key c 
        on b.dea001=c.code
    where c.code is null and b.dea001 not like '%&%'
    """
    lz_spark.create_temp_table(sql, "tmp_dw_surrogate_key")
    sql = """ 
    insert into table fresh_edw.dw_surrogate_key
    select 
        a.code, 
        a.uuid, 
        cast(current_timestamp as string) etl_time
    from tmp_dw_surrogate_key a
    """
    lz_spark.execute_sql(sql)
    lz_spark.drop_temp_table("tmp_dw_surrogate_key")
    print("Success....{0}".format(datetime.datetime.now().strftime('%Y.%m.%d-%H:%M:%S')))