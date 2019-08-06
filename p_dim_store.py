"""
-- ================================================
-- Project: 菲儿雪
-- Description: 门店维度表
-- Target table: {p_edw_schema}.dim_store
-- Source table:
-- Path:
-- Remark:
-- Version: v1.0 create
-- Author: xiehongwang
-- Create date: 2019-07-19
-- ================================================
"""
import os
import sys
sys.path.append("..")
from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    lz_spark = SparkInit(file_name)

    # 生成维表
    # sql = """ 
    # select a.eaa001 shop_code
    #     , a.eaa002 shop_name
    #     , a.eaa014 business_type
    #     , '江西省' province
    #     , '赣州市' city
    #     , '南康区' city_area
    #     --, case when a.eaa047=0 then '江西省' else b.xcb002 end province
    #     --, case when a.eaa048=0 then '赣州市' else c.xcc003 end city
    #     --, case when a.eaa049=0 then '南康区' else d.xcd003 end city_area
    #     , a.eaa005 address
    #     , a.eaa007 manager_name
    #     , a.eaa041 phone_number
    #     , a.eaa019 is_valid
    # from fresh_ods.ods_rmseaa a
    # --left join fresh_ods.ods_tpaxcb b 
    # --	on a.eaa047=b.xcb001
    # --left join fresh_ods.ods_tpaxcc c 
    # --	on a.eaa048=c.xcc001
    # --left join fresh_ods.ods_tpaxcd d
    # --	on a.eaa049=d.xcd001
    # where a.eaa001 not like '%&%'
    # """
    # lz_spark.create_temp_table(sql, "tmp_ods_store")
    # sql = "drop table if exists fresh_edw.dim_store"
    # lz_spark.execute_sql(sql)
    # # 插入数据
    # sql = """
    # create table fresh_edw.dim_store as 
    # select shop_code
    #     , shop_name
    #     , business_type
    #     , province
    #     , city
    #     , city_area
    #     , address
    #     , manager_name
    #     , phone_number
    #     , is_valid
    # from tmp_ods_store
    # """
    
    # 生成维表
    sql = """ 
    select 
        b.uuid store_sk
        , a.eaa001 store_code
        , a.eaa002 store_name
        , a.eaa014 business_type
        , null business_district
        , null store_level
        , floor(rand()*999999+1) year_rent
        , floor(rand()*999+1) area
        , case when a.eaa047=0 then '江西省' else c.xcb002 end province
        , case when a.eaa048=0 then '赣州市' else d.xcc003 end city
        , case when a.eaa049=0 then '南康区' else e.xcd003 end district
        , a.eaa005 address
        , a.eaa007 manager_name
        , null supply_pattern
        , null supply_lead_time
        , null fixed_days_supply
        , null cycle_supply_pattern_code
        , null opening_date
        , a.eaa041 phone_number
        , '9999-12-31' stop_order_date
        , case when a.eaa019='T' then 0 else 1 end is_valid
    from {p_ods_schema}.ods_rmseaa a 
    inner join  {p_edw_schema}.dw_surrogate_key b on a.eaa001=b.code
    left join  {p_ods_schema}.ods_tpaxcb c on a.eaa047=c.xcb001
    left join  {p_ods_schema}.ods_tpaxcc d on a.eaa048=d.xcc001
    left join  {p_ods_schema}.ods_tpaxcd e on a.eaa049=e.xcd001
    """
    lz_spark.create_temp_table(sql, "tmp_ods_store")

    # ods与edw开链比较，两边都有且开闭链字段相同，不开不闭
    sql = """
    select
        a.store_sk,
        a.store_code,
        a.store_name,
        a.business_type,
        a.business_district,
        a.store_level,
        a.year_rent,
        a.area,
        a.province,
        a.city,
        a.district,
        a.address,
        a.manager_name,
        a.supply_pattern,
        a.supply_lead_time,
        a.fixed_days_supply,
        a.cycle_supply_pattern_code,
        a.opening_date,
        a.phone_number,
        a.stop_order_date,
        a.is_valid,
        b.etl_begin,
        b.etl_end,
        cast(current_timestamp as string) etl_time
    from tmp_ods_store a
    inner join {p_edw_schema}.dim_store b
        on a.store_sk=b.store_sk 
        and b.etl_end='9999-12-31'
    where a.store_code=b.store_code 
        and a.stop_order_date=b.stop_order_date 
        and a.is_valid=b.is_valid
    """
    lz_spark.create_temp_table(sql, "tmp_update_chain")

    # edw开链与ods比较，edw有ods无或开闭链字段不同，闭链
    sql = """
    select
        c.store_sk,
        c.store_code,
        c.store_name,
        c.business_type,
        c.business_district,
        c.store_level,
        c.year_rent,
        c.area,
        c.province,
        c.city,
        c.district,
        c.address,
        c.manager_name,
        c.supply_pattern,
        c.supply_lead_time,
        c.fixed_days_supply,
        c.cycle_supply_pattern_code,
        c.opening_date,
        c.phone_number,
        c.stop_order_date,
        c.is_valid,
        c.etl_begin,
        '{p_input_date}' etl_end,
        cast(current_timestamp as string) etl_time
    from {p_edw_schema}.dim_store c
    left join tmp_ods_store d
        on c.store_sk=d.store_sk
    where c.etl_end='9999-12-31'
        and (c.store_code<>d.store_code
        or c.stop_order_date<>d.stop_order_date
        or c.is_valid<>d.is_valid
        or d.store_sk is null)
    """
    lz_spark.create_temp_table(sql, "tmp_close_chain")

    # ods与edw开链比较，ods有edw无或开闭链字段不同，开链
    sql = """
    select
        e.store_sk, 
        e.store_code, 
        e.store_name, 
        e.business_type, 
        e.business_district, 
        e.store_level, 
        e.year_rent,
        e.area, 
        e.province, 
        e.city, 
        e.district, 
        e.address, 
        e.manager_name, 
        e.supply_pattern,
        e.supply_lead_time, 
        e.fixed_days_supply, 
        e.cycle_supply_pattern_code, 
        e.opening_date, 
        e.phone_number, 
        e.stop_order_date, 
        e.is_valid, 
        '{p_input_date}' etl_begin,
        '9999-12-31' etl_end, 
        cast(current_timestamp as string) etl_time
    from tmp_ods_store e
    left join {p_edw_schema}.dim_store f 
        on e.store_sk=f.store_sk
        and f.etl_end='9999-12-31'
    where (e.store_code<>f.store_code 
        or e.stop_order_date<>f.stop_order_date
        or e.is_valid<>f.is_valid
        or f.store_sk is null)
    """
    lz_spark.create_temp_table(sql, "tmp_open_chain")

    # edw已闭链
    sql = """
    select
        store_sk,
        store_code,
        store_name,
        business_type,
        business_district,
        store_level,
        year_rent,
        area,
        province,
        city,
        district,
        address,
        manager_name,
        supply_pattern,
        supply_lead_time,
        fixed_days_supply,
        cycle_supply_pattern_code,
        opening_date,
        phone_number,
        stop_order_date,
        is_valid,
        etl_begin,
        etl_end,
        etl_time
    from {p_edw_schema}.dim_store
    where etl_end<>'9999-12-31'
    """
    lz_spark.create_temp_table(sql, "tmp_closed_chain")
    lz_spark.drop_temp_table("tmp_ods_store")

    # 更新门店表
    sql = """
    insert overwrite table {p_edw_schema}.dim_store
    select 
        store_sk, store_code, store_name, business_type, business_district, store_level, 
        year_rent, area, province, city, district, address, manager_name, supply_pattern, 
        supply_lead_time, fixed_days_supply, cycle_supply_pattern_code, opening_date, 
        phone_number, stop_order_date, is_valid, etl_begin, etl_end, etl_time
    from tmp_update_chain
    union all 
    select 
        store_sk, store_code, store_name, business_type, business_district, store_level, 
        year_rent, area, province, city, district, address, manager_name, supply_pattern, 
        supply_lead_time, fixed_days_supply, cycle_supply_pattern_code, opening_date, 
        phone_number, stop_order_date, is_valid, etl_begin, etl_end, etl_time
    from tmp_close_chain
    union all 
    select 
        store_sk, store_code, store_name, business_type, business_district, store_level, 
        year_rent, area, province, city, district, address, manager_name, supply_pattern, 
        supply_lead_time, fixed_days_supply, cycle_supply_pattern_code, opening_date, 
        phone_number, stop_order_date, is_valid, etl_begin, etl_end, etl_time
    from tmp_open_chain
    union all 
    select 
        store_sk, store_code, store_name, business_type, business_district, store_level, 
        year_rent, area, province, city, district, address, manager_name, supply_pattern, 
        supply_lead_time, fixed_days_supply, cycle_supply_pattern_code, opening_date, 
        phone_number, stop_order_date, is_valid, etl_begin, etl_end, etl_time
    from tmp_closed_chain
    """
    lz_spark.execute_sql(sql)
    lz_spark.drop_temp_table("tmp_update_chain")
    lz_spark.drop_temp_table("tmp_close_chain")
    lz_spark.drop_temp_table("tmp_open_chain")
    lz_spark.drop_temp_table("tmp_closed_chain")
    print("Success")

