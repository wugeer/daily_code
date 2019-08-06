"""
-- ================================================
-- Project: 菲儿雪
-- Description: 商品维度表
-- Target table: {p_edw_schema}.dim_product
-- Source table:
-- Path:
-- Remark:
-- Version: v1.0 create
-- Author: xiehongwang
-- Create date: 2019-08-06
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
    # 门店分类
    sql = """
    select 
        distinct dea005 small_class_code
        ,case when length(dea005)<>2 then substring(dea005,1,2) else dea005 end large_class_code
        ,case when length(dea005)=6 then substring(dea005,1,4) else dea005 end middle_class_code
    from fresh_ods.ods_tpadea
    """
    lz_spark.create_temp_table(sql, "tmp_store_class")
    
    sql = """
    select
        a.small_class_code
        , b.ded003 small_class_name
        , a.middle_class_code
        , c.ded003 middle_class_name
        , a.large_class_code
        , d.ded003 large_class_name
    from tmp_store_class a 
    left join fresh_ods.ods_tpaded b 
        on a.small_class_code=b.ded002 and b.ded009='T'
    left join fresh_ods.ods_tpaded c 
        on a.middle_class_code=c.ded002 --and c.ded009='T'
    left join fresh_ods.ods_tpaded d
        on a.large_class_code=d.ded002 and d.ded009='T'
    """
    lz_spark.create_temp_table(sql, "tmp_store_class_name")
    # 生成维表
    sql = """ 
    select a.dea001 product_code
        , a.dea002 product_name
        , b.small_class_name small_type
        , b.middle_class_name middle_type --大中小类这个还得问下客户
        , b.large_class_name large_type
        , c.eic001 moudle_code
        , a.dea003 unit
        , a.dea093 order_unit
        , case when a.dea063='T' then a.dea064 else null end expiration_date
        , a.dea090 min_order_qty
        , d.eib008 supply_pattern
        , d.eib009 fixed_days_supply
        , d.eib012 supply_lead_time
        , d.eib007 is_valid
        , a.dea024 sale_price
        , a.dea022 cost_price
    from fresh_ods.ods_tpadea a
    left join tmp_store_class_name b 
        on a.dea005=b.small_class_code
    left join fresh_ods.ods_rmseic c
        on a.dea001=c.eic003
    left join fresh_ods.ods_rmseib d 
        on c.eic001=d.eib001
    where d.eib007='T'
    """
    


    sql = """
    select 
        b.uuid product_sk
        , a.dea001 product_code
        --, f.eib004 cangku
        , a.dea002 product_name
        , c.small_class_name small_type
        , c.middle_class_name middle_type --大中小类这个还得问下客户
        , c.large_class_name large_type
        , a.dea003 unit
        , a.dea093 order_unit
        , d.base_qty unit_conversion_rate
        , case 
            when substring(a.dea005,1,2)='11'
                and (a.dea005<>'110102' and a.dea005<>'110601' and a.dea005<>'110607' and a.dea005<>'110801')
                and (substring(a.dea005,1,4)<>'1111' and substring(a.dea005,1,4)<>'1112' and substring(a.dea005,1,4)<>'1113') 
            then '001001' 
            when a.dea005='110102' or a.dea005='110601' or a.dea005='110607' then '001002' 
            when substring(a.dea005,1,2)='31' then '001003'
            when substring(a.dea005,1,2)='51' then '001004'
            else null end product_type
        ,case
            when a.dea005='110102' or a.dea005='110601' or a.dea005='110607' then 2 
            when substring(a.dea005,1,2)='11'
                and (a.dea005<>'110102' and a.dea005<>'110601' and a.dea005<>'110607' and a.dea005<>'110801')
                and (substring(a.dea005,1,4)<>'1111' and substring(a.dea005,1,4)<>'1112' and substring(a.dea005,1,4)<>'1113') 
            then 10
            when substring(a.dea005,1,2)='31' then 5 
            when substring(a.dea005,1,2)='51' then 30 
            else 2 end expiration_date
        , a.dea090 min_order_qty
        , null min_package_qty
        , null min_display_qty
        , null safety_stock
        , null supply_pattern
        , null supply_lead_time
        , null fixed_days_supply
        , null cycle_supply_pattern_code
        , null store_model_supply_time
        , null director_model_supply_time
        , null store_other_supply_time
        , null director_other_supply_time
        , '9999-12-31' stop_order_date
        , case 
            when substring(a.dea005,1,2)='11' 
                and substring(a.dea005,1,4)<>'1111'
                and substring(a.dea005,1,4)<>'1112'  
                and substring(a.dea005,1,4)<>'1113'
                and substring(a.dea005,1,4)<>'1106'
                and substring(a.dea005,1,4)<>'1108' 
                then '002001'
            when substring(a.dea005,1,2)='51' 
                and substring(a.dea005,1,4)<>'5104'
                then '002001'
            else '002002' end supply_method
--        , case when f.eib007='T' then 1 else 0 end is_valid
        , a.dea024 sale_price
        , a.dea022 cost_price 
    from fresh_ods.ods_tpadea a 
    inner join fresh_edw.dw_surrogate_key b 
        on a.dea001=b.code
    left join fresh_edw.dim_category c
        on a.dea005=c.small_class_code
    left join fresh_edw.dim_unit_convert d 
        on b.uuid=d.product_sk
        and a.dea093=d.other_unit
--    left join fresh_ods.ods_rmseic e
--        on a.dea001=e.eic003
--    left join fresh_ods.ods_rmseib f
--        on e.eic001=f.eib001
    """
    lz_spark.create_temp_table(sql, "tmp1")

    sql = """
    select 
        product_sk
        , product_code
        , product_name
        , small_type
        , middle_type --大中小类这个还得问下客户
        , large_type
        , unit
        , order_unit
        , unit_conversion_rate
        , product_type
        , expiration_date
        , case when product_type='001001' or product_type='001003' then expiration_date-1 
            when product_type='001002' then expiration_date
            when product_type='001004' and expiration_date<=30 then ceil(expiration_date*2.0/3)
            when product_type='001004' and expiration_date>30 then ceil(expiration_date*0.5)
        else null end shelf_life
        ,case when product_type='001001' or product_type='001003' then expiration_date-1 
            when product_type='001002' then expiration_date
            when product_type='001004' and expiration_date<=30 then ceil(expiration_date*2.0/3)
            when product_type='001004' and expiration_date>30 then ceil(expiration_date*0.5)
        else null end best_before_date
        , min_order_qty
        , min_package_qty
        , min_display_qty
        , safety_stock
        , supply_pattern
        , supply_lead_time
        , fixed_days_supply
        , cycle_supply_pattern_code
        , store_model_supply_time
        , director_model_supply_time
        , store_other_supply_time
        , director_other_supply_time
        , stop_order_date
        , supply_method
        , 1 is_valid
        , sale_price
        , cost_price 
    from tmp1
    """
    lz_spark.create_temp_table(sql, "tmp_ods_product")

    # ods与edw开链比较，两边都有且开闭链字段相同，不开不闭
    sql = """
    select 
        a.product_sk, 
        a.product_code, 
        a.product_name, 
        a.small_type
        , a.middle_type --大中小类这个还得问下客户
        , a.large_type,
        a.unit, 
        a.order_unit,
        a.unit_conversion_rate,
        a.product_type, 
        a.expiration_date, 
        a.shelf_life, 
        a.best_before_date, 
        a.min_order_qty, 
        a.min_package_qty, 
        a.min_display_qty, 
        a.safety_stock, 
        a.supply_pattern,
        a.supply_lead_time, 
        a.fixed_days_supply, 
        a.cycle_supply_pattern_code,
        a.store_model_supply_time,
        a.director_model_supply_time,
        a.store_other_supply_time,
        a.director_other_supply_time,
        a.stop_order_date,
        a.supply_method, 
        a.is_valid, 
        a.sale_price, 
        a.cost_price, 
        b.etl_begin, 
        b.etl_end, 
        cast(current_timestamp as string) etl_time
    from tmp_ods_product a 
    inner join {p_edw_schema}.dim_product b 
        on a.product_sk=b.product_sk
        and b.etl_end='9999-12-31'
    where a.product_code=b.product_code 
        and a.stop_order_date=b.stop_order_date
        and a.is_valid=b.is_valid
    """
    lz_spark.create_temp_table(sql, "tmp_update_chain")

    # edw开链与ods比较，edw有ods无或开闭链字段不同，闭链
    sql = """
    select 
        c.product_sk, 
        c.product_code, 
        c.product_name, 
        c.small_type
        , c.middle_type --大中小类这个还得问下客户
        , c.large_type,
        c.unit, 
        c.order_unit,
        c.unit_conversion_rate,
        c.product_type, 
        c.expiration_date, 
        c.shelf_life, 
        c.best_before_date, 
        c.min_order_qty, 
        c.min_package_qty, 
        c.min_display_qty, 
        c.safety_stock, 
        c.supply_pattern,
        c.supply_lead_time, 
        c.fixed_days_supply, 
        c.cycle_supply_pattern_code, 
        c.store_model_supply_time,
        c.director_model_supply_time,
        c.store_other_supply_time,
        c.director_other_supply_time,
        c.stop_order_date,
        c.supply_method, 
        c.is_valid, 
        c.sale_price, 
        c.cost_price, 
        c.etl_begin, 
        '{p_input_date}' etl_end, 
        cast(current_timestamp as string) etl_time
    from {p_edw_schema}.dim_product c
    left join tmp_ods_product d 
        on c.product_sk=d.product_sk
    where c.etl_end='9999-12-31'
        and (c.product_code<>d.product_code 
        or c.stop_order_date<>d.stop_order_date
        or c.is_valid<>d.is_valid
        or d.product_sk is null)
    """
    lz_spark.create_temp_table(sql, "tmp_close_chain")

    # ods与edw开链比较，ods有edw无或开闭链字段不同，开链
    sql = """
    select
        e.product_sk, 
        e.product_code, 
        e.product_name, 
        e.small_type
        , e.middle_type --大中小类这个还得问下客户
        , e.large_type,
        e.unit, 
        e.order_unit,
        e.unit_conversion_rate,
        e.product_type, 
        e.expiration_date, 
        e.shelf_life, 
        e.best_before_date, 
        e.min_order_qty, 
        e.min_package_qty, 
        e.min_display_qty, 
        e.safety_stock, 
        e.supply_pattern,
        e.supply_lead_time, 
        e.fixed_days_supply, 
        e.cycle_supply_pattern_code, 
        e.store_model_supply_time,
        e.director_model_supply_time,
        e.store_other_supply_time,
        e.director_other_supply_time,
        e.stop_order_date,
        e.supply_method, 
        e.is_valid, 
        e.sale_price, 
        e.cost_price, 
        '{p_input_date}' etl_begin, 
        '9999-12-31' etl_end, 
        cast(current_timestamp as string) etl_time
    from tmp_ods_product e
    left join {p_edw_schema}.dim_product f 
        on e.product_sk=f.product_sk
        and f.etl_end='9999-12-31'
    where (e.product_code<>f.product_code 
        or e.stop_order_date<>f.stop_order_date
        or e.is_valid<>f.is_valid
        or f.product_sk is null)
    """
    lz_spark.create_temp_table(sql, "tmp_open_chain")

    # edw已闭链
    sql = """
    select
        product_sk, 
        product_code, 
        product_name, 
        small_type
        , middle_type --大中小类这个还得问下客户
        , large_type,
        unit, 
        order_unit,
        unit_conversion_rate,
        product_type, 
        expiration_date, 
        shelf_life, 
        best_before_date, 
        min_order_qty, 
        min_package_qty, 
        min_display_qty, 
        safety_stock, 
        supply_pattern,
        supply_lead_time, 
        fixed_days_supply, 
        cycle_supply_pattern_code, 
        store_model_supply_time,
        director_model_supply_time,
        store_other_supply_time,
        director_other_supply_time,
        stop_order_date,
        supply_method, 
        is_valid, 
        sale_price, 
        cost_price, 
        etl_begin, 
        etl_end, 
        etl_time
    from {p_edw_schema}.dim_product
    where etl_end<>'9999-12-31'
    """
    lz_spark.create_temp_table(sql, "tmp_closed_chain")
    lz_spark.drop_temp_table("tmp_ods_product")

    # 更新产品表
    sql = """
    insert overwrite table {p_edw_schema}.dim_product 
    select 
        product_sk, product_code, product_name, small_type
        , middle_type --大中小类这个还得问下客户
        , large_type, unit, order_unit, unit_conversion_rate, 
        product_type, expiration_date, shelf_life, best_before_date, min_order_qty, 
        min_package_qty, min_display_qty, safety_stock, supply_pattern, supply_lead_time, 
        fixed_days_supply, cycle_supply_pattern_code, store_model_supply_time, 
        director_model_supply_time, store_other_supply_time, director_other_supply_time, 
        stop_order_date, supply_method, is_valid, sale_price, cost_price, etl_begin, 
        etl_end, etl_time
    from tmp_update_chain
    union all 
    select 
        product_sk, product_code, product_name,small_type
        , middle_type --大中小类这个还得问下客户
        , large_type, unit, order_unit, unit_conversion_rate, 
        product_type, expiration_date, shelf_life, best_before_date, min_order_qty, 
        min_package_qty, min_display_qty, safety_stock, supply_pattern, supply_lead_time, 
        fixed_days_supply, cycle_supply_pattern_code, store_model_supply_time, 
        director_model_supply_time, store_other_supply_time, director_other_supply_time, 
        stop_order_date, supply_method, is_valid, sale_price, cost_price, etl_begin, 
        etl_end, etl_time
    from tmp_close_chain
    union all 
    select 
        product_sk, product_code, product_name,small_type
        , middle_type --大中小类这个还得问下客户
        , large_type, unit, order_unit, unit_conversion_rate, 
        product_type, expiration_date, shelf_life, best_before_date, min_order_qty, 
        min_package_qty, min_display_qty, safety_stock, supply_pattern, supply_lead_time, 
        fixed_days_supply, cycle_supply_pattern_code, store_model_supply_time, 
        director_model_supply_time, store_other_supply_time, director_other_supply_time, 
        stop_order_date, supply_method, is_valid, sale_price, cost_price, etl_begin, 
        etl_end, etl_time
    from tmp_open_chain
    union all 
    select 
        product_sk, product_code, product_name,small_type
        , middle_type --大中小类这个还得问下客户
        , large_type, unit, order_unit, unit_conversion_rate, 
        product_type, expiration_date, shelf_life, best_before_date, min_order_qty, 
        min_package_qty, min_display_qty, safety_stock, supply_pattern, supply_lead_time, 
        fixed_days_supply, cycle_supply_pattern_code, store_model_supply_time, 
        director_model_supply_time, store_other_supply_time, director_other_supply_time, 
        stop_order_date, supply_method, is_valid, sale_price, cost_price, etl_begin, 
        etl_end, etl_time
    from tmp_closed_chain
    """
    lz_spark.execute_sql(sql)
    lz_spark.drop_temp_table("tmp_update_chain")
    lz_spark.drop_temp_table("tmp_close_chain")
    lz_spark.drop_temp_table("tmp_open_chain")
    lz_spark.drop_temp_table("tmp_closed_chain")
    print("Success")

