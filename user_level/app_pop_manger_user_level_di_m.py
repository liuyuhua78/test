#!/usr/bin/env python3
#===============================================================================
#
#         FILE: temp.temp_pop_manger_user_lv_di.py         
#
#        USAGE: ./temp_pop_manger_user_lv_di.py       
#
#  DESCRIPTION: pop管家-新用户等级    
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: liuyuhua                                
#      COMPANY: jd.com
#      VERSION: 1.0
#      CREATED: 20140305                                 
#     REVIEWER: 
#     REVISION: ---
#    SRC_TABLE:       
#    TGT_TABLE:             
#===============================================================================
import sys
import os
sys.path.append(os.getenv('HIVE_TASK'))
from HiveTask import HiveTask

ht = HiveTask()
sql = """

use temp;                                                  

set mapred.output.compress=true; 
set hive.exec.compress.output=true; 
set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec; 

insert overwrite table app.app_pop_manger_user_level_di partition(dt = '2014-08-01', tp = 'month')                 
select 
   date                as op_time              
  ,mon                 as month                
  ,cate_lv             as cate_lv              
  ,cate_id             as cate_id              
  ,''                  as cate_name            
  ,item_first_cate_cd  as item_first_cate_cd   
  ,''                  as item_first_cate_name 
  ,item_second_cate_cd as item_second_cate_cd  
  ,''                  as item_second_cate_name
  ,item_third_cate_cd  as item_third_cate_cd   
  ,''                  as item_third_cate_name 
  ,''                  as user_level_name      
  ,user_lv_month       as user_level_id        
  ,user_num            as yx_user_num          
  ,old_user            as old_user_num         
  ,new_user_num        as new_user_num         
  ,fguser              as rebuy_user_num       
  ,amount              as yx_sale_amount       
  ,ord_num             as yx_ord_num           
  ,sale_qtty           as yx_quantity          
from (
select 
   '""" + ht.data_day_str + """' as date
  ,mon
  ,'3' cate_lv
  ,item_third_cate_cd as cate_id
  ,item_first_cate_cd
  ,item_second_cate_cd
  ,item_third_cate_cd
  ,s.user_lv_month  
  ,user_num
  ,(user_num - new_user_num) as old_user
  ,new_user_num
  ,t.fguser
  ,amount
  ,ord_num
  ,sale_qtty
from
(select 	
    substr(sale_ord_dt, 1, 7) as mon
   ,item_first_cate_cd
   ,item_second_cate_cd
   ,item_third_cate_cd
   ,user_lv_month
   ,count(distinct user_log_acct) as user_num
   ,count(distinct case when new_user_flag = 1 then user_log_acct end) as new_user_num
   ,sum(sale_amount)              as amount
   ,count(distinct sale_ord_id)   as ord_num
   ,sum(sale_qtty)                as sale_qtty
 from temp.temp_pop_manger_user_lv_di          
 group by 
    substr(sale_ord_dt, 1, 7)
   ,item_first_cate_cd
   ,item_second_cate_cd
   ,item_third_cate_cd
   ,user_lv_month
)s
left outer join    
(select 
    month
   ,cate_id
   ,user_lv_month
   ,count(user_log_acct) as fguser
 from
 (select 
     month
    ,cate_id
    ,user_log_acct
    ,user_lv_month
    ,sum(ord_num)ord_num 
  from temp.temp_pop_manger_user_fugou_lv_di 
  where cate_lv = '3'
  group by month, cate_id, user_log_acct, user_lv_month 
 )w
 where ord_num >= 2         
 group by month,cate_id,user_lv_month
)t on(s.mon = t.month and s.item_third_cate_cd = t.cate_id and s.user_lv_month = t.user_lv_month)       

union all

select 
   '"""+ ht.data_day_str+ """' as date
  ,mon
  ,'2'                 as cate_lv
  ,item_second_cate_cd as cate_id
  ,item_first_cate_cd
  ,item_second_cate_cd
  ,''                  as item_third_cate_cd
  ,s.user_lv_month
  ,user_num
  ,(user_num - new_user_num) as old_user
  ,new_user_num
  ,t.fguser
  ,amount
  ,ord_num
  ,sale_qtty
from
(select 
    substr(sale_ord_dt, 1, 7) as mon
   ,item_first_cate_cd
   ,item_second_cate_cd
   ,'' 
   ,user_lv_month
   ,''
   ,count(distinct user_log_acct) as user_num
   ,count(distinct case when new_user_flag = 1 then user_log_acct end) as new_user_num
   ,sum(sale_amount)              as amount
   ,count(distinct sale_ord_id)   as ord_num
   ,sum(sale_qtty)                as sale_qtty
 from temp.temp_pop_manger_user_lv_di          
 group by 
    substr(sale_ord_dt, 1, 7)
   ,item_first_cate_cd
   ,user_lv_month
   ,item_second_cate_cd
)s
left outer join    
(select 
    month
   ,cate_id
   ,user_lv_month
   ,count(user_log_acct) as fguser
 from
 (select 
     month
    ,cate_id
    ,user_log_acct
    ,user_lv_month
    ,sum(ord_num) as ord_num 
  from temp.temp_pop_manger_user_fugou_lv_di 
  where cate_lv = '2'
  group by month, cate_id, user_log_acct, user_lv_month 
 )w
 where ord_num >= 2
 group by month, cate_id, user_lv_month
)t on(s.mon = t.month and s.item_second_cate_cd = t.cate_id and s.user_lv_month = t.user_lv_month)

union all    

select 
 '""" + ht.data_day_str + """' as date
  ,mon
  ,'1'                as cate_lv
  ,item_first_cate_cd as cate_id
  ,item_first_cate_cd
  ,''                 as item_second_cate_cd
  ,''                 as item_third_cate_cd
  ,s.user_lv_month
  ,user_num
  ,(user_num - new_user_num) as old_user
  ,new_user_num
  ,t.fguser
  ,amount
  ,ord_num 
  ,sale_qtty
from
(select 
    substr(sale_ord_dt, 1, 7) as mon
   ,item_first_cate_cd
   ,''
   ,''
   ,''
   ,''
   ,user_lv_month
   ,count(distinct user_log_acct) as user_num
   ,count(distinct case when new_user_flag = 1 then user_log_acct end) as new_user_num
   ,sum(sale_amount) as amount
   ,count(distinct sale_ord_id) as ord_num
   ,sum(sale_qtty) as sale_qtty
 from temp.temp_pop_manger_user_lv_di          
 group by 
    substr(sale_ord_dt, 1, 7)
   ,item_first_cate_cd
   ,user_lv_month
)s
left outer join    
(select 
   month
   ,cate_id
   ,user_lv_month
   ,count(user_log_acct) as fguser
 from
 (select 
     month
    ,cate_id
    ,user_log_acct
    ,user_lv_month
    ,sum(ord_num) as ord_num 
  from temp.temp_pop_manger_user_fugou_lv_di 
  where cate_lv = '1'
  group by month, cate_id, user_log_acct, user_lv_month
 )w
 where ord_num >= 2  
 group by month, cate_id, user_lv_month
)t on(s.mon = t.month and s.item_first_cate_cd = t.cate_id and s.user_lv_month = t.user_lv_month)
)t
;
"""

ht.exec_sql(schema_name = 'temp', table_name = 'temp_pop_manger_user_lv_di', sql = sql, merge_flag = True, merge_part_dir = ['dt=2014-08-01/tp=month'])                    

#==============================================================================================
#   schema_name: 必选
#    table_name: 可选
#           sql: 必选
#    merge_flag: False (default)
#  lzo_compress: 可选 False (default)  
#lzo_index_path: 依赖lzo_compress可选，不需要warehouse，实例化了表后自动找到localtion
#                '' ,[''] 压缩整个表 
#                Normal,
#                /home/use/dd_edw/db/table
#                ['partition1','partition2']
#                ['dir1','dir2']
#               
#merge_part_dir: [](default) 整个表都检测小文件  
#                [partition1,partition2]
#      min_size: 128Mb
#----------------------------------------------------------------------------------------------
#      max_size: 250Mb
#---------------------------------------------------------------------------------------------
#ht.merge_small_file(db, table, partition = [], min_size = 128*1024*1024)
#ht.CreateIndex(db, table, path = 'Normal')
#===============================================================================================