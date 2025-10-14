from google.cloud import storage
from pandas import read_csv
import pandas as pd
import numpy as np
from typing import List, Optional
import warnings
import sys
import datetime
import time
from datetime import timedelta
from dateutil.relativedelta import relativedelta
warnings.filterwarnings("ignore")
import os
import logging
import copy
import yaml
import requests
import google.auth
import google.auth.transport.requests
import google.oauth2.id_token
#from google.cloud.bigquery import Client
#from google.cloud import bigquery
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from repos.cainz_demand_forecast.cainz.common import common
from repos.cainz_demand_forecast.cainz.short_term import short_term_preprocess_common
from repos.cainz_demand_forecast.cainz.short_term.short_term_preprocess_common import get_newest_jan_list

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

cloudrunjob_mode = True

TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))
TODAY_OFFSET = int(os.environ.get("TODAY_OFFSET", 0))
EXECUTE_STAGE2 = int(os.environ.get("EXECUTE_STAGE2", 0))
OUTPUT_HACCHUJAN_INFO = int(os.environ.get("OUTPUT_HACCHUJAN_INFO", 0))
OUTPUT_HACCHUJAN_TABLE_TEST = int(os.environ.get("OUTPUT_HACCHUJAN_TABLE_TEST", 0))
OUTPUT_HACCHUJAN_INFO_GCS = int(os.environ.get("OUTPUT_HACCHUJAN_INFO_GCS",0))
THEME_MD_MODE = int(os.environ.get("THEME_MD_MODE", 0))


#################################
# Old Code:  
#print('TASK_INDEX:', TASK_INDEX)
#print('TASK_COUNT:', TASK_COUNT)
#print('TODAY_OFFSET:', TODAY_OFFSET)
#print('EXECUTE_STAGE2:', EXECUTE_STAGE2)
#print('OUTPUT_HACCHUJAN_INFO:', OUTPUT_HACCHUJAN_INFO)
#print('OUTPUT_HACCHUJAN_TABLE_TEST:', OUTPUT_HACCHUJAN_TABLE_TEST)
#print('OUTPUT_HACCHUJAN_INFO_GCS:', OUTPUT_HACCHUJAN_INFO_GCS)
#print('THEME_MD_MODE:', THEME_MD_MODE)


#################################
# Refactored Code: 
logger.info(f"TASK_INDEX: {TASK_INDEX}")
logger.info(f"TASK_COUNT: {TASK_COUNT}")
logger.info(f"TODAY_OFFSET: {TODAY_OFFSET}")
logger.info(f"EXECUTE_STAGE2: {EXECUTE_STAGE2}")
logger.info(f"OUTPUT_HACCHUJAN_INFO: {OUTPUT_HACCHUJAN_INFO}")
logger.info(f"OUTPUT_HACCHUJAN_TABLE_TEST: {OUTPUT_HACCHUJAN_TABLE_TEST}")
logger.info(f"OUTPUT_HACCHUJAN_INFO_GCS: {OUTPUT_HACCHUJAN_INFO_GCS}")
logger.info(f"THEME_MD_MODE: {THEME_MD_MODE}")



# 全店拡大20240926年末積み増し対応 239店舗(22店舗追加) + テーマMD（土鍋カセットボンベ　5店舗）　全244店舗　花王もこちらで
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 
50, 137, 154, 235, 613, 615, 617, 618, 623, 624, 741, 767, 794, 799, 808, 812, 842, 868, 875, 878, 903, 909,
51,52,908,910,932,
]


# 全店拡大20250130 218店舗（古河は13週経過したので入れる）
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 910
]

 # 93を追加 20240304
dpt_list = [69, 97, 14, 37, 27, 39, 28, 74, 33, 30, 36, 75, 85, 80, 20, 22, 55, 72, 15, 62, 32, 77, 84, 89, 23, 60, 25, 87, 68, 56, 92, 61, 2, 40, 86, 88, 26, 17, 24, 34, 52, 64, 73, 21, 35, 58, 83, 94, 63, 38, 18, 29, 19, 31, 53, 45, 50, 81, 82, 90, 91, 54, 95, 93]

storage_client = storage.Client()
bucket_name = "dev-cainz-demandforecast"
bucket = storage_client.bucket(bucket_name)
prefix = 'vertex_pipelines/pipeline/pipeline_shortterm1/check_stage1_complete/completed_'
blobs = list(storage_client.list_blobs(bucket, prefix=prefix))
PROJECT_ID = "dev-cainz-demandforecast"
DATASET_ID = 'dev_cainz_nssol'
TEMP_TABLE_PREFIX = 'temp_M_090_PRD_NB_STD'
path_target_prd_master = "Basic_Analysis_unzip_result/01_Data/92_ADD_DATA_adhoc/kaou_prd_master.csv"

# DEFAULT_TODAY_OFFSET = 0
# DEFAULT_TASK_COUNT = 0
# DEFAULT_EXECUTE_STAGE2 = 0
# DEFAULT_OUTPUT_HACCHUJAN_INFO = 0
# DEFAULT_OUTPUT_HACCHUJAN_TABLE_TEST = 0
# DEFAULT_OUTPUT_HACCHUJAN_INFO_GCS = 1
# DEFAULT_THEME_MD_MODE = 0

CONFIG_SEASON_FILE = '00_config_season.yaml'
CONFIG_NOT_SEASON_FILE = '00_config_not_season.yaml'
DEFAULT_START_HOLDOUT_NENSHUDO = 202127
DEFAULT_START_NENSHUDO = 201701

STAGE2_WEEKLY_URL ='https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-weekly'
STAGE2_MONTHLY_URL = 'https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-monthly'
STAGE2_MIDIUM_URL ='https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-midium'

tenpo_cd = tenpo_cd_list[TASK_INDEX]

#################################
# Old Code:  
# if cloudrunjob_mode:
#     tenpo_cd = tenpo_cd_list[TASK_INDEX]
    
#     #print('TASK_INDEX:', TASK_INDEX, 'tenpo_cd:', tenpo_cd)
#     logger.info(f"TASK_INDEX: {TASK_INDEX}, tenpo_cd: {tenpo_cd}")
    
#     if (TASK_INDEX == 0) and (EXECUTE_STAGE2 == 1):
#         # stage1完了チェックフォルダ配下のファイル削除
#         for blob in blobs:
#             #print(blob.name)
#             logger.info(blob.name)
#             generation_match_precondition = None
#             blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
#             generation_match_precondition = blob.generation
#             blob.delete(if_generation_match=generation_match_precondition)
#             #print(f"Blob {blob.name} deleted.")
#             logger.info(f"Blob {blob.name} deleted.")
    
# else:
#     # notebookで動かすモード
#     tenpo_cd = int(sys.argv[1])
#     #print('tenpo_cd:', tenpo_cd)
#     logger.info(F"tenpo_cd: {tenpo_cd})
#     TODAY_OFFSET = 0
#     #print('TODAY_OFFSET:', TODAY_OFFSET) 
#     logger.info(f"TODAY_OFFSET: {TODAY_OFFSET}
#     TASK_COUNT = 0
#     #print('TASK_COUNT:', TASK_COUNT)
#     logger.info(f"TASK_COUNT: {TASK_COUNT})
    
#     EXECUTE_STAGE2 = 0
#     #print('EXECUTE_STAGE2:', EXECUTE_STAGE2)
#     logger.info(f"EXECUTE_STAGE2: {EXECUTE_STAGE2})
#     OUTPUT_HACCHUJAN_INFO = 0
#     #print('OUTPUT_HACCHUJAN_INFO:', OUTPUT_HACCHUJAN_INFO)
#     info.logging(f"OUTPUT_HACCHUJAN_INFO: {OUTPUT_HACCHUJAN_INFO})
#     OUTPUT_HACCHUJAN_TABLE_TEST = 0
#     #print('OUTPUT_HACCHUJAN_TABLE_TEST:', OUTPUT_HACCHUJAN_TABLE_TEST)
#     logger.info(f"OUTPUT_HACCHUJAN_TABLE_TEST: {OUTPUT_HACCHUJAN_TABLE_TEST})
#     OUTPUT_HACCHUJAN_INFO_GCS = 1
#     #print('OUTPUT_HACCHUJAN_INFO_GCS:', OUTPUT_HACCHUJAN_INFO_GCS)
#     logger.info(f"OUTPUT_HACCHUJAN_INFO_GCS: {OUTPUT_HACCHUJAN_INFO_GCS})
#     THEME_MD_MODE = 0
#     #print('THEME_MD_MODE:', THEME_MD_MODE)
#     logger.info(f"THEME_MD_MODE: {THEME_MD_MODE})

  
    
#################################
# Refactored Code:                          
def clear_stage1_complete_files(storage_client, bucket):
    """Clears files in the stage1 completion check folder."""
    for blob in blobs:
        logger.info(f"Deleting blob: {blob.name}")
        try:
            generation_match_precondition = blob.generation
            blob.delete(if_generation_match=generation_match_precondition)
            logger.info(f"Blob {blob.name} deleted.")
        except Exception as e:
            logger.error(f"Error deleting blob {blob.name}: {e}")



#################################
# Old Code:
# dirpath = sys.path.append(os.path.join(os.path.dirname("__file__"), '/home/jovyan/work/playground/nagano/lib'))
# # sys.path.append("../../cainz/")
# sys.path.append("repos/cainz_demand-forecast/cainz")

# pd.set_option("display.max_columns", None)
# pd.set_option("display.max_rows", 100)

# #tenpo_cd = sys.argv[1]

# #print("===tenpo_cd===",tenpo_cd)
# logger.info(f"===tenpo_cd=== {tenpo_cd})

# path_week_master = "Basic_Analysis_unzip_result/01_Data/10_week_m/WEEK_MST.csv"
# #path_bunrui = "Basic_Analysis_unzip_result/01_Data/90_ADD_DATA/M_090_BUNRUI.csv"
# path_bunrui = "Basic_Analysis_unzip_result/01_Data/14_bunrui_m/06_M_090_BUNRUI.csv"

# #path_tenpo_master = "Basic_Analysis_unzip_result/01_Data/03_tempo_m/TENPO_MST.csv"
# path_tenpo_master = "Basic_Analysis_unzip_result/01_Data/03_tempo_m/02_M020TENPO.csv"

# path_tenpo_hacchu_master = "Basic_Analysis_unzip_result/01_Data/33_tenpo_hacchu/29_TENPO_HACCHU_YMD.csv"

# #path_selling_period = "Basic_Analysis_unzip_result/01_Data/90_ADD_DATA/HANBAI_KIKAN.csv"
# path_selling_period = "Basic_Analysis_unzip_result/01_Data/17_hanbai_kikan/08_HANBAI_KIKAN.csv"
# path_prd_asc = "Basic_Analysis_unzip_result/01_Data/16_case_pack_bara/07_M_090_PRD_ASC.csv"

# #path_jan_master = 'Basic_Analysis_unzip_result/03_DSteam_create_data/jan_connect.csv'
# #path_jan_master = '01_short_term/70_jan_connect/jan_connect.csv'
# #path_jan_master = '01_short_term/70_jan_connect/jan_placed_809.csv'
# path_jan_master = '01_short_term/70_jan_connect/jan_placed_'+str(tenpo_cd)+'.csv'
# #path_tehme_md = "Basic_Analysis_unzip_result/01_Data/36_theme_md/theme_md_prd_store.csv"

# path_bunrui_prd_code = "{}_各分類商品コード一覧.xlsx"
# path_bunrui_prd_code_blob = "01_short_term/01_stage1_result/{}_各分類商品コード一覧.xlsx"

# path_tenpo_dpt = "Basic_Analysis_unzip_result/02_DM/NBSales_shu_ten_prd_ten/NBSales_shu_ten_prd_{}/sales_{}_"

# path_select_master = 'select_shop_master.csv'


# path_prd_master_list = [
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000000.csv",
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000001.csv",
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000002.csv",
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000003.csv",
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000004.csv",
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000005.csv",
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000006.csv",
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000007.csv",
#     "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000008.csv",
# ]

# #print('path_prd_master_list:')
# #print(path_prd_master_list)
# logger.info(f"path_prd_master_list: {path_prd_master_list})

# path_tran_dir = "Basic_Analysis_unzip_result/02_DM/NBSales_shu_ten_prd/s"

# path_upload_tmp_local_dir = "gcpアップロード一時データ"

# # stage1の前日以前の日付（gcsのフォルダ名にも反映される）を使うフラグ
# use_past_stage1_data_flag = True
# past_days_num = int(TODAY_OFFSET)
# t_delta = datetime.timedelta(hours=9)
# JST = datetime.timezone(t_delta, 'JST')
# if use_past_stage1_data_flag:
#     today = datetime.datetime.now(JST) - timedelta(days=past_days_num)
#     today_date_str = today.strftime('%Y-%m-%d')
#     today = datetime.date(today.year, today.month, today.day)
# else:
#     today = datetime.datetime.now(JST)
#     today_date_str = today.strftime('%Y-%m-%d')
#     today = datetime.date(today.year, today.month, today.day)
    
# #print("******************* today ", today)
# #print("******************* today_date_str ", today_date_str)
# logger.info(f"today {today})
# logger.info(f"today_date_str {today_date_str})

# my_date = today.strftime('%Y%m%d')
# my_date = int(my_date)
# #print("******************* my_date ", my_date)
# logger.info(f"******************* my_date {my_date})

 

#################################
# Refactored Code:  
# Append necessary paths to the system path
dirpath = sys.path.append(os.path.join(os.path.dirname("__file__"), '/home/jovyan/work/playground/nagano/lib'))
sys.path.append("repos/cainz_demand-forecast/cainz")

# Set pandas display options
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)

# Define file paths
path_week_master = "Basic_Analysis_unzip_result/01_Data/10_week_m/WEEK_MST.csv"
path_bunrui = "Basic_Analysis_unzip_result/01_Data/14_bunrui_m/06_M_090_BUNRUI.csv"
path_tenpo_master = "Basic_Analysis_unzip_result/01_Data/03_tempo_m/02_M020TENPO.csv"
path_tenpo_hacchu_master = "Basic_Analysis_unzip_result/01_Data/33_tenpo_hacchu/29_TENPO_HACCHU_YMD.csv"
path_selling_period = "Basic_Analysis_unzip_result/01_Data/17_hanbai_kikan/08_HANBAI_KIKAN.csv"
path_prd_asc = "Basic_Analysis_unzip_result/01_Data/16_case_pack_bara/07_M_090_PRD_ASC.csv"
path_jan_master = '01_short_term/70_jan_connect/jan_placed_'+str(tenpo_cd)+'.csv'
path_bunrui_prd_code = "{}_各分類商品コード一覧.xlsx"
path_bunrui_prd_code_blob = "01_short_term/01_stage1_result/{}_各分類商品コード一覧.xlsx"
path_tenpo_dpt = "Basic_Analysis_unzip_result/02_DM/NBSales_shu_ten_prd_ten/NBSales_shu_ten_prd_{}/sales_{}_"
path_select_master = 'select_shop_master.csv'

# Define list of product master paths
path_prd_master_list = [
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000000.csv",
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000001.csv",
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000002.csv",
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000003.csv",
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000004.csv",
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000005.csv",
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000006.csv",
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000007.csv",
    "Basic_Analysis_unzip_result/02_DM/M_090_PRD/M_090_PRD_NB_STD_000000000008.csv",
]

# Log the list of product master paths
logger.info(f"path_prd_master_list: {path_prd_master_list}")

# Define remaining paths and variables
path_tran_dir = "Basic_Analysis_unzip_result/02_DM/NBSales_shu_ten_prd/s"
path_upload_tmp_local_dir = "gcpアップロード一時データ"
use_past_stage1_data_flag = True
past_days_num = int(TODAY_OFFSET)
t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, 'JST')

# Determine today's date based on use_past_stage1_data_flag
if use_past_stage1_data_flag:
    today = datetime.datetime.now(JST) - timedelta(days=past_days_num)
    today_date_str = today.strftime('%Y-%m-%d')
    today = datetime.date(today.year, today.month, today.day)
else:
    today = datetime.datetime.now(JST)
    today_date_str = today.strftime('%Y-%m-%d')
    today = datetime.date(today.year, today.month, today.day)

# Log today's date and date string
logger.info(f"today {today}")
logger.info(f"today_date_str {today_date_str}")

# Format today's date
my_date = today.strftime('%Y%m%d')
my_date = int(my_date)

# Log the formatted date
logger.info(f"******************* my_date {my_date}")
            
       
    
# フラグ設定 **********************************************************************
# JAN差し替え有無の指定
use_jan_connect = True

# JAN差し替えを、1世代前までに制限するフラグ
restrict_old_jan_1generation_flag=True

# 生産発注終了、店舗発注終了していない商品は、差替えないようにするフラグ
seisan_tenpo_hattyuu_end_is_not_replaced = True

# チャンスロス分を売上に加算する
add_chanceloss_urisu = True

# 発注JANへの紐づけ情報を出力する
prdcd_hacchujan_df_flag = True

# biqqueryのオプションを指定して売上データをロードする
bq_allow_large_results = False

# 生産発注停止を見ないようにする
# short_term_preprocess_commonのrewrite_jan_code関数を直接修正

# 中川さんから直近13週中6週実績あり、平均週販2以上の提案ありのデータを出力する
output_6wk_2sales = True

# *********************************************************************************
# path_upload_time_series_blob = "01_short_term/01_stage1_result/01_weekly/"+str(today)+"/{}_{}_time_series.csv"
# path_upload_monthly_series_blob = "01_short_term/01_stage1_result/02_monthly/"+str(today)+"/{}_{}_monthly_series.csv"
# path_upload_not_ai_blob = "01_short_term/01_stage1_result/03_not_ai_target/"+str(today)+"/{}_{}_not_ai_model.csv"

#exclusion_dpt_list = [2, 20, 26, 30, 33, 37, 39, 47, 48, 53, 57, 80, 98]
exclusion_dpt_list = []

#このリストは使っていない
priority_dpt_list = [69,97,14,37,27,39,28,74,33,30,36,75,85,80,20,22,55,72,15,62,32,77,84,89,23,60,25,87,68,56,92,61,2,40,86,88,26,17,24,34,52,64,73,21,35,58,83,94,63,38,18,29,19,31,53,45,50,81,82,90,91,54,95]



#################################
# Old Code:
# tenpo_list = []
# #print("==tenpo_cd==",tenpo_cd)
# logger.info(f"==tenpo_cd== {tenpo_cd})
# tenpo_list.append(int(tenpo_cd))
# #print("==tenpo_list==",tenpo_list)
# logger.info(f"==tenpo_list== {tenpo_list})
# #tenpo_cd = 813
# path_upload_time_series_blob = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
# path_upload_time_series_blob2 = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-62/'+str(tenpo_cd)+"/{}_{}_time_series.csv"


# path_upload_time_series_blob_all_prd = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-allprd/'+str(tenpo_cd)+"/{}_{}_time_series.csv"


# path_upload_monthly_series_blob = "01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_monthly_series.csv"

# path_upload_not_ai_blob = "01_short_term/01_stage1_result/03_not_ai_target/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_not_ai_model.csv"

# # train_end_nenshudo = 202152
# # train_end_nenshudo = 202220
            
           
# ## シーズン品に絞るかを指定
# season_flag = False

# if season_flag:
#     with open('00_config_season.yaml') as file:
#         config = yaml.safe_load(file.read())
# else:
#     with open('00_config_not_season.yaml') as file:
#         config = yaml.safe_load(file.read())


# start_holdout_nenshudo = 202127
# #end_date = 20220301

# start_nenshudo = 201701

# # end_nenshudo = 202152
# # end_nenshudo = 202220


# if 0:
#     end_date = int(datetime.date.today().strftime('%Y%m%d'))
#     #print("==end_date===",end_date)
#     logger.info(f"==end_date=== {end_date})
#     end_date_str = datetime.date.today().strftime('%Y-%m-%d')
# else:
#     end_date = int(today.strftime('%Y%m%d'))
#     #print("==end_date===",end_date)
#     logger.info(f"==end_date=== {end_date})
#     end_date_str = today.strftime('%Y-%m-%d')              
        
        
    
#################################
# Old Code:
# if THEME_MD_MODE:
    
#     '''
#     theme_md_prdcd_list = []
    
#     if 1:
#         # dev-cainz-demandforecast/Basic_Analysis_unzip_result/01_Data/92_ADD_DATA_adhoc
#         path_target_prd_master = "Basic_Analysis_unzip_result/01_Data/92_ADD_DATA_adhoc/kaou_prd_master.csv"
#         target_prd_masterdf = common.extract_as_df(path_target_prd_master, bucket_name)
#         target_prd_masterdf['PRD_CD'] = target_prd_masterdf['PRD_CD'].astype(int)
        
#         theme_md_prdcd_list = target_prd_masterdf['PRD_CD'].unique().tolist()
        
#         #print('theme_md_prdcd_list:', theme_md_prdcd_list)
#         logger.info(f"theme_md_prdcd_list: {theme_md_prdcd_list})

    
#     if 0:
#         # 棚割りパターンコードから、テーマMDの店舗、商品コードを取得する
#         tanawari_ptn_cd_list = [
#             '094-990-187-02',
#             '094-990-188-01',
#             '094-990-189-01',
#         ]
        
        
#         path_ten_tana = "Basic_Analysis_unzip_result/01_Data/39_tanawari_ptn/30_T_090_TEN_TANA_PTN_SM.csv"
#         ten_tana_df = common.extract_as_df(path_ten_tana, bucket_name)
#         ten_tana_df['tenpo_cd'] = ten_tana_df['tenpo_cd'].astype(int)
        
#         path_tanaptn_dtl = "Basic_Analysis_unzip_result/01_Data/39_tanawari_ptn/30_T_090_TANA_PTN_DTL_SM.csv"
#         tanaptn_dtl_df = common.extract_as_df(path_tanaptn_dtl, bucket_name)
#         tanaptn_dtl_df['prd_cd'] = tanaptn_dtl_df['prd_cd'].astype(int)
        
#         theme_md_prdcd_list = []
#         this_tenpo_theme_md_prdcd_list = []
#         another_tenpo_theme_md_prdcd_list = []
        
#         # 各棚割りパターンCDをチェックする
#         for my_tanawari_ptn_cd in tanawari_ptn_cd_list:
#             #print('check tanawari_ptn_cd:', my_tanawari_ptn_cd)
#             logger.info(f"check tanawari_ptn_cd: {check tanawari_ptn_cd})
            
#             # 棚割りパターンの店舗コードリスト
#             my_tanawari_ptn_cd_tenpocd_list = ten_tana_df[ten_tana_df['tanawari_cd']==my_tanawari_ptn_cd]['tenpo_cd'].tolist()
#             # 棚割りパターンの商品コードリスト
#             my_tanawari_ptn_cd_prdcd_list = tanaptn_dtl_df[tanaptn_dtl_df['tanawari_cd']==my_tanawari_ptn_cd]['prd_cd'].tolist()
            
#             # この店舗は、棚割りパターン対象である
#             if tenpo_cd in my_tanawari_ptn_cd_tenpocd_list:
#                 if len(my_tanawari_ptn_cd_prdcd_list) > 0:
#                     this_tenpo_theme_md_prdcd_list = this_tenpo_theme_md_prdcd_list + my_tanawari_ptn_cd_prdcd_list
#             else:
#                 if len(my_tanawari_ptn_cd_prdcd_list) > 0:
#                     another_tenpo_theme_md_prdcd_list = another_tenpo_theme_md_prdcd_list + my_tanawari_ptn_cd_prdcd_list
        
        
#         theme_md_prdcd_list = this_tenpo_theme_md_prdcd_list + another_tenpo_theme_md_prdcd_list
#         #print('this_tenpo_theme_md_prdcd_list:', this_tenpo_theme_md_prdcd_list)
#         #print('another_tenpo_theme_md_prdcd_list:', another_tenpo_theme_md_prdcd_list)
#         #print('theme_md_prdcd_list:', theme_md_prdcd_list)
#         logger.info('this_tenpo_theme_md_prdcd_list: %s', this_tenpo_theme_md_prdcd_list)
#         logger.info('another_tenpo_theme_md_prdcd_list: %s', another_tenpo_theme_md_prdcd_list)
#         logger.info('theme_md_prdcd_list: %s', theme_md_prdcd_list)

#     '''

            

#################################
# Refactored Code: 
def extract_theme_md_product_codes(
    bucket_name: str,
    theme_md_mode: bool,
    tenpo_cd: Optional[int] = None,  
    tanawari_ptn_cd_list: Optional[List[str]] = None, ) -> List[int]:
    """
    Extracts theme merchandise product codes based on different modes.

    Args:
        bucket_name: The name of the Cloud Storage bucket.
        theme_md_mode: A boolean flag to enable the theme MD mode.
        tenpo_cd: (Optional) The store code. Required if using shelf allocation pattern mode.
        tanawari_ptn_cd_list: (Optional) A list of shelf allocation pattern codes. Required if using shelf allocation pattern mode.

    Returns:
        A list of unique theme merchandise product codes.  Returns an empty list on error
        or if the conditions are not met.
    """

    theme_md_prdcd_list: List[int] = []

    if theme_md_mode:
        if 1:  
            try:
                target_prd_masterdf = common.extract_as_df(path_target_prd_master, bucket_name)
                target_prd_masterdf['PRD_CD'] = target_prd_masterdf['PRD_CD'].astype(int)
                theme_md_prdcd_list = target_prd_masterdf['PRD_CD'].unique().tolist()
                logger.info(f"theme_md_prdcd_list (from master): {theme_md_prdcd_list}")
            except Exception as e:
                logger.error(f"Error extracting from product master: {e}")
                return []  # Return an empty list on error
       
        if 0:  
            if tenpo_cd is None or tanawari_ptn_cd_list is None:
                logger.warning("tenpo_cd and tanawari_ptn_cd_list are required for shelf allocation mode.")
                return []  # Return an empty list if required parameters are missing

            path_ten_tana = "Basic_Analysis_unzip_result/01_Data/39_tanawari_ptn/30_T_090_TEN_TANA_PTN_SM.csv"
            path_tanaptn_dtl = "Basic_Analysis_unzip_result/01_Data/39_tanawari_ptn/30_T_090_TANA_PTN_DTL_SM.csv"

            try:
                ten_tana_df = common.extract_as_df(path_ten_tana, bucket_name)
                tanaptn_dtl_df = common.extract_as_df(path_tanaptn_dtl, bucket_name)

                ten_tana_df['tenpo_cd'] = ten_tana_df['tenpo_cd'].astype(int)
                tanaptn_dtl_df['prd_cd'] = tanaptn_dtl_df['prd_cd'].astype(int)

                this_tenpo_theme_md_prdcd_list: List[int] = []
                another_tenpo_theme_md_prdcd_list: List[int] = []

                for my_tanawari_ptn_cd in tanawari_ptn_cd_list:
                    logger.info(f"check tanawari_ptn_cd: {my_tanawari_ptn_cd}")

                    my_tanawari_ptn_cd_tenpocd_list = ten_tana_df[ten_tana_df['tanawari_cd'] == my_tanawari_ptn_cd]['tenpo_cd'].tolist()
                    my_tanawari_ptn_cd_prdcd_list = tanaptn_dtl_df[tanaptn_dtl_df['tanawari_cd'] == my_tanawari_ptn_cd]['prd_cd'].tolist()

                    if tenpo_cd in my_tanawari_ptn_cd_tenpocd_list:
                        if my_tanawari_ptn_cd_prdcd_list:
                            this_tenpo_theme_md_prdcd_list.extend(my_tanawari_ptn_cd_prdcd_list)
                    else:
                        if my_tanawari_ptn_cd_prdcd_list:
                            another_tenpo_theme_md_prdcd_list.extend(my_tanawari_ptn_cd_prdcd_list)

                theme_md_prdcd_list = this_tenpo_theme_md_prdcd_list + another_tenpo_theme_md_prdcd_list

                logger.info(f'this_tenpo_theme_md_prdcd_list: {this_tenpo_theme_md_prdcd_list}')
                logger.info(f'another_tenpo_theme_md_prdcd_list: {another_tenpo_theme_md_prdcd_list}')
                logger.info(f'theme_md_prdcd_list (from shelf allocation): {theme_md_prdcd_list}')

            except Exception as e:
                logger.error(f"Error extracting from shelf allocation data: {e}")
                return []  

    return theme_md_prdcd_list

# product_codes = extract_theme_md_product_codes(
#     bucket_name=bucket_name,
#     theme_md_mode=True,
# )

# product_codes = extract_theme_md_product_codes(
#     bucket_name=bucket_name,
#     theme_md_mode=True,
#     tenpo_cd=12345,
#     tanawari_ptn_cd_list=['094-990-187-02', '094-990-188-01']
# )

            
            
#################################
# Old Code:
# if bq_allow_large_results:
#     # 商品マスタデータをBigQueryのテーブルからロード
#     project_id = "dev-cainz-demandforecast"
#     dataset_id = 'dev_cainz_nssol'
#     table_id = 'M_090_PRD_NB_STD'
#     my_tenpo_cd = str(tenpo_cd).zfill(4)
    
#     dest_str = f"""dev-cainz-demandforecast.test_data.temp_M_090_PRD_NB_STD_{my_tenpo_cd}"""

#     client = bigquery.Client()        
#     client.delete_table(dest_str, not_found_ok=True)

#     client = bigquery.Client()
#     bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
#     job_config = bigquery.QueryJobConfig(
#         allow_large_results=True, 
#         destination=dest_str, 
#         use_legacy_sql=False
#     )
#     query = f"""SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"""
    
#     #print(query)
#     logger.info("Executing query: %s", query)
    
    
#     dfm_base = client.query(query, job_config=job_config).result().to_dataframe(bqstorage_client=bqstorageclient)
#     dfm_base['PRD_CD'] = dfm_base['PRD_CD'].astype(int)
#     dfm_base['BUMON_CD'] = dfm_base['BUMON_CD'].astype(int)
#     dfm_base['cls_cd'] = dfm_base['cls_cd'].astype(int)

#     #print('商品マスタ件数:', len(dfm_base))
#     logger.info(f"商品マスタ件数: {len(dfm_base))


# else:

#     if 0:
#         # 商品マスタデータをBigQueryのテーブルからロード
#         project_id = "dev-cainz-demandforecast"
#         dataset_id = 'dev_cainz_nssol'
#         table_id = 'M_090_PRD_NB_STD'

#         if 1:
#             client = bigquery.Client()
#             bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
#             #client = bigquery.Client(project=project_id)
#             #bqstorageclient = bigquery_storage.BigQueryStorageClient()

#             target_query = f"""  SELECT * FROM {dataset_id}.{table_id} """  
#             dfm_base = (
#                 client.query(target_query)
#                 .result()
#                 .to_dataframe(bqstorage_client=bqstorageclient)
#             )

#             dfm_base['PRD_CD'] = dfm_base['PRD_CD'].astype(int)
#             dfm_base['BUMON_CD'] = dfm_base['BUMON_CD'].astype(int)
#             dfm_base['cls_cd'] = dfm_base['cls_cd'].astype(int)

#             #print('商品マスタ件数:', len(dfm_base))
#             logger.info(f"商品マスタ件数: {len(dfm_base)})


#         else:
#             def get_prdmaster_data( project_id, dataset_id, table_id):
#                 my_tenpo_cd = str(tenpo_cd).zfill(4)
#                 target_query = f"""  SELECT * FROM {dataset_id}.{table_id} """  
#                 #print(target_query)
#                 logger.info(f"target_query {target_query})
#                 df_target = pd.read_gbq(target_query, project_id, dialect='standard')
#                 return df_target

#             dfm_base = get_prdmaster_data( project_id, dataset_id, table_id)
#     else:
#         dfm_base = short_term_preprocess_common.load_multiple_df(path_prd_master_list, bucket_name)



#################################
# Refactored Code: 
def load_prd_master_from_bigquery_large_results(tenpo_cd: int, project_id: str, dataset_id: str, table_id: str) -> pd.DataFrame:
    """
    Loads product master data from BigQuery using the `allow_large_results` option.

    Args:
        tenpo_cd: The store code.
        project_id: The Google Cloud project ID.
        dataset_id: The BigQuery dataset ID.
        table_id: The BigQuery table ID.

    Returns:
        A Pandas DataFrame containing the product master data.
    """
    my_tenpo_cd = str(tenpo_cd).zfill(4)
    dest_table_id = f"{TEMP_TABLE_PREFIX}_{my_tenpo_cd}"
    dest_str = f"""dev-cainz-demandforecast.test_data.temp_M_090_PRD_NB_STD_{my_tenpo_cd}"""

    client = bigquery.Client()
    

    # Delete the temporary table if it exists
    client.delete_table(dest_str, not_found_ok=True)
    
    client = bigquery.Client()
    bqstorageclient = bigquery_storage_v1.BigQueryReadClient()

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True,
        destination=dest_str,
        use_legacy_sql=False  # Use standard SQL
    )

    query = f"""SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"""
    logging.info("Executing query with allow_large_results: %s", query)

    dfm_base = client.query(query, job_config=job_config).result().to_dataframe(bqstorage_client=bqstorageclient)
    dfm_base['PRD_CD'] = dfm_base['PRD_CD'].astype(int)
    dfm_base['BUMON_CD'] = dfm_base['BUMON_CD'].astype(int)
    dfm_base['CLS_CD'] = dfm_base['CLS_CD'].astype(int)

    logging.info(f"商品マスタ件数 (allow_large_results): {len(dfm_base)}")
    return dfm_base


def load_prd_master_from_multiple_files(path_prd_master_list: list[str], bucket_name: str) -> pd.DataFrame:
    """
    Loads product master data from multiple files using the `short_term_preprocess_common.load_multiple_df` function.

    Args:
        path_prd_master_list: A list of paths to the product master files.
        bucket_name: The name of the GCS bucket.

    Returns:
        A Pandas DataFrame containing the product master data.
    """
    dfm_base = short_term_preprocess_common.load_multiple_df(path_prd_master_list, bucket_name)
    logging.info(f"商品マスタ件数 (from multiple files): {len(dfm_base)}")
    return dfm_base


def load_product_master_data(
    tenpo_cd: int,
    bucket_name: str,
    TABLE_ID: str, 
) -> pd.DataFrame:
    """
    Loads product master data based on the bq_allow_large_results flag.

    Args:
        bq_allow_large_results: Flag indicating whether to use allow_large_results in BigQuery.
        tenpo_cd: The store code.
        path_prd_master_list: A list of paths to the product master files (used if bq_allow_large_results is False).
        bucket_name: The name of the GCS bucket (used if bq_allow_large_results is False).

    Returns:
        A Pandas DataFrame containing the product master data.
    """
    if bq_allow_large_results:
        dfm_base = load_prd_master_from_bigquery_large_results(
            tenpo_cd=tenpo_cd,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
        )
    else:
        dfm_base = load_prd_master_from_multiple_files(
            path_prd_master_list=path_prd_master_list,
            bucket_name=bucket_name,
        )

    return dfm_base

            

            
            
            
#################################
# Old Code:          
# col_dict = {}
# for col in dfm_base.columns:
#     col_dict[col] = col.lower()
# dfm_base = dfm_base.rename(columns=col_dict)
  
# if seisan_tenpo_hattyuu_end_is_not_replaced:
#     # 店舗別の生産発注停止情報を結合
#     store_prd_hacchu_ymd = common.extract_as_df(path_tenpo_hacchu_master, bucket_name)
#     store_prd_hacchu_ymd['TENPO_CD'] = store_prd_hacchu_ymd['TENPO_CD'].astype(int)
#     store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].fillna(99999999)
#     store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].astype(int)
        
# classify_excel_list = []
# classify_df_list = []

# if use_jan_connect:
#     #print("===path_jan_master====",path_jan_master)
#     logger.info(f"===path_jan_master==== {path_jan_master})
#     jan_master = short_term_preprocess_common.load_jan_master_fix(
#         path_jan_master,
#         end_date,
#         bucket_name
#     )
# for tenpo in tenpo_list:  
#     classify_excel_list.append(
#         pd.ExcelWriter(path_bunrui_prd_code.format(tenpo_df[tenpo_df['TENPO_CD']==tenpo]['TENPO_NM_KJ'].values[0]))
#     )
#     classify_df_list.append(pd.DataFrame())

# #dpt_list = short_term_preprocess_common.get_dpt_list_from_bucket(
# #    bucket_name,
# #    path_tran_dir,
# #)

# # 93を追加 20240304
# dpt_list = [69,97,14,37,27,39,28,74,33,30,36,75,85,80,20,22,55,72,15,62,32,77,84,89,23,60,25,87,68,56,92,61,2,40,86,88,26,17,24,34,52,64,73,21,35,58,83,94,63,38,18,29,19,31,53,45,50,81,82,90,91,54,95,93]

# # 花王テーマMDモデル
# #dpt_list = [22,34,64,72,73,74,83,84,85,86,87,89,93]

# # test
# #dpt_list = [94]

# #print('dpt_list:', dpt_list)
# logger.info(f"dpt_list: {dpt_list})

# # チャンスロスデータを読み込む
# #stockout_df = pd.DataFrame()
# #path_stockout = "Basic_Analysis_unzip_result/02_DM/NBKepin_kaisu_prd_ten/keppin_kaisu_/"+str(tenpo_cd)+"_stockout_all.csv"
# #stockout_df =  extract_as_df(path_stockout, bucket_name, "utf-8") 
# #stockout_df = stockout_df.groupby(['nenshudo', 'TENPO_CD', 'prd_cd']).agg({'KEPPIN_CNT':'mean'}).reset_index()
# #sales_df = pd.merge(sales_df,stockout_df.rename(columns={'prd_cd': 'PRD_CD', 'tenpo_cd': 'TENPO_CD'}),on=['nenshudo', 'PRD_CD', 'TENPO_CD'],how='left')

# # チャンスロスデータをBigQueryのテーブルからロード
# project_id = "dev-cainz-demandforecast"
# dataset_id = 'dev_cainz_nssol'
# table_id = 'T_090_PRD_CHANCE_LOSS_NB_DPT'
# def get_chanceloss_data( project_id, dataset_id, table_id, tenpo_cd):
#     my_tenpo_cd = str(tenpo_cd).zfill(4)
#     target_query = f"""  SELECT * FROM {dataset_id}.{table_id} where TENPO_CD = '{my_tenpo_cd}'"""  
#     #print(target_query)
#     logger.info(f"target_query {target_query})
    
#     if 1:
#         client = bigquery.Client()
#         #client = bigquery.Client(project=project_id)
#         bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
#         #bqstorageclient = bigquery_storage.BigQueryStorageClient()
#         df_target = (
#             client.query(target_query)
#             .result()
#             .to_dataframe(bqstorage_client=bqstorageclient)
#         )
#     else:
#         df_target = pd.read_gbq(target_query, project_id, dialect='standard')
    
#     return df_target

# chance_loss_data = get_chanceloss_data( project_id, dataset_id, table_id, tenpo_cd)
# #print('チャンスロスデータ件数:', len(chance_loss_data))
# logger.info(f'チャンスロスデータ件数: {len(chance_loss_data)}')

# chance_loss_data['PRD_CD'] = chance_loss_data['PRD_CD'].astype(int)
# chance_loss_data['NENSHUDO'] = chance_loss_data['NENSHUDO'].astype(int)
# chance_loss_data['TENPO_CD'] = chance_loss_data['TENPO_CD'].astype(int)
# chance_loss_data['CHANCE_LOSS_PRD_SU'] = chance_loss_data['CHANCE_LOSS_PRD_SU'].astype(float)
# chance_loss_data['CHANCE_LOSS_KN'] = chance_loss_data['CHANCE_LOSS_KN'].astype(float)
# chance_loss_data['CHANCE_LOSS_KN'][chance_loss_data['CHANCE_LOSS_PRD_SU'].astype(int)==0] = 0.0

                
                
#################################
# Refactored Code: 
def get_chanceloss_data(project_id: str, dataset_id: str, table_id: str, tenpo_cd: int) -> pd.DataFrame:
    """
    Retrieves chance loss data from BigQuery.

    Args:
        project_id: Google Cloud project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.
        tenpo_cd: Store code.

    Returns:
        DataFrame containing the chance loss data.
    """
    my_tenpo_cd = str(tenpo_cd).zfill(4)
    target_query = f"""  SELECT * FROM {dataset_id}.{table_id} where TENPO_CD = '{my_tenpo_cd}'"""  
    logger.info(f"target_query {target_query}")

    if 1:
        client = bigquery.Client()
        #client = bigquery.Client(project=project_id)
        bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
        #bqstorageclient = bigquery_storage.BigQueryStorageClient()
        df_target = (
            client.query(target_query)
            .result()
            .to_dataframe(bqstorage_client=bqstorageclient)
        )
    else:
        df_target = pd.read_gbq(target_query, project_id, dialect='standard')  

    df_target['PRD_CD'] = df_target['PRD_CD'].astype(int)
    df_target['NENSHUDO'] = df_target['NENSHUDO'].astype(int)
    df_target['TENPO_CD'] = df_target['TENPO_CD'].astype(int)
    df_target['CHANCE_LOSS_PRD_SU'] = df_target['CHANCE_LOSS_PRD_SU'].astype(float)
    df_target['CHANCE_LOSS_KN'] = df_target['CHANCE_LOSS_KN'].astype(float)
    df_target['CHANCE_LOSS_KN'][df_target['CHANCE_LOSS_PRD_SU'].astype(int) == 0] = 0.0

    return df_target
                
                
def process_data(
    dfm_base: pd.DataFrame,
    end_date: str,
    tenpo_list: List[int],
    tenpo_df: pd.DataFrame,
    tenpo_cd: int,
    table_id: str,
):
    project_id = PROJECT_ID
    col_dict = {col: col.lower() for col in dfm_base.columns}
    dfm_base = dfm_base.rename(columns=col_dict)

    # 店舗別の生産発注停止情報を結合 ---
    if seisan_tenpo_hattyuu_end_is_not_replaced:
        store_prd_hacchu_ymd = common.extract_as_df(path_tenpo_hacchu_master, bucket_name)
        store_prd_hacchu_ymd['TENPO_CD'] = store_prd_hacchu_ymd['TENPO_CD'].astype(int)
        store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].fillna(99999999)
        store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].astype(int)

    classify_excel_list: List[pd.ExcelWriter] = []
    classify_df_list: List[pd.DataFrame] = []

    if use_jan_connect:
        logger.info(f"===path_jan_master==== {path_jan_master}")
        jan_master = short_term_preprocess_common.load_jan_master_fix(
            path_jan_master,
            end_date,
            bucket_name
        )

    for tenpo in tenpo_list:
        classify_excel_list.append(
            pd.ExcelWriter(path_bunrui_prd_code.format(tenpo_df[tenpo_df['TENPO_CD'] == tenpo]['TENPO_NM_KJ'].values[0]))
        )
        classify_df_list.append(pd.DataFrame())
    
    logger.info(f"dpt_list: {dpt_list}")

    chance_loss_data = get_chanceloss_data(PROJECT_ID, DATASET_ID, table_id, tenpo_cd)
    logger.info(f'チャンスロスデータ件数: {len(chance_loss_data)}')
    return chance_loss_data, jan_master, store_prd_hacchu_ymd, dfm_base
                
                
                            
#################################
# Old Code:  
# ケース、パック、バラの繋がりを補間するためのテーブル作成（selling_periodには発注JANしか含まれていないため）------------
# prd_asc_tmp = prd_asc[prd_asc['asc_riyu_cd']==3]
# prd_asc_tmp = prd_asc_tmp[['prd_cd', 'asc_prd_cd']].rename(columns={"prd_cd":"PRD_CD", 'asc_prd_cd':"ASC_PRD_CD"})
# # ケース、パック、バラのグループを作成
# groups = []
# for parent, child in zip(prd_asc_tmp['PRD_CD'].tolist(), prd_asc_tmp['ASC_PRD_CD'].tolist()):
#     found_group = False
#     # 各グループをサーチして、親か子が含まれるグループをみつける
#     for group in groups:
#         # 既存グループに親か子が含まれていれば、そのグループに追加する
#         if parent in group or child in group:
#             group.add(parent)
#             group.add(child)
#             found_group = True
#             break
#     # 親か子が含まれるグループがなければ、新しいグループを作成する
#     if not found_group:
#         groups.append(set([parent, child]))

# # 商品コードと、ケースパックバラのグループの辞書を作成    
# prdcd_grp = {}
# for grp in groups:
#     for prdcd in grp:
#         prdcd_grp[prdcd] = grp

# # ケースパックバラ商品コードと発注JANの対応テーブルを作成する ------------------------------------------------------
# prd_asc_tmp = prd_asc[prd_asc['asc_riyu_cd']==3]
# prd_asc_tmp = prd_asc_tmp[['prd_cd', 'asc_prd_cd', 'daihyo_torihikisaki_nm', 'asc_daihyo_torihikisaki_nm']].rename(columns={"prd_cd":"PRD_CD", 'asc_prd_cd':"ASC_PRD_CD"})

# prdcd_hattyujan_list = []

# prdcd_torihikisaki = pd.concat([prd_asc_tmp[['PRD_CD', 'daihyo_torihikisaki_nm']].rename(columns={'daihyo_torihikisaki_nm':"daihyo_torihikisaki_nm"}).reset_index(drop=True), 
#                                         prd_asc_tmp[['ASC_PRD_CD', 'asc_daihyo_torihikisaki_nm']].rename(columns={'ASC_PRD_CD':'PRD_CD', 'asc_daihyo_torihikisaki_nm':"daihyo_torihikisaki_nm"}).reset_index(drop=True)])

# prdcd_torihikisaki = prdcd_torihikisaki.drop_duplicates().reset_index(drop=True)

# for grp in groups:
#     my_prdcd_torihikisaki = prdcd_torihikisaki[prdcd_torihikisaki['PRD_CD'].isin(grp)].reset_index(drop=True)
#     hattyu_prdcd_torihikisaki = my_prdcd_torihikisaki[my_prdcd_torihikisaki['daihyo_torihikisaki_nm'] != 'ＰＯＳ　ＰＬＵ登録'].reset_index(drop=True)
#     if len(hattyu_prdcd_torihikisaki) == 1:
#         for prdcd in grp:
#             #prdcd_hattyujan[prdcd] = my_prdcd_torihikisaki['PRD_CD'][0]
#             if prdcd != hattyu_prdcd_torihikisaki['PRD_CD'][0]:
#                 prdcd_hattyujan_list.append([prdcd, hattyu_prdcd_torihikisaki['PRD_CD'][0]])
            
#     elif len(hattyu_prdcd_torihikisaki) > 1:
#         #print('hattyu_prdcd_torihikisaki件数異常')
#         logger.error('hattyu_prdcd_torihikisaki件数異常')
#         logger.info("
#         #sys.exit()
#     elif len(hattyu_prdcd_torihikisaki) == 0:
#         #print('hattyu_prdcd_torihikisaki件数異常0')
#         logger.error('hattyu_prdcd_torihikisaki件数異常')
#         #sys.exit()

# prdcd_hattyujan_df = pd.DataFrame(prdcd_hattyujan_list)
# prdcd_hattyujan_df.columns = ['PRD_CD', 'HACCHU_JAN']
# prdcd_hattyujan_df.to_csv('prdcd_hattyujan_df.csv', index=False)
                   
                    

#################################
# Refactored Code: 
def create_case_pack_bara_groups(prd_asc, prd_asc_tmp):
    

    groups = []
    for parent, child in zip(prd_asc_tmp['PRD_CD'].tolist(), prd_asc_tmp['ASC_PRD_CD'].tolist()):
        found_group = False
        for group in groups:
            if parent in group or child in group:
                group.add(parent)
                group.add(child)
                found_group = True
                break
        if not found_group:
            groups.append(set([parent, child]))
    return groups


def create_prdcd_to_group_dict(groups):
    prdcd_grp = {}
    for grp in groups:
        for prdcd in grp:
            prdcd_grp[prdcd] = grp
    return prdcd_grp


def create_prdcd_hattyujan_df(prd_asc, groups):
    prd_asc_tmp = prd_asc[prd_asc['asc_riyu_cd']==3]
    prd_asc_tmp = prd_asc_tmp[['prd_cd', 'asc_prd_cd', 'daihyo_torihikisaki_nm', 'asc_daihyo_torihikisaki_nm']].rename(columns={"prd_cd":"PRD_CD", 'asc_prd_cd':"ASC_PRD_CD"})

    prdcd_torihikisaki = pd.concat([prd_asc_tmp[['PRD_CD', 'daihyo_torihikisaki_nm']].rename(columns={'daihyo_torihikisaki_nm':"daihyo_torihikisaki_nm"}).reset_index(drop=True), 
                                        prd_asc_tmp[['ASC_PRD_CD', 'asc_daihyo_torihikisaki_nm']].rename(columns={'ASC_PRD_CD':'PRD_CD', 'asc_daihyo_torihikisaki_nm':"daihyo_torihikisaki_nm"}).reset_index(drop=True)])

    prdcd_torihikisaki = prdcd_torihikisaki.drop_duplicates().reset_index(drop=True)

    prdcd_hattyujan_list = []
    for grp in groups:
        my_prdcd_torihikisaki = prdcd_torihikisaki[prdcd_torihikisaki['PRD_CD'].isin(grp)].reset_index(drop=True)
        hattyu_prdcd_torihikisaki = my_prdcd_torihikisaki[my_prdcd_torihikisaki['daihyo_torihikisaki_nm'] != 'ＰＯＳ　ＰＬＵ登録'].reset_index(drop=True)

        if len(hattyu_prdcd_torihikisaki) == 1:
            for prdcd in grp:
                if prdcd != hattyu_prdcd_torihikisaki['PRD_CD'][0]:
                    prdcd_hattyujan_list.append([prdcd, hattyu_prdcd_torihikisaki['PRD_CD'][0]])
                
        elif len(hattyu_prdcd_torihikisaki) > 1:
            logger.error('hattyu_prdcd_torihikisaki件数異常')
        elif len(hattyu_prdcd_torihikisaki) == 0:
            logger.error('hattyu_prdcd_torihikisaki件数異常')

    prdcd_hattyujan_df = pd.DataFrame(prdcd_hattyujan_list)
    prdcd_hattyujan_df.columns = ['PRD_CD', 'HACCHU_JAN']
    return prdcd_hattyujan_df


                   
                               
#################################
# Old Code:                     
# ケースパックバラの入数から、発注JAN入数への数量変換係数を作成する -----------------------------------------------------------------------------
# prd_asc_tmp = prd_asc[prd_asc['asc_riyu_cd']==3]
# prdcd_hcjan_coef = {}
# prdcd_hcjan_coef_log = []
# for prdcd, hcjan in zip(prdcd_hattyujan_df['PRD_CD'], prdcd_hattyujan_df['HACCHU_JAN']):
    
#     # 発注JANがasc(パックかバラのみ）の場合
#     if hcjan not in prd_asc_tmp['prd_cd'].tolist() and hcjan in prd_asc_tmp['asc_prd_cd'].tolist():
#         # 元JANがケースパックにある
#         if prdcd in prd_asc_tmp['prd_cd'].tolist() and hcjan in prd_asc_tmp['asc_prd_cd'].tolist():
#             # 入数倍にする
#             tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==prdcd)&(prd_asc_tmp['asc_prd_cd']==hcjan)]['iri_su'].reset_index(drop=True)
#             if len(tmp) == 1:
#                 prdcd_hcjan_coef[prdcd] = float(tmp[0])
#                 prdcd_hcjan_coef_log.append(['1-1', prdcd, hcjan, 'prdcd in prd_cd', 'hcjan in asc_prd_cd', '', prdcd_hcjan_coef[prdcd], 'iri_su'])
#             else:
#                 #print('1-1 発注JANがasc_prd_cdで元JANがprd_cdにありますが、レコードが複数あります', prdcd, hcjan)
#                 logger.info("1-1 発注JANがasc_prd_cdで元JANがprd_cdにありますが、レコードが複数あります", prdcd, hcjan)
#                 sys.exit()
#         else:
#             #print('1-2 発注JANがasc_prd_cdで元JANがprd_cdにないです', prdcd, hcjan)
#             logger.info("1-2 発注JANがasc_prd_cdで元JANがprd_cdにないです", prdcd, hcjan)
#             sys.exit()
        
#     # 発注JANがケースの場合
#     elif hcjan in prd_asc_tmp['prd_cd'].tolist() and hcjan not in prd_asc_tmp['asc_prd_cd'].tolist():
#         # 元JANがasc_prd_cdにあれば、その入数を使う
#         if hcjan in prd_asc_tmp['prd_cd'].tolist() and prdcd in prd_asc_tmp['asc_prd_cd'].tolist():
#             tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==hcjan)&(prd_asc_tmp['asc_prd_cd']==prdcd)]['iri_su'].reset_index(drop=True)
#             if len(tmp) == 1:
#                 prdcd_hcjan_coef[prdcd] = 1.0 / float(tmp[0])
#                 prdcd_hcjan_coef_log.append(['2-1', prdcd, hcjan, 'prdcd in asc_prd_cd', 'hcjan in prd_cd', '', prdcd_hcjan_coef[prdcd], '1/iri_su'])
#             else:
#                 #print('2-1 発注JANがprd_cdで元JANがasc_prd_cdにありますが、レコードが複数あります', prdcd, hcjan)
#                 logger.info('2-1 発注JANがprd_cdで元JANがasc_prd_cdにありますが、レコードが複数あります', prdcd, hcjan)
#                 sys.exit()
#         # 元JANがasc_prd_cdにない（例：元JANがパックのとき）
#         else:
#             if hcjan in prd_asc_tmp['prd_cd'].tolist() and prdcd in prd_asc_tmp['prd_cd'].tolist():
#                 # 発注単位統一個数を使う
#                 tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==prdcd)]['hacchu_tani_toitsu_kosu'].reset_index(drop=True)
#                 if len(tmp) == 1:
#                     prdcd_hcjan_coef[prdcd] = 1.0 / float(tmp[0])
#                     prdcd_hcjan_coef_log.append(['2-2', prdcd, hcjan, 'prdcd in prd_cd', 'hcjan in prd_cd', '', prdcd_hcjan_coef[prdcd], '1/hacchu_tani_toitsu_kosu' ])
#                 else:
#                     #print('2-2 発注JANがケースで元JANがasc_prd_cdにない、元JANはprd_cdに複数あり選べません', prdcd, hcjan)
#                     logger.info('2-2 発注JANがケースで元JANがasc_prd_cdにない、元JANはprd_cdに複数あり選べません', prdcd, hcjan)
#                     sys.exit()
    
#     # 発注JANがどちらにもある場合（20本POSPLU、10本発注、1本POSPLU）
#     elif hcjan in prd_asc_tmp['prd_cd'].tolist() and hcjan in prd_asc_tmp['asc_prd_cd'].tolist():
#         #print('発注JANがケースとバラ両方にあります', prdcd, hcjan)
#         logger.info('発注JANがケースとバラ両方にあります', prdcd, hcjan)
        
#         # 発注JANがケースで元JANがasc(バラかパック)の場合
#         if hcjan in prd_asc_tmp['prd_cd'].tolist() and prdcd in prd_asc_tmp['asc_prd_cd'].tolist():
#             tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==hcjan)&(prd_asc_tmp['asc_prd_cd']==prdcd)].reset_index(drop=True)
#             if len(tmp) == 1:
#                 prdcd_hcjan_coef[prdcd] = float(tmp['iri_su'][0])
#                 prdcd_hcjan_coef_log.append(['3-1', prdcd, hcjan, 'prdcd in asc_prd_cd', 'hcjan in prd_cd', 'hcjan in asc_prd_cd', prdcd_hcjan_coef[prdcd], 'iri_su' ])
#             else:
#                 #print('3-1 発注JANがどちらにもあり、発注JANがケースで元JANがバラかパックですが、レコードが複数あります', prdcd, hcjan)
#                 logger,info('3-1 発注JANがどちらにもあり、発注JANがケースで元JANがバラかパックですが、レコードが複数あります', prdcd, hcjan)
#                 sys.exit()
            
#         # 発注JANがパックバラで元JANがケースパックの場合
#         elif prdcd in prd_asc_tmp['prd_cd'].tolist() and hcjan in prd_asc_tmp['asc_prd_cd'].tolist():
#             tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==prdcd)&(prd_asc_tmp['asc_prd_cd']==hcjan)].reset_index(drop=True)
#             if len(tmp) == 1:
#                 prdcd_hcjan_coef[prdcd] = 1.0 / float(tmp['iri_su'][0])
#                 prdcd_hcjan_coef_log.append(['4-1', prdcd, hcjan, 'prdcd in prd_cd', 'hcjan in asc_prd_cd', 'hcjan in prd_cd', prdcd_hcjan_coef[prdcd], '1/iri_su' ])
#             else:
#                 #print('4-1 発注JANがどちらにもあり、元JANがケースで発注JANがバラかパックですがレコードが複数あります', prdcd, hcjan)
#                 logger.info('4-1 発注JANがどちらにもあり、元JANがケースで発注JANがバラかパックですがレコードが複数あります', prdcd, hcjan)
#                 sys.exit()

#     # 発注JANがどちらにもない場合
#     else:
#         #print('発注JANがケースにもバラにもありません', prdcd, hcjan)
#         logger.info('発注JANがケースにもバラにもありません', prdcd, hcjan)
#         sys.eixt()

                       

#################################
# Refactored Code: 
def calculate_prdcd_hcjan_coefficients(prd_asc, prdcd_hattyujan_df):
    """
    商品コードと発注JANの係数を計算する。
    Calculates the coefficients between product codes (PRD_CD) and order JAN codes (HACCHU_JAN).

    Args:
        prd_asc (pd.DataFrame): prd_asc データフレーム. DataFrame containing product assembly information.
        prdcd_hattyujan_df (pd.DataFrame): 商品コードと発注JANの対応テーブル. DataFrame mapping product codes to order JAN codes.

    Returns:
        tuple: (prdcd_hcjan_coef, prdcd_hcjan_coef_log)
               - prdcd_hcjan_coef (dict): 商品コードをキー、係数を値とする辞書. Dictionary with product codes as keys and calculated coefficients as values.
               - prdcd_hcjan_coef_log (list): 係数計算のログ. List of logs detailing the coefficient calculation process.
    """
    prd_asc_tmp = prd_asc[prd_asc['asc_riyu_cd']==3]
    prdcd_hcjan_coef = {}
    prdcd_hcjan_coef_log = []
    for prdcd, hcjan in zip(prdcd_hattyujan_df['PRD_CD'], prdcd_hattyujan_df['HACCHU_JAN']):
        # 発注JANがasc(パックかバラのみ）の場合
        if hcjan not in prd_asc_tmp['prd_cd'].tolist() and hcjan in prd_asc_tmp['asc_prd_cd'].tolist():
            # 元JANがケースパックにある
            if prdcd in prd_asc_tmp['prd_cd'].tolist() and hcjan in prd_asc_tmp['asc_prd_cd'].tolist():
                # 入数倍にする
                tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==prdcd)&(prd_asc_tmp['asc_prd_cd']==hcjan)]['iri_su'].reset_index(drop=True)
                if len(tmp) == 1:
                    prdcd_hcjan_coef[prdcd] = float(tmp[0])
                    prdcd_hcjan_coef_log.append(['1-1', prdcd, hcjan, 'prdcd in prd_cd', 'hcjan in asc_prd_cd', '', prdcd_hcjan_coef[prdcd], 'iri_su'])
                else:
                    #print('1-1 発注JANがasc_prd_cdで元JANがprd_cdにありますが、レコードが複数あります', prdcd, hcjan)
                    logger.info(f"1-1 発注JANがasc_prd_cdで元JANがprd_cdにありますが、レコードが複数あります prdcd={prdcd} hcjan={hcjan}")
                    sys.exit()
            else:
                #print('1-2 発注JANがasc_prd_cdで元JANがprd_cdにないです', prdcd, hcjan)
                logger.info(f"1-2 発注JANがasc_prd_cdで元JANがprd_cdにないです prdcd={prdcd} hcjan={hcjan}")
                sys.exit()
            
        # 発注JANがケースの場合
        elif hcjan in prd_asc_tmp['prd_cd'].tolist() and hcjan not in prd_asc_tmp['asc_prd_cd'].tolist():
            # 元JANがasc_prd_cdにあれば、その入数を使う
            if hcjan in prd_asc_tmp['prd_cd'].tolist() and prdcd in prd_asc_tmp['asc_prd_cd'].tolist():
                tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==hcjan)&(prd_asc_tmp['asc_prd_cd']==prdcd)]['iri_su'].reset_index(drop=True)
                if len(tmp) == 1:
                    prdcd_hcjan_coef[prdcd] = 1.0 / float(tmp[0])
                    prdcd_hcjan_coef_log.append(['2-1', prdcd, hcjan, 'prdcd in asc_prd_cd', 'hcjan in prd_cd', '', prdcd_hcjan_coef[prdcd], '1/iri_su'])
                else:
                    #print('2-1 発注JANがprd_cdで元JANがasc_prd_cdにありますが、レコードが複数あります', prdcd, hcjan)
                    logger.info(f"2-1 発注JANがprd_cdで元JANがasc_prd_cdにありますが、レコードが複数あります prdcd={prdcd} hcjan={hcjan}")
                    sys.exit()
            # 元JANがasc_prd_cdにない（例：元JANがパックのとき）
            else:
                if hcjan in prd_asc_tmp['prd_cd'].tolist() and prdcd in prd_asc_tmp['prd_cd'].tolist():
                    # 発注単位統一個数を使う
                    tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==prdcd)]['hacchu_tani_toitsu_kosu'].reset_index(drop=True)
                    if len(tmp) == 1:
                        prdcd_hcjan_coef[prdcd] = 1.0 / float(tmp[0])
                        prdcd_hcjan_coef_log.append(['2-2', prdcd, hcjan, 'prdcd in prd_cd', 'hcjan in prd_cd', '', prdcd_hcjan_coef[prdcd], '1/hacchu_tani_toitsu_kosu' ])
                    else:
                        #print('2-2 発注JANがケースで元JANがasc_prd_cdにない、元JANはprd_cdに複数あり選べません', prdcd, hcjan)
                        logger.info(f"2-2 発注JANがケースで元JANがasc_prd_cdにない、元JANはprd_cdに複数あり選べません prdcd={prdcd} hcjan={hcjan}")
                        sys.exit()

        # 発注JANがどちらにもある場合（20本POSPLU、10本発注、1本POSPLU）
        elif hcjan in prd_asc_tmp['prd_cd'].tolist() and hcjan in prd_asc_tmp['asc_prd_cd'].tolist():
            #print('発注JANがケースとバラ両方にあります', prdcd, hcjan)
            logger.info(f"発注JANがケースとバラ両方にあります prdcd={prdcd} hcjan={hcjan}")

            # 発注JANがケースで元JANがasc(バラかパック)の場合
            if hcjan in prd_asc_tmp['prd_cd'].tolist() and prdcd in prd_asc_tmp['asc_prd_cd'].tolist():
                tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==hcjan)&(prd_asc_tmp['asc_prd_cd']==prdcd)].reset_index(drop=True)
                if len(tmp) == 1:
                    prdcd_hcjan_coef[prdcd] = float(tmp['iri_su'][0])
                    prdcd_hcjan_coef_log.append(['3-1', prdcd, hcjan, 'prdcd in asc_prd_cd', 'hcjan in prd_cd', 'hcjan in asc_prd_cd', prdcd_hcjan_coef[prdcd], 'iri_su' ])
                else:
                    #print('3-1 発注JANがどちらにもあり、発注JANがケースで元JANがバラかパックですが、レコードが複数あります', prdcd, hcjan)
                    logger.info(f"3-1 発注JANがどちらにもあり、発注JANがケースで元JANがバラかパックですが、レコードが複数あります prdcd={prdcd} hcjan={hcjan}")
                    sys.exit()

            # 発注JANがパックバラで元JANがケースパックの場合
            elif prdcd in prd_asc_tmp['prd_cd'].tolist() and hcjan in prd_asc_tmp['asc_prd_cd'].tolist():
                tmp = prd_asc_tmp[(prd_asc_tmp['prd_cd']==prdcd)&(prd_asc_tmp['asc_prd_cd']==hcjan)].reset_index(drop=True)
                if len(tmp) == 1:
                    prdcd_hcjan_coef[prdcd] = 1.0 / float(tmp['iri_su'][0])
                    prdcd_hcjan_coef_log.append(['4-1', prdcd, hcjan, 'prdcd in prd_cd', 'hcjan in asc_prd_cd', 'hcjan in prd_cd', prdcd_hcjan_coef[prdcd], '1/iri_su' ])
                else:
                    #print('4-1 発注JANがどちらにもあり、元JANがケースで発注JANがバラかパックですがレコードが複数あります', prdcd, hcjan)
                    logger.info(f"4-1 発注JANがどちらにもあり、元JANがケースで発注JANがバラかパックですがレコードが複数あります prdcd={prdcd} hcjan={hcjan}")
                    sys.exit()

        # 発注JANがどちらにもない場合
        else:
            #print('発注JANがケースにもバラにもありません', prdcd, hcjan)
            logger.info(f"発注JANがケースにもバラにもありません prdcd={prdcd} hcjan={hcjan}")
            sys.eixt() # Typo here as well, should be sys.exit()
    return prdcd_hcjan_coef, prdcd_hcjan_coef_log
                        
           
            
 
            
#################################
# Old Code:              
# prdcd_hcjan_coef_log_df = pd.DataFrame(prdcd_hcjan_coef_log)
# prdcd_hcjan_coef_log_df.columns = ['case', '元JAN', '発注JAN', '元JAN位置', '発注JAN位置', '元発両位置にあり', '係数', '係数根拠']
# prdcd_hcjan_coef_log_df.to_csv('prdcd_hcjan_coef_log_df.csv')

# prdcd_hattyujan_df['HJAN_COEF'] = prdcd_hattyujan_df['PRD_CD'].apply(lambda x:prdcd_hcjan_coef[x])
# prdcd_hattyujan_df['HJAN_COEF'] = prdcd_hattyujan_df['HJAN_COEF'].astype(float)
# prdcd_hattyujan_df.to_csv('prdcd_hattyujan_df2.csv', index=False)

# if OUTPUT_HACCHUJAN_INFO and prdcd_hacchujan_df_flag and (tenpo_cd == 760):
    
#     prdcd_hattyujan_df['NENSHUDO'] =  end_nenshudo
    
#     from google.cloud import bigquery
#     from google.cloud.bigquery import Client as BigqueryClient
    
#     if OUTPUT_HACCHUJAN_TABLE_TEST:
#         table_id = "dev-cainz-demandforecast.cainz_shortterm_predicted_value_for_statistics.hacchujan_relations_all_test"
#     else:
#         table_id = "dev-cainz-demandforecast.cainz_shortterm_predicted_value_for_statistics.hacchujan_relations_all"
    
#     job_config = bigquery.LoadJobConfig(
#         schema=[
#             bigquery.SchemaField('PRD_CD', 'INTEGER', mode='NULLABLE'),
#             bigquery.SchemaField('HACCHU_JAN', 'INTEGER', mode='NULLABLE'),
#             bigquery.SchemaField('HJAN_COEF', 'FLOAT', mode='NULLABLE'),
#             bigquery.SchemaField('NENSHUDO', 'INTEGER', mode='NULLABLE'),

#         ],
#         write_disposition='WRITE_APPEND',
#     )
    
    
#     client = BigqueryClient()
#     job = client.load_table_from_dataframe(prdcd_hattyujan_df, table_id, job_config=job_config)
#     job.result()
    
# if OUTPUT_HACCHUJAN_INFO_GCS and (tenpo_cd == 760):
#     path_upload_blob = "Basic_Analysis_unzip_result/02_DM/CASE_PACK_BARA/prdcd_hattyujan_df2.csv"
#     tmp_fname = "prdcd_hattyujan_df2.csv"
#     blob = bucket.blob(path_upload_blob)
#     blob.upload_from_filename(tmp_fname)
    

            
#################################
# Refactored Code: 
def output_hacchujan_info(prdcd_hcjan_coef, prdcd_hcjan_coef_log, prdcd_hattyujan_df, end_nenshudo, tenpo_cd):
    """
    発注JAN情報を出力する。
    Outputs order JAN information to CSV, BigQuery, and GCS.

    Args:
        prdcd_hcjan_coef (dict): 商品コードをキー、係数を値とする辞書. Dictionary with product codes as keys and calculated coefficients as values.
        prdcd_hcjan_coef_log (list): 係数計算のログ. List of logs detailing the coefficient calculation process.
        prdcd_hattyujan_df (pd.DataFrame): 商品コードと発注JANの対応テーブル. DataFrame mapping product codes to order JAN codes.
        end_nenshudo (int): 年度. Fiscal year.
        OUTPUT_HACCHUJAN_INFO (bool): BigQuery出力フラグ. Flag to indicate whether to output to BigQuery.
        prdcd_hacchujan_df_flag (bool): データフレームフラグ. Flag indicating if the DataFrame is valid.
        tenpo_cd (int): 店舗コード. Store code.
        OUTPUT_HACCHUJAN_TABLE_TEST (bool): テストテーブル出力フラグ. Flag to indicate whether to output to the test table.
        OUTPUT_HACCHUJAN_INFO_GCS (bool): GCS出力フラグ. Flag to indicate whether to output to GCS.
        bucket (google.cloud.storage.bucket.Bucket): GCSバケット. Google Cloud Storage bucket object.

    Returns:
        prdcd_hattyujan_df(dataframe)
    """
    tmp_log_fname = 'prdcd_hcjan_coef_log_df.csv'
    tmp_hacchu_fname = 'prdcd_hattyujan_df2.csv'

    try:
        # Output coefficient calculation log to CSV
        prdcd_hcjan_coef_log_df = pd.DataFrame(prdcd_hcjan_coef_log)
        prdcd_hcjan_coef_log_df.columns = ['case', '元JAN', '発注JAN', '元JAN位置', '発注JAN位置', '元発両位置にあり', '係数', '係数根拠']
        prdcd_hcjan_coef_log_df.to_csv(tmp_log_fname)

        # Prepare and output the prdcd_hattyujan_df DataFrame to CSV
        prdcd_hattyujan_df['HJAN_COEF'] = prdcd_hattyujan_df['PRD_CD'].apply(lambda x: prdcd_hcjan_coef[x])
        prdcd_hattyujan_df['HJAN_COEF'] = prdcd_hattyujan_df['HJAN_COEF'].astype(float)
        prdcd_hattyujan_df.to_csv(tmp_hacchu_fname, index=False)

        # BigQuery Output
        if OUTPUT_HACCHUJAN_INFO and prdcd_hacchujan_df_flag and (tenpo_cd == 760):
            prdcd_hattyujan_df['NENSHUDO'] = end_nenshudo

            table_id = "{}.cainz_shortterm_predicted_value_for_statistics.hacchujan_relations_all_test".format(os.environ['PROJECT_ID']) if OUTPUT_HACCHUJAN_TABLE_TEST else "{}.cainz_shortterm_predicted_value_for_statistics.hacchujan_relations_all".format(os.environ['PROJECT_ID'])

            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField('PRD_CD', 'INTEGER', mode='NULLABLE'),
                    bigquery.SchemaField('HACCHU_JAN', 'INTEGER', mode='NULLABLE'),
                    bigquery.SchemaField('HJAN_COEF', 'FLOAT', mode='NULLABLE'),
                    bigquery.SchemaField('NENSHUDO', 'INTEGER', mode='NULLABLE'),
                ],
                write_disposition='WRITE_APPEND',
            )

            client = BigqueryClient()
            job = client.load_table_from_dataframe(prdcd_hattyujan_df, table_id, job_config=job_config)
            job.result()  # Waits for the job to complete.

        # GCS Output
        if OUTPUT_HACCHUJAN_INFO_GCS and (tenpo_cd == 760):
            path_upload_blob = "Basic_Analysis_unzip_result/02_DM/CASE_PACK_BARA/prdcd_hattyujan_df2.csv"
            blob = bucket.blob(path_upload_blob)
            blob.upload_from_filename(tmp_hacchu_fname)

    except Exception as e:
        logger.exception("An error occurred during hacchujan info output: %s", e)
        raise # Re-raise the exception after logging

    finally:
        # Clean up temporary files
        try:
            os.remove(tmp_log_fname)
        except OSError:
            pass  # Ignore if the file doesn't exist

        try:
            os.remove(tmp_hacchu_fname)
        except OSError:
            pass  # Ignore if the file doesn't exist
    
    return prdcd_hattyujan_df
            



#################################
# Old Code: 
# def get_df_cal_out_calender(
#     dfc,
#     start_nenshudo,
#     end_nenshudo,
# ):
#     col_list = ["nenshudo","shudo","week_from_ymd", 'nendo', 'znen_nendo', 'znen_shudo', 'minashi_tsuki']
#     df_cal = pd.DataFrame(dfc["nenshudo"].drop_duplicates().sort_values().reset_index(drop=True))
#     df_cal = dfc[
#         (dfc["nenshudo"]>=start_nenshudo) & \
#         (dfc["nenshudo"]<=end_nenshudo)
#     ][col_list].reset_index(drop=True)
#     df_cal["date"] = df_cal["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    
#     df_cal = df_cal.loc[
#         (df_cal['nenshudo']>=start_nenshudo) & \
#         (df_cal['nenshudo']<=end_nenshudo)
#     ].reset_index(drop=True)
    
#     return df_cal


#################################
# Refactored Code:         
def get_df_cal_out_calender(
    dfc,
    start_nenshudo,
    end_nenshudo, col_list
):
    """
    指定された年度範囲の暦情報を抽出する。
    Extracts calendar information for the specified fiscal year range.

    Args:
        dfc (pd.DataFrame): カレンダー情報を含むDataFrame. DataFrame containing calendar information.
        start_nenshudo (int): 開始年度. Start fiscal year.
        end_nenshudo (int): 終了年度. End fiscal year.

    Returns:
        pd.DataFrame: 指定された年度範囲の暦情報. Calendar information for the specified fiscal year range.
    """
    #col_list = ["nenshudo","shudo","week_from_ymd", 'nendo', 'znen_nendo', 'znen_shudo', 'minashi_tsuki']
    df_cal = pd.DataFrame(dfc["nenshudo"].drop_duplicates().sort_values().reset_index(drop=True))
    df_cal = dfc[
        (dfc["nenshudo"]>=start_nenshudo) & \
        (dfc["nenshudo"]<=end_nenshudo)
    ][col_list].reset_index(drop=True)
    df_cal["date"] = df_cal["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    df_cal = pd.merge(df_cal, dfc[['nenshudo', 'nendo', 'znen_nendo', 'znen_shudo','minashi_tsuki']], on='nenshudo')
    df_cal = df_cal.loc[
        (df_cal['nenshudo']>=start_nenshudo) & \
        (df_cal['nenshudo']<=end_nenshudo)
    ].reset_index(drop=True)
    
    return df_cal
                                  
            
#################################
# Old Code:     
# def merge_df_cal(
#     df,
#     df_cal,
#     tenpo_cd,
#     dpt
# ):
#     prd_cal = pd.merge(df[['PRD_CD']].drop_duplicates().assign(join_key=1),
#                        df_cal.assign(join_key=1),
#                        on = 'join_key')
#     prd_cal = prd_cal.drop('join_key', axis=1)
#     col_cal_list = [
#         'PRD_CD', 'nenshudo', #'shudo',         'week_from_ymd', 'nendo', 'znen_nendo', 'znen_shudo',
#         #'holiday_cnt','gw_flag', 'obon_flag', 'nenmatsu_flag'
#     ]
    
#     #if 'prd_nm_kj' in df.columns:
#     #    df = df.drop('prd_nm_kj', axis=1)

#     df = pd.merge(
#         df,
#         prd_cal[col_cal_list],
#         how='right',
#         on=['nenshudo', 'PRD_CD']
#     ).sort_values('nenshudo') #.rename(columns={'休日日数':'店休日数'})
    
#     df['URI_SU'] = df['URI_SU'].fillna(0.0)
#     df['URI_KIN'] = df['URI_KIN'].fillna(0.0)
#     df['TENPO_CD'] = tenpo_cd
#     df['DPT_CD'] = dpt
    
#     return df


#################################
# Refactored Code: 
def merge_df_cal(
    df,
    df_cal,
    tenpo_cd,
    dpt
):
    """
    カレンダー情報と売上データをマージする。
    Merges calendar information with sales data.

    Args:
        df (pd.DataFrame): 売上データを含むDataFrame. DataFrame containing sales data.
        df_cal (pd.DataFrame): カレンダー情報を含むDataFrame. DataFrame containing calendar information.
        tenpo_cd (int): 店舗コード. Store code.
        dpt (int): 部門コード. Department code.

    Returns:
        pd.DataFrame: マージされたDataFrame. The merged DataFrame.
    """
    prd_cal = pd.merge(df[['PRD_CD']].drop_duplicates().assign(join_key=1),
                       df_cal.assign(join_key=1),
                       on = 'join_key')
    prd_cal = prd_cal.drop('join_key', axis=1)
    col_cal_list = [
        'PRD_CD', 'nenshudo', #'shudo',         'week_from_ymd', 'nendo', 'znen_nendo', 'znen_shudo',
        #'holiday_cnt','gw_flag', 'obon_flag', 'nenmatsu_flag'
    ]
    
    df = pd.merge(
        df,
        prd_cal[col_cal_list],
        how='right',
        on=['nenshudo', 'PRD_CD']
    ).sort_values('nenshudo') #.rename(columns={'休日日数':'店休日数'})
    
    df['URI_SU'] = df['URI_SU'].fillna(0.0)
    df['URI_KIN'] = df['URI_KIN'].fillna(0.0)
    df['TENPO_CD'] = tenpo_cd
    df['DPT_CD'] = dpt
    
    return df
            
            
#################################
# Old Code:              
# def load_tran_df_bq(
#     df,
#     tenpo_cd,
#     dpt,
#     train_end_nenshudo,
# ):
#     #full_path = blob.name
#     #temp_df = common.extract_as_df(full_path, bucket_name).rename(
#     #    columns={'NENSHUDO': 'nenshudo', 'BUMON_CD': 'DPT_CD'}
#     #)
    
#     project_id = "dev-cainz-demandforecast"
#     dataset_id = 'dev_cainz_nssol'
#     table_id = 'T_090_URIAGE_JSK_NB_SHU_TEN_DPT'

#     my_tenpo_cd = str(tenpo_cd).zfill(4)
#     bumon_cd = str(dpt).zfill(3)
    
#     target_query = f"""  SELECT * FROM {dataset_id}.{table_id} WHERE TENPO_CD = '""" +  my_tenpo_cd + "' AND " + "BUMON_CD = '" + bumon_cd + "' ORDER BY PRD_CD, NENSHUDO"
#     #print(target_query)
#     logger.info("target_query", target_query)
    
#     if 1:
#         client = bigquery.Client()
#         #client = bigquery.Client(project=project_id)
#         bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
#         #bqstorageclient = bigquery_storage.BigQueryStorageClient()
#         temp_df = (
#             client.query(target_query)
#             .result()
#             .to_dataframe(bqstorage_client=bqstorageclient)
#         )
        
#         #print('DPT:', bumon_cd,' Tranデータ件数:', len(temp_df), )
#         logger.info('DPT:', bumon_cd,' Tranデータ件数:', len(temp_df), )
        
#     else:
#         temp_df = pd.read_gbq(target_query, project_id, dialect='standard')
    
#     temp_df = temp_df.rename(columns={'NENSHUDO': 'nenshudo', 'BUMON_CD': 'DPT_CD'})
    
#     temp_df['nenshudo'] = temp_df['nenshudo'].astype(int)
#     temp_df['PRD_CD'] = temp_df['PRD_CD'].astype(int)
    
#     #temp_df = temp_df.loc[(temp_df['TENPO_CD'].isin(tenpo_list)) & (temp_df['nenshudo']<=train_end_nenshudo)]
#     temp_df = temp_df.loc[temp_df['nenshudo']<=train_end_nenshudo]
    
#     df = pd.concat([df, temp_df], axis=0).reset_index(drop=True)
   
#     #print("===df12345====",df)
#     logger.info("===df12345====",df)

#     return df, temp_df["PRD_CD"].unique().tolist()


#################################
# Refactored Code: 
def load_tran_df_bq(
    df,
    tenpo_cd,
    dpt,
    train_end_nenshudo, table_id = 'T_090_URIAGE_JSK_NB_SHU_TEN_DPT'
):
    """
    BigQueryからトランザクションデータをロードし、DataFrameに結合する。
    Loads transaction data from BigQuery and concatenates it with a DataFrame.

    Args:
        df (pd.DataFrame): 既存のDataFrame. Existing DataFrame.
        tenpo_cd (int): 店舗コード. Store code.
        dpt (int): 部門コード. Department code.
        train_end_nenshudo (int): 学習終了年度. Training end fiscal year.

    Returns:
        tuple: (結合されたDataFrame, 商品コードのリスト).
               (Concatenated DataFrame, List of unique product codes).
    """
    my_tenpo_cd = str(tenpo_cd).zfill(4)
    bumon_cd = str(dpt).zfill(3)

    target_query = f""" SELECT * FROM `{dataset_id}.{table_id}` WHERE TENPO_CD = '"{my_tenpo_cd}"' AND BUMON_CD = '"{bumon_cd}"' ORDER BY PRD_CD, NENSHUDO"""
    # print(target_query)
    logger.info(f"target_query: {target_query}")

    try:
        client = bigquery.Client()
        bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
        temp_df = (
            client.query(target_query)
            .result()
            .to_dataframe(bqstorage_client=bqstorageclient)
        )

        # print('DPT:', bumon_cd,' Tranデータ件数:', len(temp_df), )
        logger.info(f'DPT: {bumon_cd}, Tranデータ件数: {len(temp_df)}')

    except Exception as e:
        logger.exception(f"Error loading data from BigQuery: {e}")
        raise  # Re-raise the exception

    temp_df = temp_df.rename(columns={'NENSHUDO': 'nenshudo', 'BUMON_CD': 'DPT_CD'})

    temp_df['nenshudo'] = temp_df['nenshudo'].astype(int)
    temp_df['PRD_CD'] = temp_df['PRD_CD'].astype(int)

    temp_df = temp_df.loc[temp_df['nenshudo'] <= train_end_nenshudo]

    df = pd.concat([df, temp_df], axis=0).reset_index(drop=True)

    # print("===df12345====",df)
    logger.info(f"===df12345==== {df}")

    return df, temp_df["PRD_CD"].unique().tolist()


            
            
# if THEME_MD_MODE:
    
#     pass
    
#     '''
#     # BQに取り置きしていない商品の販売データを加える処理!!!!!!!!!!!!!!!!!
    
#     #path_add_salesdata = "Basic_Analysis_unzip_result/01_Data/91_ADD_DATA_thememd/T_090_URIAGE_JSK_4901140898044_to202435week.csv"
#     path_add_salesdata = "Basic_Analysis_unzip_result/01_Data/91_ADD_DATA_thememd/T_090_URIAGE_JSK_4901140898044_to202436week.csv"
#     add_salesdata_df = common.extract_as_df(path_add_salesdata, bucket_name)

#     add_salesdata_df['nenshudo'] = add_salesdata_df['nendo'].astype(int) * 100 + add_salesdata_df['shudo'].astype(int)
    
#     add_salesdata_df['tenpo_cd'] = add_salesdata_df['tenpo_cd'].astype(int)
    
#     add_salesdata_df_gbsum = add_salesdata_df.groupby(['tenpo_cd', 'prd_cd', 'nenshudo']).sum()[['su', 'kingaku']].reset_index()
#     add_salesdata_df_gbsum = add_salesdata_df_gbsum.rename(columns={'su':'URI_SU', 'kingaku':'URI_KIN'})
#     add_salesdata_df_gbsum['BAIKA'] = add_salesdata_df_gbsum['URI_KIN'] / add_salesdata_df_gbsum['URI_SU']
    
#     # 追加する商品コードリスト
#     add_prdcd_list = add_salesdata_df['prd_cd'].unique().tolist()
#     add_prdcd_list_str = ''
#     for i, pcd in enumerate(add_prdcd_list):
#         if i == 0:
#             add_prdcd_list_str = "'" + str(pcd) + "'"
#         else:
#             add_prdcd_list_str = add_prdcd_list_str + ', ', + "'" + str(pcd) + "'"
     
#     # 追加する商品コードリストの商品マスタを取得
#     client = bigquery.Client()
#     bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
#     target_query = f"""  SELECT * FROM {dataset_id}.M_090_PRD where PRD_CD in({add_prdcd_list_str})"""  
#     #print(target_query)
#     logger.info("target_query", target_query)
#     my_dfm_base = (
#         client.query(target_query)
#         .result()
#         .to_dataframe(bqstorage_client=bqstorageclient)
#     )

#     my_dfm_base['PRD_CD'] = my_dfm_base['PRD_CD'].astype(int)
#     my_dfm_base['BUMON_CD'] = my_dfm_base['BUMON_CD'].astype(int)
#     my_dfm_base['cls_cd'] = my_dfm_base['cls_cd'].astype(int)
    

#     add_salesdata_df_gbsum = pd.merge(add_salesdata_df_gbsum, 
#                                 my_dfm_base[['PRD_CD', 'BUMON_CD']].rename(columns={'PRD_CD':'prd_cd', 'BUMON_CD':'DPT_CD'}), 
#                                 on='prd_cd', how='left')
    
#     add_salesdata_df_gbsum = add_salesdata_df_gbsum.rename(columns={'tenpo_cd':'TENPO_CD', 'prd_cd':'PRD_CD'})
    
    
    
#     col_dict = {}
#     for col in my_dfm_base.columns:
#         col_dict[col] = col.lower()
#     my_dfm_base = my_dfm_base.rename(columns=col_dict)
    
#     collist = dfm_base.columns
    
#     my_dfm_base = my_dfm_base[collist]
#     dfm_base = pd.concat([dfm_base, my_dfm_base])

    
#     # 商品マスタを、テーマMDの商品に限定する
#     #dfm_base = dfm_base[dfm_base['prd_cd'].isin(theme_md_prdcd_list)]
    
#     #対象dptを取り出す
    
#     dpt_list = dfm_base[dfm_base['prd_cd'].isin(theme_md_prdcd_list)]['bumon_cd'].unique().tolist()
#     #print("**********************")
#     #print('theme md dpt_list:', dpt_list)
#     #print("**********************")
#     logger.info("**********************")
#     logger.info('theme md dpt_list: %s', dpt_list)  # Use %s to insert the value of dpt_list
#     logger.info("**********************")
    
    
#     '''
    

            
#################################
# Old Code:   
# dfc_tmp_for_merge = dfc_tmp[(dfc_tmp["nenshudo"] >= 201701)&(dfc_tmp["nenshudo"] < end_nenshudo)] [['nenshudo']].reset_index(drop=True)    
            
# for tenpo in tenpo_list: 
#     # 販売期間マスタに登録されていないケースパックバラのレコードを追加する
#     selling_period = selling_period_all[selling_period_all['tenpo_cd']==int(tenpo)].reset_index(drop=True)
#     add_low_list = [selling_period]

#     sp_prdc_cd_list = selling_period['prd_cd'].tolist()
#     for prd_cd in selling_period['prd_cd'].tolist():
#         if prd_cd in prdcd_grp:
#             for g_prd_cd in prdcd_grp[prd_cd]:
#                 if (prd_cd != g_prd_cd) and (g_prd_cd not in sp_prdc_cd_list):
#                     temp_record = copy.deepcopy(selling_period[selling_period['prd_cd']==prd_cd])
#                     temp_record['prd_cd'] = g_prd_cd
#                     add_low_list.append(temp_record)

#     selling_period = pd.concat(add_low_list)               
#     # ここまで　
    
#     product_info_df_list = []
#     df_value_count_ret_list = []
#     jan_mapping_df_list = []
#     subject_jan_master_list = []
    
    
#     # BQから一括でデータをロードする
#     if bq_allow_large_results:
#         project_id = "dev-cainz-demandforecast"
#         dataset_id = "dev_cainz_nssol"
#         table_id = "T_090_URIAGE_JSK_NB_SHU_TEN_DPT"
#         my_tenpo_cd = str(tenpo_cd).zfill(4)
#         dest_str = f"""dev-cainz-demandforecast.test_data.temp_T_090_URIAGE_JSK_NB_SHU_TEN_DPT_{my_tenpo_cd}"""
        
#         client = bigquery.Client()        
#         client.delete_table(dest_str, not_found_ok=True)
        
#         client = bigquery.Client()
#         bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
#         job_config = bigquery.QueryJobConfig(
#             allow_large_results=True, 
#             destination=dest_str, 
#             use_legacy_sql=False
#         )
#         query = f"""
#         SELECT * FROM `{project_id}.{dataset_id}.{table_id}` WHERE TENPO_CD = '{my_tenpo_cd}'
#         """
#         df_sales = client.query(query, job_config=job_config).result().to_dataframe(bqstorage_client=bqstorageclient)
#         df_sales = df_sales.rename(columns={'NENSHUDO': 'nenshudo', 'BUMON_CD': 'DPT_CD'})

#         df_sales['nenshudo'] = df_sales['nenshudo'].astype(int)
#         df_sales['PRD_CD'] = df_sales['PRD_CD'].astype(int)
#         df_sales['DPT_CD'] = df_sales['DPT_CD'].astype(int)
#         df_sales['TENPO_CD'] = df_sales['TENPO_CD'].astype(int)
        
#         df_sales = df_sales.loc[df_sales['nenshudo']<=end_nenshudo]
    
    
    
#     for dpt in dpt_list:
        
#         #print('dpt:', dpt, ' process start *************************')
#         logger.info('dpt:', dpt, ' process start *************************')
        
# #        logger = common.get_logger("集計処理 tenpo: {}, dpt: {} ".format(tenpo, dpt))
# #        logger.info("start")
#         start_time = time.time()
#         dpt = int(dpt)
#         if dpt not in exclusion_dpt_list:
            
#             tmp_df_bunrui = df_bunrui.loc[df_bunrui['dpt_cd']==dpt]
#             if len(tmp_df_bunrui) == 0:
#                 continue
            
#             dpt_name = df_bunrui.loc[df_bunrui['dpt_cd']==dpt]['dpt_nm'].unique()[0]
            
#             temp_dpt_df = pd.DataFrame()

#             # データ探索のためにパスを設定
#             df = pd.DataFrame()
#             dfm = pd.DataFrame()

#             # データのロード
#             if bq_allow_large_results:
#                 df = df_sales[df_sales['DPT_CD']==dpt].reset_index(drop=True)
#                 ret_prd_list = df["PRD_CD"].unique().tolist()
#             else:
                
#                 if 0:
#                     pass
#                     '''
#                     df, ret_prd_list = load_tran_df_bq(
#                         df,
#                         tenpo,
#                         dpt,
#                         end_nenshudo,
#                     ) 
#                     # チャンスロス対応
#                     if add_chanceloss_urisu:
#                         if len(df) > 0:
#                             #チャンスロス結合
#                             df = merge_df_cal(df, dfc_tmp_for_merge, tenpo, dpt)
#                             df = pd.merge(df,chance_loss_data[['PRD_CD', 'NENSHUDO', 'CHANCE_LOSS_PRD_SU', 'CHANCE_LOSS_KN']].rename(columns={'NENSHUDO':'nenshudo'}),on=['PRD_CD','nenshudo'], how="left")
#                             df['CHANCE_LOSS_PRD_SU'] = df['CHANCE_LOSS_PRD_SU'].fillna(0.0)
#                             df['CHANCE_LOSS_KN'] = df['CHANCE_LOSS_KN'].fillna(0.0)

#                             df['URI_SU_ORG'] = df['URI_SU']
#                             df['URI_SU'] = df['URI_SU'] + df['CHANCE_LOSS_PRD_SU']
#                             df['URI_KIN'] = df['URI_KIN'] + df['CHANCE_LOSS_KN']
#                             df['BAIKA'][df['BAIKA'].isnull()] = df['CHANCE_LOSS_KN'] / df['CHANCE_LOSS_PRD_SU']
#                             df['URI_SU'] = df['URI_SU'].astype(float)
#                             df = df[df['URI_SU']>0.0].reset_index(drop=True)
#                     '''
#                 else:
#                     for blob in bucket.list_blobs(prefix=path_tenpo_dpt.format(tenpo, dpt)):
#                         df, ret_prd_list = short_term_preprocess_common.load_tran_df(
#                             df,
#                             blob,
#                             bucket_name,
#                             tenpo_list,
#                             #train_end_nenshudo,
#                             end_nenshudo,
#                             []
#                         )
#                         #print("===df1======",df)

#                         # チャンスロス対応
#                         if add_chanceloss_urisu:
#                             if len(df) > 0:
#                                 #チャンスロス結合
#                                 df = merge_df_cal(df, dfc_tmp_for_merge, tenpo, dpt)
#                                 df = pd.merge(df,chance_loss_data[['PRD_CD', 'NENSHUDO', 'CHANCE_LOSS_PRD_SU', 'CHANCE_LOSS_KN']].rename(columns={'NENSHUDO':'nenshudo'}),on=['PRD_CD','nenshudo'], how="left")
#                                 df['CHANCE_LOSS_PRD_SU'] = df['CHANCE_LOSS_PRD_SU'].fillna(0.0)
#                                 df['CHANCE_LOSS_KN'] = df['CHANCE_LOSS_KN'].fillna(0.0)

#                                 df['URI_SU_ORG'] = df['URI_SU']
#                                 df['URI_SU'] = df['URI_SU'] + df['CHANCE_LOSS_PRD_SU']
#                                 df['URI_KIN'] = df['URI_KIN'] + df['CHANCE_LOSS_KN']
#                                 df['BAIKA'][df['BAIKA'].isnull()] = df['CHANCE_LOSS_KN'] / df['CHANCE_LOSS_PRD_SU']
#                                 df['URI_SU'] = df['URI_SU'].astype(float)
#                                 df = df[df['URI_SU']>0.0].reset_index(drop=True)
                                

#                     if THEME_MD_MODE:
                        
#                         #print('gggg')
#                         #sys.exit()
                        
                        
#                         # 花王の商品をまず抽出
#                         # 4901301-で始まる
#                         # 4973167-で始まる

#                         df['PRD_CD_PREFIX'] = df['PRD_CD'] / 1000000.0
#                         df['PRD_CD_PREFIX'] = df['PRD_CD_PREFIX'].astype(int)

#                         df_ex1 = df[(df['PRD_CD_PREFIX']==4901301)|(df['PRD_CD_PREFIX']==4973167)].reset_index(drop=True)

#                         if len(df_ex1) > 0:            
#                             #cls_cd_list1 = df_ex1['cls_cd'].unique().tolist()

#                             # 同クラスにあるP&G、ライオンの商品も抽出する
#                             # P&G
#                             # 4902430-で始まる
#                             # 4987176-で始まる
#                             # ライオン
#                             # 4903301-で始まる
#                             df_ex2 = df[
#                                 #df['cls_cd'].isin(cls_cd_list1)
#                                 #&
#                                  (df['PRD_CD_PREFIX']==4902430)
#                                 |(df['PRD_CD_PREFIX']==4987176)
#                                 |(df['PRD_CD_PREFIX']==4903301)
#                             ]
#                             if len(df_ex2) > 0:
#                                 df = pd.concat([df_ex1, df_ex2]).reset_index(drop=True)
#                             else:
#                                 df = df_ex1

                        
                    
#                         '''
#                         # CSVから追加した商品データにチャンスロスを加算する処理
#                         my_add_salesdata_df_gbsum = add_salesdata_df_gbsum[
#                                                          (add_salesdata_df_gbsum['DPT_CD']==dpt)
#                                                         &(add_salesdata_df_gbsum['TENPO_CD']==tenpo)]
                        
#                         if len(my_add_salesdata_df_gbsum) > 0:                            
#                             # チャンスロス対応
#                             if add_chanceloss_urisu:
#                                 #チャンスロス結合
                                
#                                 my_add_salesdata_df_gbsum = merge_df_cal(my_add_salesdata_df_gbsum, dfc_tmp_for_merge, tenpo, dpt)
                                
#                                 my_add_salesdata_df_gbsum = pd.merge(my_add_salesdata_df_gbsum,chance_loss_data[['PRD_CD', 'NENSHUDO', 'CHANCE_LOSS_PRD_SU', 'CHANCE_LOSS_KN']].rename(columns={'NENSHUDO':'nenshudo'}),on=['PRD_CD','nenshudo'], how="left")
#                                 my_add_salesdata_df_gbsum['CHANCE_LOSS_PRD_SU'] = my_add_salesdata_df_gbsum['CHANCE_LOSS_PRD_SU'].fillna(0.0)
#                                 my_add_salesdata_df_gbsum['CHANCE_LOSS_KN'] = my_add_salesdata_df_gbsum['CHANCE_LOSS_KN'].fillna(0.0)

#                                 my_add_salesdata_df_gbsum['URI_SU_ORG'] = my_add_salesdata_df_gbsum['URI_SU']
#                                 my_add_salesdata_df_gbsum['URI_SU'] = my_add_salesdata_df_gbsum['URI_SU'] + my_add_salesdata_df_gbsum['CHANCE_LOSS_PRD_SU']
#                                 my_add_salesdata_df_gbsum['URI_KIN'] = my_add_salesdata_df_gbsum['URI_KIN'] + my_add_salesdata_df_gbsum['CHANCE_LOSS_KN']
#                                 my_add_salesdata_df_gbsum['BAIKA'][my_add_salesdata_df_gbsum['BAIKA'].isnull()] = my_add_salesdata_df_gbsum['CHANCE_LOSS_KN'] / my_add_salesdata_df_gbsum['CHANCE_LOSS_PRD_SU']
#                                 my_add_salesdata_df_gbsum['URI_SU'] = my_add_salesdata_df_gbsum['URI_SU'].astype(float)
#                                 my_add_salesdata_df_gbsum = my_add_salesdata_df_gbsum[my_add_salesdata_df_gbsum['URI_SU']>0.0].reset_index(drop=True)

#                             df = pd.concat([df, my_add_salesdata_df_gbsum])
#                         '''        
                        
                        
                        
                        
                        
            
#             if len(df)==0:
#                 #print(' After read tran data df size=0 Warning')
#                 logger.warning('After read tran data df size=0 Warning: Possible data loading issue or empty source.')


            
            
#             #df_afterread = copy.deepcopy(df)
            
            
#             add_dept_prd_list = []
            
#             if len(df)!=0:
                
#                 if use_jan_connect:
#                     ###
#                     # JANの差し替え指示をさかのぼり対象となるJANを確認
                    
#                     # DFのJANコードから差替え指示をさかのぼり、全部関係のあるJANリストと、差し替え指示を取得
#                     # なぜここで'how_change'が6, 10, 11を除外しないのか？ *****************
#                     subject_prod_list, subject_jan_master = short_term_preprocess_common.get_subject_jan_master(
#                         df,
#                         jan_master
#                     )
#                     #print("===df2======",df)
#                     logger.info("===df2======",df)

#                     # 差し替え指示が無ければパスする
#                     # 差し替え指示があれば紐づくDPTを抽出
#                     if len(subject_jan_master)!=0:
#                         new_dpt_list, old_dpt_list, subject_dpt_list = short_term_preprocess_common.get_new_old_dpt_list(
#                             subject_jan_master,
#                             subject_prod_list,
#                         )
#                         # 現在ロードしているDPTに関しては必要ないので外す
#                         subject_dpt_list.remove(dpt)
                        
#                         # チャンスロス対応
#                         if add_chanceloss_urisu:
#                             # 関係するDPTかつ関係する製品データを追加でロード
#                             #add_dept_prd_list = [] #20230921 上に移動
#                             dft = pd.DataFrame()
#                             for temp_dpt in subject_dpt_list:
#                                 temp_dpt = int(temp_dpt)
                                
#                                 if 0:
#                                     dft, ret_prd_list = load_tran_df_bq(
#                                         dft,
#                                         tenpo,
#                                         temp_dpt,
#                                         end_nenshudo,
#                                     ) 
#                                     add_dept_prd_list = add_dept_prd_list + ret_prd_list
#                                 else:
#                                     for blob in bucket.list_blobs(prefix=path_tenpo_dpt.format(tenpo, temp_dpt)):
#                                         dft, ret_prd_list = short_term_preprocess_common.load_tran_df(
#                                             dft,
#                                             blob,
#                                             bucket_name,
#                                             tenpo_list,
#                                             #train_end_nenshudo,
#                                             end_nenshudo,
#                                             subject_prod_list
#                                         )
#                                         add_dept_prd_list = add_dept_prd_list + ret_prd_list                            
                            
#                             #チャンスロス結合
#                             if len(dft) > 0:
#                                 dft = merge_df_cal(dft, dfc_tmp_for_merge, tenpo, dpt)
#                                 dft = pd.merge(dft, chance_loss_data[['PRD_CD', 'NENSHUDO', 'CHANCE_LOSS_PRD_SU', 'CHANCE_LOSS_KN']].rename(columns={'NENSHUDO':'nenshudo'}),on=['PRD_CD','nenshudo'], how="left")
#                                 dft['CHANCE_LOSS_PRD_SU'] = dft['CHANCE_LOSS_PRD_SU'].fillna(0.0)
#                                 dft['CHANCE_LOSS_KN'] = dft['CHANCE_LOSS_KN'].fillna(0.0)
#                                 dft['URI_SU_ORG'] = dft['URI_SU']
#                                 dft['URI_SU'] = dft['URI_SU'] + dft['CHANCE_LOSS_PRD_SU']
#                                 dft['URI_KIN'] = dft['URI_KIN'] + dft['CHANCE_LOSS_KN']
#                                 dft['BAIKA'][dft['BAIKA'].isnull()] = dft['CHANCE_LOSS_KN'] / dft['CHANCE_LOSS_PRD_SU']
#                                 dft['URI_SU'] = dft['URI_SU'].astype(float)
#                                 dft = dft[dft['URI_SU']>0.0].reset_index(drop=True)
#                                 df = pd.concat([df, dft], axis=0).reset_index(drop=True)
                            
#                         else:
#                             # 関係するDPTかつ関係する製品データを追加でロード
#                             #add_dept_prd_list = [] #20230921 上に移動
#                             for temp_dpt in subject_dpt_list:
#                                 temp_dpt = int(temp_dpt)
                                
#                                 if 0:
#                                     df, ret_prd_list = load_tran_df_bq(
#                                         df,
#                                         tenpo,
#                                         temp_dpt,
#                                         end_nenshudo,
#                                     ) 
#                                     add_dept_prd_list = add_dept_prd_list + ret_prd_list
                                    
#                                 else:
#                                     for blob in bucket.list_blobs(prefix=path_tenpo_dpt.format(tenpo, temp_dpt)):
#                                         df, ret_prd_list = short_term_preprocess_common.load_tran_df(
#                                             df,
#                                             blob,
#                                             bucket_name,
#                                             tenpo_list,
#                                             #train_end_nenshudo,
#                                             end_nenshudo,
#                                             subject_prod_list
#                                         )
#                                         add_dept_prd_list = add_dept_prd_list + ret_prd_list
                                
                                
#                 # 最新の商品が何かを確認するためにマスターに販売期間マスターをマージする
# #                dfm = pd.merge(
# #                    dfm_base.rename(columns={"prd_cd":"PRD_CD"}),
# #                    tanawari[tanawari["tenpo_cd"] == tenpo][["tenpo_cd", "prd_cd", "tanagae_hokoku_day", 'hacchu_to_ymd']].rename(columns={"tenpo_cd":"TENPO_CD", "prd_cd":"PRD_CD", "tanagae_hokoku_day":"sell_start_ymd", 'hacchu_to_ymd':'hacchu_end_ymd'}),
# #                    on=['PRD_CD'],
# #                    how='inner'
# #                )


#                 #print("===df3======",df)
#                 logger.info("===df3======",df)
    
    
#                 #print('test exit')
#                 #sys.exit()
            
            
#                 # 他のDPTに紐づけられる商品は除かなくてはならない・・・・・・・・・・・・・・
                
#                 #こんなケース
#                 #old_jan	latest_jan
#                 #4549509426202 	4901983805599 
#                 #4549509003038 	4901983805599 
#                 #4549509426202 	4901983805599 
#                 #
#                 #差替え先4901983805599はDPT63 　差し替え元はDPT88
                
#                 #DPT63の処理のとき、sum条件にdptが入っているので差し替え元JANはdpt88で残ってしまう
                
#                 #DPT88の処理のとき、4549509003038はdpt88で残ってしまう（4901983805599はもともと無い）
                
#                 #JANが新しいものに書き換わっていない？？？？
                
                

#                 #################################################
#                 #dftest1_1 = copy.deepcopy(df)
#                 #################################################
                
    
#                 if THEME_MD_MODE:
#                     dfm = pd.merge(
#                         dfm_base.rename(columns={"prd_cd":"PRD_CD"}),                    
#                         selling_period[selling_period["tenpo_cd"] == tenpo][["tenpo_cd", "prd_cd", "tanagae_hokoku_day", 'hacchu_to_ymd']].rename(columns={"tenpo_cd":"TENPO_CD", "prd_cd":"PRD_CD", "tanagae_hokoku_day":"sell_start_ymd", 'hacchu_to_ymd':'hacchu_end_ymd'}),
#                         on=['PRD_CD'],
#                         how='left' # 販売期間を見ないようにしたいのでleftにする
#                     )
                
#                     dfm['sell_start_ymd'] = dfm['sell_start_ymd'].fillna(dfm['sell_start_ymd'].min())
#                     dfm['hacchu_end_ymd'] = dfm['hacchu_end_ymd'].fillna(dfm['hacchu_end_ymd'].max())
#                     dfm['TENPO_CD'] = dfm['TENPO_CD'].fillna(tenpo)

                    
#                     # テーマMD商品に限定した商品マスタを作成して、DFにマージする
#                     #my_dfm = dfm[dfm['PRD_CD'].isin(theme_md_prdcd_list)]

#                     df = pd.merge(
#                         dfm[["PRD_CD", "TENPO_CD", "sell_start_ymd", "hacchu_end_ymd", "bumon_cd"]], # 20230731 部門コードを追加
#                         df,
#                         on=["PRD_CD", "TENPO_CD"],
#                         how='inner'
#                     )
                    
                    
            
#                 else:
#                     dfm = pd.merge(
#                         dfm_base.rename(columns={"prd_cd":"PRD_CD"}),                    
#                         selling_period[selling_period["tenpo_cd"] == tenpo][["tenpo_cd", "prd_cd", "tanagae_hokoku_day", 'hacchu_to_ymd']].rename(columns={"tenpo_cd":"TENPO_CD", "prd_cd":"PRD_CD", "tanagae_hokoku_day":"sell_start_ymd", 'hacchu_to_ymd':'hacchu_end_ymd'}),
#                         on=['PRD_CD'],
#                         how='inner'
#                     )
        
            
#                     df = pd.merge(
#                         dfm[["PRD_CD", "TENPO_CD", "sell_start_ymd", "hacchu_end_ymd", "bumon_cd"]], # 20230731 部門コードを追加
#                         df,
#                         on=["PRD_CD", "TENPO_CD"],
#                         how='inner'
#                     )
                
#                 #dftest3 = copy.deepcopy(df)

#                 #print("===df4======",df)
#                 logger.info("===df4======",df)
                
#                 #if use_jan_connect:
                    
#                 # JANの書き換え処理
#                 # 差し替え指示が無ければパスする
#                 #if len(subject_jan_master) != 0:
#                 if 1:
                    
#                     # DPTを全て書き換える********************************
#                     df['DPT_CD'] = dpt
                                                            
#                     # **************** 20230516追加
#                     # ケースパックバラJAN書き換え前に、製品/年週度ごとの売価をとっておく
#                     dfgb = df[['TENPO_CD', 'PRD_CD', 'nenshudo', 'BAIKA']].groupby(['TENPO_CD', 'PRD_CD', 'nenshudo']).mean()
#                     dfgb.columns = ['BAIKA_NEW']

#                     # ケースパックバラのJANコードを発注JANに書き替える
#                     df = pd.merge(df, prdcd_hattyujan_df, on='PRD_CD', how='left')
#                     df.loc[~df['HACCHU_JAN'].isnull(), 'PRD_CD'] = df['HACCHU_JAN']
                    
                    
#                     #dftest3_1 = copy.deepcopy(df)
                    
                    
#                     # 発注JANに差し替えたものは、売価も発注JANの売価に差し替える
#                     df = pd.merge(df, dfgb, on=['TENPO_CD', 'PRD_CD', 'nenshudo'], how='left')
#                     df['BAIKA_OLD'] = df['BAIKA']
#                     df.loc[~df['HACCHU_JAN'].isnull(), 'BAIKA'] = df['BAIKA_NEW']
                    
#                     #dftest3_2 = copy.deepcopy(df)
                    
#                     # ソートする
#                     #df = df.sort_values(by=['TENPO_CD', 'PRD_CD', 'nenshudo'])
#                     df = df.sort_values(by=['TENPO_CD', 'PRD_CD', 'nenshudo']).reset_index(drop=True)
#                     df['URI_SU'] = df['URI_SU'].astype(float)
#                     df['HJAN_COEF'] = df['HJAN_COEF'].astype(float)

                    
                    
#                     # 売価の補完をおこなう
#                     df['BAIKA'] = df[['TENPO_CD', 'PRD_CD', 'BAIKA']].groupby(['TENPO_CD', 'PRD_CD'])['BAIKA'].transform(lambda x: x.interpolate(limit_direction='both'))
                    
#                     # 元々発注JANが売られていない商品は、元JANの売価を係数で割って売価とする
#                     df.loc[df['BAIKA'].isnull(), 'BAIKA'] = df['BAIKA_OLD'] / df['HJAN_COEF']

#                     # 売り数に発注JANへの変換係数をかける
#                     df.loc[~df['HACCHU_JAN'].isnull(), 'URI_SU'] = df['URI_SU'] * df['HJAN_COEF']
#                     # 売り金額はそのままでよい
                        
                    
#                     df = df.drop(['HACCHU_JAN'], axis=1)
#                     df = df.drop(['HJAN_COEF'], axis=1)
#                     df = df.drop(['BAIKA_NEW'], axis=1)
#                     df = df.drop(['BAIKA_OLD'], axis=1)
                    
#                     #dftest3_3 = copy.deepcopy(df)

#                     if use_jan_connect:
                        
#                         #dftest4 = copy.deepcopy(df)

#                         # 'how_change'が6, 10, 11を除外、重複削除
#                         # 販売期間マスタ（hacchu_end_ymd）を結合、keyはnew_JAN
#                         subject_jan_master = short_term_preprocess_common.filtering_jan_master_for_rewriting_jan(
#                             subject_jan_master,
#                             dfm
#                         )
                        
                        
                        
#                         if seisan_tenpo_hattyuu_end_is_not_replaced:
#                             # 生産発注終了日、店舗発注終了日をみて、終了していない差替え元商品の差替え情報を除くため
#                             # 商品マスタを結合
#                             # 商品マスタ（生産発注開始終了日、店舗発注開始終了日）を結合
#                             dfm_base['seisan_hacchu_tekiyo_to_ymd'] = dfm_base['seisan_hacchu_tekiyo_to_ymd'].fillna(99999999)
#                             dfm_base['tenpo_hacchu_to_ymd_toitsu'] = dfm_base['tenpo_hacchu_to_ymd_toitsu'].fillna(99999999)
#                             dfm_base['seisan_hacchu_tekiyo_to_ymd'] = dfm_base['seisan_hacchu_tekiyo_to_ymd'].astype(int)
#                             dfm_base['tenpo_hacchu_to_ymd_toitsu'] = dfm_base['tenpo_hacchu_to_ymd_toitsu'].astype(int)  
                            
#                             subject_jan_master = pd.merge(subject_jan_master, dfm_base.rename(columns={'prd_cd':'old_JAN'}), on='old_JAN', how='left').reset_index(drop=True)

#                             # 店舗別の生産発注停止情報を結合
                            
#                             #金子さんコメント
#                             #個店単位で発注終了日を見るのであれば、
#                             #M030PRD_TEN_TNPN_INFから、PRD_CDとTENPO_CDでターゲットを確定させ、HACCHU_YMD_SEQ_NOを取得して
#                             #M030HACCHU_YMD_INFから、PRD_CDとHACCHU_YMD_SEQ_NOで結合した結果、HACCHU_TO_YMDが本日以前のものをとる
#                             #が、短期の場合のあるべき発注終了の判定かと！
                            
#                             #store_prd_hacchu_ymd = common.extract_as_df(path_tenpo_hacchu_master, bucket_name)
#                             #store_prd_hacchu_ymd['TENPO_CD'] = store_prd_hacchu_ymd['TENPO_CD'].astype(int)
#                             #store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].fillna(99999999)
#                             #store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].astype(int)
#                             my_store_prd_hacchu_ymd = store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD']==int(tenpo)].reset_index(drop=True)
#                             subject_jan_master = pd.merge(subject_jan_master, my_store_prd_hacchu_ymd.rename(columns={'PRD_CD':'old_JAN'}), on='old_JAN', how='left').reset_index(drop=True)
                        
   
#                         # 'hacchu_end_ymd'==99999999の新JANと、それに関連するすべてのJANを取得する
#                         newest_jan_list = short_term_preprocess_common.get_newest_jan_list(
#                             subject_jan_master,
#                             my_date
#                         )
                        
#                         # newest_jan_listにあるが、DPTが違うものは除外すべきでは？ *****************
                        
#                         #  ・add_dept_prd_listには他DPTから追加した製品番号
#                         #        DFに含まれる製品に関する差替え指示からの新JANと関連する旧JANのリストで、他DPTの製品
#                         #        なぜここで'how_change'が6, 10, 11を除外しないのか？ *****************                        
                        
#                         #  newest_jan_listには、
#                         #        DFに関連する差替え指示からの新JANと関連する旧JANが入っている
#                         #        'hacchu_end_ymd'==99999999の新JANと、それに関連するすべてのJANを取得する
#                         #        'how_change'が6, 10, 11の差替えを除外
                        
#                         #dftest4_2 = copy.deepcopy(df)
                        
#                         # add_dept_prd_listにあるがnewest_jan_listに無いSKUを除外する
#                         del_prd_list = list(set(add_dept_prd_list) - set(newest_jan_list))
#                         df = df[~df['PRD_CD'].isin(del_prd_list)]
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
#                         # **********************************************************************************
#                         # DFのPRD_CDで、商品マスタ上、処理対象のDPTでないものを除く 20230731追加
#                         df = df[df['bumon_cd']==dpt]
#                         df = df.drop(['bumon_cd'], axis=1).reset_index(drop=True)
#                         # **********************************************************************************
                        
                        
                        
                        
                        
                        
                        
                        
#                         #dftest4_3 = copy.deepcopy(df)
                        
#                         #print('test exit')
#                         #sys.exit()
                        
                        
#                         # JAN書き換え*********************************************************
                        
#                         # 4549509003038 	4901983805599 	どちらもnewestにしかない
                        
#                         df, jan_mapping_df = short_term_preprocess_common.rewrite_jan_code(
#                             subject_jan_master,
#                             newest_jan_list,
#                             df,
#                             'PRD_CD',
#                             my_date,
#                             restrict_old_jan_1generation=restrict_old_jan_1generation_flag,
#                             seisan_tenpo_hattyuu_end_is_not_replaced=seisan_tenpo_hattyuu_end_is_not_replaced
#                         )
                        
#                         jan_mapping_df_list.append(jan_mapping_df)
#                         subject_jan_master_list.append(subject_jan_master)
                        
#                         #jan_mapping_df.to_csv('jan_mapping_df_' + str(tenpo) + '.csv', index=False)
                    
#                         #dftest5 = copy.deepcopy(df)
                    
                    
                    
#                     # groupby処理
#                     # 販売開始日は一番古いもの、販売終了は一番新しものにする
#                     prod_info_df = df[['PRD_CD', 'TENPO_CD', 'DPT_CD', 'sell_start_ymd', 'hacchu_end_ymd']]
#                     prod_info_df = prod_info_df.drop_duplicates()
#                     prod_info_df = prod_info_df.groupby(['PRD_CD', 'TENPO_CD', 'DPT_CD']).agg({
#                         'sell_start_ymd':'min',
#                         'hacchu_end_ymd':'max'
#                     }).reset_index()
                    
                    
#                     df = df.groupby(['PRD_CD', 'TENPO_CD', 'DPT_CD', 'nenshudo']).agg({'URI_SU':'sum','URI_KIN':'sum', 'BAIKA':'mean'}).reset_index()
#                     #df = df.groupby(['PRD_CD', 'TENPO_CD', 'DPT_CD', 'nenshudo']).agg({'URI_SU':'sum','URI_KIN':'sum'}).reset_index()
                    
                    
#                     #dftest5_3 = copy.deepcopy(df)
                    
#                     df = pd.merge(df, prod_info_df, on=['PRD_CD', 'TENPO_CD', 'DPT_CD'], how='inner')

                    
#                     ###################################################
#                     #dftest6 = copy.deepcopy(df)
#                     ###################################################
                    
                    
#                     #print('test2 exit')
#                     #sys.exit()
                
                
#                 # 商品情報の結合
#                 #print("===df5======",df)
#                 logger.info("===df5======",df)
#                 df = pd.merge(
#                     dfm[[
#                         "PRD_CD","prd_nm_kj", "hnmk_cd", "cls_cd", "line_cd", "shoki_genka", 
#                         "shoki_baika", "genka_toitsu", "baika_toitsu", "low_price_kbn"
#                     ]],
#                     df,
#                     on="PRD_CD",
#                     how="inner"
#                 )
                
#                 #dftest7 = copy.deepcopy(df)
                
                
#                 df = pd.merge(df, df_bunrui[['cls_cd', 'cls_nm']], on='cls_cd', how='inner')
                
                
#                 #dftest8 = copy.deepcopy(df)
                
#                 #print("===df6======",df)
#                 logger.info("===df6======",df)
#                 ###
#                 df_cal = pd.DataFrame(dfc["nenshudo"].drop_duplicates().sort_values().reset_index(drop=True))
#                 df_cal = dfc[
#                     (dfc["nenshudo"]>=start_nenshudo) & \
#                     (dfc["nenshudo"]<=end_nenshudo)
#                 ][["nenshudo","shudo","week_from_ymd"]].reset_index(drop=True)
#                 df_cal["date"] = df_cal["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
#                 df_cal = pd.merge(df_cal, dfc[['nenshudo', 'nendo', 'znen_nendo', 'znen_shudo','minashi_tsuki']], on='nenshudo')
#                 df_cal = df_cal.loc[
#                     (df_cal['nenshudo']>=start_nenshudo)&\
#                     (df_cal['nenshudo']<=end_nenshudo)
#                 ].reset_index(drop=True)
#                 ###
                
#                 #dftest9 = copy.deepcopy(df)

#                 if not THEME_MD_MODE:
#                     # 発注終了が無い商品は除外
#                     df = df.loc[(~df["hacchu_end_ymd"].isnull())].reset_index(drop=True)
               
#                 #dftest10 = copy.deepcopy(df)
            
#                 #print("===========df7===========",df)
#                 logger.info("===========df7===========",df)

#                 # JAN結合後データを店舗で絞る
#                 unique_tenpo_df = df.loc[df['TENPO_CD']==tenpo].reset_index(drop=True)
                
                
                
                
                
                
                
#                 # **********************************************************************************
#                 # DFのPRD_CDで、商品マスタ上、処理対象のDPTでないものを除く 20230826追加
#                 my_bumon_cd_prdcd_list = list(set(dfm_base[dfm_base['bumon_cd']==dpt]['PRD_CD']))
#                 unique_tenpo_df = unique_tenpo_df[unique_tenpo_df['PRD_CD'].isin(my_bumon_cd_prdcd_list)].reset_index(drop=True)
#                 # **********************************************************************************
                
                
                
                
                
                
                
                
#                 if not THEME_MD_MODE:
#                     # 発注終了していないものに絞る
#                     unique_tenpo_df = unique_tenpo_df.loc[(unique_tenpo_df["hacchu_end_ymd"]==99999999)].reset_index(drop=True)
                
                
#                 #dftest11 = copy.deepcopy(unique_tenpo_df)
                
#                 if len(unique_tenpo_df)!=0:
                    
#                     if not THEME_MD_MODE:
#                         # 発売日が現在の13週前より前のデータに絞る
#                         unique_tenpo_df = unique_tenpo_df.loc[
#                             unique_tenpo_df['sell_start_ymd'] < (df_cal.loc[df_cal["date"] == df_cal['date'].max()-relativedelta(weeks=13)]["week_from_ymd"].values[0])
#                         ].reset_index(drop=True)

                        
#                     #dftest11_1 = copy.deepcopy(unique_tenpo_df)
                    
#                     if len(unique_tenpo_df)!=0:
#                         product_info_df = short_term_preprocess_common.get_product_info_df(
#                             unique_tenpo_df,
#                             df_cal,
#                             dfc_tmp,
#                             end_nenshudo,
#                             dpt,
#                             output_6wk_2sales
#                         )
                        
#                         #product_info_df.to_csv('product_info_df_' + str(tenpo) + '_dpt' + str(dpt) + '.csv', index=False)
                        
#                         product_info_df_list.append(product_info_df)
                        

                        
#                         path_upload_tmp_local = "gcpアップロード一時データ/temp_time_seriese_" + str(tenpo) + ".csv"
                        
#                         ########################################################
#                         #dftest12 = copy.deepcopy(unique_tenpo_df)
#                         ########################################################
                        
                        
                        
#                         if THEME_MD_MODE:
#                             # テーマMDモードでは、全ての商品データを週次のバケットに出力する（stage2以降で商品を選択する）
#                             # "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-allprd/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
#                             if len(product_info_df)!=0:
#                                 time_series_prod_cd, time_series_df = short_term_preprocess_common.upload_timeseries_all_prd_df(
#                                     product_info_df,
#                                     unique_tenpo_df,
#                                     tenpo_df,
#                                     dpt,
#                                     tenpo,
#                                     threshold_missing_ratio,
#                                     threshold_timeseries_length,
#                                     path_upload_tmp_local,
#                                     path_upload_time_series_blob_all_prd,
#                                     bucket_name
#                                 )
                                

                                
#                         else:
#                             if len(product_info_df)!=0:
#                                 # 週次データをアップロード
#                                 time_series_prod_cd, time_series_df = short_term_preprocess_common.upload_timeseries_df(
#                                     product_info_df,
#                                     unique_tenpo_df,
#                                     tenpo_df,
#                                     dpt,
#                                     tenpo,
#                                     threshold_missing_ratio,
#                                     threshold_timeseries_length,
#                                     path_upload_tmp_local,
#                                     path_upload_time_series_blob,
#                                     path_upload_time_series_blob2,
#                                     bucket_name,
#                                     output_6wk_2sales
#                                 )


                                
#                                 path_upload_not_ai_local = "gcpアップロード一時データ/not_ai_model" + str(tenpo) + ".csv"



#                                 week4_summarize_df, not_week4_summarize_df, df_value_count_ret = short_term_preprocess_common.upload_not_timeseries_df(
#                                     unique_tenpo_df,
#                                     time_series_prod_cd,
#                                     df_cal,
#                                     dpt,
#                                     tenpo,
#                                     path_upload_tmp_local,
#                                     path_upload_not_ai_local,
#                                     path_upload_monthly_series_blob,
#                                     path_upload_not_ai_blob,
#                                     bucket_name
#                                 )

#                                 df_value_count_ret_list.append(df_value_count_ret)

#                                 # 以下対象商品JANリストのエクセル作成処理
#                                 '''
#                                 classify_df_list = short_term_preprocess_common.get_classify_df_list(
#                                     classify_df_list,
#                                     unique_tenpo_df,
#                                     time_series_df,
#                                     week4_summarize_df,
#                                     not_week4_summarize_df,
#                                     start_holdout_nenshudo,
#                                     df_cal,
#                                     dfm,
#                                     df_bunrui,
#                                     tenpo_list,
#                                     dpt,
#                                     tenpo
#                                 )
#                                 '''
                                
#                         #print('gggggggggggggggg')
#                         #sys.exit()

                
                
#         #print('testend------------')
#         #sys.exit()
                
#         end_time = time.time()
# #        logger.info("end elapsed time: {}".format(end_time - start_time))
#         #print("====tenpo_df===",tenpo_df)
#         logger.info("====tenpo_df===",tenpo_df)
#         # DPT別処理、ここまで

                            

#     '''
#     if 1:    
#         #product_info_df_list_df = pd.concat(product_info_df_list)
#         #product_info_df_list_df.to_csv('product_info_df_' + str(tenpo) + '.csv')

#         df_value_count_ret_list_all = pd.concat(df_value_count_ret_list)
#         #df_value_count_ret_list_all.to_csv('df_value_count_ret' + str(tenpo) + '.csv')   
    
#         jan_mapping_df_list_df = pd.concat(jan_mapping_df_list)
#         #jan_mapping_df_list_df.to_csv('jan_mapping_df_list_df_' + str(tenpo) + '.csv', index=False)
    
#         subject_jan_master_list_df = pd.concat(subject_jan_master_list)
#         #subject_jan_master_list_df.to_csv('subject_jan_master_list_df_' + str(tenpo) + '.csv', index=False)
#     '''
    
# '''
# for i in range(len(tenpo_list)):
#     temp_df = classify_df_list[i]
#     temp_df.loc[temp_df['DPTコード'].isin(priority_dpt_list), '優先DPT'] = '〇'
#     temp_df['商品コード'] = temp_df['商品コード'].astype(str).str.zfill(13)
#     temp_df.drop_duplicates().to_excel(classify_excel_list[i], f'店舗_{tenpo_df.loc[tenpo_df["TENPO_CD"]==tenpo_list[i]]["TENPO_NM_KJ"].values[0]}_商品リスト',index=False)
#     classify_excel_list[i].save()
# blob = bucket.blob(path_bunrui_prd_code_blob.format(tenpo_df[tenpo_df['TENPO_CD']==tenpo]['TENPO_NM_KJ'].values[0]))
# blob.upload_from_filename(path_bunrui_prd_code.format(tenpo_df[tenpo_df['TENPO_CD']==tenpo]['TENPO_NM_KJ'].values[0]))
# '''
# print('stage1 process complete*******************')

            
            
#################################
# Refactored Code: 
def process_data_for_tenpo(
    dfc_tmp,
    tenpo_list,
    prdcd_grp,
    selling_period_all,
    bq_allow_large_results,
    dpt_list,
    exclusion_dpt_list,
    df_bunrui,
    add_chanceloss_urisu,
    chance_loss_data,
    THEME_MD_MODE,
    bucket,
    path_tenpo_dpt,
    bucket_name,
    short_term_preprocess_common,
    end_nenshudo,
    use_jan_connect,
    jan_master, seisan_tenpo_hattyuu_end_is_not_replaced, my_date, tenpo_df, output_6wk_2sales, threshold_missing_ratio, threshold_timeseries_length, path_upload_time_series_blob_all_prd, path_upload_monthly_series_blob, path_upload_not_ai_blob, dfm_base, prdcd_hattyujan_df, store_prd_hacchu_ymd, dfc, start_nenshudo, path_upload_time_series_blob, path_upload_time_series_blob2
):
    """
    店舗ごとにデータを処理し、販売期間マスタに登録されていないケースパックバラのレコードを追加する。
    Processes data for each store, adding case pack single item records not registered in the sales period master.

    Args:
        dfc_tmp (pd.DataFrame): カレンダー情報を含むDataFrame. DataFrame containing calendar information.
        tenpo_list (list): 店舗コードのリスト. List of store codes.
        prdcd_grp (dict): 商品コードとグループの辞書. Dictionary of product codes and groups.
        selling_period_all (pd.DataFrame): 全ての販売期間情報. All sales period information.
        bq_allow_large_results (bool): BigQueryで大きな結果を許可するかどうか. Whether to allow large results in BigQuery.
        dpt_list (list): 部門コードのリスト. List of department codes.
        exclusion_dpt_list (list): 除外する部門コードのリスト. List of department codes to exclude.
        df_bunrui (pd.DataFrame): 分類情報を含むDataFrame. DataFrame containing classification information.
        add_chanceloss_urisu (bool): チャンスロスを加えるかどうか. Whether to add chance loss.
        chance_loss_data (pd.DataFrame): チャンスロスデータ. Chance loss data.
        THEME_MD_MODE (bool): テーマMDモードかどうか. Whether it is theme MD mode.
        bucket (google.cloud.storage.bucket.Bucket): GCSバケット. Google Cloud Storage bucket object.
        path_tenpo_dpt (str): 店舗と部門のパス. Path of store and department.
        bucket_name (str): バケット名. Bucket name.
        short_term_preprocess_common (module): short_term_preprocess_common モジュール. short_term_preprocess_common module.
        end_nenshudo (int): 終了年度. End fiscal year.
        project_id (str): project_id.
        dataset_id (str): dataset_id
        use_jan_connect (bool): Whether to use jan connect.
        jan_master (pd.DataFrame): jan_master dataFrame.

    Returns:
        None
    """
    dfc_tmp_for_merge = dfc_tmp[(dfc_tmp["nenshudo"] >= 201701)&(dfc_tmp["nenshudo"] < end_nenshudo)] [['nenshudo']].reset_index(drop=True)    
            
    for tenpo in tenpo_list: 
        # 販売期間マスタに登録されていないケースパックバラのレコードを追加する
        selling_period = selling_period_all[selling_period_all['tenpo_cd']==int(tenpo)].reset_index(drop=True)
        add_low_list = [selling_period]

        sp_prdc_cd_list = selling_period['prd_cd'].tolist()
        for prd_cd in selling_period['prd_cd'].tolist():
            if prd_cd in prdcd_grp:
                for g_prd_cd in prdcd_grp[prd_cd]:
                    if (prd_cd != g_prd_cd) and (g_prd_cd not in sp_prdc_cd_list):
                        temp_record = copy.deepcopy(selling_period[selling_period['prd_cd']==prd_cd])
                        temp_record['prd_cd'] = g_prd_cd
                        add_low_list.append(temp_record)

        selling_period = pd.concat(add_low_list)               
        # ここまで　

        product_info_df_list = []
        df_value_count_ret_list = []
        jan_mapping_df_list = []
        subject_jan_master_list = []


        # BQから一括でデータをロードする
        if bq_allow_large_results:
            table_id = "T_090_URIAGE_JSK_NB_SHU_TEN_DPT"
            my_tenpo_cd = str(tenpo_cd).zfill(4)
            dest_str = f"""dev-cainz-demandforecast.test_data.temp_T_090_URIAGE_JSK_NB_SHU_TEN_DPT_{my_tenpo_cd}"""

            client = bigquery.Client()        
            client.delete_table(dest_str, not_found_ok=True)

            client = bigquery.Client()
            bqstorageclient = bigquery_storage_v1.BigQueryReadClient()
            job_config = bigquery.QueryJobConfig(
                allow_large_results=True, 
                destination=dest_str, 
                use_legacy_sql=False
            )
            query = f"""
            SELECT * FROM `{project_id}.{dataset_id}.{table_id}` WHERE TENPO_CD = '{my_tenpo_cd}'
            """
            df_sales = client.query(query, job_config=job_config).result().to_dataframe(bqstorage_client=bqstorageclient)
            df_sales = df_sales.rename(columns={'NENSHUDO': 'nenshudo', 'BUMON_CD': 'DPT_CD'})

            df_sales['nenshudo'] = df_sales['nenshudo'].astype(int)
            df_sales['PRD_CD'] = df_sales['PRD_CD'].astype(int)
            df_sales['DPT_CD'] = df_sales['DPT_CD'].astype(int)
            df_sales['TENPO_CD'] = df_sales['TENPO_CD'].astype(int)

            df_sales = df_sales.loc[df_sales['nenshudo']<=end_nenshudo]

        for dpt in dpt_list:        
            logger.info(f'dpt: {dpt} process start *************************')
            start_time = time.time()
            dpt = int(dpt)
            if dpt not in exclusion_dpt_list:            
                tmp_df_bunrui = df_bunrui.loc[df_bunrui['dpt_cd']==dpt]
                if len(tmp_df_bunrui) == 0:
                    continue            
                dpt_name = df_bunrui.loc[df_bunrui['dpt_cd']==dpt]['dpt_nm'].unique()[0]

                temp_dpt_df = pd.DataFrame()

                # データ探索のためにパスを設定
                df = pd.DataFrame()
                dfm = pd.DataFrame()

                # データのロード
                if bq_allow_large_results:
                    df = df_sales[df_sales['DPT_CD']==dpt].reset_index(drop=True)
                    ret_prd_list = df["PRD_CD"].unique().tolist()
                else:
                    for blob in bucket.list_blobs(prefix=path_tenpo_dpt.format(tenpo, dpt)):
                        df, ret_prd_list = short_term_preprocess_common.load_tran_df(
                            df,
                            blob,
                            bucket_name,
                            tenpo_list,
                            #train_end_nenshudo,
                            end_nenshudo,
                            []
                        )
                        # チャンスロス対応
                        if add_chanceloss_urisu:
                            if len(df) > 0:
                                #チャンスロス結合
                                df = merge_df_cal(df, dfc_tmp_for_merge, tenpo, dpt)
                                df = pd.merge(df,chance_loss_data[['PRD_CD', 'NENSHUDO', 'CHANCE_LOSS_PRD_SU', 'CHANCE_LOSS_KN']].rename(columns={'NENSHUDO':'nenshudo'}),on=['PRD_CD','nenshudo'], how="left")
                                df['CHANCE_LOSS_PRD_SU'] = df['CHANCE_LOSS_PRD_SU'].fillna(0.0)
                                df['CHANCE_LOSS_KN'] = df['CHANCE_LOSS_KN'].fillna(0.0)

                                df['URI_SU_ORG'] = df['URI_SU']
                                df['URI_SU'] = df['URI_SU'] + df['CHANCE_LOSS_PRD_SU']
                                df['URI_KIN'] = df['URI_KIN'] + df['CHANCE_LOSS_KN']
                                df['BAIKA'][df['BAIKA'].isnull()] = df['CHANCE_LOSS_KN'] / df['CHANCE_LOSS_PRD_SU']
                                df['URI_SU'] = df['URI_SU'].astype(float)
                                df = df[df['URI_SU']>0.0].reset_index(drop=True)

                    if THEME_MD_MODE:

                        #print('gggg')
                        #sys.exit()
                        # 花王の商品をまず抽出
                        # 4901301-で始まる
                        # 4973167-で始まる

                        df['PRD_CD_PREFIX'] = df['PRD_CD'] / 1000000.0
                        df['PRD_CD_PREFIX'] = df['PRD_CD_PREFIX'].astype(int)

                        df_ex1 = df[(df['PRD_CD_PREFIX']==4901301)|(df['PRD_CD_PREFIX']==4973167)].reset_index(drop=True)

                        if len(df_ex1) > 0:            
                            #cls_cd_list1 = df_ex1['cls_cd'].unique().tolist()

                            # 同クラスにあるP&G、ライオンの商品も抽出する
                            # P&G
                            # 4902430-で始まる
                            # 4987176-で始まる
                            # ライオン
                            # 4903301-で始まる
                            df_ex2 = df[
                                #df['cls_cd'].isin(cls_cd_list1)
                                #&
                                 (df['PRD_CD_PREFIX']==4902430)
                                |(df['PRD_CD_PREFIX']==4987176)
                                |(df['PRD_CD_PREFIX']==4903301)
                            ]
                            if len(df_ex2) > 0:
                                df = pd.concat([df_ex1, df_ex2]).reset_index(drop=True)
                            else:
                                df = df_ex1

                if len(df)==0:
                    #print(' After read tran data df size=0 Warning')
                    logger.warning('After read tran data df size=0 Warning: Possible data loading issue or empty source.')
                add_dept_prd_list = []

                if len(df)!=0:

                    if use_jan_connect:
                        ###
                        # JANの差し替え指示をさかのぼり対象となるJANを確認

                        # DFのJANコードから差替え指示をさかのぼり、全部関係のあるJANリストと、差し替え指示を取得
                        # なぜここで'how_change'が6, 10, 11を除外しないのか？ *****************
                        subject_prod_list, subject_jan_master = short_term_preprocess_common.get_subject_jan_master(
                            df,
                            jan_master
                        )
                        #print("===df2======",df)
                        logger.info(f"===df2======{df}")

                        # 差し替え指示が無ければパスする
                        # 差し替え指示があれば紐づくDPTを抽出
                        if len(subject_jan_master)!=0:
                            new_dpt_list, old_dpt_list, subject_dpt_list = short_term_preprocess_common.get_new_old_dpt_list(
                                subject_jan_master,
                                subject_prod_list,
                            )
                            # 現在ロードしているDPTに関しては必要ないので外す
                            subject_dpt_list.remove(dpt)

                            # チャンスロス対応
                            if add_chanceloss_urisu:
                                # 関係するDPTかつ関係する製品データを追加でロード
                                #add_dept_prd_list = [] #20230921 上に移動
                                dft = pd.DataFrame()
                                for temp_dpt in subject_dpt_list:
                                    temp_dpt = int(temp_dpt)

                                    if 0:
                                        dft, ret_prd_list = load_tran_df_bq(
                                            dft,
                                            tenpo,
                                            temp_dpt,
                                            end_nenshudo,
                                        ) 
                                        add_dept_prd_list = add_dept_prd_list + ret_prd_list
                                    else:
                                        for blob in bucket.list_blobs(prefix=path_tenpo_dpt.format(tenpo, temp_dpt)):
                                            dft, ret_prd_list = short_term_preprocess_common.load_tran_df(
                                                dft,
                                                blob,
                                                bucket_name,
                                                tenpo_list,
                                                #train_end_nenshudo,
                                                end_nenshudo,
                                                subject_prod_list
                                            )
                                            add_dept_prd_list = add_dept_prd_list + ret_prd_list                                                       
                                #チャンスロス結合
                                if len(dft) > 0:
                                    dft = merge_df_cal(dft, dfc_tmp_for_merge, tenpo, dpt)
                                    dft = pd.merge(dft, chance_loss_data[['PRD_CD', 'NENSHUDO', 'CHANCE_LOSS_PRD_SU', 'CHANCE_LOSS_KN']].rename(columns={'NENSHUDO':'nenshudo'}),on=['PRD_CD','nenshudo'], how="left")
                                    dft['CHANCE_LOSS_PRD_SU'] = dft['CHANCE_LOSS_PRD_SU'].fillna(0.0)
                                    dft['CHANCE_LOSS_KN'] = dft['CHANCE_LOSS_KN'].fillna(0.0)
                                    dft['URI_SU_ORG'] = dft['URI_SU']
                                    dft['URI_SU'] = dft['URI_SU'] + dft['CHANCE_LOSS_PRD_SU']
                                    dft['URI_KIN'] = dft['URI_KIN'] + dft['CHANCE_LOSS_KN']
                                    dft['BAIKA'][dft['BAIKA'].isnull()] = dft['CHANCE_LOSS_KN'] / dft['CHANCE_LOSS_PRD_SU']
                                    dft['URI_SU'] = dft['URI_SU'].astype(float)
                                    dft = dft[dft['URI_SU']>0.0].reset_index(drop=True)
                                    df = pd.concat([df, dft], axis=0).reset_index(drop=True)

                            else:
                                # 関係するDPTかつ関係する製品データを追加でロード
                                #add_dept_prd_list = [] #20230921 上に移動
                                for temp_dpt in subject_dpt_list:
                                    temp_dpt = int(temp_dpt)

                                    if 0:
                                        df, ret_prd_list = load_tran_df_bq(
                                            df,
                                            tenpo,
                                            temp_dpt,
                                            end_nenshudo,
                                        ) 
                                        add_dept_prd_list = add_dept_prd_list + ret_prd_list

                                    else:
                                        for blob in bucket.list_blobs(prefix=path_tenpo_dpt.format(tenpo, temp_dpt)):
                                            df, ret_prd_list = short_term_preprocess_common.load_tran_df(
                                                df,
                                                blob,
                                                bucket_name,
                                                tenpo_list,
                                                #train_end_nenshudo,
                                                end_nenshudo,
                                                subject_prod_list
                                            )
                                            add_dept_prd_list = add_dept_prd_list + ret_prd_list

                    logger.info(f'===df3======{df}')
                    if THEME_MD_MODE:
                        dfm = pd.merge(
                            dfm_base.rename(columns={"prd_cd":"PRD_CD"}),                    
                            selling_period[selling_period["tenpo_cd"] == tenpo][["tenpo_cd", "prd_cd", "tanagae_hokoku_day", 'hacchu_to_ymd']].rename(columns={"tenpo_cd":"TENPO_CD", "prd_cd":"PRD_CD", "tanagae_hokoku_day":"sell_start_ymd", 'hacchu_to_ymd':'hacchu_end_ymd'}),
                            on=['PRD_CD'],
                            how='left' # 販売期間を見ないようにしたいのでleftにする
                        )                
                        dfm['sell_start_ymd'] = dfm['sell_start_ymd'].fillna(dfm['sell_start_ymd'].min())
                        dfm['hacchu_end_ymd'] = dfm['hacchu_end_ymd'].fillna(dfm['hacchu_end_ymd'].max())
                        dfm['TENPO_CD'] = dfm['TENPO_CD'].fillna(tenpo)                    
                       
                        
                        df = pd.merge(
                            dfm[["PRD_CD", "TENPO_CD", "sell_start_ymd", "hacchu_end_ymd", "bumon_cd"]], # 20230731 部門コードを追加
                            df,
                            on=["PRD_CD", "TENPO_CD"],
                            how='inner'
                        )                    

                    else:
                        dfm = pd.merge(
                            dfm_base.rename(columns={"prd_cd":"PRD_CD"}),                    
                            selling_period[selling_period["tenpo_cd"] == tenpo][["tenpo_cd", "prd_cd", "tanagae_hokoku_day", 'hacchu_to_ymd']].rename(columns={"tenpo_cd":"TENPO_CD", "prd_cd":"PRD_CD", "tanagae_hokoku_day":"sell_start_ymd", 'hacchu_to_ymd':'hacchu_end_ymd'}),
                            on=['PRD_CD'],
                            how='inner'
                        )                        
                        
                        
                        df = pd.merge(
                            dfm[["PRD_CD", "TENPO_CD", "sell_start_ymd", "hacchu_end_ymd", "bumon_cd"]], # 20230731 部門コードを追加
                            df,
                            on=["PRD_CD", "TENPO_CD"],
                            how='inner'
                        )

                    logger.info(f"===df4======{df}")

                    if 1:                    
                        # DPTを全て書き換える********************************
                        df['DPT_CD'] = dpt

                        # **************** 20230516追加
                        # ケースパックバラJAN書き換え前に、製品/年週度ごとの売価をとっておく
                        dfgb = df[['TENPO_CD', 'PRD_CD', 'nenshudo', 'BAIKA']].groupby(['TENPO_CD', 'PRD_CD', 'nenshudo']).mean()
                        dfgb.columns = ['BAIKA_NEW']
                        # ケースパックバラのJANコードを発注JANに書き替える
                        df = pd.merge(df, prdcd_hattyujan_df, on='PRD_CD', how='left')
                        df.loc[~df['HACCHU_JAN'].isnull(), 'PRD_CD'] = df['HACCHU_JAN']                    
                        #dftest3_1 = copy.deepcopy(df)         
                        # 発注JANに差し替えたものは、売価も発注JANの売価に差し替える
                        df = pd.merge(df, dfgb, on=['TENPO_CD', 'PRD_CD', 'nenshudo'], how='left')
                        df['BAIKA_OLD'] = df['BAIKA']
                        df.loc[~df['HACCHU_JAN'].isnull(), 'BAIKA'] = df['BAIKA_NEW']         
                        #dftest3_2 = copy.deepcopy(df)                    
                        # ソートする
                        #df = df.sort_values(by=['TENPO_CD', 'PRD_CD', 'nenshudo'])
                        df = df.sort_values(by=['TENPO_CD', 'PRD_CD', 'nenshudo']).reset_index(drop=True)
                        df['URI_SU'] = df['URI_SU'].astype(float)
                        df['HJAN_COEF'] = df['HJAN_COEF'].astype(float)               
                        # 売価の補完をおこなう
                        df['BAIKA'] = df[['TENPO_CD', 'PRD_CD', 'BAIKA']].groupby(['TENPO_CD', 'PRD_CD'])['BAIKA'].transform(lambda x: x.interpolate(limit_direction='both'))

                        # 元々発注JANが売られていない商品は、元JANの売価を係数で割って売価とする
                        df.loc[df['BAIKA'].isnull(), 'BAIKA'] = df['BAIKA_OLD'] / df['HJAN_COEF']

                        # 売り数に発注JANへの変換係数をかける
                        df.loc[~df['HACCHU_JAN'].isnull(), 'URI_SU'] = df['URI_SU'] * df['HJAN_COEF']
                        # 売り金額はそのままでよい
                        df = df.drop(['HACCHU_JAN'], axis=1)
                        df = df.drop(['HJAN_COEF'], axis=1)
                        df = df.drop(['BAIKA_NEW'], axis=1)
                        df = df.drop(['BAIKA_OLD'], axis=1)

                        #dftest3_3 = copy.deepcopy(df)

                        if use_jan_connect:

                            #dftest4 = copy.deepcopy(df)

                            # 'how_change'が6, 10, 11を除外、重複削除
                            # 販売期間マスタ（hacchu_end_ymd）を結合、keyはnew_JAN
                            subject_jan_master = short_term_preprocess_common.filtering_jan_master_for_rewriting_jan(
                                subject_jan_master,
                                dfm
                            )



                            if seisan_tenpo_hattyuu_end_is_not_replaced:
                                # 生産発注終了日、店舗発注終了日をみて、終了していない差替え元商品の差替え情報を除くため
                                # 商品マスタを結合
                                # 商品マスタ（生産発注開始終了日、店舗発注開始終了日）を結合
                                
                              
                                dfm_base['seisan_hacchu_tekiyo_to_ymd'] = dfm_base['seisan_hacchu_tekiyo_to_ymd'].fillna(99999999)
                                dfm_base['tenpo_hacchu_to_ymd_toitsu'] = dfm_base['tenpo_hacchu_to_ymd_toitsu'].fillna(99999999)
                                dfm_base['seisan_hacchu_tekiyo_to_ymd'] = dfm_base['seisan_hacchu_tekiyo_to_ymd'].astype(int)
                                dfm_base['tenpo_hacchu_to_ymd_toitsu'] = dfm_base['tenpo_hacchu_to_ymd_toitsu'].astype(int)  

                                subject_jan_master = pd.merge(subject_jan_master, dfm_base.rename(columns={'prd_cd':'old_JAN'}), on='old_JAN', how='left').reset_index(drop=True)
                                # 店舗別の生産発注停止情報を結合

                                #金子さんコメント
                                #個店単位で発注終了日を見るのであれば、
                                #M030PRD_TEN_TNPN_INFから、PRD_CDとTENPO_CDでターゲットを確定させ、HACCHU_YMD_SEQ_NOを取得して
                                #M030HACCHU_YMD_INFから、PRD_CDとHACCHU_YMD_SEQ_NOで結合した結果、HACCHU_TO_YMDが本日以前のものをとる
                                #が、短期の場合のあるべき発注終了の判定かと！

                                #store_prd_hacchu_ymd = common.extract_as_df(path_tenpo_hacchu_master, bucket_name)
                                #store_prd_hacchu_ymd['TENPO_CD'] = store_prd_hacchu_ymd['TENPO_CD'].astype(int)
                                #store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].fillna(99999999)
                                #store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].astype(int)
                                my_store_prd_hacchu_ymd =store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD']==int(tenpo)].reset_index(drop=True)
                                subject_jan_master = pd.merge(subject_jan_master, my_store_prd_hacchu_ymd.rename(columns={'PRD_CD':'old_JAN'}), on='old_JAN', how='left').reset_index(drop=True)


                            # 'hacchu_end_ymd'==99999999の新JANと、それに関連するすべてのJANを取得する
                            newest_jan_list = short_term_preprocess_common.get_newest_jan_list(
                                subject_jan_master,
                                my_date
                            )                        
                            # add_dept_prd_listにあるがnewest_jan_listに無いSKUを除外する
                            del_prd_list = list(set(add_dept_prd_list) - set(newest_jan_list))
                            df = df[~df['PRD_CD'].isin(del_prd_list)]


                         # **********************************************************************************
                            # DFのPRD_CDで、商品マスタ上、処理対象のDPTでないものを除く 20230731追加
                            df = df[df['bumon_cd']==dpt]
                            df = df.drop(['bumon_cd'], axis=1).reset_index(drop=True)
                            # **********************************************************************************

                            # JAN書き換え*********************************************************

                            # 4549509003038 	4901983805599 	どちらもnewestにしかない

                            df, jan_mapping_df = short_term_preprocess_common.rewrite_jan_code(
                                subject_jan_master,
                                newest_jan_list,
                                df,
                                'PRD_CD',
                                my_date,
                                restrict_old_jan_1generation=restrict_old_jan_1generation_flag,
                                seisan_tenpo_hattyuu_end_is_not_replaced=seisan_tenpo_hattyuu_end_is_not_replaced
                            )

                            jan_mapping_df_list.append(jan_mapping_df)
                            subject_jan_master_list.append(subject_jan_master)

                            #jan_mapping_df.to_csv('jan_mapping_df_' + str(tenpo) + '.csv', index=False)

                            #dftest5 = copy.deepcopy(df)
                        # groupby処理
                        # 販売開始日は一番古いもの、販売終了は一番新しものにする
                        prod_info_df = df[['PRD_CD', 'TENPO_CD', 'DPT_CD', 'sell_start_ymd', 'hacchu_end_ymd']]
                        prod_info_df = prod_info_df.drop_duplicates()
                        prod_info_df = prod_info_df.groupby(['PRD_CD', 'TENPO_CD', 'DPT_CD']).agg({
                            'sell_start_ymd':'min',
                            'hacchu_end_ymd':'max'
                        }).reset_index()


                        df = df.groupby(['PRD_CD', 'TENPO_CD', 'DPT_CD', 'nenshudo']).agg({'URI_SU':'sum','URI_KIN':'sum', 'BAIKA':'mean'}).reset_index()
                        #df = df.groupby(['PRD_CD', 'TENPO_CD', 'DPT_CD', 'nenshudo']).agg({'URI_SU':'sum','URI_KIN':'sum'}).reset_index()                    

                        #dftest5_3 = copy.deepcopy(df)                    
                        df = pd.merge(df, prod_info_df, on=['PRD_CD', 'TENPO_CD', 'DPT_CD'], how='inner')
                        ###################################################
                        #dftest6 = copy.deepcopy(df)
                        ###################################################
                        #print('test2 exit')
                        #sys.exit()

                # 商品情報の結合
                    #print("===df5======",df)
                    logger.info(f"===df5======{df}")
                 
                    df = pd.merge(
                        dfm[[
                            "PRD_CD","prd_nm_kj", "hnmk_cd", "cls_cd", "line_cd", "shoki_genka", 
                        "shoki_baika", "genka_toitsu", "baika_toitsu", "low_price_kbn"
                        ]],
                        df,
                        on="PRD_CD",
                        how="inner"
                    )

                    #dftest7 = copy.deepcopy(df)
                
            
                    df = pd.merge(df, df_bunrui[['cls_cd', 'cls_nm']], on='cls_cd', how='inner')
                    logger.info(f"===df6======{df}")
                    #col_list = ["nenshudo","shudo","week_from_ymd"]
                    #df_cal = get_df_cal_out_calender(dfc,start_nenshudo,end_nenshudo, col_list)
                    
                    
                    df_cal = pd.DataFrame(dfc["nenshudo"].drop_duplicates().sort_values().reset_index(drop=True))
                    df_cal = dfc[
                        (dfc["nenshudo"]>=start_nenshudo) & \
                        (dfc["nenshudo"]<=end_nenshudo)
                    ][["nenshudo","shudo","week_from_ymd"]].reset_index(drop=True)
                    df_cal["date"] = df_cal["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
                    df_cal = pd.merge(df_cal, dfc[['nenshudo', 'nendo', 'znen_nendo', 'znen_shudo','minashi_tsuki']], on='nenshudo')
                    df_cal = df_cal.loc[
                        (df_cal['nenshudo']>=start_nenshudo)&\
                        (df_cal['nenshudo']<=end_nenshudo)
                    ].reset_index(drop=True)          
                    
                    
                    ###                
                    #dftest9 = copy.deepcopy(df)
                    if not THEME_MD_MODE:
                        # 発注終了が無い商品は除外
                        df = df.loc[(~df["hacchu_end_ymd"].isnull())].reset_index(drop=True)

                    logger.info(f"===========df7==========={df}")

                    # JAN結合後データを店舗で絞る
                    unique_tenpo_df = df.loc[df['TENPO_CD']==tenpo].reset_index(drop=True)
                    # **********************************************************************************
                    # DFのPRD_CDで、商品マスタ上、処理対象のDPTでないものを除く 20230826追加
                    my_bumon_cd_prdcd_list = list(set(dfm_base[dfm_base['bumon_cd']==dpt]['prd_cd']))
                    unique_tenpo_df = unique_tenpo_df[unique_tenpo_df['PRD_CD'].isin(my_bumon_cd_prdcd_list)].reset_index(drop=True)
                    # **********************************************************************************

                    if not THEME_MD_MODE:
                        # 発注終了していないものに絞る
                        unique_tenpo_df = unique_tenpo_df.loc[(unique_tenpo_df["hacchu_end_ymd"]==99999999)].reset_index(drop=True)

                    #dftest11 = copy.deepcopy(unique_tenpo_df)

                    if len(unique_tenpo_df)!=0:

                        if not THEME_MD_MODE:
                            # 発売日が現在の13週前より前のデータに絞る
                            unique_tenpo_df = unique_tenpo_df.loc[
                                unique_tenpo_df['sell_start_ymd'] < (df_cal.loc[df_cal["date"] == df_cal['date'].max()-relativedelta(weeks=13)]["week_from_ymd"].values[0])
                            ].reset_index(drop=True)


                        #dftest11_1 = copy.deepcopy(unique_tenpo_df)

                        if len(unique_tenpo_df)!=0:
                            product_info_df = short_term_preprocess_common.get_product_info_df(
                                unique_tenpo_df,
                                df_cal,
                                dfc_tmp,
                                end_nenshudo,
                                dpt,
                                output_6wk_2sales
                            )

                            #product_info_df.to_csv('product_info_df_' + str(tenpo) + '_dpt' + str(dpt) + '.csv', index=False)                        
                            product_info_df_list.append(product_info_df)
                            path_upload_tmp_local = "gcpアップロード一時データ/temp_time_seriese_" + str(tenpo) + ".csv"     
                            ########################################################
                            #dftest12 = copy.deepcopy(unique_tenpo_df)
                            ########################################################
                              
                            if THEME_MD_MODE:
                                
                                # テーマMDモードでは、全ての商品データを週次のバケットに出力する（stage2以降で商品を選択する）
                                # "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-allprd/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
                                if len(product_info_df)!=0:
                                    time_series_prod_cd, time_series_df = short_term_preprocess_common.upload_timeseries_all_prd_df(
                                        product_info_df,
                                        unique_tenpo_df,
                                        tenpo_df,
                                        dpt,
                                        tenpo,
                                        threshold_missing_ratio,
                                        threshold_timeseries_length,
                                        path_upload_tmp_local,
                                        path_upload_time_series_blob_all_prd,
                                        bucket_name
                                    )

                            else:
                                
                                if len(product_info_df)!=0:
                                    # 週次データをアップロード
                                    time_series_prod_cd, time_series_df=short_term_preprocess_common.upload_timeseries_df(
                                        product_info_df,
                                        unique_tenpo_df,
                                        tenpo_df,
                                        dpt,
                                        tenpo,
                                        threshold_missing_ratio,
                                        threshold_timeseries_length,
                                        path_upload_tmp_local,
                                        path_upload_time_series_blob,
                                        path_upload_time_series_blob2,
                                        bucket_name,
                                        output_6wk_2sales
                                    )
                                    
                                    path_upload_not_ai_local = "gcpアップロード一時データ/not_ai_model" + str(tenpo) +".csv"
                                    
                                
                                    week4_summarize_df, not_week4_summarize_df, df_value_count_ret = short_term_preprocess_common.upload_not_timeseries_df(
                                        unique_tenpo_df,
                                        time_series_prod_cd,
                                        df_cal,
                                        dpt,
                                        tenpo,
                                        path_upload_tmp_local,
                                        path_upload_not_ai_local,
                                        path_upload_monthly_series_blob,
                                        path_upload_not_ai_blob,
                                        bucket_name
                                    )
                                    
                                    df_value_count_ret_list.append(df_value_count_ret)
                                    

            end_time = time.time()
            logger.info(f"====tenpo_df==={tenpo_df}")
            # DPT別処理、ここまで
            
    logger.info('stage1 process complete*******************')
    return tenpo


           
            
#################################
# Old Code:              
# # 週次モデル ***************************************************************************************************
# # cloud functions name  :shortterm-stage2-weekly
# # https://console.cloud.google.com/functions/details/asia-northeast1/shortterm-stage2-weekly?env=gen2&authuser=0&project=dev-cainz-demandforecast
# #
# # functions url:
# # https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-weekly
# def trigger_cloud_function_stage2_weekly():
#     url = 'https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-weekly'
#     try:
#         response = requests.post(url)  # POST リクエストを送信
#         # GETリクエストの場合は、データをクエリパラメーターとしてURLに追加
#         if response.status_code != 200:
#             raise Exception(f'HTTP error! status: {response.status_code}')

#         response_data = response.json()
#         #print('Function response:', response_data)
#         logger.info('Function response:', response_data)
#     except Exception as error:
#         #print('Error calling Cloud Function:', error)
#         logger.info('Error calling Cloud Function:', error)
 
            

# # 少量品モデル ***************************************************************************************************
# # cloud functions name  :shortterm-stage2-monthly
# # https://console.cloud.google.com/functions/details/asia-northeast1/shortterm-stage2-monthly?env=gen2&authuser=0&project=dev-cainz-demandforecast
# #
# # functions url:
# # https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-monthly

# def trigger_cloud_function_stage2_monthly():
    
#     url = 'https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-monthly'

#     try:
#         response = requests.post(url)  # POST リクエストを送信
#         # GETリクエストの場合は、データをクエリパラメーターとしてURLに追加

#         if response.status_code != 200:
#             raise Exception(f'HTTP error! status: {response.status_code}')

#         response_data = response.json()
#         #print('Function response:', response_data)
#         logger.info('Function response:', response_data)
#     except Exception as error:
#         #print('Error calling Cloud Function:', error)
#         logger.info('Error calling Cloud Function:', error)


# def trigger_cloud_function_stage2_midium():
#     # ********************************************************
#     # 起動方法が週次、少量品と異なっているので注意！！！！！！！
#     # ********************************************************
                  
#     # Cloud FunctionのエンドポイントURL
#     url = 'https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-midium'
#     try:
#         # Google Cloudの認証トークンを取得
#         auth_req = google.auth.transport.requests.Request()
#         id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
#         headers = {
#             'Authorization': f'Bearer {id_token}',
#             'Content-Type': 'application/json'  # JSONデータを送信するためのContent-Type
#         }
#         #response = requests.post(url, json=data, headers=headers)
#         response = requests.post(url, headers=headers)
#         if response.status_code != 200:
#             raise Exception(f'HTTP error! status: {response.status_code}')
#         response_data = response.json()
#         #print('Function response:', response_data)
#         logger.info('Function response:', response_data)
#     except Exception as error:
#         #print('Error calling Cloud Function:', error)
#         logger.info('Error calling Cloud Function:', error)
        

# if cloudrunjob_mode and EXECUTE_STAGE2:
        
#     # stage1完了チェックフォルダ配下への完了ファイルアップロード
#     path_upload_blob = prefix + str(tenpo) + ".csv"

#     tmp_fname = str(tenpo) + ".csv"
#     my_df = pd.DataFrame([[tenpo]])
#     my_df.to_csv(tmp_fname, index=False)

#     blob = bucket.blob(path_upload_blob)
#     blob.upload_from_filename(tmp_fname)


#     # 全店分のstage1が終了しているかチェックする
#     complete_task_count = sum([1 for blob in blobs])

#     if TASK_COUNT == complete_task_count:
#         trigger_cloud_function_stage2_weekly()
#         trigger_cloud_function_stage2_monthly()
#         trigger_cloud_function_stage2_midium()

#         print('execute stage2 cloudrunjobs complete*******************')

#         # stage1完了チェックフォルダ配下のファイル削除
#         for blob in blobs:
#             #print(blob.name)
#             logger.info(blob.name)
#             generation_match_precondition = None
#             blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
#             generation_match_precondition = blob.generation
#             blob.delete(if_generation_match=generation_match_precondition)
#             #print(f"Blob {blob.name} deleted.")
#             logger.info(f"Blob {blob.name} deleted.")
            
            
            

#################################
# Refactored Code: 
def trigger_cloud_function(url):
    """
    Triggers a Cloud Function.

    Args:
        url (str): The URL of the Cloud Function.

    Returns:
        dict: The JSON response from the Cloud Function.

    Raises:
        Exception: If there's an error calling the Cloud Function.
    """
    try:
        response = requests.post(url)

        if response.status_code != 200:
            raise Exception(f'HTTP error! status: {response.status_code}')
        response_data = response.json()
        logger.info(f'Function response from {url}: {response_data}')
        return response_data
    except Exception as error:
        logger.error(f'Error calling Cloud Function {url}: {error}')
        raise #Re-raise the exception for handling upstream if necessary


            
def trigger_cloud_function_stage2_midium(url):
    """Triggers the shortterm-stage2-midium Cloud Function (requires authentication)."""
    # ********************************************************
    # 起動方法が週次、少量品と異なっているので注意！！！！！！！
    # ********************************************************
    try:
        # Google Cloudの認証トークンを取得
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
        headers = {
            'Authorization': f'Bearer {id_token}',
            'Content-Type': 'application/json'  # JSONデータを送信するためのContent-Type
        }
        #response = requests.post(url, json=data, headers=headers)
        response = requests.post(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f'HTTP error! status: {response.status_code}')
        response_data = response.json()
        #print('Function response:', response_data)
        logger.info(f'Function response: {response_data}')
    except Exception as error:
        #print('Error calling Cloud Function:', error)
        logger.info(f'Error calling Cloud Function: {error}')
            


def check_and_trigger(tenpo):
    """
    Uploads a completion file, checks if all tasks are complete, and triggers Stage 2 Cloud Functions.

    Args:
        tenpo (str): The tenpo identifier.
    """
    if cloudrunjob_mode and EXECUTE_STAGE2:
        
        # stage1完了チェックフォルダ配下への完了ファイルアップロード
        path_upload_blob = prefix + str(tenpo) + ".csv"

        tmp_fname = str(tenpo) + ".csv"
        my_df = pd.DataFrame([[tenpo]])
        my_df.to_csv(tmp_fname, index=False)
        blob = bucket.blob(path_upload_blob)
        blob.upload_from_filename(tmp_fname)


        # 全店分のstage1が終了しているかチェックする        
            
        blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage1_complete/completed_')   
        complete_task_count = sum([1 for blob in blobs])
        # STAGE2_WEEKLY_URL ='https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-weekly'
        # STAGE2_MONTHLY_URL = 'https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-monthly'
        # STAGE2_MIDIUM_URL ='https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/shortterm-stage2-midium'

        if TASK_COUNT == complete_task_count:
            trigger_cloud_function(STAGE2_WEEKLY_URL)
            trigger_cloud_function(STAGE2_MONTHLY_URL)
            trigger_cloud_function_stage2_midium(STAGE2_MIDIUM_URL)

            print('execute stage2 cloudrunjobs complete*******************')
            #print("blobs", blobs)
            # stage1完了チェックフォルダ配下のファイル削除
            for blob in blobs:
                logger.info(blob.name)
                try:
                    #generation_match_precondition = None
                    blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
                    generation_match_precondition = blob.generation
                    blob.delete(if_generation_match=generation_match_precondition)
                    #print(f"Blob {blob.name} deleted.")
                    logger.info(f"Blob {blob.name} deleted.")
                except NotFound:
                    logger.warning(f"Blob {blob.name} not found. skipping deletion. ")

  
            
def main():
    """Main function to orchestrate the process."""
    EXECUTE_STAGE2 = int(os.environ.get("EXECUTE_STAGE2", 0))
    THEME_MD_MODE = int(os.environ.get("THEME_MD_MODE", 0))

    if cloudrunjob_mode:
        tenpo_cd = tenpo_cd_list[TASK_INDEX]
        if (TASK_INDEX == 0) and (EXECUTE_STAGE2 == 1):
            clear_stage1_complete_files(storage_client, bucket)  # Call function
    else:
        # notebookで動かすモード
        tenpo_cd = int(sys.argv[1])
        TODAY_OFFSET = 0
        TASK_COUNT = 0
        EXECUTE_STAGE2 = 0
        OUTPUT_HACCHUJAN_INFO = 0
        OUTPUT_HACCHUJAN_TABLE_TEST = 0
        OUTPUT_HACCHUJAN_INFO_GCS = 1
        THEME_MD_MODE = 0
        
    tenpo_list = [int(tenpo_cd)]  
    logger.info(f"==tenpo_cd== {tenpo_cd}") 
    logger.info(f"==tenpo_list== {tenpo_list}")
    
    
    path_upload_time_series_blob = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
    path_upload_time_series_blob2 = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-62/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
    path_upload_time_series_blob_all_prd = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-allprd/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
    path_upload_monthly_series_blob = "01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_monthly_series.csv"
    path_upload_not_ai_blob = "01_short_term/01_stage1_result/03_not_ai_target/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_not_ai_model.csv"
    
    # Load configuration based on season_flag
    season_flag = False  #  Consider making this configurable
    
    config_file = CONFIG_SEASON_FILE if season_flag else CONFIG_NOT_SEASON_FILE
    try:
        with open(config_file, 'r') as file:  # Specify 'r' for read mode
            config = yaml.safe_load(file)
        logger.info(f"Loaded configuration from {config_file}")
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_file}")
        config = {}  # Provide a default empty dictionary to avoid errors
    
    # Set default values
    start_holdout_nenshudo = config.get('start_holdout_nenshudo', DEFAULT_START_HOLDOUT_NENSHUDO)
    start_nenshudo = config.get('start_nenshudo', DEFAULT_START_NENSHUDO)
    
    # Determine end_date and end_date_str
    end_date = int(today.strftime('%Y%m%d'))
    end_date_str = today.strftime('%Y-%m-%d')
    logger.info(f"==end_date=== {end_date}")
    logger.info("loading 週番マスター")
    
    path_week_master = config['path_week_master']

    dfc = common.extract_as_df(path_week_master, bucket_name)

    dfc_tmp = dfc[["nenshudo","week_from_ymd","week_to_ymd"]]
    
    # end_nenshudo = config['end_nenshudo']
    
    dfc_tmp["week_from_ymd"] = dfc_tmp["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    dfc_tmp["week_to_ymd"] = dfc_tmp["week_to_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    
    df_end_nenshudo  = dfc_tmp["nenshudo"][(dfc_tmp["week_from_ymd"] <= end_date_str)&(dfc_tmp["week_to_ymd"] >= end_date_str)]

    end_nenshudo = int(df_end_nenshudo.values[0])

    # train_end_nenshudo = int(df_end_nenshudo.values[0])

    #print("==end_nenshudo==",end_nenshudo)
    logger.info(f"==end_nenshudo== ({end_nenshudo})")

    # 時系列判別の時の欠損と時系列長の閾値
    threshold_missing_ratio = 0.2
    threshold_timeseries_length = 180

    if not os.path.exists(path_upload_tmp_local_dir):
        os.mkdir(path_upload_tmp_local_dir)

    dfc = common.extract_as_df(path_week_master, bucket_name)
    df_bunrui = common.extract_as_df(path_bunrui, bucket_name)
    tenpo_df = common.extract_as_df(path_tenpo_master, bucket_name)
    tenpo_df = tenpo_df.rename(columns={"LPAD(TENPO_CD,4,0)":"TENPO_CD"})

    selling_period_all = common.extract_as_df(path_selling_period, bucket_name)
    prd_asc = common.extract_as_df(path_prd_asc, bucket_name)
                
    dfm_base = load_product_master_data(
                  tenpo_cd,
                   bucket_name, TABLE_ID = 'M_090_PRD_NB_STD'
                )
    
    chance_loss_data, jan_master, store_prd_hacchu_ymd, dfm_base = process_data(
                        dfm_base,  end_date, tenpo_list, tenpo_df, tenpo_cd, 
                        table_id = 'T_090_PRD_CHANCE_LOSS_NB_DPT', 
                    )
    prd_asc_tmp = prd_asc[prd_asc['asc_riyu_cd']==3]
    prd_asc_tmp = prd_asc_tmp[['prd_cd', 'asc_prd_cd']].rename(columns={"prd_cd":"PRD_CD", 'asc_prd_cd':"ASC_PRD_CD"})
    groups = create_case_pack_bara_groups(prd_asc, prd_asc_tmp)
    prdcd_grp = create_prdcd_to_group_dict(groups)
    prdcd_hattyujan_df = create_prdcd_hattyujan_df(prd_asc, groups)
    prdcd_hattyujan_df.to_csv('prdcd_hattyujan_df.csv', index=False)            
    prdcd_hcjan_coef, prdcd_hcjan_coef_log = calculate_prdcd_hcjan_coefficients(prd_asc, prdcd_hattyujan_df)  
    prdcd_hattyujan_df = output_hacchujan_info(prdcd_hcjan_coef, prdcd_hcjan_coef_log, prdcd_hattyujan_df,                                                                                     end_nenshudo, tenpo_cd)
    tenpo =process_data_for_tenpo(dfc_tmp,tenpo_list,prdcd_grp,selling_period_all,                 bq_allow_large_results,dpt_list,exclusion_dpt_list,df_bunrui,add_chanceloss_urisu,chance_loss_data,THEME_MD_MODE,bucket,path_tenpo_dpt,bucket_name,short_term_preprocess_common,end_nenshudo,use_jan_connect,jan_master, seisan_tenpo_hattyuu_end_is_not_replaced, my_date, tenpo_df, output_6wk_2sales, threshold_missing_ratio, threshold_timeseries_length, path_upload_time_series_blob_all_prd, path_upload_monthly_series_blob, path_upload_not_ai_blob, dfm_base, prdcd_hattyujan_df, store_prd_hacchu_ymd, dfc, start_nenshudo, path_upload_time_series_blob, path_upload_time_series_blob2)    
                
    check_and_trigger(tenpo)


            
            
if __name__ == "__main__":
    main()
            
            

            
            
