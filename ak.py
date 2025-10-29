######################This is stage 2 Monthly test main.py

import bisect
from google.cloud import bigquery, storage
import requests
import google.auth
import google.auth.transport.requests
import google.oauth2.id_token
from pandas import read_csv
import pandas as pd
import numpy as np
import warnings
import sys
import datetime
import time
from google.cloud.bigquery import Client as BigqueryClient
import os
import openpyxl
import numpy as np
import yaml
import logging
import yaml
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import time
import copy
from io import BytesIO
import pytz
import swifter
import statsmodels.api as st
from google.cloud import bigquery
import logging

sys.path.append("repos/cainz_demand-forecast/cainz/")
from common import common #, dr_automation
sys.path.append("repos/cainz_demand-forecast/cainz/")

warnings.filterwarnings("ignore")

project_id = "dev-cainz-demandforecast"

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))
TODAY_OFFSET = int(os.environ.get("TODAY_OFFSET", 0))
OUTPUT_TABLE_SUFFIX = os.environ.get("OUTPUT_TABLE_SUFFIX", "amol_monthly_test")
CALL_NEXT_PIPELINE = int(os.environ.get("CALL_NEXT_PIPELINE", 0))
OUTPUT_METRICS_VALUE = int(os.environ.get("OUTPUT_METRICS_VALUE", 0))
OUTPUT_COLLECTED_SALES_VALUE = int(os.environ.get("OUTPUT_COLLECTED_SALES_VALUE", 0))
SEASONAL_TRAINDATA_TABLE = str(os.environ.get("SEASONAL_TRAINDATA_TABLE", "monthly-train-seasonal-12-2025-06-10_obon_218str"))

TURN_BACK_YYYYMMDD = os.environ.get("TURN_BACK_YYYYMMDD", "")


# print('TASK_INDEX:', TASK_INDEX)
# print('TASK_COUNT:', TASK_COUNT)
# print('TODAY_OFFSET:', TODAY_OFFSET)
# print('OUTPUT_TABLE_SUFFIX:', OUTPUT_TABLE_SUFFIX)
# print('CALL_NEXT_PIPELINE:', CALL_NEXT_PIPELINE)
# print('OUTPUT_METRICS_VALUE:', OUTPUT_METRICS_VALUE)
# print('SEASONAL_TRAINDATA_TABLE:', SEASONAL_TRAINDATA_TABLE)
# print('TURN_BACK_YYYYMMDD:', TURN_BACK_YYYYMMDD)

#####################################
# REFACTORED CODE
logger.info(f'TASK INDEX: {TASK_INDEX}')
logger.info(f'TASK COUNT: {TASK_COUNT}')
logger.info(f'TODAY OFFSET: {TODAY_OFFSET}')
logger.info(f'OUTPUT_TABLE_SUFFIX: {OUTPUT_TABLE_SUFFIX}')
logger.info(f'CALL NEXT_PIPELINE: {CALL_NEXT_PIPELINE}')
logger.info(f'OUTPUT_METRICS_VALUE: {OUTPUT_METRICS_VALUE}')
logger.info(f'SEASONAL_TRAINDATA_TABLE: {SEASONAL_TRAINDATA_TABLE}')
logger.info(f'TURN_BACK_YYYYMMDD: {TURN_BACK_YYYYMMDD}')



# 全店拡大20250130 218店舗（古河は13週経過したので入れる）
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 910
]

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)

cloudrunjob_mode = False


if cloudrunjob_mode:
    tenpo_cd = tenpo_cd_list[TASK_INDEX]
    logger.info(f'TASK_INDEX: {TASK_INDEX}, tenpo_cd: {tenpo_cd}')

    if (TASK_INDEX == 0) and (CALL_NEXT_PIPELINE == 1):
        # stage2完了チェックフォルダ配下のファイル削除
        storage_client = storage.Client()
        bucket_name = "dev-cainz-demandforecast"
        bucket = storage_client.bucket(bucket_name)

        blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_monthly_complete/completed_')
        for blob in blobs:
            logger.info(blob.name) # Original print(blob.name)
            generation_match_precondition = None
            blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
            generation_match_precondition = blob.generation
            try:
                blob.delete(if_generation_match=generation_match_precondition)
                logger.info(f"Blob {blob.name} deleted.")
            except Exception as e:
                logger.error(f"Error deleting blob {blob.name}: {e}")

else:
    tenpo_cd = int(sys.argv[1])
    logger.info(f'tenpo_cd: {tenpo_cd}')
    TODAY_OFFSET = 0
    logger.info(f'TODAY_OFFSET: {TODAY_OFFSET}')
    OUTPUT_TABLE_SUFFIX = 'amol_monthly_test'
    logger.info(f'OUTPUT_TABLE_SUFFIX: {OUTPUT_TABLE_SUFFIX}')
    OUTPUT_COLLECTED_SALES_VALUE = 0
    logger.info(f'OUTPUT_COLLECTED_SALES_VALUE: {OUTPUT_COLLECTED_SALES_VALUE}')
    OUTPUT_METRICS_VALUE = 0
    logger.info(f'OUTPUT_METRICS_VALUE: {OUTPUT_METRICS_VALUE}')
    SEASONAL_TRAINDATA_TABLE = 'monthly-train-seasonal-12-2025-01-30_218newstr'
    logger.info(f'SEASONAL_TRAINDATA_TABLE: {SEASONAL_TRAINDATA_TABLE}')
    TURN_BACK_YYYYMMDD = ''
    logger.info(f'TURN_BACK_YYYYMMDD: {TURN_BACK_YYYYMMDD}')



## シーズン品に絞るかを指定
season_flag = False

if season_flag:
    with open('input_data/00_config_season.yaml') as file:
        config = yaml.safe_load(file.read())
else:
    with open('input_data/00_config_not_season.yaml') as file:
        config = yaml.safe_load(file.read())

model_name=config['model_name']

# 必要なパスの取り出し
path_week_master = config['path_week_master']

# stage1の前日以前の過去データを使うフラグ*******
use_past_stage1_data_flag = True
past_days_num = int(TODAY_OFFSET)
t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, 'JST')
if use_past_stage1_data_flag:
    today = datetime.datetime.now(JST) - timedelta(days=past_days_num)
    today_date_str = today.strftime('%Y-%m-%d')
    today = datetime.date(today.year, today.month, today.day)
else:
    today = datetime.datetime.now(JST)
    today_date_str = today.strftime('%Y-%m-%d')
    today = datetime.date(today.year, today.month, today.day)

my_date = today.strftime('%Y%m%d')
my_date = int(my_date)

# flag設定 *************************************************************
# JAN差し替えファイルを使うかどうか指定する
use_jan_connect = True

# ODAS補正の改良を有効にする
odas_imprvmnt = True

# 目的変数を対数化
logarithmize_target_variable = False

# 階層モデルを使う
# hierarchical_model = True

# モデル2分割(3分割のときはFalseにする、　階層モデルを使う際は無効)
# model_devide_2 = False

# MinMaxにない商品、MinMax下限が0の商品を除く
restrict_minmax = True

# 店舗発注が終わっている商品を除く
restrinct_tenpo_hacchu_end = True

# 階層モデルデータから、3分割上位を抜く
extract_hieralchymodeldata_without_large = True

# 販売のない販売期間のweightを0にする(予測データでは不要)
no_sales_term_weight_zero = False
no_sales_term_set_ave = False

# さらに、販売のある期間で販売数が０の週のweightを半分にする(予測データではweightは無いのでコメントのみ)
sales0_on_salesterm_weight_half = False
sales0_on_salesterm_weight_half_coef = 0.1

# 直近過去8週の補正ずみ売り数データをBQにアップする
#OUTPUT_COLLECTED_SALES_VALUE
output_collected_sales_value = True

# 予測期間を5+15週から、5+4週にする
prediction_term_4week = False

# 4週にせず可変にする場合は、下記をTrueに設定する
prediction_term_valiable = True    ############ 年末積み増し対応の場合は20週予測にするので、ここをFalseにする。prediction_term_4weekもFalseにすること ##########

#prediction_term = 46
prediction_term = 5 # 5週未来予測　トータル11週　20240709から本番化
contex_term_valiable = False
contex_term=4
#contex_term=52

# 価格を事前に既知にする
kakaku_jizen_kichi = True

# 月別で予測するデータを出力する
# monthly_data_pred = False

# 評価指標データを作成する
make_metrics_data = True

# シーズン品のデータを出力する
devide_season_items = True

# モデル学習上の現在日時を巻き戻す
turn_back_time = False
#turn_back_yyyymmdd = 20230424
if turn_back_time:
    turn_back_yyyymmdd = int(TURN_BACK_YYYYMMDD)
    today_date_str = str(TURN_BACK_YYYYMMDD)
    logger.info(f"******************* today_date_str_turnback {today_date_str}")

# メトリックスの出力先をテスト用テーブルにする
output_metrics_test_tbl = False
# 新店の参照店舗のデータを作成する
add_reference_store = True
# 新店と既存店を同じモデルにする
add_reference_store_unitedmodel = True
# 繁忙期フラグ
salesup_flag = True

# 93を追加 20240304
dpt_list = [69,97,14,37,27,39,28,74,33,30,36,75,85,80,20,22,55,72,15,62,32,77,84,89,23,60,25,87,68,56,92,61,2,40,86,88,26,17,24,34,52,64,73,21,35,58,83,94,63,38,18,29,19,31,53,45,50,81,82,90,91,54,95,93]

exclusion_dpt_list = [2,20,26,30,33,37,39,47,48,53,57,80,98]


def get_ranker_prdcd_list():
    
    bunrui_query = f"""
    WITH my2125rank AS (
        SELECT DISTINCT PRD_CD 
        FROM `dev-cainz-demandforecast.dev_cainz_nssol.T_090_URIAGE_JSK_NB_SHU_TEN_DPT_20242125_RANK`
        where RANK in ('S', 'A')
    ), 
    nearrank AS (
        SELECT DISTINCT PRD_CD 
        FROM `dev-cainz-demandforecast.dev_cainz_nssol.T_090_URIAGE_JSK_NB_SHU_TEN_DPT_NEAR13WK_RANK` 
        WHERE RANK IN ('S', 'A')
    )
    SELECT a.PRD_CD 
    FROM my2125rank a
    INNER JOIN nearrank b ON a.PRD_CD = b.PRD_CD;
    """

    df_ranker_prdcd_df = pd.read_gbq(bunrui_query, project_id, dialect='standard')
    df_ranker_prdcd_df['PRD_CD'] = df_ranker_prdcd_df['PRD_CD'].astype(int)
    return df_ranker_prdcd_df['PRD_CD'].tolist()


def get_prd_bunrui():
    bunrui_table_id = 'M00_PRD_BUNRUI'
    bunrui_query = f"""
    SELECT PRD_CD, cls_cd, CLS_NM, DPT_CD, DPT_NM
    FROM {dataset_id}.{bunrui_table_id}
    """
    df_bunrui = pd.read_gbq(bunrui_query, project_id, dialect='standard')
    logger.info(f"{df_bunrui}")
    return df_bunrui


def calc_incriment_nenshudo(value_nenshudo):
    str_value_nenshudo = str(value_nenshudo)
    value_nenshudo_part = str_value_nenshudo[5:8]
    logger.info(f"====value_nenshudo==== {value_nenshudo_part}")
    return value_nenshudo


def get_last_year_sales_amount(df):
#    df[["time_leep8"]] = 0
    df_tmp = df[["商品コード","前年週開始日付","8週平均ema"]]
    df_tmp = df_tmp.rename(columns={'8週平均ema': 'time_leap8'})
    df_tmp2 = pd.merge(df,df_tmp,on = ["商品コード","前年週開始日付"])
    return df_tmp2


def get_delete_duplicate(created_table_id, original_table_id):
#    bunrui_table_id = 'M00_PRD_BUNRUI'
#    created_table_id = created_table_id
    bunrui_query = f"""
    create or replace table `dev-cainz-demandforecast.kida_test3."""+str(created_table_id)+"""` as
select PrdCd, DiscountRate, LineCd, ClassCd, DPTCd, weekstartdatestamp, tenpo_cd, PreviousYearSalesActualQuantity,time_leep8  
 from `dev-cainz-demandforecast.kida_test3."""+str(original_table_id)+"""`
 where time_leep8 != 0 group by PrdCd, DiscountRate, LineCd, ClassCd, DPTCd, weekstartdatestamp, tenpo_cd, PreviousYearSalesActualQuantity,time_leep8 """
    df_bunrui = pd.read_gbq(bunrui_query, project_id, dialect='standard')
    logger.info(f"{df_bunrui}")
    return df_bunrui


def extract_as_df_with_encoding(SOURCE_BLOB_NAME, BUCKET_NAME, encoding):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(SOURCE_BLOB_NAME)
    with blob.open(mode="rb") as f:
        df = pd.read_csv(f, encoding=encoding)
    return df

def extract_as_df(
    source_blob_name,
    bucket_name="dev-cainz-demandforecast",
    encoding="utf-8",
    usecols=None,
):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    content = blob.download_as_bytes()
    df = pd.read_csv(BytesIO(content), encoding=encoding, usecols=usecols)
    return df


def load_multiple_df(
    path_list,
    bucket_name,
    usecols=None
):
    df = pd.DataFrame()
    for path in path_list:
        df_tmp = extract_as_df(path, bucket_name, usecols=usecols)
        df = pd.concat([df, df_tmp], axis=0).reset_index(drop=True)        
    return df


def interpolate_df(sales_df, df_calendar):
    max_nenshudo = sales_df["nenshudo"].max()
    min_nenshudo = sales_df["nenshudo"].min()
    df_calendar_tmp = df_calendar[["nenshudo"]]
    df_calendar_tmp = df_calendar_tmp[df_calendar_tmp["nenshudo"] <= max_nenshudo].reset_index(drop=True)
    df_calendar_tmp = df_calendar_tmp[df_calendar_tmp["nenshudo"] >= min_nenshudo].reset_index(drop=True)
    prd_cd_list = sales_df['PRD_CD'].values.tolist()
    prd_cd_list = list(set(prd_cd_list))

    for prd_cd in prd_cd_list:
        df_calendar_tmp[prd_cd] = 0


    df_calendar_tmp = pd.melt(df_calendar_tmp, id_vars=["nenshudo"], var_name="PRD_CD", value_name="delete")
    df_calendar_tmp = df_calendar_tmp[["nenshudo", "PRD_CD"]]

    sales_df["key"] = sales_df["nenshudo"].astype(str).str.cat(sales_df["PRD_CD"].astype(str), sep='-')
    df_calendar_tmp["key"] = df_calendar_tmp["nenshudo"].astype(str).str.cat(df_calendar_tmp["PRD_CD"].astype(str), sep='-')

    merged_sales_df = pd.merge(df_calendar_tmp, sales_df[["key", "URI_SU", "TENPO_CD", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']], how="left", on="key")
    
    
    merged_sales_df['URI_SU'] = merged_sales_df['URI_SU'].fillna(0)
    
    merged_sales_df[merged_sales_df['baika_toitsu'] == 0]['baika_toitsu'] = np.nan
    merged_sales_df['baika_toitsu'] = merged_sales_df['baika_toitsu'].astype(float)
    merged_sales_df['baika_toitsu'] = merged_sales_df.groupby(["PRD_CD"])['baika_toitsu'].transform(lambda x: x.interpolate(limit_direction='both'))
    
    merged_sales_df['BAIKA'] = merged_sales_df['BAIKA'].astype(float)
    merged_sales_df['BAIKA'] = merged_sales_df.groupby(["PRD_CD"])['BAIKA'].transform(lambda x: x.interpolate(limit_direction='both'))

    merged_sales_df['DPT'] = merged_sales_df['DPT'].astype(float)
    merged_sales_df['DPT'] = merged_sales_df.groupby(["PRD_CD"])['DPT'].transform(lambda x: x.interpolate(limit_direction='both'))
    merged_sales_df['DPT'] = merged_sales_df['DPT'].astype(int)
        
    merged_sales_df['line_cd'] = merged_sales_df['line_cd'].astype(float)
    merged_sales_df['line_cd'] = merged_sales_df.groupby(["PRD_CD"])['line_cd'].transform(lambda x: x.interpolate(limit_direction='both'))
    merged_sales_df['line_cd'] = merged_sales_df['line_cd'].astype(int)
    
    merged_sales_df['cls_cd'] = merged_sales_df['cls_cd'].astype(float)
    merged_sales_df['cls_cd'] = merged_sales_df.groupby(["PRD_CD"])['cls_cd'].transform(lambda x: x.interpolate(limit_direction='both'))    
    merged_sales_df['cls_cd'] = merged_sales_df['cls_cd'].astype(int)

    merged_sales_df['hnmk_cd'] = merged_sales_df['hnmk_cd'].astype(float)
    merged_sales_df['hnmk_cd'] = merged_sales_df.groupby(["PRD_CD"])['hnmk_cd'].transform(lambda x: x.interpolate(limit_direction='both'))    
    merged_sales_df['hnmk_cd'] = merged_sales_df['hnmk_cd'].astype(int)
    
    merged_sales_df = merged_sales_df.fillna(0)
    merged_sales_df = merged_sales_df.drop("key", axis=1)

    return merged_sales_df


def interpolate_df2(sales_df):
    
    sales_df['前年売上実績数量'] = sales_df['前年売上実績数量'].fillna(0)
    
    if 1:    
        sales_df[sales_df['baika_toitsu'] == 0]['baika_toitsu'] = np.nan
        sales_df['baika_toitsu'] = sales_df['baika_toitsu'].astype(float)
        sales_df['baika_toitsu'] = sales_df.groupby(["商品コード"])['baika_toitsu'].transform(lambda x: x.interpolate(limit_direction='both'))

        sales_df['BAIKA'] = sales_df['BAIKA'].astype(float)
        sales_df['BAIKA'] = sales_df.groupby(["商品コード"])['BAIKA'].transform(lambda x: x.interpolate(limit_direction='both'))

        sales_df['DPT'] = sales_df['DPT'].astype(float)
        sales_df['DPT'] = sales_df.groupby(["商品コード"])['DPT'].transform(lambda x: x.interpolate(limit_direction='both'))
        sales_df['DPT'] = sales_df['DPT'].astype(int)

        sales_df['line_cd'] = sales_df['line_cd'].astype(float)
        sales_df['line_cd'] = sales_df.groupby(["商品コード"])['line_cd'].transform(lambda x: x.interpolate(limit_direction='both'))
        sales_df['line_cd'] = sales_df['line_cd'].astype(int)

        sales_df['cls_cd'] = sales_df['cls_cd'].astype(float)
        sales_df['cls_cd'] = sales_df.groupby(["商品コード"])['cls_cd'].transform(lambda x: x.interpolate(limit_direction='both'))    
        sales_df['cls_cd'] = sales_df['cls_cd'].astype(int)

        sales_df['hnmk_cd'] = sales_df['hnmk_cd'].astype(float)
        sales_df['hnmk_cd'] = sales_df.groupby(["商品コード"])['hnmk_cd'].transform(lambda x: x.interpolate(limit_direction='both'))    
        sales_df['hnmk_cd'] = sales_df['hnmk_cd'].astype(int)

    return sales_df


def get_df_cal_out_calender(
    dfc,
    start_nenshudo,
    end_nenshudo,
):
    col_list = ["nenshudo","shudo","week_from_ymd", 'nendo', 'znen_nendo', 'znen_shudo', 'minashi_tsuki']
    df_cal = pd.DataFrame(dfc["nenshudo"].drop_duplicates().sort_values().reset_index(drop=True))
    df_cal = dfc[
        (dfc["nenshudo"]>=start_nenshudo) & \
        (dfc["nenshudo"]<=end_nenshudo)
    ][col_list].reset_index(drop=True)
    df_cal["date"] = df_cal["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    
    df_cal = df_cal.loc[
        (df_cal['nenshudo']>=start_nenshudo) & \
        (df_cal['nenshudo']<=end_nenshudo)
    ].reset_index(drop=True)
    
    return df_cal


def odas_correct(df_calendar, bucket_name, tenpo_cd, use_jan_connect=True):
    # old odas 
    if use_jan_connect:
        path_odas_list = "01_short_term/60_cached_data/07_odas_old/ODAS_old.csv"
        df_odas_old = extract_as_df(path_odas_list, bucket_name, "utf-8", ["店番","JAN","数量","売上計上日"])
    else:
        path_odas_list = [
                "Basic_Analysis_unzip_result/01_Data/22_ODAS_oldmodel/ODAS_old_20170101-20171231.csv",
                "Basic_Analysis_unzip_result/01_Data/22_ODAS_oldmodel/ODAS_old_20180101-20181231.csv",
                "Basic_Analysis_unzip_result/01_Data/22_ODAS_oldmodel/ODAS_old_20190101-20191231.csv",
                "Basic_Analysis_unzip_result/01_Data/22_ODAS_oldmodel/ODAS_old_20200101-20201231.csv",
                "Basic_Analysis_unzip_result/01_Data/22_ODAS_oldmodel/ODAS_old_20210101-20211227.csv"
                ]
        #df_odas_old = extract_as_df(path_odas_list, bucket_name, "utf-8", ["店番","JAN","数量","売上計上日"])
        df_odas_old = load_multiple_df(
            path_odas_list,
            bucket_name
        )
        # JANがnullのものがあるので除外]
        df_odas_old = df_odas_old.loc[~df_odas_old['JAN'].isnull()]

        df_odas_old = df_odas_old.loc[df_odas_old['店番'] == int(tenpo_cd)]

        # 書き換え処理
        #target_df = target_df.rename(columns={'JAN':'PRD_CD'})
        #target_df = ext_common.jan_replace(target_df, jan_df)
        #target_df = target_df.rename(columns={'PRD_CD':'JAN'})

        # 集計処理に必要なマスターのロードと加工
        path_week_master = "short_term_train/01_Data/10_week_m/WEEK_MST.csv"
        dfc = extract_as_df(path_week_master, bucket_name)
        df_cal = get_df_cal_out_calender(
            dfc,
            dfc['nenshudo'].min(),
            dfc['nenshudo'].max(),
        )

        # 書き換え処理
        #target_df = ext_common.rewrite_old_odas_jan_code(
        #    target_df,
        #    jan_df,
        #    df_cal,
        #)
        
        uriage_keijobi = df_odas_old["売上計上日"].to_list()
        week_from_ymd = df_cal["week_from_ymd"].to_list()
        uriage_keijobi_weekly = uriage_keijobi.copy()
        for no_uriage_keijobi, keijobi in enumerate(uriage_keijobi):
            # 二分探索
            insert = bisect.bisect(week_from_ymd, keijobi)-1
            uriage_keijobi_weekly[no_uriage_keijobi] = week_from_ymd[insert]             

        df_odas_old["売上計上日"] = uriage_keijobi_weekly
        df_odas_old = df_odas_old[~(df_odas_old["JAN"].isnull()) & ~(df_odas_old["売上計上日"].isnull())]
        df_odas_old = df_odas_old.groupby(["店番", "JAN", "売上計上日"]).agg({
            "数量":"sum",
        }).reset_index()
        df_odas_old['JAN'] = df_odas_old['JAN'].astype(int)
        df_odas_old['売上計上日'] = df_odas_old['売上計上日'].astype(int)

        logger.info(f"{tenpo_cd} df_odas_old shape: {df_odas_old.shape}")
        #df_odas_old.to_csv('df_odas_old.csv')
        
    
    df_odas_old = df_odas_old[df_odas_old['店番'] == int(tenpo_cd)].reset_index(drop=True)    
    df_odas_old = df_odas_old.rename(columns={"店番":"TENPO_CD", "JAN":"PRD_CD", "数量": "odas_amount","売上計上日":"sales_ymd"})

    df_odas_calender = pd.DataFrame()

    for wfy,wty,nsd in zip(df_calendar["week_from_ymd"],df_calendar["week_to_ymd"],df_calendar["nenshudo"]):
        tmp = df_odas_old[(df_odas_old["sales_ymd"]>=wfy)&(df_odas_old["sales_ymd"]<=wty)]
        tmp["nenshudo"] = nsd
        df_odas_calender = pd.concat([df_odas_calender,tmp])
        df_odas_calender = df_odas_calender.reset_index(drop=True)

    df_odas_calender = df_odas_calender.dropna(subset=['PRD_CD']).reset_index(drop=True)

    # new odas 
    if use_jan_connect:
        path_new_odas = "01_short_term/60_cached_data/08_odas_new/ODAS_new.csv"
        df_odas_new = extract_as_df(path_new_odas, bucket_name, "utf-8", ["tenpo_cd","prd_cd","amount","sales_date"])
    else:
        path_odas_list = ["Basic_Analysis_unzip_result/01_Data/21_ODAS/Odas_Order_Detail__c.csv"]
        
        df_odas_new = load_multiple_df(
            path_odas_list,
            bucket_name
        )
        
        logger.info(f'***df_odas_new 0 shape: {df_odas_new.shape}')
        # ここはtenpo_cdをint型にしなくてよいか？　差し替え前データの店舗コードは文字列("""")なのでよいはず
        df_odas_new = df_odas_new[df_odas_new['tenpo_cd']==int(tenpo_cd)].reset_index(drop=True)

        logger.info('***df_odas_new 1 shape:', df_odas_new.shape)
        
        df_odas_new['prd_cd_len'] = df_odas_new['prd_cd'].apply(lambda x:len(x))
        df_odas_new = df_odas_new[df_odas_new['prd_cd_len']<=13]
        
        logger.info(f'***df_odas_new 2 shape: {df_odas_new.shape}')

        df_odas_new['prd_cd_isnumelic'] = df_odas_new['prd_cd'].apply(lambda x:x.isnumeric())
        df_odas_new = df_odas_new[df_odas_new['prd_cd_isnumelic']==True]
        
        logger.info(f'***df_odas_new 3 shape: {df_odas_new.shape}')

        df_odas_new['prd_cd'] = df_odas_new['prd_cd'].astype(int)
        
        logger.info(f'{tenpo_cd} df_odas_new shape:  {df_odas_new.shape}')
        
        #df_odas_new.to_csv('df_odas_new.csv')
    
    
    df_odas_new = df_odas_new[df_odas_new['tenpo_cd'] == int(tenpo_cd)].reset_index(drop=True)
    df_odas_new = df_odas_new.rename(columns={"tenpo_cd":"TENPO_CD", "prd_cd":"PRD_CD", "amount": "odas_amount","sales_date":"sales_ymd"})

    if 1:
        df_odas_new1 = df_odas_new[df_odas_new['sales_ymd'].str.contains('-')]
        df_odas_new1['sales_ymd'] = pd.to_datetime(df_odas_new1['sales_ymd'], format='%Y-%m-%d')
        
        df_odas_new2 = df_odas_new[~df_odas_new['sales_ymd'].str.contains('-')]
        df_odas_new2['sales_ymd'] = pd.to_datetime(df_odas_new2['sales_ymd'], format='%Y/%m/%d')
        
        df_odas_new = pd.concat([df_odas_new1, df_odas_new2])
    else:
        df_odas_new['sales_ymd'] = pd.to_datetime(df_odas_new['sales_ymd'], format='%Y/%m/%d')

    df_odas_new['sales_ymd'] = df_odas_new['sales_ymd'].dt.strftime('%Y%m%d')
    df_odas_new['sales_ymd'] = df_odas_new['sales_ymd'].astype(int)

    df_odas_calender_new = pd.DataFrame()

    for wfy,wty,nsd in zip(df_calendar["week_from_ymd"],df_calendar["week_to_ymd"],df_calendar["nenshudo"]):
        tmp = df_odas_new[(df_odas_new["sales_ymd"]>=wfy)&(df_odas_new["sales_ymd"]<=wty)]
        tmp["nenshudo"] = nsd
        df_odas_calender_new = pd.concat([df_odas_calender_new,tmp])
        df_odas_calender_new = df_odas_calender_new.reset_index(drop=True)

    df_odas_calender_new = df_odas_calender_new.dropna(subset=['PRD_CD']).reset_index(drop=True)
    logger.info(f"===df_odas_calender_new=== {df_odas_calender_new}")
    
    odas_merge_df = pd.concat([df_odas_calender, df_odas_calender_new]).reset_index(drop=True)

    return odas_merge_df

    #odas_merge_df_sum = odas_merge_df.groupby(['TENPO_CD', 'PRD_CD', 'odas_amount', 'sales_ymd', 'nenshudo'], as_index=False).sum().reset_index(drop=True)

    #return odas_merge_df_sum

    
def calc_nenshudo(my_nenshudo, offset, max_syudo_dic):
    if offset < 0:
        if (my_nenshudo%100 + offset) > 0:
            my_nenshudo = my_nenshudo + offset
        else:
            my_nenshudo = int(my_nenshudo/100)*100 - 100 \
                         + max_syudo_dic[int(my_nenshudo/100)-1] + (my_nenshudo%100 + offset) 
    else:
        if (my_nenshudo%100 + offset) > max_syudo_dic[int(my_nenshudo/100)]:
            my_nenshudo = int(my_nenshudo/100)*100 + 100 \
                         + (my_nenshudo%100 + offset) - max_syudo_dic[int(my_nenshudo/100)]       
        else:
            my_nenshudo = my_nenshudo + offset
    return my_nenshudo


def calc_nenshudo2(my_nenshudo, offset, dfc_tmp):
    nenshudo_list = sorted(dfc_tmp['nenshudo'].tolist())
    idx = -1
    try:
        idx = nenshudo_list.index(my_nenshudo)
    except:
        return None    
    try:
        return(nenshudo_list[idx+offset])
    except:
        None
    return my_nenshudo


#　最頻値計算（引数は0以上）pandasやscipyより3倍速い
def np_mode2(srs):
    arr = list(srs)
    count = np.bincount(arr) 
    return np.argmax(count)

#　最頻値計算（引数は0以下も可）pandasやscipyより2倍速い
def np_mode3(srs):
    arr = list(srs)
    unique, freq = np.unique(arr, return_counts=True) 
    return unique[np.argmax(freq)] 


def load_kikaku_data(
    kikaku_master,
    blob,
    bucket_name
):
    full_path = blob.name
    temp_df = extract_as_df(f"{full_path}", bucket_name)
    temp_df = temp_df[["BUMON_CD", "HANBAI_FROM_YMD", "HANBAI_TO_YMD", "KIKAKU_TYP_CD", "TENPO_CD", "PRD_CD", "KIKAKU_BAIKA"]]
    temp_df.columns = ["DPT", "HANBAI_FROM_YMD", "HANBAI_TO_YMD", "KIKAKU_TYP_CD", "TENPO_CD", "PRD_CD", "KIKAKU_BAIKA"]
    kikaku_master = pd.concat([kikaku_master, temp_df], axis=0).reset_index(drop=True)
    
    return kikaku_master

'''
# これは過去の店別単品履歴なので不要
def load_price_data(
    path_price_list,
    tenpo_cd_list,
    bucket_name
):
    # 店別売価マスターをロード
    add_list_price = load_multiple_df(path_price_list, bucket_name)
    add_list_price = add_list_price.loc[add_list_price['TENPO_CD'].isin(tenpo_cd_list)].reset_index(drop=True)
    add_list_price['unique_columns'] = add_list_price['PRD_CD'].astype(str) + '_' + add_list_price['MAINT_FROM_YMD'].astype(str) + '_' + add_list_price['MAINT_TO_YMD'].astype(str)
        
    return add_list_price
'''


def load_sales_data(dpt_list, tenpo_cd, path_tran, bucket_name, tenpo_cd_ref, today, path_tran_ref, THEME_MD_MODE=False, theme_md_prdcd_list=None):
    """
    Loads sales data for various departments and handles new store specific logic.

    Args:
        dpt_list (list): A list of department codes.
        tenpo_cd (int): The current store code.
        path_tran (str): Path pattern for transaction files (e.g., "path/{dpt}/{tenpo}_monthly_series.csv").
        bucket_name (str): The Google Cloud Storage bucket name.
        tenpo_cd_ref (int or None): The reference store code for new store processing, or None if not applicable.
        today (str): Current date string, used in file paths.
        path_tran_ref (str): Path pattern for reference store transaction files.
        THEME_MD_MODE (bool, optional): Flag for Theme MD mode. Defaults to False.
        theme_md_prdcd_list (list, optional): List of product codes for Theme MD mode. Defaults to None.

    Returns:
        pandas.DataFrame: A DataFrame containing the combined sales data.
    """
    sales_df = pd.DataFrame() # Initialize sales_df here as it's modified in the function

    for dpt in dpt_list:
        logger.info(f"{dpt}") # Converted print to logger.info
        temp_sales_df = pd.DataFrame()
        try:
            temp_sales_df = extract_as_df_with_encoding(path_tran.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")
            if 'Unnamed: 0' in temp_sales_df.columns:
                temp_sales_df = temp_sales_df.drop('Unnamed: 0', axis=1)

            temp_sales_df['DPT'] = int(dpt)
            sales_df = pd.concat([sales_df, temp_sales_df], axis=0).reset_index(drop=True)
            # del temp_sales_df # This 'del' is often not strictly necessary in Python due to garbage collection
        except Exception as e: # Catch specific exception for better debugging
            logger.warning(f"Could not load data for DPT {dpt} from {path_tran.format(dpt, str(tenpo_cd))}. Error: {e}. Skipping.")
            continue


    # 新店の処理 (New Store Processing)
    if tenpo_cd_ref is not None:
        logger.info('新店処理') # Converted print to logger.info

        # 対象店舗の週次データを読み込む(対象店舗では週次に分類されているかもしれないため)
        path_tran_small = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_time_series.csv"

        # sales_df is already initialized and populated above.
        # The comment '#sales_df = pd.DataFrame()' implies it *was* reset here,
        # but the current logic appends to the existing sales_df.
        # I'm preserving the current append logic.
        for dpt in dpt_list:
            logger.info(f"{dpt}") # Converted print to logger.info
            temp_sales_df = pd.DataFrame()
            try:
                temp_sales_df = extract_as_df_with_encoding(path_tran_small.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")
                #logger.debug(f'件数: {len(temp_sales_df)}') # Converted print to logger.debug
                if 'Unnamed: 0' in temp_sales_df.columns:
                    temp_sales_df = temp_sales_df.drop('Unnamed: 0', axis=1)

                temp_sales_df['DPT'] = int(dpt)
                sales_df = pd.concat([sales_df, temp_sales_df], axis=0).reset_index(drop=True)
            except Exception as e:
                logger.warning(f"Could not load small data for DPT {dpt} from {path_tran_small.format(dpt, str(tenpo_cd))}. Error: {e}. Skipping.")
                continue

        # 参照店舗の少量品データを読み込む (Load small quantity data from reference store)
        sales_df_ref = pd.DataFrame()
        for dpt in dpt_list:
            logger.info(f"{dpt}") # Converted print to logger.info
            temp_sales_df_ref = pd.DataFrame()
            try:
                temp_sales_df_ref = extract_as_df_with_encoding(path_tran_ref.format(dpt, str(tenpo_cd_ref)), bucket_name, "utf-8")
                #logger.debug(f'件数: {len(temp_sales_df_ref)}') # Converted print to logger.debug
                if 'Unnamed: 0' in temp_sales_df_ref.columns:
                    temp_sales_df_ref = temp_sales_df_ref.drop('Unnamed: 0', axis=1)

                if THEME_MD_MODE:
                    if theme_md_prdcd_list is not None: # Added check for None
                        temp_sales_df_ref = temp_sales_df_ref[temp_sales_df_ref['PRD_CD'].isin(theme_md_prdcd_list)]
                        logger.info(f'テーマMD ref件数: {len(temp_sales_df_ref)}') # Converted print to logger.info

                temp_sales_df_ref['DPT'] = int(dpt)
                sales_df_ref = pd.concat([sales_df_ref, temp_sales_df_ref], axis=0).reset_index(drop=True)
            except Exception as e:
                logger.warning(f"Could not load reference data for DPT {dpt} from {path_tran_ref.format(dpt, str(tenpo_cd_ref))}. Error: {e}. Skipping.")
                continue

        # 対象店舗の販売データを、参照店舗の少量品データにある商品に限定する (Limit target store's sales data to products in reference store's small quantity data)
        if not sales_df_ref.empty: # Add a check to ensure sales_df_ref is not empty before attempting unique()
            prdcd_list = sales_df_ref['PRD_CD'].unique().tolist()
            sales_df = sales_df[sales_df['PRD_CD'].isin(prdcd_list)].reset_index(drop=True)
            sales_df['TENPO_CD'] = tenpo_cd
        else:
            logger.warning("sales_df_ref is empty. Cannot filter sales_df by PRD_CD. Sales_df will not be filtered for new store.")

    return sales_df


def process_minmax_data(restrict_minmax, dpt_list, tenpo_cd, bucket_name, sales_df, extract_as_df_with_encoding, time_log_list):
    """
    Processes MinMax inventory data, filters sales data based on MinMax presence and values.

    Args:
        restrict_minmax (bool): Flag to indicate if MinMax restriction should be applied.
        dpt_list (list): List of department IDs.
        tenpo_cd (str): Store code.
        bucket_name (str): Name of the cloud storage bucket where data is located.
        sales_df (pd.DataFrame): DataFrame containing sales data.
        extract_as_df_with_encoding (callable): Function to extract DataFrame from storage.

    Returns:
        tuple: A tuple containing:
            - sales_df (pd.DataFrame): The filtered sales DataFrame.
            - nominmax_prdcd_df (pd.DataFrame): Products found in sales but lacking MinMax data.
            - minmax0_prdcd_df (pd.DataFrame): Products with MinMax values of (0,0).
            - time_log_list (list): List to log execution times.
    """
    if restrict_minmax:
        # MinMax情報を読み込む
        path_minmax = "Basic_Analysis_unzip_result/02_DM/NBMinMax_ten_prd_ten/min_max_{}_{}_000000000000.csv"
        minmax_df = pd.DataFrame()
        for dpt in dpt_list:
            temp_df = pd.DataFrame()
            try:
                temp_df = extract_as_df_with_encoding(path_minmax.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")
                if 'Unnamed: 0' in temp_df.columns:
                    temp_df = temp_df.drop('Unnamed: 0', axis=1)

                temp_df['DPT'] = int(dpt)

                minmax_df = pd.concat([minmax_df, temp_df], axis=0).reset_index(drop=True)
                del temp_df
            except Exception: # Catch specific exceptions if known, or a general Exception
                continue

        logger.info(f'minmax_df.shape: {minmax_df.shape}')         

        # MinMaxレコードの製品別最新レコードを取り出す
        minmax_df['TOROKU_YMD_TOROKU_HMS'] = minmax_df['TOROKU_YMD'].astype(str) + minmax_df['TOROKU_HMS'].astype(str) + minmax_df['NENSHUDO'].astype(str)
        prdcd_nenshudomax_df = minmax_df.groupby(['PRD_CD'])['TOROKU_YMD_TOROKU_HMS'].max().reset_index()
        minmax_df_newest = pd.merge(prdcd_nenshudomax_df, minmax_df, on=['PRD_CD', 'TOROKU_YMD_TOROKU_HMS'], how='inner')

        del minmax_df

        # minmaxの無い製品番号を調査用にとっておく
        nominmax_prdcd_df = pd.DataFrame(sales_df[~sales_df['PRD_CD'].isin(minmax_df_newest['PRD_CD'].tolist())]['PRD_CD'].unique())
        nominmax_prdcd_df.columns = ['PRD_CD']
        nominmax_prdcd_df['reason'] = 'no_minmax'
        nominmax_prdcd_df['HACCHU_TO_YMD'] = ''
        # ---------------------------------------

        # minmaxデータのある製品に絞る
        sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest['PRD_CD'].tolist())]
        minmax_df_newest2 = minmax_df_newest[minmax_df_newest['PRD_CD'].isin(sales_df['PRD_CD'].tolist())]
        del minmax_df_newest

        # This 'if 1:' is preserved as per the request, meaning the subsequent block will always execute.
        if 1: 
            # MaxMinx0のみ除外
            minmax_df_newest2_not_minmax0 =minmax_df_newest2[~((minmax_df_newest2['HOJU_MIN_SU']==0)&(minmax_df_newest2['HOJU_MAX_SU']==0))]

            # minmax0の商品番号を調査用にとっておく ------------------------------
            minmax0_prdcd_df = pd.DataFrame(sales_df[~sales_df['PRD_CD'].isin(minmax_df_newest2_not_minmax0['PRD_CD'].tolist())]['PRD_CD'].unique())
            minmax0_prdcd_df.columns = ['PRD_CD']
            minmax0_prdcd_df['reason'] = 'minmax0'
            minmax0_prdcd_df['HACCHU_TO_YMD'] = ''
            # --------------------------------------------------------------------

            sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest2_not_minmax0['PRD_CD'].tolist())]
            sales_df = sales_df.reset_index(drop=True)
        else:
            # This block will not be executed due to 'if 1:' being true.
            # 下限0意外の商品に絞る
            minmax_df_newest2_not_min0 =minmax_df_newest2[minmax_df_newest2['HOJU_MIN_SU']>0]
            sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest2_not_min0['PRD_CD'].tolist())]
        length = len(sales_df['PRD_CD'].unique())
        logger.info(f'restrict minmax sales_df SKU: {length}')
        del minmax_df_newest2
        del minmax_df_newest2_not_minmax0
        restrict_minmax_t = time.time()
        time_log_list.append(['restrict_minmax_t', restrict_minmax_t])

        return sales_df, nominmax_prdcd_df, minmax0_prdcd_df, time_log_list


def restrict_tenpo_hacchu_end_func(restrinct_tenpo_hacchu_end, tenpo_cd, bucket_name, sales_df, my_date, extract_as_df, time_log_list):
    """
    Restricts sales data based on store-specific production/order end dates.

    Args:
        restrinct_tenpo_hacchu_end (bool): Flag to indicate if restriction should be applied.
        tenpo_cd (int): Store code.
        bucket_name (str): Name of the cloud storage bucket where data is located.
        sales_df (pd.DataFrame): DataFrame containing sales data.
        my_date (int): Current date or cutoff date in integer format (e.g., YYYYMMDD).
        extract_as_df (callable): Function to extract DataFrame from storage.

    Returns:
        tuple: A tuple containing:
            - sales_df (pd.DataFrame): The filtered sales DataFrame.
            - hacchu_teishi_prdcd_df2 (pd.DataFrame): Products identified as having order end dates.
            - time_log_list (list): List to log execution times.
    """
    if restrinct_tenpo_hacchu_end:
        # 店舗別の生産発注停止情報を結合
        path_tenpo_hacchu_master = "Basic_Analysis_unzip_result/01_Data/33_tenpo_hacchu/29_TENPO_HACCHU_YMD.csv"
        store_prd_hacchu_ymd = extract_as_df(path_tenpo_hacchu_master, bucket_name)    
        store_prd_hacchu_ymd['TENPO_CD'] = store_prd_hacchu_ymd['TENPO_CD'].astype(int)
        # The original code had store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD']==tenpo_cd].reset_index(drop=True)
        # which creates a temporary DataFrame but doesn't assign it back. 
        # The following line effectively performs this filtering.
        store_prd_hacchu_ymd = store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD']==tenpo_cd].reset_index(drop=True)


        # 店舗別発注終了日
        store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].fillna(99999999)
        store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].astype(int)
        # 対象店舗データに絞ってマージ - This line is redundant as it was already done above.
        # Keeping it for strict adherence to "no change in logic" but noting its redundancy.
        store_prd_hacchu_ymd = store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD']==int(tenpo_cd)].reset_index(drop=True)


        # 発注停止商品番号を調査用にとっておく-----------------------------------------
        hacchu_end_prdlist = store_prd_hacchu_ymd[store_prd_hacchu_ymd['HACCHU_TO_YMD'] <= my_date]['PRD_CD'].astype(int).tolist()
        hacchu_teishi_prdcd_df = pd.DataFrame(sales_df[sales_df['PRD_CD'].isin(hacchu_end_prdlist)]['PRD_CD'].unique())
        hacchu_teishi_prdcd_df.columns=['PRD_CD']
        hacchu_teishi_prdcd_df['reason'] = 'hacchu_teishi'
        store_prd_hacchu_ymd_2 = store_prd_hacchu_ymd[store_prd_hacchu_ymd['HACCHU_TO_YMD'] <= my_date][['PRD_CD', 'HACCHU_TO_YMD']].drop_duplicates()
        hacchu_teishi_prdcd_df2 = pd.merge(hacchu_teishi_prdcd_df, store_prd_hacchu_ymd_2, on='PRD_CD', how='left')
        # -----------------------------------------------------------------------------
        # 発注停止以外の商品に絞る
        # This re-calculates hacchu_end_prdlist, which is redundant but kept for "no change in logic".
        hacchu_end_prdlist = store_prd_hacchu_ymd[store_prd_hacchu_ymd['HACCHU_TO_YMD'] <= my_date]['PRD_CD'].astype(int).tolist()
        sales_df = sales_df[~sales_df['PRD_CD'].isin(hacchu_end_prdlist)]
        sales_df = sales_df.reset_index(drop=True)
        length = len(sales_df['PRD_CD'].unique())
        logger.info(f'exclude store hacchuend sales_df SKU: {length}')

        del store_prd_hacchu_ymd

        restrict_hacchu_end_t = time.time()
        time_log_list.append(['restrict_hacchu_end_t', restrict_hacchu_end_t])

        return sales_df, hacchu_teishi_prdcd_df2, time_log_list


def calculate_advanced_metrics(OUTPUT_METRICS_VALUE, today_nenshudo, dfc_tmp, train_df, calc_nenshudo2):
    """
    Calculates various sales metrics for different time periods, including 13-week
    metrics and standard deviations with and without zero sales.

    Args:
        OUTPUT_METRICS_VALUE (bool): Flag to indicate if metrics calculation should proceed.
        today_nenshudo (int): Current 'nenshudo' (year-week) value.
        dfc_tmp (pd.DataFrame): Temporary DataFrame, likely containing calendar information
                                 used by calc_nenshudo2.
        train_df (pd.DataFrame): Training DataFrame containing '商品コード' (Product Code),
                                 '年週度' (Year-Week), and '売上実績数量' (Actual Sales Quantity).
        calc_nenshudo2 (callable): Function to calculate 'nenshudo' for past periods.

    Returns:
        tuple: A tuple containing:
            - metrics_result (pd.DataFrame): DataFrame containing all calculated sales metrics.
            - train_df (pd.DataFrame): The modified train_df.
    """    
    # Re-calculating lastNyer_nenshudo variables as they are used in this block.
    # This assumes the caller will pass a train_df that has been processed by the prior function,
    # or that these calculations are self-contained. For strict "no change in logic",
    # they are repeated here.
    last1yer_nenshudo = calc_nenshudo2(today_nenshudo, -52, dfc_tmp)
    last2yer_nenshudo = calc_nenshudo2(today_nenshudo, -104, dfc_tmp)
    last13week_nenshudo = calc_nenshudo2(today_nenshudo, -13, dfc_tmp)

    # Re-creating train_df_on_sales and related dict as per original logic.
    # This implies that the 'train_df' passed to this function
    # should be the original one or one that hasn't had '1stsales_nenshudo' removed yet.
    # To strictly adhere to "no change in logic", we'll replicate the previous steps if needed
    # to ensure train_df_on_sales is correctly prepared.

    # Sales start week calculation (replicated from previous function to ensure self-contained logic)
    train_df_exist_sales = train_df[train_df['売上実績数量'] >= 0.001].copy()
    if not train_df_exist_sales.empty: # Check to avoid error on empty slice
        train_df_exist_sales['nenshudo_exist_uri_su_min'] = train_df_exist_sales.groupby("商品コード", as_index=False)['年週度'].transform(lambda x: x.min())
        train_df_exist_sales = train_df_exist_sales[['商品コード', 'nenshudo_exist_uri_su_min']].drop_duplicates()
        prdcd_1stsalesnenshudo_dict = dict(zip(train_df_exist_sales['商品コード'], train_df_exist_sales['nenshudo_exist_uri_su_min']))
    else:
        prdcd_1stsalesnenshudo_dict = {}

    train_df['1stsales_nenshudo'] = train_df['商品コード'].apply(lambda x:prdcd_1stsalesnenshudo_dict.get(x, today_nenshudo))
    train_df_on_sales = train_df[train_df['年週度'] >= train_df['1stsales_nenshudo']].copy() # .copy() to avoid SettingWithCopyWarning
    train_df = train_df.drop('1stsales_nenshudo', axis=1) # Ensure '1stsales_nenshudo' is removed

    # Previous metrics that are merged later
    year1_std = train_df_on_sales[train_df_on_sales['年週度']>=last1yer_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正無し'})
    year2_std = train_df_on_sales[train_df_on_sales['年週度']>=last2yer_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正無し'})
    year1_nosales_weekcount = train_df_on_sales[(train_df_on_sales['年週度']>=last1yer_nenshudo)&(train_df_on_sales['売上実績数量']<0.001)][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.count()).rename(columns={'売上実績数量':'欠損週数_直近1年_補正無し'})


    week13_metrics = train_df_on_sales[train_df_on_sales['年週度']>=last13week_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード').describe()['売上実績数量'].drop('count', axis=1).rename(columns={'mean':'売上実績数量_直近13週実績平均_補正無し', 'std':'売上実績数量_直近13週実績std_補正無し', 'min':'売上実績数量_直近13週実績最小_補正無し', '25%':'売上実績数量_直近13週実績25%_補正無し', '50%':'売上実績数量_直近13週実績50%_補正無し', '75%':'売上実績数量_直近13週実績75%_補正無し', 'max':'売上実績数量_直近13週実績最大_補正無し'})
    #'count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'
    week13_median = train_df_on_sales[train_df_on_sales['年週度']>=last13week_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.median()).rename(columns={'売上実績数量':'売上実績数量_直近13週実績平均_中央値'})


    # 売り数０を除外
    train_df_on_sales_exclude_zeros = train_df_on_sales[train_df_on_sales['売上実績数量']>=0.001].copy() # Ensure operating on a copy

    #standard deviation/past 1 year (excluding 0)/past 1 year/unadjusted
    year1_std_exclude0 = train_df_on_sales_exclude_zeros[train_df_on_sales_exclude_zeros['年週度']>=last1yer_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正無し_0除外'})


    #standard deviation/past 2 years (excluding 0)/past 2 years/unadjusted
    year2_std_exclude0 = train_df_on_sales_exclude_zeros[train_df_on_sales_exclude_zeros['年週度']>=last2yer_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正無し_0除外'})

    metrics_result = pd.merge(year1_std, year2_std, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year1_std_exclude0, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year2_std_exclude0, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, week13_median, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, week13_metrics, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year1_nosales_weekcount, on='商品コード', how='left')

    return metrics_result, train_df



def preprocess_sales_data(sales_df: pd.DataFrame, df_calendar: pd.DataFrame,
                          no_sales_term_weight_zero: bool, no_sales_term_set_ave: bool) -> pd.DataFrame:
    """
    Preprocesses sales data, handling periods with no sales based on specified strategies.

    Args:
        sales_df (pd.DataFrame): The main DataFrame containing sales data.
        df_calendar (pd.DataFrame): DataFrame containing calendar information (nendo, shudo, etc.).
        no_sales_term_weight_zero (bool): If True, applies a strategy to impute early no-sales periods
                                           using first year's sales data.
        no_sales_term_set_ave (bool): If True and no_sales_term_weight_zero is also True,
                                      applies an average-based imputation for products
                                      with less than a year of sales history.

    Returns:
        pd.DataFrame: The preprocessed sales DataFrame.
    """
    logger.info("Starting sales data preprocessing.")

    if no_sales_term_weight_zero:
        logger.info("Applying 'no_sales_term_weight_zero' strategy.")

        # 前年年週度を結合
        df_calendar['nenshudo'] = df_calendar['nendo'] * 100 + df_calendar['shudo']
        df_calendar['znen_nenshudo'] = df_calendar['znen_nendo'] * 100 + df_calendar['znen_shudo']

        sales_df = pd.merge(sales_df, df_calendar[['nendo', 'shudo', 'nenshudo', 'znen_nenshudo']],
                            left_on='nenshudo', right_on='nenshudo', how='left')

        # 販売期間のある最初の年週度をとってくる
        sales_df_exist_sales = sales_df[sales_df['URI_SU'] >= 0.001].copy() # Added .copy() to avoid SettingWithCopyWarning
        sales_df_exist_sales['nenshudo_exist_uri_su_min'] = sales_df_exist_sales.groupby("PRD_CD", as_index=False)['nenshudo'].transform(lambda x: x.min())

        sales_df_exist_sales2 = sales_df_exist_sales[['PRD_CD', 'nenshudo_exist_uri_su_min']].drop_duplicates()
        # del sales_df_exist_sales # Commented out as it's typically handled by Python's garbage collector unless memory is critical

        prdcd_1stsalesnenshudo_dict = dict(zip(sales_df_exist_sales2['PRD_CD'], sales_df_exist_sales2['nenshudo_exist_uri_su_min']))
        logger.debug(f"Identified first sales nenshudo for {len(prdcd_1stsalesnenshudo_dict)} products.")

        # 販売実績最初の週
        sales_df['1stsales_nenshudo'] = sales_df['PRD_CD'].apply(lambda x: prdcd_1stsalesnenshudo_dict.get(x, sales_df['nenshudo'].max() + 1)) # Added .get with default for robustness

        # 実績販売期間のデータ
        sales_df_existsales = sales_df[sales_df['nenshudo'] >= sales_df['1stsales_nenshudo']].copy() # Added .copy()
        logger.debug(f"Filtered sales_df to {len(sales_df_existsales)} rows after first sale.")

        # 実績販売データが1年分あるものに限定する
        sales_df_existsales['jisseki_uriage_wk_count'] = sales_df_existsales.groupby(["PRD_CD"], as_index=False)['nendo'].transform(lambda x:len(x))

        sales_df_existsales_under1yer = None # Initialize to None for scope
        if no_sales_term_set_ave:
            sales_df_existsales_under1yer = sales_df_existsales[sales_df_existsales['jisseki_uriage_wk_count'] < 52].copy() # Added .copy()
            logger.info(f"Identified {len(sales_df_existsales_under1yer['PRD_CD'].unique())} products with less than 52 weeks of sales.")

        # This 'if 1:' block is an unconditional execution, often from development
        if 1:
            sales_df_existsales = sales_df_existsales[sales_df_existsales['jisseki_uriage_wk_count'] >= 52].copy() # Added .copy()
            logger.info(f"Proceeding with {len(sales_df_existsales['PRD_CD'].unique())} products having >= 52 weeks of sales.")

            # 製品別で、週ごとに実績販売最初の年度を出す
            sales_df_existsales['sales_exist_nendo_min_for_shudo'] = sales_df_existsales.groupby(["PRD_CD", "shudo"], as_index=False)['nendo'].transform(lambda x: x.min())

            # 販売実績ありの期間で、最初の1年分だけ抽出する
            sales_df_existsales2 = sales_df_existsales[(sales_df_existsales['sales_exist_nendo_min_for_shudo']==sales_df_existsales['nendo'])].copy() # Added .copy()

            # 最初の1年分の販売データで、販売の無かった期間を埋める
            sales_df_existsales2 = sales_df_existsales2.rename(columns={'URI_SU':'URI_SU_1ST'})

            sales_df2 = pd.merge(sales_df, sales_df_existsales2[['PRD_CD', 'shudo', 'URI_SU_1ST', 'jisseki_uriage_wk_count']],
                                 left_on=['PRD_CD', 'shudo'], right_on=['PRD_CD', 'shudo'], how='left')

            # Impute early no-sales periods with data from the first year's sales (URI_SU_1ST)
            # Using .loc for direct assignment to avoid SettingWithCopyWarning
            mask_impute_1st_year = sales_df2['nenshudo'] < sales_df2['1stsales_nenshudo']
            sales_df2.loc[mask_impute_1st_year, 'URI_SU'] = sales_df2.loc[mask_impute_1st_year, 'URI_SU_1ST']
            logger.info("Imputed early no-sales periods using first year's sales data (URI_SU_1ST).")

        # Start of the remaining part of the original code
        if no_sales_term_set_ave:
            logger.info("Applying 'no_sales_term_set_ave' strategy for products with < 1 year sales.")
            # 売りのある期間の売り数平均値を出す
            sales_df3 = sales_df2[sales_df2['nenshudo']>=sales_df2['1stsales_nenshudo']].reset_index(drop=True).copy() # Added .copy()
            sales_df3['URI_SU_ave'] = sales_df3.groupby(["PRD_CD"], as_index=False)['URI_SU'].transform(lambda x: x.mean())
            prdcd_urisuave_dict = dict(zip(sales_df3['PRD_CD'], sales_df3['URI_SU_ave']))
            del sales_df3 # Clean up temporary DataFrame

            # Using .get for robustness against missing keys
            sales_df2['URI_SU_ave'] = sales_df2['PRD_CD'].apply(lambda x: prdcd_urisuave_dict.get(x, 0))
            logger.debug("Calculated average sales for existing sales periods.")

            # sales_df2.to_csv('sales_df2_before_setave.csv') # For debugging, typically not in production

            # 売りのある期間が1年以下の商品について、売りの無かった期間の売り数に「売り数平均値」をセットする
            if sales_df_existsales_under1yer is not None: # Check if this df was created
                prdcd_list_sales_udr1year = sales_df_existsales_under1yer['PRD_CD'].unique().tolist()
                mask_impute_average = (sales_df2['PRD_CD'].isin(prdcd_list_sales_udr1year)) & \
                                      (sales_df2['nenshudo'] < sales_df2['1stsales_nenshudo'])

                sales_df2.loc[mask_impute_average, 'URI_SU'] = sales_df2.loc[mask_impute_average, 'URI_SU_ave']
                logger.info(f"Imputed early no-sales periods for {len(prdcd_list_sales_udr1year)} products (under 1 year sales) using average sales.")
            else:
                logger.warning("no_sales_term_set_ave is True but sales_df_existsales_under1yer was not populated. No average imputation performed.")


            # sales_df2.to_csv('sales_df2_after_setave.csv') # For debugging, typically not in production
            sales_df2 = sales_df2.drop('URI_SU_ave', axis=1) # Clean up temporary column

        sales_df = sales_df2.reset_index(drop=True)
        logger.debug("Final sales_df updated from sales_df2.")

        # 後始末 (Cleanup)
        columns_to_drop = [
            'nendo', 'shudo', 'znen_nenshudo', '1stsales_nenshudo',
            'URI_SU_1ST', 'jisseki_uriage_wk_count'
        ]
        # Filter out columns that might not exist due to logic paths or previous drops
        existing_columns_to_drop = [col for col in columns_to_drop if col in sales_df.columns]
        sales_df = sales_df.drop(existing_columns_to_drop, axis=1)
        logger.info(f"Cleaned up temporary columns: {existing_columns_to_drop}")

        # Explicitly delete large DataFrames and dictionaries to free memory
        del sales_df2
        del prdcd_1stsalesnenshudo_dict
        del sales_df_existsales
        del sales_df_existsales2
        logger.debug("Cleaned up intermediate DataFrames and dictionaries.")

    logger.info("Sales data preprocessing complete.")
    return sales_df


def updated_sales_df(sales_df):
    if odas_imprvmnt == False:
        logger.info("odas_imprvmnt == False")
        sales_df["URI_SU"] = sales_df["URI_SU"] - sales_df["odas_amount"]
    else:
        odas_correction_start_t = time.time()
        logger.info('start odas correction improvements *******************************')
        sales_df["URI_SU_NEW"] = sales_df["URI_SU"] - sales_df["odas_amount"]
        sales_df["URI_SU_NEW_org"] = sales_df["URI_SU_NEW"]
        sales_df["URI_SU_NEW"][sales_df["URI_SU_NEW"]<0] = 0
        sales_df['URI_SU_NEW_STD'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW'].transform(lambda x:x.std())    
        sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW'].transform(lambda x:x.rolling(window=8).apply(lambda y: np_mode2(y)))    
        sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_8MODE'].transform(lambda x: x.interpolate(limit_direction='both'))

        sales_df["URI_SU_NEW_2SIGMA_LOWER"] = sales_df['URI_SU_NEW_8MODE'] - 2*sales_df["URI_SU_NEW_STD"]
        sales_df["URI_SU_NEW_2SIGMA_UPPER"] = sales_df['URI_SU_NEW_8MODE'] + 2*sales_df["URI_SU_NEW_STD"]

        sales_df["URI_SU_NEW_2SIGMA"] = sales_df["URI_SU_NEW"]
        sales_df["URI_SU_NEW_2SIGMA"][
            (sales_df["URI_SU_NEW"] < sales_df["URI_SU_NEW_2SIGMA_LOWER"])
            |(sales_df["URI_SU_NEW"] > sales_df["URI_SU_NEW_2SIGMA_UPPER"])                       
        ] = np.nan
        sales_df['URI_SU_NEW_2SIGMA'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_2SIGMA'].transform(lambda x: x.interpolate(limit_direction='both'))

        # 外れ値を補間しなおした売り数の8週平均をとる
        sales_df['URI_SU_NEW_2SIGMA_8EMA'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_2SIGMA'].transform(lambda x: x.ewm(span=8).mean())

        logger.info('processing odas correction improvements *******************************')
        odas_correction_end_t = time.time()
        elapsed_time = odas_correction_end_t - odas_correction_start_t
        logger.info(f"odas correction elapsed time: {elapsed_time:.3f} seconds")

        sales_df["URI_SU_NEW_OVER0"] = sales_df["URI_SU_NEW_org"]
        sales_df["URI_SU_NEW_OVER0"][sales_df["URI_SU_NEW_OVER0"]<0] = sales_df['URI_SU_NEW_2SIGMA_8EMA'][sales_df["URI_SU_NEW_OVER0"]<0]
        sales_df["URI_SU_NEW_OVER0"][sales_df["URI_SU_NEW_OVER0"]<0] = 0

        sales_df['URI_SU_NEW_OVER0_8MODE'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_OVER0'].transform(lambda x:x.rolling(window=8).apply(lambda y: np_mode2(y)))
        sales_df['URI_SU_NEW_OVER0_8MODE'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_8MODE'].transform(lambda x: x.interpolate(limit_direction='both'))
        sales_df['URI_SU_NEW_OVER0_STD'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_OVER0'].transform(lambda x:x.std())

        sales_df['URI_SU_NEW_OVER0_TH'] = sales_df['URI_SU_NEW_OVER0_8MODE'] + 2*sales_df['URI_SU_NEW_OVER0_STD']
        # MODE+2σをODASがないときのスパイク補正の閾値とする
        sales_df['URI_SU_NEW_OVER0_TH2'] = sales_df['URI_SU_NEW_OVER0_8MODE'] + 2*sales_df['URI_SU_NEW_OVER0_STD']

        sales_df['URI_SU_NEW_OVER0_ROLLMAX'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_OVER0'].transform(lambda x: x.rolling(7).max()).shift(-4)
        sales_df['URI_SU_NEW_OVER0_ROLLMAX'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_ROLLMAX'].transform(lambda x: x.interpolate(limit_direction='both'))

        # 7週移動min（-ピーク検出用）
        sales_df['URI_SU_NEW_OVER0_ROLLMIN'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_OVER0'].transform(lambda x: x.rolling(7).min()).shift(-4)
        sales_df['URI_SU_NEW_OVER0_ROLLMIN'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_ROLLMIN'].transform(lambda x: x.interpolate(limit_direction='both'))

        # ODAS7週移動max（補正ありの検出用
        sales_df['odas_amount_ROLLMAX'] = sales_df.groupby("PRD_CD",as_index=False)['odas_amount'].transform(lambda x: x.rolling(7).max()).shift(-4)
        sales_df['odas_amount_ROLLMAX'] = sales_df.groupby(["PRD_CD"])['odas_amount_ROLLMAX'].transform(lambda x: x.interpolate(limit_direction='both'))

        # ODAS補正があって、マイナスが発生していて、スパイクも残っている
        # のであれば、nanをセットして、線形補完する
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT'] = sales_df['URI_SU_NEW_OVER0']
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_TYPE'] = np.nan

        # ＋スパイクが残る場合
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT'][
        (sales_df['odas_amount_ROLLMAX'] > 0)
        &(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] > sales_df['URI_SU_NEW_OVER0_TH'])
        #&(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] >= (sales_df['odas_amount_ROLLMAX']*0.7))
        &(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] >= (sales_df['odas_amount_ROLLMAX']*0.45))
        &(sales_df['URI_SU_NEW_OVER0'] > sales_df['URI_SU_NEW_OVER0_TH'])
        ] = np.nan
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_TYPE'][
        (sales_df['odas_amount_ROLLMAX'] > 0)
        &(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] > sales_df['URI_SU_NEW_OVER0_TH'])
        #&(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] >= (sales_df['odas_amount_ROLLMAX']*0.7))
        &(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] >= (sales_df['odas_amount_ROLLMAX']*0.45))
        &(sales_df['URI_SU_NEW_OVER0'] > sales_df['URI_SU_NEW_OVER0_TH'])
        ] = '_+spike'

        # -スパイクが残る場合
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT'][
        (sales_df['URI_SU_NEW_OVER0'] < 0)    
        &(sales_df['odas_amount_ROLLMAX'] > 0)
        &(sales_df['URI_SU_NEW_OVER0_ROLLMIN'] < (sales_df['URI_SU_NEW_OVER0_TH']*(-1)))
        ] = np.nan
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_TYPE'][
        (sales_df['URI_SU_NEW_OVER0'] < 0)    
        &(sales_df['odas_amount_ROLLMAX'] > 0)
        &(sales_df['URI_SU_NEW_OVER0_ROLLMIN'] < (sales_df['URI_SU_NEW_OVER0_TH']*(-1)))
        ] = '_-spike'

        # ODAS値が無いのにスパイクのあるものをどうするか？
        # ＋スパイクが残る場合
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT'][
        (sales_df['odas_amount_ROLLMAX'] == 0)
        &(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] > sales_df['URI_SU_NEW_OVER0_TH2'])
        &(sales_df['URI_SU_NEW_OVER0'] > sales_df['URI_SU_NEW_OVER0_TH2'])

        &(sales_df['URI_SU_NEW_OVER0'] > (sales_df['URI_SU_NEW_OVER0_8MODE'] + 1) * 8)
        &(sales_df['URI_SU_NEW_OVER0'] >= 100)


        ] = np.nan
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_TYPE'][
        (sales_df['odas_amount_ROLLMAX'] == 0)
        &(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] > sales_df['URI_SU_NEW_OVER0_TH2'])
        &(sales_df['URI_SU_NEW_OVER0'] > sales_df['URI_SU_NEW_OVER0_TH2'])

        &(sales_df['URI_SU_NEW_OVER0'] > (sales_df['URI_SU_NEW_OVER0_8MODE'] + 1) * 8)
        &(sales_df['URI_SU_NEW_OVER0'] >= 100)

        ] = '_+spike_without_odas'

        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_bk'] = sales_df['URI_SU_NEW_OVER0_IMPLVMNT']

        sales_df['URI_SU_NEW_OVER0_IMPLVMNT'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_IMPLVMNT'].transform(lambda x: x.interpolate(limit_direction='both'))

        sales_df['URI_SU'] = sales_df['URI_SU_NEW_OVER0_IMPLVMNT']

        #sales_df = sales_df.drop('URI_SU_NEW_8EMA', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_STD', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_8MODE', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_2SIGMA_LOWER', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_2SIGMA_UPPER', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_2SIGMA', axis=1)

        #sales_df = sales_df.drop('URI_SU_NEW_2SIGMA_BK', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_2SIGMA_8EMA', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_OVER0', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_OVER0_8MODE', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_OVER0_STD', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_OVER0_TH', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_OVER0_TH2', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_OVER0_ROLLMAX', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_OVER0_ROLLMIN', axis=1)
        sales_df = sales_df.drop('odas_amount_ROLLMAX', axis=1)
        sales_df = sales_df.drop('URI_SU_NEW_OVER0_IMPLVMNT', axis=1)

        logger.info('end odas Correction improvements *******************************')
        odas_correction_end_t = time.time()
        elapsed_time = odas_correction_end_t - odas_correction_start_t
        logger.info(f"odas correction elapsed time: {elapsed_time:.3f} seconds")
    return sales_df


def analyze_sales_metrics(sales_df: pd.DataFrame, dfc_tmp: pd.DataFrame, dfc: pd.DataFrame, today_nenshudo: int, OUTPUT_METRICS_VALUE: bool) -> pd.DataFrame:
    train_df =  copy.deepcopy(sales_df)
    train_df = train_df.rename(columns={'PRD_CD':'商品コード', 'nenshudo':'年週度', 'URI_SU':'売上実績数量'})
    #last1yer_nenshudo = calc_nenshudo(today_nenshudo, -52)
    last1yer_nenshudo = calc_nenshudo2(today_nenshudo, -52, dfc_tmp)
    # 2年前の年週度
    #last2yer_nenshudo = calc_nenshudo(today_nenshudo, -104)
    last2yer_nenshudo = calc_nenshudo2(today_nenshudo, -104, dfc_tmp)
    # 13週前の年週度
    #last13week_nenshudo = calc_nenshudo(today_nenshudo, -13)
    last13week_nenshudo = calc_nenshudo2(today_nenshudo, -13, dfc_tmp)

    # 販売開始年週度を求める
    train_df_exist_sales = train_df[train_df['売上実績数量'] >= 0.001]
    train_df_exist_sales['nenshudo_exist_uri_su_min'] = train_df_exist_sales.groupby("商品コード", as_index=False)['年週度'].transform(lambda x: x.min())

    train_df_exist_sales = train_df_exist_sales[['商品コード', 'nenshudo_exist_uri_su_min']].drop_duplicates()
    prdcd_1stsalesnenshudo_dict = dict(zip(train_df_exist_sales['商品コード'], train_df_exist_sales['nenshudo_exist_uri_su_min']))

    # 販売実績最初の週
    train_df['1stsales_nenshudo'] = train_df['商品コード'].apply(lambda x:prdcd_1stsalesnenshudo_dict.get(x, today_nenshudo))

    # 実績販売期間のデータ
    train_df_on_sales = train_df[train_df['年週度'] >= train_df['1stsales_nenshudo']]

    train_df = train_df.drop('1stsales_nenshudo', axis=1)


    #標準偏差/直近1年（0含む）/直近1年/補正あり
    year1_std_hosei = train_df_on_sales[train_df_on_sales['年週度']>=last1yer_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正あり'})

    #標準偏差/直近2年（0含む）/直近2年/補正あり
    year2_std_hosei = train_df_on_sales[train_df_on_sales['年週度']>=last2yer_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正あり'})

    # 売り数０を除外
    train_df_on_sales = train_df_on_sales[train_df_on_sales['売上実績数量']>=0.001]

    #標準偏差/直近1年（0除外）/直近1年/補正あり
    year1_std_exclude0_hosei = train_df_on_sales[train_df_on_sales['年週度']>=last1yer_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正あり_0除外'})


    year2_std_exclude0_hosei = train_df_on_sales[train_df_on_sales['年週度']>=last2yer_nenshudo][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正あり_0除外'})

    #標準偏差/直近3年（当週±前後5週)（0含む）/補正無あり
    prev5_nenshudo = calc_nenshudo2(today_nenshudo, -6, dfc_tmp)
    after5_nenshudo = calc_nenshudo2(today_nenshudo, 6, dfc_tmp)

    prev1year_nenshudo = calc_nenshudo2(today_nenshudo, -52, dfc_tmp)
    prev5_prev1year_nenshudo = calc_nenshudo2(prev1year_nenshudo, -6, dfc_tmp)
    after5_prev1year_nenshudo = calc_nenshudo2(prev1year_nenshudo, 6, dfc_tmp)

    prev2year_nenshudo = calc_nenshudo2(prev1year_nenshudo, -52, dfc_tmp)
    prev5_prev2year_nenshudo = calc_nenshudo2(prev2year_nenshudo, -6, dfc_tmp)
    after5_prev2year_nenshudo = calc_nenshudo2(prev2year_nenshudo, 6, dfc_tmp)

    year3_beforeafter6wk_std_hosei = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週std_補正あり'})


    #標準偏差/直近3年 (当月±前後1か月)（0含む）/補正無あり
    train_df_on_sales = pd.merge(train_df_on_sales, dfc[['nenshudo', 'minashi_tsuki']].rename(columns={'nenshudo':'年週度'}), on='年週度', how='left')

    prev1_nenshudo = calc_nenshudo2(today_nenshudo, -1, dfc_tmp)
    this_minashi_tsuki = train_df_on_sales[train_df_on_sales['年週度']==prev1_nenshudo].reset_index()['minashi_tsuki'][0]
    prev_minashi_tsuki = this_minashi_tsuki - 1
    if prev_minashi_tsuki < 0:
        prev_minashi_tsuki = 12
    after_minashi_tsuki = this_minashi_tsuki + 1
    if after_minashi_tsuki > 12:
        after_minashi_tsuki = 1

    prev3year_nenshudo = calc_nenshudo2(today_nenshudo, -(52*3+10), dfc_tmp)
    year3_beforeafter1month_std_hosei = train_df_on_sales[
        (train_df_on_sales['年週度']>=prev3year_nenshudo)
        &(train_df_on_sales['minashi_tsuki'].isin([prev_minashi_tsuki, this_minashi_tsuki, after_minashi_tsuki]))
                                       ][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後1ヶ月std_補正あり'})


    # 中央値
    year3_beforeafter6wk_median_hosei = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.median()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週median_補正あり'})    

    # 中央絶対偏差(median absolute deviation)
    year3_beforeafter6wk_mad_hosei = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:st.robust.scale.mad(x)).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週mad_補正あり'})


    # 平均
    year3_beforeafter6wk_mean_hosei = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.mean()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週mean_補正あり'})

    # 変動係数(coefficient of variation)
    year3_beforeafter6wk_cov_hosei = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']].groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()/x.mean()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週cov_補正あり'})

    metrics_result = pd.merge(metrics_result, year1_std_hosei, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year2_std_hosei, on='商品コード', how='left')

    metrics_result = pd.merge(metrics_result, year1_std_exclude0_hosei, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year2_std_exclude0_hosei, on='商品コード', how='left')

    metrics_result = pd.merge(metrics_result, year3_beforeafter6wk_std_hosei, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year3_beforeafter1month_std_hosei, on='商品コード', how='left')

    metrics_result = pd.merge(metrics_result, year3_beforeafter6wk_median_hosei, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year3_beforeafter6wk_mad_hosei, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year3_beforeafter6wk_mean_hosei, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year3_beforeafter6wk_cov_hosei, on='商品コード', how='left')

    metrics_result['TENPO_CD'] = tenpo_cd
    metrics_result['NENSHUDO'] = today_nenshudo
    metrics_result['MODEL_TYPE'] = 'small_qty'

    metrics_result = metrics_result.rename(columns={
        '商品コード':'PrdCd', 
        '売上実績数量_直近1年std_補正無し':'ActualSalesAmount_1YearStd', 
        '売上実績数量_直近2年std_補正無し':'ActualSalesAmount_2YearStd',
       '売上実績数量_直近1年std_補正無し_0除外':'ActualSalesAmount_1YearStd_Exclude0',
        '売上実績数量_直近2年std_補正無し_0除外':'ActualSalesAmount_2YearStd_Exclude0',
       '売上実績数量_直近13週実績平均_中央値':'ActualSalesAmount_13WeekMedian', 
        '売上実績数量_直近13週実績平均_補正無し':'ActualSalesAmount_13WeekMean',
       '売上実績数量_直近13週実績std_補正無し':'ActualSalesAmount_13WeekStd', 
        '売上実績数量_直近13週実績最小_補正無し':'ActualSalesAmount_13WeekMin',
       '売上実績数量_直近13週実績25%_補正無し':'ActualSalesAmount_13Week25Percentile', 
        '売上実績数量_直近13週実績50%_補正無し':'ActualSalesAmount_13Week50Percentile',
       '売上実績数量_直近13週実績75%_補正無し':'ActualSalesAmount_13Week70Percentile', 
        '売上実績数量_直近13週実績最大_補正無し':'ActualSalesAmount_13Week7Max', 
        '欠損週数_直近1年_補正無し':'ActualSalesAmount_1Year_Sales0WeekNum',
       '売上実績数量_直近1年std_補正あり':'ActualSalesAmount_1YearStd_Corrected', 
        '売上実績数量_直近2年std_補正あり':'ActualSalesAmount_2YearStd_Corrected', 
        '売上実績数量_直近1年std_補正あり_0除外':'ActualSalesAmount_1YearStd_Corrected_Exclude0',
       '売上実績数量_直近2年std_補正あり_0除外':'ActualSalesAmount_2YearStd_Corrected_Exclude0',


        '売上実績数量_直近3年前後6週std_補正あり':'ActualSalesAmount_3Yearba6weekStd_Corrected', 
        '売上実績数量_直近3年前後1ヶ月std_補正あり':'ActualSalesAmount_3Yearba1monthStd_Corrected', 


        '売上実績数量_直近3年前後6週median_補正あり':'ActualSalesAmount_3Yearba6weekMedian_Corrected',
        '売上実績数量_直近3年前後6週mad_補正あり':'ActualSalesAmount_3Yearba6weekMad_Corrected',
        '売上実績数量_直近3年前後6週mean_補正あり':'ActualSalesAmount_3Yearba6weekMean_Corrected',
        '売上実績数量_直近3年前後6週cov_補正あり':'ActualSalesAmount_3Yearba6weekCov_Corrected',
    })

#         metrics_result['ActualSalesAmount_1YearStd'] = metrics_result['ActualSalesAmount_1YearStd'].fillna(0.0)
#         metrics_result['ActualSalesAmount_2YearStd'] = metrics_result['ActualSalesAmount_2YearStd'].fillna(0.0)
#         metrics_result['ActualSalesAmount_1YearStd_Exclude0'] = metrics_result['ActualSalesAmount_1YearStd_Exclude0'].fillna(0.0)
#         metrics_result['ActualSalesAmount_2YearStd_Exclude0'] = metrics_result['ActualSalesAmount_2YearStd_Exclude0'].fillna(0.0)

#         metrics_result['ActualSalesAmount_13WeekStd'] = metrics_result['ActualSalesAmount_13WeekStd'].fillna(0.0)

#         metrics_result['ActualSalesAmount_1YearStd_Corrected'] = metrics_result['ActualSalesAmount_1YearStd_Corrected'].fillna(0.0)
#         metrics_result['ActualSalesAmount_2YearStd_Corrected'] = metrics_result['ActualSalesAmount_2YearStd_Corrected'].fillna(0.0)
#         metrics_result['ActualSalesAmount_1YearStd_Corrected_Exclude0'] = metrics_result['ActualSalesAmount_1YearStd_Corrected_Exclude0'].fillna(0.0)
#         metrics_result['ActualSalesAmount_2YearStd_Corrected_Exclude0'] = metrics_result['ActualSalesAmount_2YearStd_Corrected_Exclude0'].fillna(0.0)


#         metrics_result['ActualSalesAmount_3Yearba6weekStd_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekStd_Corrected'].fillna(0.0)
#         metrics_result['ActualSalesAmount_3Yearba1monthStd_Corrected'] = metrics_result['ActualSalesAmount_3Yearba1monthStd_Corrected'].fillna(0.0)

#         metrics_result['ActualSalesAmount_3Yearba6weekMedian_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekMedian_Corrected'].fillna(0.0)
#         metrics_result['ActualSalesAmount_3Yearba6weekMad_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekMad_Corrected'].fillna(0.0)
#         metrics_result['ActualSalesAmount_3Yearba6weekMean_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekMean_Corrected'].fillna(0.0)
#         metrics_result['ActualSalesAmount_3Yearba6weekCov_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekCov_Corrected'].fillna(0.0)

    columns_to_fill = [
    'ActualSalesAmount_1YearStd',
    'ActualSalesAmount_2YearStd',
    'ActualSalesAmount_1YearStd_Exclude0',
    'ActualSalesAmount_2YearStd_Exclude0',
    'ActualSalesAmount_13WeekStd',
    'ActualSalesAmount_1YearStd_Corrected',
    'ActualSalesAmount_2YearStd_Corrected',
    'ActualSalesAmount_1YearStd_Corrected_Exclude0',
    'ActualSalesAmount_2YearStd_Corrected_Exclude0',
    'ActualSalesAmount_3Yearba6weekStd_Corrected',
    'ActualSalesAmount_3Yearba1monthStd_Corrected',
    'ActualSalesAmount_3Yearba6weekMedian_Corrected',
    'ActualSalesAmount_3Yearba6weekMad_Corrected',
    'ActualSalesAmount_3Yearba6weekMean_Corrected',
    'ActualSalesAmount_3Yearba6weekCov_Corrected'
]

    for col in columns_to_fill:
        metrics_result[col] = metrics_result[col].fillna(0.0)


    if output_metrics_test_tbl:
        table_id = "dev-cainz-demandforecast.cainz_shortterm_predicted_value_for_statistics.metrics_stage2_20240109_217str"
    else:
        table_id = "dev-cainz-demandforecast.cainz_shortterm_predicted_value_for_statistics.metrics_stage2"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('PrdCd', 'INTEGER', mode='NULLABLE'),

            bigquery.SchemaField('ActualSalesAmount_1YearStd', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_2YearStd', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_1YearStd_Exclude0', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_2YearStd_Exclude0', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_13WeekMedian', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_13WeekMean', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_13WeekStd',  'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_13WeekMin', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_13Week25Percentile',  'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_13Week50Percentile', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_13Week70Percentile',  'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_13Week7Max',  'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_1Year_Sales0WeekNum', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_1YearStd_Corrected',  'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_2YearStd_Corrected',  'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_1YearStd_Corrected_Exclude0', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_2YearStd_Corrected_Exclude0', 'FLOAT', mode='NULLABLE'),

            bigquery.SchemaField('TENPO_CD', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('NENSHUDO', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('MODEL_TYPE', 'STRING', mode='NULLABLE'),

            bigquery.SchemaField('ActualSalesAmount_3Yearba6weekStd_Corrected', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_3Yearba1monthStd_Corrected', 'FLOAT', mode='NULLABLE'),

            bigquery.SchemaField('ActualSalesAmount_3Yearba6weekMedian_Corrected', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_3Yearba6weekMad_Corrected', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_3Yearba6weekMean_Corrected', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ActualSalesAmount_3Yearba6weekCov_Corrected', 'FLOAT', mode='NULLABLE'),

        ],
        write_disposition='WRITE_APPEND',
    )

    upload_complete = False
    while upload_complete == False:
        try:
            client = BigqueryClient()
            job = client.load_table_from_dataframe(metrics_result, table_id, job_config=job_config)
            job.result()

            upload_complete = True

        except Exception as e:
            logger.info(f'errtype: {str(type(e))}')
            logger.info(f'err: {str(e)}')
            logger.info('data upload retry')
            time.sleep(20)

    return metrics_result, sales_df


def assign_training_weights(df_vx_test, no_sales_term_weight_zero, time_log_list):
    """
    Assigns training weights to a DataFrame based on sales data or a time-based formula.

    Args:
        df_vx_test (pd.DataFrame): The input DataFrame containing sales data,
                                   including 'PrdCd', 'SalesAmount', and 'weekstartdatestamp'.
        no_sales_term_weight_zero (bool): If True, sets weight to 0 for periods before
                                          the first recorded sale for each product.
                                          If False, applies a time-based weighting formula.

    Returns:
        pd.DataFrame: The DataFrame with an added or modified 'training_weight' column.
    """
    if no_sales_term_weight_zero:
        # SKU別にみて、最初に販売の無い期間はウェイトを0にしておく
        # デフォルト値設定
        df_vx_test['training_weight'] = 10000
        # 販売期間のある最初の週をとってくる
        df_vx_test_exist_sales = df_vx_test[df_vx_test['SalesAmount'] >= 0.001]
        df_vx_test_exist_sales['weekstartdatestamp_exist_sales_min'] = df_vx_test_exist_sales.groupby("PrdCd", as_index=False)['weekstartdatestamp'].transform(lambda x: x.min())
        df_vx_test_exist_sales2 = df_vx_test_exist_sales[['PrdCd', 'weekstartdatestamp_exist_sales_min']].drop_duplicates()
        del df_vx_test_exist_sales
        prdcd_1stsalesweekstartdatestamp_dict = dict(zip(df_vx_test_exist_sales2['PrdCd'], df_vx_test_exist_sales2['weekstartdatestamp_exist_sales_min']))
        weekstartdatestamp_min = df_vx_test['weekstartdatestamp'].min()
        df_vx_test['1stsalesweekstartdatestamp'] = df_vx_test['PrdCd'].apply(lambda x:prdcd_1stsalesweekstartdatestamp_dict.get(x, weekstartdatestamp_min))
        df_vx_test['training_weight'][df_vx_test['weekstartdatestamp'] < df_vx_test['1stsalesweekstartdatestamp']] = 0.0

        df_vx_test = df_vx_test.drop('1stsalesweekstartdatestamp', axis=1)
        del df_vx_test_exist_sales2
        del prdcd_1stsalesweekstartdatestamp_dict

    else:
        df_vx_test['training_weight'] = df_vx_test['weekstartdatestamp'].apply(lambda x:(x.year-2019)*1000.0*2.4 + (x.month/12)*800)
    weight_end_t = time.time()
    time_log_list.append(['weight_end_t', weight_end_t])   

    return df_vx_test, time_log_list


def process_known_prices(
    df_vx_test,
    kakaku_jizen_kichi,
    bucket_name,
    bucket, 
    dpt_list,
    tenpo_cd,
    extract_as_df,
    load_kikaku_data,
    common, 
    time_log_list, target_week_from_ymd):
    if not kakaku_jizen_kichi:
        # If the flag is False, return the original DataFrame unchanged.
        return df_vx_test
    
    kikaku_process_time_list = []
    kikaku_process_time_list.append(['start', time.time()])
    path_pliceline = "Basic_Analysis_unzip_result/01_Data/35_pliceline/pliceline_shuusei_20240404.csv"
    pliceline_df = extract_as_df(path_pliceline, bucket_name)
    if len(pliceline_df) > 0:
        df_vx_test = pd.merge(df_vx_test, pliceline_df[['PRD_CD', 'MAINT_FROM_YMD', 'BAIKA']].rename(columns={'PRD_CD':'PrdCd', 'BAIKA':'PLICELINE_BAIKA'}),
                on='PrdCd', how='left')

        df_vx_test['BAIKA'][(df_vx_test['WeekStartDate']>=df_vx_test['MAINT_FROM_YMD'])] =  df_vx_test['PLICELINE_BAIKA']
        df_vx_test = df_vx_test.drop(columns=['MAINT_FROM_YMD', 'PLICELINE_BAIKA']).reset_index(drop=True)

    path_kikaku_master = "Basic_Analysis_unzip_result/02_DM/NBKikaku_prd_ten_test20240115/kikaku_inf_"
    # 企画マスターは、SQLで店舗別出力の追加が必要

    #path_list_price= "Basic_Analysis_unzip_result/02_DM/NBRireki_ten_prd_ten/rireki_"
    path_longs = "Basic_Analysis_unzip_result/02_DM/NBLongs_prd/longs_"

    kikaku_master = pd.DataFrame()
    list_price_df = pd.DataFrame()#使ってない
    longs_df = pd.DataFrame()

    for dpt in dpt_list:
        dpt_kikaku_path = f"{path_kikaku_master}{dpt}_{tenpo_cd}_"
        dpt_longs_path = f"{path_longs}{dpt}_"

        for blob in bucket.list_blobs(prefix=dpt_kikaku_path):
            kikaku_master = load_kikaku_data(kikaku_master, blob, bucket_name)

        for blob in bucket.list_blobs(prefix=dpt_longs_path):
            temp_df = extract_as_df(blob.name, bucket_name = "dev-cainz-demandforecast")
            temp_df = temp_df.loc[temp_df['TENPO_CD']==tenpo_cd]
            longs_df = pd.concat([longs_df, temp_df], axis=0).reset_index(drop=True)
            del temp_df

    kikaku_process_time_list.append(['kikakumaster loaded', time.time()])

    # 販促名のマスターをロード
    path_kikaku_type = "Basic_Analysis_unzip_result/01_Data/90_ADD_DATA/M010KIKAKU_TYP.csv"
    kikaku_type = extract_as_df(path_kikaku_type, bucket_name)

    kikaku_process_time_list.append(['kikakutype loaded', time.time()])

    patn_jan_mapping = "01_short_term/70_jan_connect/jan_connect_"+str(tenpo_cd)+".csv"
    jan_df = extract_as_df(patn_jan_mapping, bucket_name, encoding="utf-8", usecols=["old_jan","latest_jan"])

    '''
    kikaku_master = short_term_preprocess_common.rewrite_jan_code(
        subject_jan_master,
        newest_jan_list,
        kikaku_master,
        "PRD_CD"
    )
    '''
    kikaku_master = pd.merge(kikaku_master, jan_df[['old_jan', 'latest_jan']].rename(columns={'old_jan':'PRD_CD'}), on='PRD_CD', how='left')
    kikaku_master.loc[~kikaku_master['latest_jan'].isnull(), 'PRD_CD'] = kikaku_master['latest_jan']
    kikaku_master = kikaku_master.drop(['latest_jan'], axis=1)
    kikaku_master = kikaku_master[~kikaku_master['PRD_CD'].isna()].reset_index(drop=True)
    kikaku_master['PRD_CD'] = kikaku_master['PRD_CD'].astype(int)
    kikaku_process_time_list.append(['jan replaced', time.time()])

    # 商品マスター変更予約(普段はデータが入っていないようだ・・・・)
    path_dfm_yoyaku = "Basic_Analysis_unzip_result/01_Data/29_PRD_YOYAKU/23_M_090_PRD_YOYAKU.csv"
    prd_yoyaku = extract_as_df(path_dfm_yoyaku, bucket_name)
    kikaku_process_time_list.append(['prd_yoyaku loaded', time.time()])

    #店別売価変更予約
    path_list_price_yoyaku = "Basic_Analysis_unzip_result/01_Data/30_TEN_TNPN_YOYAKU/24_M030PRD_TEN_TNPN_INF_YOYAKU.csv"
    list_price_yoyaku = common.extract_as_df(path_list_price_yoyaku, bucket_name)
    list_price_yoyaku = list_price_yoyaku[list_price_yoyaku['TENPO_CD']==tenpo_cd].reset_index(drop=True)

    kikaku_process_time_list.append(['list_price_yoyaku loaded', time.time()])

    check_baika_list = ['baika_toitsu', 'BAIKA']

    if len(prd_yoyaku)!=0:
        prd_yoyaku['prd_cd'] = prd_yoyaku['prd_cd'].astype(int)
        prd_yoyaku['koshin_ymd'] = prd_yoyaku['koshin_ymd'].astype(int)
        prd_yoyaku['baika_toitsu'] = prd_yoyaku['baika_toitsu'].astype(float)
        for i in range(len(prd_yoyaku)):
            df_vx_test.loc[(df_vx_test['PrdCd']==prd_yoyaku['prd_cd'][i])
                            &(df_vx_test['WeekStartDate']>=prd_yoyaku['koshin_ymd'][i]), 'baika_toitsu'] = prd_yoyaku['baika_toitsu'][i]
        del prd_yoyaku

    kikaku_process_time_list.append(['prd_yoyaku processed', time.time()])
    # 店別売価の予約データを抽出
    temp_list_price_yoyaku = list_price_yoyaku.loc[list_price_yoyaku['PRD_CD'].isin(df_vx_test['PrdCd'].unique().tolist())].reset_index(drop=True)


    # 店別売価の予約データが有ればBAIKAの書き換えを行う
    if len(temp_list_price_yoyaku)!=0:
        #for i in range(len(temp_list_price_yoyaku)):
        #    df_vx_test.loc[(df_vx_test['PrdCd']==temp_list_price_yoyaku['PRD_CD'][i])
        #                      &
        #                      (df_vx_test['WeekStartDate']>=temp_list_price_yoyaku['MAINT_FROM_YMD'][i]),
        #                      'BAIKA']=temp_list_price_yoyaku['BAIKA'][i]

        df_vx_test = pd.merge(df_vx_test, temp_list_price_yoyaku[['PRD_CD', 'MAINT_FROM_YMD', 'BAIKA']].rename(columns={'PRD_CD':'PrdCd', 'BAIKA':'KIKAKU_BAIKA'}),
                on='PrdCd', how='left')

        df_vx_test['BAIKA'][(df_vx_test['WeekStartDate']>=df_vx_test['MAINT_FROM_YMD'])] =  df_vx_test['KIKAKU_BAIKA']

        df_vx_test = df_vx_test.drop(columns=['MAINT_FROM_YMD', 'KIKAKU_BAIKA']).reset_index(drop=True)            

    del temp_list_price_yoyaku
    del list_price_yoyaku

    kikaku_process_time_list.append(['list_price_yoyaku processed', time.time()])

    # 20240305 add
    kikaku_master = kikaku_master[kikaku_master['HANBAI_TO_YMD']>=target_week_from_ymd]
    debug_df_list = []

    # kikaku_masterのkikaku_type_cdでループを回す
    for kk, kikaku in enumerate(kikaku_master['KIKAKU_TYP_CD'].unique().tolist()):
        df_vx_test[str(kikaku)] = None
        check_baika_list.append(str(kikaku))
        temp_kikaku_df = kikaku_master.loc[(kikaku_master['KIKAKU_TYP_CD'] == kikaku)].reset_index(drop=True)
        temp_kikaku_df = temp_kikaku_df.sort_values(['PRD_CD', 'HANBAI_FROM_YMD'])
        while len(temp_kikaku_df) > 0:
            dup = temp_kikaku_df['PRD_CD'].duplicated()
            temp_kikaku_df_1 = temp_kikaku_df[~dup]
            temp_kikaku_df = temp_kikaku_df[dup]

            df_vx_test = pd.merge(df_vx_test, temp_kikaku_df_1[['PRD_CD', 'HANBAI_FROM_YMD', 'HANBAI_TO_YMD', 'KIKAKU_BAIKA']].rename(columns={'PRD_CD':'PrdCd'}), on = 'PrdCd', how='left')

            df_vx_test[str(kikaku)][(df_vx_test['WeekStartDate']>=df_vx_test['HANBAI_FROM_YMD'])&(df_vx_test['WeekStartDate']<=df_vx_test['HANBAI_TO_YMD'])] =  df_vx_test['KIKAKU_BAIKA']

            df_vx_test = df_vx_test.drop(columns=['HANBAI_FROM_YMD', 'HANBAI_TO_YMD', 'KIKAKU_BAIKA']).reset_index(drop=True)
            
        del temp_kikaku_df 
        
    kikaku_process_time_list.append(['kikaku_master processed', time.time()])
    longs_df['HANBAI_TO_YMD'] = longs_df['HANBAI_TO_YMD'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    longs_df = longs_df[~longs_df['HANBAI_TO_YMD'].isna()]
    longs_df['HANBAI_TO_YMD'] = longs_df['HANBAI_TO_YMD'].astype(int)
    longs_df = longs_df.loc[longs_df['HANBAI_TO_YMD']>target_week_from_ymd].reset_index(drop=True)
    
    if len(longs_df)!=0:
        df_vx_test['店舗売変'] = None
        check_baika_list.append('店舗売変')
        longs_df = longs_df.sort_values(['PRD_CD', 'HANBAI_FROM_YMD'])
        while len(longs_df) > 0:
            dup = longs_df['PRD_CD'].duplicated()
            longs_df_1 = longs_df[~dup]
            longs_df = longs_df[dup]

            df_vx_test = pd.merge(df_vx_test, longs_df_1[['PRD_CD', 'HANBAI_FROM_YMD', 'HANBAI_TO_YMD', 'KIKAKU_BAIKA']].rename(columns={'PRD_CD':'PrdCd'}), on='PrdCd', how='left')

            df_vx_test['店舗売変'][(df_vx_test['WeekStartDate']>=df_vx_test['HANBAI_FROM_YMD'])&(df_vx_test['WeekStartDate']<=df_vx_test['HANBAI_TO_YMD'])] =  df_vx_test['KIKAKU_BAIKA']

            df_vx_test = df_vx_test.drop(columns=['HANBAI_FROM_YMD', 'HANBAI_TO_YMD', 'KIKAKU_BAIKA']).reset_index(drop=True)
    
            
    kikaku_process_time_list.append(['longs processed', time.time()])
    df_vx_test['BAIKA_NEW'] = df_vx_test[check_baika_list].min(axis=1)
    df_vx_test['BAIKA'][df_vx_test['WeekStartDate'] >= target_week_from_ymd] = df_vx_test[df_vx_test['WeekStartDate'] >= target_week_from_ymd]['BAIKA_NEW']
    kikaku_process_time_list.append(['baika_new processed', time.time()])

    check_baika_list.append('BAIKA_NEW')
    check_baika_list.remove('baika_toitsu')
    check_baika_list.remove('BAIKA')

    # holdout後のもののTANKA, 割引率、割引額をdrop
    df_vx_test = df_vx_test.drop(check_baika_list, axis=1)
    kikaku_process_time_list.append(['kikaku all processed', time.time()])
    time_log_list = time_log_list + kikaku_process_time_list
    
    return df_vx_test, time_log_list

    

def upload_collected_sales_data(
    df_vx_test,
    df_calendar,
    today_nenshudo,
    OUTPUT_COLLECTED_SALES_VALUE,
    BigqueryClient,
    bigquery, max_syudo_dic
):
    """
    Extracts relevant sales data, transforms it, and uploads it to a BigQuery table.

    This function operates based on a flag `OUTPUT_COLLECTED_SALES_VALUE`.
    If True, it filters `df_vx_test` for a specific date range, adds new columns,
    drops existing ones, and attempts to upload the resulting DataFrame to BigQuery.
    It includes a retry mechanism for the BigQuery upload.
    """
    if OUTPUT_COLLECTED_SALES_VALUE:

        extract_start_nenshudo = calc_nenshudo(today_nenshudo, -13, max_syudo_dic)
        extract_start_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == extract_start_nenshudo].values[0]
        today_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == today_nenshudo].values[0]

        # 8週前から先週までのデータを抽出
        df_vx_test_colected_sales_value =  df_vx_test[(extract_start_week_from_ymd <= df_vx_test['WeekStartDate'])&(df_vx_test['WeekStartDate'] < today_week_from_ymd)].reset_index(drop=True)

        df_vx_test_colected_sales_value['NENSHUDO'] = today_nenshudo
        df_vx_test_colected_sales_value['MODEL_TYPE'] = 'SMALL_QTY'

        # The original code had a commented-out .to_csv line. I will keep it commented.
        #df_vx_test_colected_sales_value.to_csv('df_vx_test_colected_sales_value.csv')

        # Drop columns as per original logic. Chaining .drop() calls for conciseness.
        df_vx_test_colected_sales_value = df_vx_test_colected_sales_value.drop(
            columns=['SalesAmount8ema', 'training_weight', 'hierarchical_model_id', 'qty_model_type']
        )

        # The 'from google.cloud import bigquery' line is already done at the top
        # of this function's definition, as it should be for imports.
        table_id = "dev-cainz-demandforecast.cainz_shortterm_predicted_value_for_statistics.corrected_sales_values_all"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('PrdCd', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('WeekStartDate', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('PreviousYearSalesActualQuantity', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('time_leap8', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('SalesAmount', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('baika_toitsu', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('BAIKA', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('DPT', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('line_cd', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('cls_cd', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('hnmk_cd', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('weekstartdatestamp', 'DATETIME', mode='NULLABLE'),
                bigquery.SchemaField('tenpo_cd', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('TenpoCdPrdCd', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('NENSHUDO', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('MODEL_TYPE', 'STRING', mode='NULLABLE'),
            ],
            write_disposition='WRITE_APPEND',
        )

        upload_complete = False
        while upload_complete == False:
            try:
                client = BigqueryClient()
                job = client.load_table_from_dataframe(df_vx_test_colected_sales_value, table_id, job_config=job_config)
                job.result() # Waits for the job to complete.

                upload_complete = True

            except Exception as e:
                logging.error('errtype: %s', str(type(e)))
                logging.error('err: %s', str(e))
                logging.info('data upload retry')
                time.sleep(20)

        del df_vx_test_colected_sales_value
    


def process_salesup_flags(
    df_vx_test,
    salesup_flag,
    tenpo_cd,
):
    """
    Processes sales-up flags by querying BigQuery and updating the df_vx_test DataFrame.

    Args:
        df_vx_test: The main DataFrame to which sales-up flags will be added.
                    Expected column: 'PrdCd'.
        salesup_flag: A boolean flag to determine if the sales-up processing should run.
        tenpo_cd: The store code to filter BigQuery queries.
        project_id: The Google Cloud project ID for BigQuery queries.
                    Defaults to "dev-cainz-demandforecast".
    """
    if salesup_flag:

        def get_salesup_prdcd_list(my_salesup_table, my_flag_col_name, tenpo_cd):
            # project_id is now passed as an argument to the main function
            # and used here directly.
            dataset_id = 'short_term_cloudrunjobs'
            target_query = f"""  SELECT DISTINCT PrdCd FROM `{project_id}.{dataset_id}.{my_salesup_table}` WHERE tenpo_cd = """ + str(tenpo_cd) \
                            + f""" AND {my_flag_col_name} > 0"""
            logging.info("Executing BigQuery query: %s", target_query) # Converted print to logging.info
            salesup_prdcd_df = pd.read_gbq(target_query, project_id, dialect='standard')
            salesup_prdcd_list = list(salesup_prdcd_df['PrdCd'].astype(int))
            return salesup_prdcd_list


        salesup_train_table_name_list = [
    'monthly-train-middlesmall2025-06-10_obon_218str',
  'monthly-train-large2025-06-10_obon_218str',
  'monthly-train-seasonal-12-2025-06-10_obon_218str'
                            ]

        flag_col_name_list = ['BusyPeriodFlagNenmatsu',
                          'BusyPeriodFlagGw',
                          'BusyPeriodFlagNewLife',
                          'BusyPeriodFlagEndRainySsn',
                          'BusyPeriodFlagObon']

        for my_flag_col_name in flag_col_name_list:
            df_vx_test[my_flag_col_name] = 0 # Initialize flag column to 0
            for my_salesup_table in salesup_train_table_name_list:
                my_salesup_prdcd_list = get_salesup_prdcd_list(my_salesup_table, my_flag_col_name, tenpo_cd)

                # Use .loc for safe assignment to avoid SettingWithCopyWarning
                # Ensure PrdCd in df_vx_test is numeric for .isin() to work correctly
                # (assuming PrdCd is already int/numeric from previous steps)
                df_vx_test.loc[df_vx_test['PrdCd'].isin(my_salesup_prdcd_list), my_flag_col_name] = 1

    return df_vx_test



def divide_and_upload_seasonal_items(
    df_vx_test,
    devide_season_items, 
    SEASONAL_TRAINDATA_TABLE,
    tenpo_cd,
    today, 
    OUTPUT_TABLE_SUFFIX,
    BigqueryClient,
    project_id="dev-cainz-demandforecast" 
):
    """
    Identifies seasonal items, extracts them from the main DataFrame,
    and uploads the seasonal data to a BigQuery table.

    Args:
        df_vx_test: The main DataFrame containing product data, including 'PrdCd'.
                    This DataFrame will be modified (seasonal items removed).
        devide_season_items: A boolean flag to determine if seasonal item processing should occur.
        SEASONAL_TRAINDATA_TABLE: The BigQuery table name containing seasonal product IDs.
        tenpo_cd: The store code to filter BigQuery queries for seasonal products.
        today: A value representing the current date, used in the output table name.
        OUTPUT_TABLE_SUFFIX: A suffix for the output BigQuery table name.
        BigqueryClient: A callable that returns an instance of a BigQuery client.
        project_id: The Google Cloud project ID for BigQuery queries and uploads.
                    Defaults to "dev-cainz-demandforecast".

    Returns:
        pd.DataFrame: The df_vx_test DataFrame with seasonal items removed.
    """
    if devide_season_items:

        dataset_id = 'short_term_cloudrunjobs'
        table_id_seasonal_query = SEASONAL_TRAINDATA_TABLE # Renamed to avoid conflict with upload table_id

        target_query = f"""  SELECT DISTINCT PrdCd FROM `{project_id}.{dataset_id}.{table_id_seasonal_query}` WHERE tenpo_cd = """ + str(tenpo_cd)

        logging.info("Executing BigQuery query for seasonal items: %s", target_query) # Converted print to logging.info
        seasonal_prdcd_df = pd.read_gbq(target_query, project_id, dialect='standard')
        seasonal_prdcd_list = list(seasonal_prdcd_df['PrdCd'].astype(int))

        df_vx_test_seasonal = df_vx_test[df_vx_test['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)

        # Build the target table ID for uploading seasonal data
        table_id_seasonal_upload = f"{project_id}.{dataset_id}.monthly-test-seasonal-{str(today)}{str(OUTPUT_TABLE_SUFFIX)}"

        # The 'prediction_table_smallqty_season' variable is defined but not directly used
        # in the provided snippet's logic, only for string assignment.
        prediction_table_smallqty_season = "monthly-test-seasonal-" + str(today)  + str(OUTPUT_TABLE_SUFFIX)


        if len(df_vx_test_seasonal) > 0:
            upload_complete = False
            while upload_complete == False:
                try:
                    client = BigqueryClient()
                    job = client.load_table_from_dataframe(df_vx_test_seasonal, table_id_seasonal_upload) # Use the correct table ID
                    job.result()

                    logging.info("==data-uploaded-bq======= Table: %s", table_id_seasonal_upload) # Converted print to logging.info

                    upload_complete = True

                except Exception as e:
                    logging.error('errtype: %s', str(type(e))) # Converted print to logging.error
                    logging.error('err: %s', str(e))           # Converted print to logging.error
                    logging.info('data upload retry')           # Converted print to logging.info
                    time.sleep(20)

        # シーズン品以外の商品にする (Make it non-seasonal products)
        df_vx_test = df_vx_test[~df_vx_test['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)

    # Return the modified df_vx_test
    return df_vx_test



def trigger_cloud_function_monthly(prediction_table_smallqty_season):
    url = 'https://vertex-pipeline-shortterm1-monthly3-64590398402.asia-northeast1.run.app'
    data = {
        'prediction_table_smallqty_large': prediction_table_3div,
        'prediction_table_smallqty_middlesmall': prediction_table_hierarchy,
        'prediction_table_smallqty_season': prediction_table_smallqty_season,
    }

    try:
        # Google Cloudの認証トークンを取得
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
        headers = {
            'Authorization': f'Bearer {id_token}',
            'Content-Type': 'application/json'  # JSONデータを送信するためのContent-Type
        }
        response = requests.post(url, json=data, headers=headers)
        if response.status_code != 200:
            raise Exception(f'HTTP error! status: {response.status_code}')
        response_data = response.json()
        logger.info(f'Function response: {response_data}')
    except Exception as error:
        logger.info(f'Error calling Cloud Function:  {error}')



def main():
    time_log_list = []
    start_t = time.time()
    time_log_list.append(['start_t', start_t])
    tenpo_cd_ref = None
    path_tran_ref = None
    col_lag = config['col_lag']
    bucket_name = config['bucket_name']
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    col_id = '商品コード'
    col_target = config["target"]
    col_time = config["feature_timeline"]
        
    dfc = common.extract_as_df(path_week_master, bucket_name)
    print("####################dfc##############", dfc)
    df_calendar = extract_as_df_with_encoding("Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv","dev-cainz-demandforecast","utf-8")
    print("############df_calendar##############", df_calendar)
    dfc_tmp = df_calendar[["nenshudo", "week_from_ymd", "week_to_ymd"]]
    dfc_tmp["week_from_ymd"] = dfc_tmp["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    dfc_tmp["week_to_ymd"] = dfc_tmp["week_to_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    df_today_nenshudo  = dfc_tmp["nenshudo"][(dfc_tmp["week_from_ymd"] <= today_date_str)&(dfc_tmp["week_to_ymd"] >= today_date_str)]
    today_nenshudo = df_today_nenshudo.values[0]
    logger.info(f'today_nenshudo: {today_nenshudo}')

    max_syudo_dic = dfc[['nendo', 'shudo']].groupby(['nendo']).max()['shudo'].to_dict()
    path_tran = "01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_monthly_series.csv"

    if add_reference_store:
        # 参照店舗の紐づけ情報を読み込む
        path_reference_store = "Basic_Analysis_unzip_result/01_Data/37_reference_store/reference_store.csv"
        reference_store_df = extract_as_df(path_reference_store, bucket_name)
        print("#############reference_store_df###########", reference_store_df)
        reference_store_df["OPEN_DATE"] = reference_store_df["OPEN_DATE"].apply(lambda x : pd.to_datetime(str(x)))
        reference_store_df["OPEN_DATE_REF"] = reference_store_df["OPEN_DATE_REF"].apply(lambda x : pd.to_datetime(str(x)))

        newstore_refstore_dict = dict(zip(reference_store_df['STORE'], reference_store_df['STORE_REF']))
        newstore_opendate_dict = dict(zip(reference_store_df['STORE'], reference_store_df['OPEN_DATE']))

        # 参照店舗の有無をチェックして、あればpath_tran_refを設定する
        if tenpo_cd in newstore_refstore_dict:
            tenpo_cd_ref = newstore_refstore_dict[tenpo_cd]
            logger.info(f'参照店舗: {tenpo_cd_ref}')      
            path_tran_ref ="01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd_ref)+"/{}_{}_monthly_series.csv"


    #############################func 1
    sales_df = load_sales_data(dpt_list, tenpo_cd, path_tran, bucket_name, tenpo_cd_ref, today, path_tran_ref, THEME_MD_MODE=False, theme_md_prdcd_list=None)
    
    logger.info(f'tran len {len(sales_df)}')
    print("###############sales_df###################", sales_df) 
    length = len(sales_df['PRD_CD'].unique().tolist())
    logger.info(f'SKU {length}')

    if(len(sales_df) <= 0):
        logger.info("=====There is no sales data, please check: stage1 has been executed======")
        sys.exit(1)

    sales_df = sales_df[['PRD_CD', 'nenshudo', 'URI_SU','TENPO_CD', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    length = len(sales_df['PRD_CD'].unique())
    logger.info(f'sales_df SKU: {length}')
    trandata_load_t = time.time()
    time_log_list.append(['trandata_load_t', trandata_load_t])

    #########################func 2 
    sales_df, nominmax_prdcd_df, minmax0_prdcd_df, time_log_list = process_minmax_data(restrict_minmax, dpt_list, tenpo_cd, bucket_name, sales_df, extract_as_df_with_encoding, time_log_list)

    ###############################func 3
    sales_df, hacchu_teishi_prdcd_df2, time_log_list = restrict_tenpo_hacchu_end_func(restrinct_tenpo_hacchu_end, tenpo_cd, bucket_name, sales_df, my_date, extract_as_df, time_log_list)
    print("########### sales_df#############", sales_df)           
    train_df = copy.deepcopy(sales_df)
    df_calendar_tmp_sales = df_calendar[["nenshudo","week_from_ymd"]]

    train_end_nenshudo = calc_nenshudo(today_nenshudo, -6, max_syudo_dic)
    logger.info(f'train_end_nenshudo {train_end_nenshudo}')

    target_nenshudo = calc_nenshudo(today_nenshudo, -5, max_syudo_dic)
    logger.info(f'target_nenshudo: {target_nenshudo}')

    if contex_term_valiable:
        start_nenshudo = calc_nenshudo(today_nenshudo, -(5+contex_term), max_syudo_dic)
        logger.info(f'start_nenshudo: {start_nenshudo}')    
    else:
        start_nenshudo = calc_nenshudo(today_nenshudo, -25, max_syudo_dic)
        logger.info(f'start_nenshudo: {start_nenshudo}')


    if prediction_term_4week:                                # 4週未来  トータル10週
        end_nenshudo = calc_nenshudo(today_nenshudo, 4, max_syudo_dic)
        logger.info(f'end_nenshudo: {end_nenshudo}')
    elif prediction_term_valiable:                           # 5週未来　トータル11週
        end_nenshudo = calc_nenshudo(today_nenshudo, prediction_term, max_syudo_dic)
    else:                                                     # 14週未来　トータル20週
        end_nenshudo = calc_nenshudo(today_nenshudo, 14, max_syudo_dic)

        logger.info(f'end_nenshudo: {end_nenshudo}')

    if output_collected_sales_value == True:
        prev_nenshudo = calc_nenshudo(today_nenshudo, -1, max_syudo_dic)
        train_df = train_df[train_df['nenshudo']<=prev_nenshudo].reset_index(drop=True)
        train_df = interpolate_df(train_df, df_calendar)
        df_calendar_expand = df_calendar[['nenshudo','week_from_ymd']][df_calendar['nenshudo']>=today_nenshudo].reset_index(drop=True)
        df_calendar_expand = df_calendar_expand[['nenshudo','week_from_ymd']][df_calendar_expand['nenshudo']<=end_nenshudo].reset_index(drop=True)     
    else:  
        train_df = train_df[train_df['nenshudo']<=train_end_nenshudo].reset_index(drop=True) 
        train_df = interpolate_df(train_df, df_calendar)
        df_calendar_expand = df_calendar[['nenshudo','week_from_ymd']][df_calendar['nenshudo']>=target_nenshudo].reset_index(drop=True) # 20230131実行
        df_calendar_expand = df_calendar_expand[['nenshudo','week_from_ymd']][df_calendar_expand['nenshudo']<=end_nenshudo].reset_index(drop=True) # 

    train_df = pd.merge(train_df, df_calendar_tmp_sales, on ="nenshudo", how ="left").reset_index(drop=True)
    train_df = train_df.rename(columns={'PRD_CD':'商品コード', 'week_from_ymd':'週開始日付', 'URI_SU':'売上実績数量', 'nenshudo':'年週度'})


    ###############################func 4
    if OUTPUT_METRICS_VALUE:
        metrics_result, train_df = calculate_advanced_metrics(OUTPUT_METRICS_VALUE, today_nenshudo, dfc_tmp, train_df, calc_nenshudo2)
        print("##############metrics_result############", metrics_result)
        print("################train_df###############", train_df)
    train_df_prd = train_df[['商品コード']]
    df_calendar_expand = df_calendar_expand.assign(join_key=0).drop_duplicates().reset_index(drop=True)
    train_df_prd = train_df_prd.assign(join_key=0).drop_duplicates().reset_index(drop=True)
    temp_df = pd.merge(df_calendar_expand,train_df_prd,on="join_key",how='outer').drop('join_key', axis=1).reset_index(drop=True)
    del train_df_prd
    temp_df[['売上実績数量']] = 0
    temp_df = temp_df.rename(columns={'week_from_ymd':'週開始日付', 'nenshudo':'年週度'})
    train_df = pd.concat([train_df, temp_df], axis=0).reset_index(drop=True)
    del temp_df
    train_df[['店舗コード']] = tenpo_cd
    train_df = train_df[['商品コード','週開始日付','店舗コード','売上実績数量','年週度', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    target_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == target_nenshudo].values[0]
    start_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == start_nenshudo].values[0]
    context_end_nenshudo = target_nenshudo
    context_end_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == context_end_nenshudo].values[0]
    end_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == end_nenshudo].values[0]
    dftarget = copy.deepcopy(train_df)
    dftarget['週開始日付_予測対象'] = dftarget['週開始日付']
    #train_df = train_df.sort_values('年週度').reset_index(drop=True)
    dftarget = dftarget.sort_values('年週度').reset_index(drop=True)
    del train_df
    #dftarget2 = dftarget[['商品コード','週開始日付_予測対象']]
    dftarget2 = dftarget[['商品コード','週開始日付_予測対象', '店舗コード', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    del dftarget
    df_zen_calendar = df_calendar[["week_from_ymd", "znen_week_from_ymd", "nenshudo", 'znen_nendo', 'znen_shudo']]
    df_zen_calendar = df_zen_calendar.rename(columns={'week_from_ymd': '週開始日付_予測対象', 'znen_week_from_ymd':'前年週開始日付'})
    dftarget2 = pd.merge(dftarget2, df_zen_calendar, on="週開始日付_予測対象")
    dftarget2["zen_nenshudo"] = dftarget2["znen_nendo"] * 100 + dftarget2["znen_shudo"]
    dftarget2 = dftarget2.drop('znen_nendo', axis=1)
    dftarget2 = dftarget2.drop('znen_shudo', axis=1)
    logger.info("==============週開始日付_予測対象================")
    sales_df = sales_df[['PRD_CD', 'nenshudo', 'URI_SU','TENPO_CD', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    sales_df = sales_df.drop_duplicates().reset_index(drop=True)
    merge_cal_end_t = time.time()
    time_log_list.append(['merge_cal_end_t', merge_cal_end_t])

    
    ###############################func 5
    sales_df = preprocess_sales_data(sales_df, df_calendar, no_sales_term_weight_zero, no_sales_term_set_ave)
    print("############sales_df###############", sales_df)
    no_sales_term_correct_end_t = time.time()
    time_log_list.append(['no_sales_term_correct_end_t', no_sales_term_correct_end_t])    
    df_odas_calender = odas_correct(df_calendar, bucket_name, tenpo_cd, use_jan_connect=use_jan_connect)
    df_odas_calender = df_odas_calender.groupby(['TENPO_CD', 'PRD_CD', 'nenshudo'], as_index=False).agg({"odas_amount":'sum', 'sales_ymd':'max'})
    sales_df = pd.merge(sales_df, df_odas_calender, on =["PRD_CD", "nenshudo", "TENPO_CD"], how="left")
    del df_odas_calender
    sales_df = sales_df.drop('sales_ymd', axis=1)
    sales_df["odas_amount"] = sales_df["odas_amount"].fillna(0)

    
    ###############################func 6
    sales_df = updated_sales_df(sales_df)
    print("############sales_df###############", sales_df)
    sales_df = sales_df.drop('odas_amount', axis=1)
    sales_df = sales_df.drop_duplicates().reset_index(drop=True)
    odas_correct_end_t = time.time()
    time_log_list.append(['odas_correct_end_t', odas_correct_end_t])   


    ###############################func 7
    if OUTPUT_METRICS_VALUE:
        metrics_result, sales_df = analyze_sales_metrics(sales_df, dfc_tmp, dfc, today_nenshudo, OUTPUT_METRICS_VALUE)
    
    if logarithmize_target_variable:
        logger.info('start logarithmize_target_variable')
        sales_df['URI_SU'] = np.log1p(sales_df['URI_SU'])
        logger.info('end logarithmize_target_variable')
    sales_df_saved = copy.deepcopy(sales_df)

    if logarithmize_target_variable:
        logger.info('start logarithmize_target_variable')
        sales_df['URI_SU'] = np.log1p(sales_df['URI_SU'])
        logger.info('end logarithmize_target_variable')

    # 20230823 上からこちらに移動（補正後の値をとっておくため）
    # ここで、URI_SUをセーブして、あとで使ってる　******************************************************
    sales_df_saved = copy.deepcopy(sales_df)
    
    sales_df = pd.merge(sales_df,df_calendar,on="nenshudo")

    sales_df = sales_df.rename(columns={'URI_SU':'前年売上実績数量', 'PRD_CD':'商品コード', 'week_from_ymd': '前年週開始日付'})
    sales_df = sales_df.drop('nenshudo', axis=1).reset_index(drop=True)

    #dftarget3 = pd.merge(dftarget2, sales_df, on=['商品コード','前年週開始日付'], how='left')
    dftarget3 = pd.merge(dftarget2.rename(columns={'店舗コード':'TENPO_CD'}), sales_df[['商品コード', '前年週開始日付', '前年売上実績数量']], on=['商品コード', '前年週開始日付'], how='left')
    del dftarget2
    ### ここで補間が必要！！！！！！！！ 20240131追加
    dftarget3 = interpolate_df2(dftarget3)
    dftarget3['TENPO_CD'] = tenpo_cd

    dftarget3 = dftarget3.fillna(0).reset_index(drop=True)

    dftarget3 = dftarget3[["商品コード","週開始日付_予測対象","前年週開始日付","前年売上実績数量","TENPO_CD","nenshudo", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    # タイムリープ
    dftarget3 = dftarget3.drop_duplicates().reset_index(drop=True)
    dftarget3['time_leap8'] = dftarget3.groupby('商品コード',as_index=False)['前年売上実績数量'].transform(lambda x: x.ewm(span=8).mean())

    df_vx_test = dftarget3[['商品コード','週開始日付_予測対象','前年売上実績数量', 'time_leap8','nenshudo', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    sales_df_saved_tenpo = sales_df_saved[sales_df_saved['TENPO_CD'] == tenpo_cd].reset_index(drop=True)
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # ここで上でセーブしていたデータを取ってくるので要注意
    sales_df_saved_tenpo = sales_df_saved_tenpo[['PRD_CD','nenshudo','URI_SU']]
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    sales_df_saved_tenpo = sales_df_saved_tenpo.rename(columns={'PRD_CD':'商品コード'})
    sales_df_saved = pd.merge(df_vx_test,sales_df_saved_tenpo,on=['商品コード','nenshudo'],how='left')
    del sales_df_saved_tenpo
    sales_df_saved['URI_SU'] = sales_df_saved['URI_SU'].fillna(0).reset_index(drop=True)

    timeleap8_end_t = time.time()
    time_log_list.append(['timeleap8_end_t', timeleap8_end_t])   

    sales_df_saved['SalesAmount8ema'] = sales_df_saved.groupby("商品コード", as_index=False)['URI_SU'].transform(lambda x: x.ewm(span=8).mean())
    sales_df_saved['SalesAmount8ema'] = sales_df_saved.groupby(["商品コード"])['SalesAmount8ema'].transform(lambda x: x.interpolate(limit_direction='both'))
    sales_df_saved['SalesAmount8ema'][sales_df_saved['週開始日付_予測対象'] >= target_week_from_ymd] = np.nan

    sales8ema_end_t = time.time()
    time_log_list.append(['sales8ema_end_t', sales8ema_end_t])   

    if output_collected_sales_value == False:
        # 予測対象期間の売り数にnanを設定
        sales_df_saved['URI_SU'][sales_df_saved['週開始日付_予測対象'] >= target_week_from_ymd] = np.nan

    df_vx_test = sales_df_saved.rename(columns={'商品コード': 'PrdCd', '週開始日付_予測対象':'WeekStartDate', '割引率':'DiscountRate','前年売上実績数量':'PreviousYearSalesActualQuantity','URI_SU':'SalesAmount'})
    del sales_df_saved

    df_vx_test = df_vx_test[['PrdCd', 'WeekStartDate', 'PreviousYearSalesActualQuantity',  'time_leap8', 'SalesAmount','SalesAmount8ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    df_vx_test['weekstartdatestamp'] = pd.to_datetime(df_vx_test['WeekStartDate'], format = '%Y%m%d')

    df_vx_test = df_vx_test.dropna(subset=['PreviousYearSalesActualQuantity']).reset_index(drop=True)
    df_vx_test = df_vx_test.dropna(subset=['time_leap8']).reset_index(drop=True)

    weekdatestamp_end_t = time.time()
    time_log_list.append(['weekdatestamp_end_t', weekdatestamp_end_t])  
    # 20240229 add
    df_vx_test = df_vx_test[df_vx_test['WeekStartDate']>=20201219].reset_index(drop=True)

    
    ###############################func 8
    df_vx_test, time_log_list = assign_training_weights(df_vx_test, no_sales_term_weight_zero, time_log_list)

    
    ###############################func 9
    df_vx_test, time_log_list = process_known_prices(df_vx_test, kakaku_jizen_kichi, bucket_name, bucket, dpt_list, tenpo_cd, extract_as_df, load_kikaku_data, common, time_log_list, target_week_from_ymd)

    print("#############df_vx_test############", df_vx_test)
    kakaku_jizen_kichi_end_t = time.time()
    time_log_list.append(['kakaku_jizen_kichi_end_t', kakaku_jizen_kichi_end_t])   
    # 階層モデルの上位下位に分ける列を作成
    threshold_small_wave_nendo=2020
    df_vx_test['SalesAmount_Mean'] = df_vx_test.groupby("PrdCd")["SalesAmount"].transform(lambda x: x.mean())

    df_vx_test['hierarchical_model_id'] = np.nan
    df_vx_test['hierarchical_model_id'][df_vx_test['SalesAmount_Mean'] < df_vx_test['SalesAmount_Mean'].median()] = str(tenpo_cd) + '_small_qty'
    df_vx_test['hierarchical_model_id'][df_vx_test['SalesAmount_Mean'] >= df_vx_test['SalesAmount_Mean'].median()] =  str(tenpo_cd) + '_not_small_qty'            

    # 3分割モデルに分ける列を作成
    def get_33_percentile(x):
        return np.percentile(x, q=[33])[0]
    def get_66_percentile(x):
        return np.percentile(x, q=[66])[0]

    df_vx_test['qty_model_type'] = np.nan
    df_vx_test['SalesAmount_Mean_33'] = get_33_percentile(df_vx_test['SalesAmount_Mean'])
    df_vx_test['SalesAmount_Mean_66'] = get_66_percentile(df_vx_test['SalesAmount_Mean'])
    df_vx_test['qty_model_type'][df_vx_test['SalesAmount_Mean'] >= df_vx_test['SalesAmount_Mean_66']] = 'large'
    df_vx_test['qty_model_type'][(df_vx_test['SalesAmount_Mean_66'] > df_vx_test['SalesAmount_Mean'])
                & (df_vx_test['SalesAmount_Mean'] >= df_vx_test['SalesAmount_Mean_33'])] = 'middle'
    df_vx_test['qty_model_type'][df_vx_test['SalesAmount_Mean'] < df_vx_test['SalesAmount_Mean_33']] = 'small'
    df_vx_test = df_vx_test.drop('SalesAmount_Mean', axis=1)                         
    df_vx_test = df_vx_test.drop('SalesAmount_Mean_33', axis=1)
    df_vx_test = df_vx_test.drop('SalesAmount_Mean_66', axis=1)

    df_vx_test['PreviousYearSalesActualQuantity'] = df_vx_test['PreviousYearSalesActualQuantity'].astype(float)
    df_vx_test['SalesAmount'] = df_vx_test['SalesAmount'].astype(float)
    df_vx_test['tenpo_cd'] = tenpo_cd
    df_vx_test = df_vx_test.drop_duplicates().reset_index(drop=True)
    df_vx_test['PrdCd'] = df_vx_test['PrdCd'].astype(int)
    df_vx_test['DPT'] = df_vx_test['DPT'].astype(int)
    df_vx_test['line_cd'] = df_vx_test['line_cd'].astype(int)
    df_vx_test['cls_cd'] = df_vx_test['cls_cd'].astype(int)
    df_vx_test['hnmk_cd'] = df_vx_test['hnmk_cd'].astype(int)
    df_vx_test['TenpoCdPrdCd'] = str(tenpo_cd) + '_' + df_vx_test['PrdCd'].astype(str) 

    div3_col_end_t = time.time()
    time_log_list.append(['div3_col_end_t', div3_col_end_t])   

    
    ###############################func 10
    upload_collected_sales_data(df_vx_test, df_calendar, today_nenshudo, OUTPUT_COLLECTED_SALES_VALUE, BigqueryClient, bigquery, max_syudo_dic)

    output_corrected_sales_val_end_t = time.time()
    time_log_list.append(['output_corrected_sales_val_end_t', output_corrected_sales_val_end_t])   
    df_vx_test = df_vx_test[df_vx_test['PrdCd'] > 0]
    monthly_data_end_t = time.time()
    time_log_list.append(['monthly_data_end_t', monthly_data_end_t])   


    ###############################func 11
    df_vx_test = process_salesup_flags(df_vx_test, salesup_flag, tenpo_cd)
    print("#################df_vx_test############", df_vx_test)
    # ここで最後にTEST期間開始以降、TEST期間終了以前に絞っている***********************************************
    df_vx_test =  df_vx_test[df_vx_test['WeekStartDate'] >= start_week_from_ymd].reset_index(drop=True)
    df_vx_test =  df_vx_test[df_vx_test['WeekStartDate'] <= end_week_from_ymd].reset_index(drop=True)
    # **************************************************************************************************
    df_vx_test['SalesAmount'][df_vx_test['WeekStartDate'] >= target_week_from_ymd] = np.nan
    # **************************************************************************************************
    df_vx_test = df_vx_test.drop('WeekStartDate', axis=1)
    df_vx_test = df_vx_test[df_vx_test['PrdCd'] > 0]

    ###############################func 12
    df_vx_test = divide_and_upload_seasonal_items(df_vx_test, devide_season_items, SEASONAL_TRAINDATA_TABLE, tenpo_cd, today, OUTPUT_TABLE_SUFFIX, BigqueryClient, project_id="dev-cainz-demandforecast" )
    
    # 上位、中位、下位は、予測対象ランク商品に絞る
    ranker_prdcd_list = get_ranker_prdcd_list()
    df_vx_test = df_vx_test[df_vx_test['PrdCd'].isin(ranker_prdcd_list)].reset_index(drop=True)
    df_vx_test_hieralchy = df_vx_test[df_vx_test['qty_model_type'] != 'large']
    logger.info('data upload start')
    table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "monthly-test-" + str(today) + str(OUTPUT_TABLE_SUFFIX)

    prediction_table_hierarchy = "monthly-test-" + str(today) + str(OUTPUT_TABLE_SUFFIX)

    if len(df_vx_test_hieralchy) > 0:
        upload_complete = False
        while upload_complete == False:
            try:
                client = BigqueryClient()
                job = client.load_table_from_dataframe(df_vx_test_hieralchy, table_id)
                job.result()
                upload_complete = True

            except Exception as e:
                logger.info(f'errtype: {str(type(e))}')
                logger.info(f'err: {str(e)}')
                logger.info('data upload retry')
                time.sleep(20)

    logger.info("==data-uploaded-bq hieralchical model ===")
    df_vx_test = df_vx_test.drop('hierarchical_model_id', axis=1)
    upload_hieralchy_data_end_t = time.time()
    time_log_list.append(['upload_hieralchy_data_end_t', upload_hieralchy_data_end_t])   

    # データを３つにわける
    table_id1 = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "monthly-test-large" + str(today) + str(OUTPUT_TABLE_SUFFIX)
    table_id2 = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "monthly-test-middle" + str(today) + str(OUTPUT_TABLE_SUFFIX)
    table_id3 = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "monthly-test-small" + str(today) + str(OUTPUT_TABLE_SUFFIX)

    prediction_table_3div = "monthly-test-large" + str(today) + str(OUTPUT_TABLE_SUFFIX)

    df_vx_test = df_vx_test[df_vx_test['PrdCd'] > 0]

    df_vx_test_large = df_vx_test[df_vx_test['qty_model_type'] == 'large']

    if len(df_vx_test_large) > 0:
        upload_complete = False
        while upload_complete == False:
            try:
                client = BigqueryClient()
                job = client.load_table_from_dataframe(df_vx_test_large, table_id1)
                job.result()

                upload_complete = True

            except Exception as e:
                logger.info(f'errtype: {str(type(e))}')
                logger.info(f'err: {str(e)}')
                logger.info('data upload retry')
                time.sleep(20)


    logger.info("==data-uploaded-bq===")
    end_t = time.time()
    elapsed_time = end_t - start_t
    logger.info(f"Elapsed time: {elapsed_time:.3f} seconds")
    upload_div3_data_end_t = time.time()
    time_log_list.append(['upload_div3_data_end_t', upload_div3_data_end_t])   

    if cloudrunjob_mode and CALL_NEXT_PIPELINE:
        # stage2完了チェックフォルダ配下への完了ファイルアップロード
        path_upload_blob = "vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_monthly_complete/completed_" + str(tenpo_cd) + ".csv"

        tmp_fname = str(tenpo_cd) + ".csv"
        my_df = pd.DataFrame([[tenpo_cd]])
        my_df.to_csv(tmp_fname, index=False)

        blob = bucket.blob(path_upload_blob)
        blob.upload_from_filename(tmp_fname)

        # 全店分のstage2が終了しているかチェックする
        blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_monthly_complete/completed_')
        complete_task_count = sum([1 for blob in blobs])

        if TASK_COUNT == complete_task_count:
            trigger_cloud_function_monthly(prediction_table_smallqty_season)
            logger.info('call batch prediction and post process pipeline complete*******************')

            # stage2完了チェックフォルダ配下のファイル削除
            blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_monthly_complete/completed_')
            for blob in blobs:
                logger.info(f"{blob.name}")
                generation_match_precondition = None
                blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
                generation_match_precondition = blob.generation
                blob.delete(if_generation_match=generation_match_precondition)
                logger.info(f"Blob {blob.name} deleted.")

    cloudfunction_process_end_t = time.time()
    time_log_list.append(['cloudfunction_process_end_t', cloudfunction_process_end_t])   
    pd.DataFrame(time_log_list).to_csv('time_log_list.csv')
    logger.info("Stage completed")

   
            
if __name__ == "__main__":
    main()













###########################################This is stage 2 monthly test logs for referencing so that you can use this data to mock data
Requirement already satisfied: pyarrow==11.0.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 1)) (11.0.0)
Requirement already satisfied: DateTime==5.1 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 2)) (5.1)
Requirement already satisfied: httplib2==0.22.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 3)) (0.22.0)
Requirement already satisfied: japanize-matplotlib==1.1.3 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 4)) (1.1.3)
Requirement already satisfied: joblib==1.2.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 5)) (1.2.0)
Requirement already satisfied: json5==0.9.11 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 6)) (0.9.11)
Requirement already satisfied: matplotlib==3.5.3 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 7)) (3.5.3)
Requirement already satisfied: oauth2client==4.1.3 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 8)) (4.1.3)
Requirement already satisfied: oauthlib==3.2.2 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 9)) (3.2.2)
Requirement already satisfied: openpyxl==3.1.2 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 10)) (3.1.2)
Requirement already satisfied: PyYAML==6.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 11)) (6.0)
Requirement already satisfied: pandas==1.3.5 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 12)) (1.3.5)
Requirement already satisfied: pandas-gbq==0.19.2 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 13)) (0.19.2)
Requirement already satisfied: pandarallel==1.6.5 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 14)) (1.6.5)
Requirement already satisfied: google==3.0.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 15)) (3.0.0)
Requirement already satisfied: google-auth==2.16.3 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 16)) (2.16.3)
Requirement already satisfied: google-auth-httplib2==0.1.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 17)) (0.1.0)
Requirement already satisfied: google-auth-oauthlib==1.0.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 18)) (1.0.0)
Requirement already satisfied: google-cloud-bigquery==3.7.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 19)) (3.7.0)
Requirement already satisfied: google-cloud-bigquery-storage==2.19.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 20)) (2.19.0)
Requirement already satisfied: google-cloud-storage==2.7.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 21)) (2.7.0)
Requirement already satisfied: swifter==1.4.0 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 22)) (1.4.0)
Requirement already satisfied: statsmodels==0.13.5 in /opt/conda/lib/python3.10/site-packages (from -r requirements.txt (line 23)) (0.13.5)
Requirement already satisfied: numpy>=1.16.6 in /opt/conda/lib/python3.10/site-packages (from pyarrow==11.0.0->-r requirements.txt (line 1)) (1.22.4)
Requirement already satisfied: zope.interface in /opt/conda/lib/python3.10/site-packages (from DateTime==5.1->-r requirements.txt (line 2)) (7.2)
Requirement already satisfied: pytz in /opt/conda/lib/python3.10/site-packages (from DateTime==5.1->-r requirements.txt (line 2)) (2025.2)
Requirement already satisfied: pyparsing!=3.0.0,!=3.0.1,!=3.0.2,!=3.0.3,<4,>=2.4.2 in /opt/conda/lib/python3.10/site-packages (from httplib2==0.22.0->-r requirements.txt (line 3)) (3.2.3)
Requirement already satisfied: cycler>=0.10 in /opt/conda/lib/python3.10/site-packages (from matplotlib==3.5.3->-r requirements.txt (line 7)) (0.12.1)
Requirement already satisfied: fonttools>=4.22.0 in /opt/conda/lib/python3.10/site-packages (from matplotlib==3.5.3->-r requirements.txt (line 7)) (4.56.0)
Requirement already satisfied: kiwisolver>=1.0.1 in /opt/conda/lib/python3.10/site-packages (from matplotlib==3.5.3->-r requirements.txt (line 7)) (1.4.8)
Requirement already satisfied: packaging>=20.0 in /opt/conda/lib/python3.10/site-packages (from matplotlib==3.5.3->-r requirements.txt (line 7)) (21.3)
Requirement already satisfied: pillow>=6.2.0 in /opt/conda/lib/python3.10/site-packages (from matplotlib==3.5.3->-r requirements.txt (line 7)) (11.1.0)
Requirement already satisfied: python-dateutil>=2.7 in /opt/conda/lib/python3.10/site-packages (from matplotlib==3.5.3->-r requirements.txt (line 7)) (2.9.0.post0)
Requirement already satisfied: pyasn1>=0.1.7 in /opt/conda/lib/python3.10/site-packages (from oauth2client==4.1.3->-r requirements.txt (line 8)) (0.6.1)
Requirement already satisfied: pyasn1-modules>=0.0.5 in /opt/conda/lib/python3.10/site-packages (from oauth2client==4.1.3->-r requirements.txt (line 8)) (0.4.2)
Requirement already satisfied: rsa>=3.1.4 in /opt/conda/lib/python3.10/site-packages (from oauth2client==4.1.3->-r requirements.txt (line 8)) (4.9)
Requirement already satisfied: six>=1.6.1 in /opt/conda/lib/python3.10/site-packages (from oauth2client==4.1.3->-r requirements.txt (line 8)) (1.17.0)
Requirement already satisfied: et-xmlfile in /opt/conda/lib/python3.10/site-packages (from openpyxl==3.1.2->-r requirements.txt (line 10)) (2.0.0)
Requirement already satisfied: setuptools in /opt/conda/lib/python3.10/site-packages (from pandas-gbq==0.19.2->-r requirements.txt (line 13)) (75.8.2)
Requirement already satisfied: db-dtypes<2.0.0,>=1.0.4 in /opt/conda/lib/python3.10/site-packages (from pandas-gbq==0.19.2->-r requirements.txt (line 13)) (1.4.2)
Requirement already satisfied: pydata-google-auth>=1.5.0 in /opt/conda/lib/python3.10/site-packages (from pandas-gbq==0.19.2->-r requirements.txt (line 13)) (1.9.1)
Requirement already satisfied: google-api-core<3.0.0dev,>=2.10.2 in /opt/conda/lib/python3.10/site-packages (from pandas-gbq==0.19.2->-r requirements.txt (line 13)) (2.24.2)
Requirement already satisfied: dill>=0.3.1 in /opt/conda/lib/python3.10/site-packages (from pandarallel==1.6.5->-r requirements.txt (line 14)) (0.4.0)
Requirement already satisfied: psutil in /opt/conda/lib/python3.10/site-packages (from pandarallel==1.6.5->-r requirements.txt (line 14)) (5.9.5)
Requirement already satisfied: beautifulsoup4 in /opt/conda/lib/python3.10/site-packages (from google==3.0.0->-r requirements.txt (line 15)) (4.13.3)
Requirement already satisfied: cachetools<6.0,>=2.0.0 in /opt/conda/lib/python3.10/site-packages (from google-auth==2.16.3->-r requirements.txt (line 16)) (5.5.2)
Requirement already satisfied: requests-oauthlib>=0.7.0 in /opt/conda/lib/python3.10/site-packages (from google-auth-oauthlib==1.0.0->-r requirements.txt (line 18)) (2.0.0)
Requirement already satisfied: grpcio<2.0dev,>=1.47.0 in /opt/conda/lib/python3.10/site-packages (from google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (1.71.0)
Requirement already satisfied: proto-plus<2.0.0dev,>=1.15.0 in /opt/conda/lib/python3.10/site-packages (from google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (1.26.1)
Requirement already satisfied: google-cloud-core<3.0.0dev,>=1.6.0 in /opt/conda/lib/python3.10/site-packages (from google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (2.3.2)
Requirement already satisfied: google-resumable-media<3.0dev,>=0.6.0 in /opt/conda/lib/python3.10/site-packages (from google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (2.7.2)
Requirement already satisfied: protobuf!=3.20.0,!=3.20.1,!=4.21.0,!=4.21.1,!=4.21.2,!=4.21.3,!=4.21.4,!=4.21.5,<5.0.0dev,>=3.19.5 in /opt/conda/lib/python3.10/site-packages (from google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (3.20.3)
Requirement already satisfied: requests<3.0.0dev,>=2.21.0 in /opt/conda/lib/python3.10/site-packages (from google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (2.32.3)
Requirement already satisfied: dask>=2.10.0 in /opt/conda/lib/python3.10/site-packages (from dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (2024.2.1)
Requirement already satisfied: tqdm>=4.33.0 in /opt/conda/lib/python3.10/site-packages (from swifter==1.4.0->-r requirements.txt (line 22)) (4.67.1)
Requirement already satisfied: patsy>=0.5.2 in /opt/conda/lib/python3.10/site-packages (from statsmodels==0.13.5->-r requirements.txt (line 23)) (1.0.1)
Requirement already satisfied: scipy>=1.3 in /opt/conda/lib/python3.10/site-packages (from statsmodels==0.13.5->-r requirements.txt (line 23)) (1.7.3)
Requirement already satisfied: click>=8.1 in /opt/conda/lib/python3.10/site-packages (from dask>=2.10.0->dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (8.1.8)
Requirement already satisfied: cloudpickle>=1.5.0 in /opt/conda/lib/python3.10/site-packages (from dask>=2.10.0->dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (3.1.1)
Requirement already satisfied: fsspec>=2021.09.0 in /opt/conda/lib/python3.10/site-packages (from dask>=2.10.0->dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (2025.3.0)
Requirement already satisfied: partd>=1.2.0 in /opt/conda/lib/python3.10/site-packages (from dask>=2.10.0->dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (1.4.2)
Requirement already satisfied: toolz>=0.10.0 in /opt/conda/lib/python3.10/site-packages (from dask>=2.10.0->dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (1.0.0)
Requirement already satisfied: importlib-metadata>=4.13.0 in /opt/conda/lib/python3.10/site-packages (from dask>=2.10.0->dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (8.6.1)
Requirement already satisfied: googleapis-common-protos<2.0.0,>=1.56.2 in /opt/conda/lib/python3.10/site-packages (from google-api-core<3.0.0dev,>=2.10.2->pandas-gbq==0.19.2->-r requirements.txt (line 13)) (1.69.2)
Requirement already satisfied: grpcio-status<2.0.dev0,>=1.33.2 in /opt/conda/lib/python3.10/site-packages (from google-api-core[grpc]!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (1.49.0rc1)
Requirement already satisfied: google-crc32c<2.0dev,>=1.0 in /opt/conda/lib/python3.10/site-packages (from google-resumable-media<3.0dev,>=0.6.0->google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (1.5.0)
Requirement already satisfied: charset_normalizer<4,>=2 in /opt/conda/lib/python3.10/site-packages (from requests<3.0.0dev,>=2.21.0->google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (3.4.1)
Requirement already satisfied: idna<4,>=2.5 in /opt/conda/lib/python3.10/site-packages (from requests<3.0.0dev,>=2.21.0->google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (3.10)
Requirement already satisfied: urllib3<3,>=1.21.1 in /opt/conda/lib/python3.10/site-packages (from requests<3.0.0dev,>=2.21.0->google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (1.26.20)
Requirement already satisfied: certifi>=2017.4.17 in /opt/conda/lib/python3.10/site-packages (from requests<3.0.0dev,>=2.21.0->google-cloud-bigquery==3.7.0->-r requirements.txt (line 19)) (2025.1.31)
Requirement already satisfied: soupsieve>1.2 in /opt/conda/lib/python3.10/site-packages (from beautifulsoup4->google==3.0.0->-r requirements.txt (line 15)) (2.6)
Requirement already satisfied: typing-extensions>=4.0.0 in /opt/conda/lib/python3.10/site-packages (from beautifulsoup4->google==3.0.0->-r requirements.txt (line 15)) (4.13.0)
Requirement already satisfied: zipp>=3.20 in /opt/conda/lib/python3.10/site-packages (from importlib-metadata>=4.13.0->dask>=2.10.0->dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (3.21.0)
Requirement already satisfied: locket in /opt/conda/lib/python3.10/site-packages (from partd>=1.2.0->dask>=2.10.0->dask[dataframe]>=2.10.0->swifter==1.4.0->-r requirements.txt (line 22)) (1.0.0)
Note: you may need to restart the kernel to use updated packages.








%run main.py 760
/opt/conda/lib/python3.10/site-packages/dask/dataframe/_pyarrow_compat.py:23: UserWarning: You are using pyarrow version 11.0.0 which is known to be insecure. See https://www.cve.org/CVERecord?id=CVE-2023-47248 for further details. Please upgrade to pyarrow>=14.0.1 or install pyarrow-hotfix to patch your current version.
  warnings.warn(
2025-10-24 16:07:46,327 - INFO - TASK INDEX: 0
2025-10-24 16:07:46,328 - INFO - TASK COUNT: 1
2025-10-24 16:07:46,329 - INFO - TODAY OFFSET: 0
2025-10-24 16:07:46,330 - INFO - OUTPUT_TABLE_SUFFIX: amol_monthly_test
2025-10-24 16:07:46,330 - INFO - CALL NEXT_PIPELINE: 0
2025-10-24 16:07:46,331 - INFO - OUTPUT_METRICS_VALUE: 0
2025-10-24 16:07:46,332 - INFO - SEASONAL_TRAINDATA_TABLE: monthly-train-seasonal-12-2025-06-10_obon_218str
2025-10-24 16:07:46,332 - INFO - TURN_BACK_YYYYMMDD: 
2025-10-24 16:07:46,333 - INFO - tenpo_cd: 760
2025-10-24 16:07:46,333 - INFO - TODAY_OFFSET: 0
2025-10-24 16:07:46,334 - INFO - OUTPUT_TABLE_SUFFIX: amol_monthly_test
2025-10-24 16:07:46,334 - INFO - OUTPUT_COLLECTED_SALES_VALUE: 0
2025-10-24 16:07:46,335 - INFO - OUTPUT_METRICS_VALUE: 0
2025-10-24 16:07:46,335 - INFO - SEASONAL_TRAINDATA_TABLE: monthly-train-seasonal-12-2025-01-30_218newstr
2025-10-24 16:07:46,336 - INFO - TURN_BACK_YYYYMMDD: 
2025-10-24 16:07:46,367 - extract_as_df - INFO - start
2025-10-24 16:07:46,436 - extract_as_df - INFO - end elapsed time: 0.06924867630004883
####################dfc##############       nenshudo  nendo  shudo  minashi_tsuki  week_from_ymd  week_to_ymd  \
0       200301   2003      1              3       20030224     20030302   
1       200302   2003      2              3       20030303     20030309   
2       200303   2003      3              3       20030310     20030316   
3       200304   2003      4              3       20030317     20030323   
4       200305   2003      5              3       20030324     20030330   
...        ...    ...    ...            ...            ...          ...   
1456    203048   2030     48              1       20310120     20310126   
1457    203049   2030     49              2       20310127     20310202   
1458    203050   2030     50              2       20310203     20310209   
1459    203051   2030     51              2       20310210     20310216   
1460    203052   2030     52              2       20310217     20310223   

      znen_nendo  znen_shudo  znen_week_from_ymd  znen_week_to_ymd  \
0           2002           1            20020225          20020303   
1           2002           2            20020304          20020310   
2           2002           3            20020311          20020317   
3           2002           4            20020318          20020324   
4           2002           5            20020325          20020331   
...          ...         ...                 ...               ...   
1456        2029          48            20300121          20300127   
1457        2029          49            20300128          20300203   
1458        2029          50            20300204          20300210   
1459        2029          51            20300211          20300217   
1460        2029          52            20300218          20300224   

      znen_minashi_tsuki  
0                      3  
1                      3  
2                      3  
3                      3  
4                      3  
...                  ...  
1456                   1  
1457                   2  
1458                   2  
1459                   2  
1460                   2  

[1461 rows x 11 columns]
############df_calendar##############       nenshudo  nendo  shudo  minashi_tsuki  week_from_ymd  week_to_ymd  \
0       200301   2003      1              3       20030224     20030302   
1       200302   2003      2              3       20030303     20030309   
2       200303   2003      3              3       20030310     20030316   
3       200304   2003      4              3       20030317     20030323   
4       200305   2003      5              3       20030324     20030330   
...        ...    ...    ...            ...            ...          ...   
1456    203048   2030     48              1       20310120     20310126   
1457    203049   2030     49              2       20310127     20310202   
1458    203050   2030     50              2       20310203     20310209   
1459    203051   2030     51              2       20310210     20310216   
1460    203052   2030     52              2       20310217     20310223   

      znen_nendo  znen_shudo  znen_week_from_ymd  znen_week_to_ymd  \
0           2002           1            20020225          20020303   
1           2002           2            20020304          20020310   
2           2002           3            20020311          20020317   
3           2002           4            20020318          20020324   
4           2002           5            20020325          20020331   
...          ...         ...                 ...               ...   
1456        2029          48            20300121          20300127   
1457        2029          49            20300128          20300203   
1458        2029          50            20300204          20300210   
1459        2029          51            20300211          20300217   
1460        2029          52            20300218          20300224   

      znen_minashi_tsuki  
0                      3  
1                      3  
2                      3  
3                      3  
4                      3  
...                  ...  
1456                   1  
1457                   2  
1458                   2  
1459                   2  
1460                   2  

[1461 rows x 11 columns]
2025-10-24 16:07:46,788 - INFO - today_nenshudo: 202535
2025-10-24 16:07:46,848 - INFO - 69
#############reference_store_df###########     STORE        STORE_NAME  OPEN_DATE  STORE_REF STORE_NAME_REF  \
0     872            SF湘南平塚   20220622        849      SF名古屋アクルス   
1     873                壬生   20220617        790         宇都宮テクノ   
2     874               佐久平   20220927        775            仙台港   
3     875              アリオ柏   20221025        832         横浜いずみ野   
4     876              常陸太田   20230321        168           石岡玉里   
5     877                久居   20230418        815            四日市   
6     878              西友福生   20230426        832         横浜いずみ野   
7     874               佐久平   20220928        775            仙台港   
8     876              常陸太田   20230322        168           石岡玉里   
9     877                久居   20230419        815            四日市   
10    879                直方   20230712        840           熊本宇土   
11    900                阿見   20231025        856             水戸   
12    907              紀伊川辺   20231122        742            旭飯岡   
13    902               岡山南   20231129        855           姫路大津   
14    904              太田丸山   20231206        844          前橋小島田   
15    910                古河   20241023        833             小山   
16    868      カインズそよら湘南茅ヶ崎   20230601        832     カインズ横浜いずみ野   
17    875  カインズ セブンパーク アリオ柏   20221025        832     カインズ横浜いずみ野   
18    878          カインズ西友福生   20230426        832     カインズ横浜いずみ野   
19    903     カインズ リヴィンオズ大泉   20231115        832     カインズ横浜いずみ野   
20    908            あべのａｎｄ   20240904        832     カインズ横浜いずみ野   
21    909          カインズ西友平塚   20240717        832     カインズ横浜いずみ野   
22    911           カインズ日吉津   20250311        741       カインズＦＣ津山   
23    932   カインズステップガーデン藤原台   20241120        832     カインズ横浜いずみ野   
24    934        カインズリヴィン田無   20241122        832     カインズ横浜いずみ野   
25    954           カインズつくば   20250418        856         カインズ水戸   

    OPEN_DATE_REF  
0        20180928  
1        20100915  
2        20090527  
3        20160831  
4        19960905  
5        20131120  
6        20160831  
7        20090527  
8        19960905  
9        20131119  
10       20171024  
11       20200218  
12       20051109  
13       20190611  
14       20180626  
15       20161115  
16       20160831  
17       20160831  
18       20160831  
19       20160831  
20       20160831  
21       20160831  
22       20051130  
23       20160831  
24       20160831  
25       20200218  
2025-10-24 16:07:47,177 - INFO - 97
2025-10-24 16:07:47,297 - INFO - 14
2025-10-24 16:07:47,753 - INFO - 37
2025-10-24 16:07:47,791 - WARNING - Could not load data for DPT 37 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/37_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F37_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/37_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:07:47,792 - INFO - 27
2025-10-24 16:07:48,572 - INFO - 39
2025-10-24 16:07:48,926 - INFO - 28
2025-10-24 16:07:50,001 - INFO - 74
2025-10-24 16:07:50,665 - INFO - 33
2025-10-24 16:07:51,224 - INFO - 30
2025-10-24 16:07:51,789 - INFO - 36
2025-10-24 16:07:53,017 - INFO - 75
2025-10-24 16:07:56,517 - INFO - 85
2025-10-24 16:07:58,388 - INFO - 80
2025-10-24 16:07:58,424 - WARNING - Could not load data for DPT 80 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/80_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F80_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/80_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:07:58,425 - INFO - 20
2025-10-24 16:07:58,462 - WARNING - Could not load data for DPT 20 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/20_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F20_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/20_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:07:58,463 - INFO - 22
2025-10-24 16:08:00,227 - INFO - 55
2025-10-24 16:08:00,263 - WARNING - Could not load data for DPT 55 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/55_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F55_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/55_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:08:00,264 - INFO - 72
2025-10-24 16:08:02,590 - INFO - 15
2025-10-24 16:08:04,984 - INFO - 62
2025-10-24 16:08:06,996 - INFO - 32
2025-10-24 16:08:08,921 - INFO - 77
2025-10-24 16:08:10,979 - INFO - 84
2025-10-24 16:08:13,553 - INFO - 89
2025-10-24 16:08:16,009 - INFO - 23
2025-10-24 16:08:18,411 - INFO - 60
2025-10-24 16:08:20,789 - INFO - 25
2025-10-24 16:08:23,893 - INFO - 87
2025-10-24 16:08:26,441 - INFO - 68
2025-10-24 16:08:29,052 - INFO - 56
2025-10-24 16:08:29,085 - WARNING - Could not load data for DPT 56 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/56_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F56_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/56_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:08:29,086 - INFO - 92
2025-10-24 16:08:33,643 - INFO - 61
2025-10-24 16:08:36,884 - INFO - 2
2025-10-24 16:08:39,864 - INFO - 40
2025-10-24 16:08:43,168 - INFO - 86
2025-10-24 16:08:46,434 - INFO - 88
2025-10-24 16:08:49,993 - INFO - 26
2025-10-24 16:08:53,158 - INFO - 17
2025-10-24 16:08:56,895 - INFO - 24
2025-10-24 16:09:00,742 - INFO - 34
2025-10-24 16:09:04,981 - INFO - 52
2025-10-24 16:09:05,024 - WARNING - Could not load data for DPT 52 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/52_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F52_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/52_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,025 - INFO - 64
2025-10-24 16:09:05,060 - WARNING - Could not load data for DPT 64 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/64_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F64_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/64_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,061 - INFO - 73
2025-10-24 16:09:05,100 - WARNING - Could not load data for DPT 73 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/73_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F73_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/73_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,101 - INFO - 21
2025-10-24 16:09:05,142 - WARNING - Could not load data for DPT 21 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/21_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F21_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/21_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,143 - INFO - 35
2025-10-24 16:09:05,185 - WARNING - Could not load data for DPT 35 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/35_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F35_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/35_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,186 - INFO - 58
2025-10-24 16:09:05,220 - WARNING - Could not load data for DPT 58 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/58_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F58_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/58_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,221 - INFO - 83
2025-10-24 16:09:05,258 - WARNING - Could not load data for DPT 83 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/83_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F83_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/83_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,259 - INFO - 94
2025-10-24 16:09:05,294 - WARNING - Could not load data for DPT 94 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/94_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F94_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/94_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,295 - INFO - 63
2025-10-24 16:09:05,331 - WARNING - Could not load data for DPT 63 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/63_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F63_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/63_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,332 - INFO - 38
2025-10-24 16:09:05,366 - WARNING - Could not load data for DPT 38 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/38_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F38_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/38_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,368 - INFO - 18
2025-10-24 16:09:05,406 - WARNING - Could not load data for DPT 18 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/18_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F18_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/18_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,407 - INFO - 29
2025-10-24 16:09:05,443 - WARNING - Could not load data for DPT 29 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/29_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F29_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/29_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,444 - INFO - 19
2025-10-24 16:09:05,479 - WARNING - Could not load data for DPT 19 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/19_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F19_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/19_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,480 - INFO - 31
2025-10-24 16:09:05,518 - WARNING - Could not load data for DPT 31 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/31_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F31_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/31_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,519 - INFO - 53
2025-10-24 16:09:05,560 - WARNING - Could not load data for DPT 53 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/53_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F53_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/53_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,561 - INFO - 45
2025-10-24 16:09:05,601 - WARNING - Could not load data for DPT 45 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/45_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F45_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/45_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,602 - INFO - 50
2025-10-24 16:09:05,640 - WARNING - Could not load data for DPT 50 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/50_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F50_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/50_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,641 - INFO - 81
2025-10-24 16:09:05,674 - WARNING - Could not load data for DPT 81 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/81_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F81_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/81_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,675 - INFO - 82
2025-10-24 16:09:05,712 - WARNING - Could not load data for DPT 82 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/82_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F82_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/82_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,713 - INFO - 90
2025-10-24 16:09:05,751 - WARNING - Could not load data for DPT 90 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/90_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F90_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/90_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,752 - INFO - 91
2025-10-24 16:09:05,783 - WARNING - Could not load data for DPT 91 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/91_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F91_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/91_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,784 - INFO - 54
2025-10-24 16:09:05,821 - WARNING - Could not load data for DPT 54 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/54_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F54_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/54_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,822 - INFO - 95
2025-10-24 16:09:05,860 - WARNING - Could not load data for DPT 95 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/95_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F95_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/95_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,861 - INFO - 93
2025-10-24 16:09:05,892 - WARNING - Could not load data for DPT 93 from 01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/93_760_monthly_series.csv. Error: 404 GET https://storage.googleapis.com/download/storage/v1/b/dev-cainz-demandforecast/o/01_short_term%2F01_stage1_result%2F02_monthly%2F2025-10-25-6%2F760%2F93_760_monthly_series.csv?alt=media: No such object: dev-cainz-demandforecast/01_short_term/01_stage1_result/02_monthly/2025-10-25-6/760/93_760_monthly_series.csv: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>). Skipping.
2025-10-24 16:09:05,894 - INFO - tran len 3450215
###############sales_df###################                 PRD_CD                  prd_nm_kj      hnmk_cd cls_cd line_cd  \
0        4549509798071            ◆カーペット　１畳　ＭＯ／ＧＹ  20024550000   6900     690   
1        4549509798071            ◆カーペット　１畳　ＭＯ／ＧＹ  20024550000   6900     690   
2        4549509798071            ◆カーペット　１畳　ＭＯ／ＧＹ  20024550000   6900     690   
3        4549509798071            ◆カーペット　１畳　ＭＯ／ＧＹ  20024550000   6900     690   
4        4549509798071            ◆カーペット　１畳　ＭＯ／ＧＹ  20024550000   6900     690   
...                ...                        ...          ...    ...     ...   
3450210  4549509844235  スマイリア　チキン　７歳以上用　お試し用　１００ｇ  20021480000   3402     340   
3450211  4549509844235  スマイリア　チキン　７歳以上用　お試し用　１００ｇ  20021480000   3402     340   
3450212  4549509844235  スマイリア　チキン　７歳以上用　お試し用　１００ｇ  20021480000   3402     340   
3450213  4549509844235  スマイリア　チキン　７歳以上用　お試し用　１００ｇ  20021480000   3402     340   
3450214  4549509844235  スマイリア　チキン　７歳以上用　お試し用　１００ｇ  20021480000   3402     340   

         shoki_genka shoki_baika  genka_toitsu baika_toitsu low_price_kbn  \
0              496.0         980         565.0         1480             0   
1              496.0         980         565.0         1480             0   
2              496.0         980         565.0         1480             0   
3              496.0         980         565.0         1480             0   
4              496.0         980         565.0         1480             0   
...              ...         ...           ...          ...           ...   
3450210        125.0         198         125.0          248             1   
3450211        125.0         198         125.0          248             1   
3450212        125.0         198         125.0          248             1   
3450213        125.0         198         125.0          248             1   
3450214        125.0         198         125.0          248             1   

        TENPO_CD DPT_CD nenshudo  URI_SU  URI_KIN   BAIKA sell_start_ymd  \
0            760     69   202226     1.0   1164.0  1280.0       20231125   
1            760     69   202232     1.0   1164.0  1280.0       20231125   
2            760     69   202237     2.0   2328.0  1280.0       20231125   
3            760     69   202238     1.0   1346.0  1480.0       20231125   
4            760     69   202242     1.0   1346.0  1480.0       20231125   
...          ...    ...      ...     ...      ...     ...            ...   
3450210      760     34   202528     1.0    226.0   248.0       20231215   
3450211      760     34   202529     1.0    226.0   248.0       20231215   
3450212      760     34   202532    12.0   1353.0   248.0       20231215   
3450213      760     34   202533     2.0    226.0   248.0       20231215   
3450214      760     34   202534     6.0    677.0   248.0       20231215   

        hacchu_end_ymd          cls_nm  DPT  
0             99999999       ひらおりカーペット   69  
1             99999999       ひらおりカーペット   69  
2             99999999       ひらおりカーペット   69  
3             99999999       ひらおりカーペット   69  
4             99999999       ひらおりカーペット   69  
...                ...             ...  ...  
3450210       99999999  ドッグ・ハイプレミアムフード   34  
3450211       99999999  ドッグ・ハイプレミアムフード   34  
3450212       99999999  ドッグ・ハイプレミアムフード   34  
3450213       99999999  ドッグ・ハイプレミアムフード   34  
3450214       99999999  ドッグ・ハイプレミアムフード   34  

[3450215 rows x 20 columns]
2025-10-24 16:09:06,131 - INFO - SKU 33583
2025-10-24 16:09:06,824 - INFO - sales_df SKU: 33583
2025-10-24 16:10:05,677 - INFO - minmax_df.shape: (1964062, 15)
2025-10-24 16:10:30,978 - INFO - restrict minmax sales_df SKU: 30580
2025-10-24 16:11:20,605 - INFO - exclude store hacchuend sales_df SKU: 27613
########### sales_df#############                 PRD_CD nenshudo  URI_SU TENPO_CD baika_toitsu   BAIKA  DPT  \
0        4549509798071   202226     1.0      760         1480  1280.0   69   
1        4549509798071   202232     1.0      760         1480  1280.0   69   
2        4549509798071   202237     2.0      760         1480  1280.0   69   
3        4549509798071   202238     1.0      760         1480  1480.0   69   
4        4549509798071   202242     1.0      760         1480  1480.0   69   
...                ...      ...     ...      ...          ...     ...  ...   
2958503  4549509844235   202528     1.0      760          248   248.0   34   
2958504  4549509844235   202529     1.0      760          248   248.0   34   
2958505  4549509844235   202532    12.0      760          248   248.0   34   
2958506  4549509844235   202533     2.0      760          248   248.0   34   
2958507  4549509844235   202534     6.0      760          248   248.0   34   

        line_cd cls_cd      hnmk_cd  
0           690   6900  20024550000  
1           690   6900  20024550000  
2           690   6900  20024550000  
3           690   6900  20024550000  
4           690   6900  20024550000  
...         ...    ...          ...  
2958503     340   3402  20021480000  
2958504     340   3402  20021480000  
2958505     340   3402  20021480000  
2958506     340   3402  20021480000  
2958507     340   3402  20021480000  

[2958508 rows x 10 columns]
2025-10-24 16:11:21,009 - INFO - train_end_nenshudo 202529
2025-10-24 16:11:21,010 - INFO - target_nenshudo: 202530
2025-10-24 16:11:21,010 - INFO - start_nenshudo: 202510
2025-10-24 16:15:20,021 - INFO - ==============週開始日付_予測対象================
2025-10-24 16:15:23,175 - INFO - Starting sales data preprocessing.
2025-10-24 16:15:23,176 - INFO - Sales data preprocessing complete.
############sales_df###############                 PRD_CD nenshudo  URI_SU TENPO_CD baika_toitsu   BAIKA  DPT  \
0        4549509798071   202226     1.0      760         1480  1280.0   69   
1        4549509798071   202232     1.0      760         1480  1280.0   69   
2        4549509798071   202237     2.0      760         1480  1280.0   69   
3        4549509798071   202238     1.0      760         1480  1480.0   69   
4        4549509798071   202242     1.0      760         1480  1480.0   69   
...                ...      ...     ...      ...          ...     ...  ...   
2958278  4549509844235   202528     1.0      760          248   248.0   34   
2958279  4549509844235   202529     1.0      760          248   248.0   34   
2958280  4549509844235   202532    12.0      760          248   248.0   34   
2958281  4549509844235   202533     2.0      760          248   248.0   34   
2958282  4549509844235   202534     6.0      760          248   248.0   34   

        line_cd cls_cd      hnmk_cd  
0           690   6900  20024550000  
1           690   6900  20024550000  
2           690   6900  20024550000  
3           690   6900  20024550000  
4           690   6900  20024550000  
...         ...    ...          ...  
2958278     340   3402  20021480000  
2958279     340   3402  20021480000  
2958280     340   3402  20021480000  
2958281     340   3402  20021480000  
2958282     340   3402  20021480000  

[2958283 rows x 10 columns]
2025-10-24 16:15:30,535 - INFO - ===df_odas_calender_new===       TENPO_CD         PRD_CD  odas_amount  sales_ymd  nenshudo
0        760.0  4550596031668         21.0   20221027    202235
1        760.0  4965888050263          5.0   20221027    202235
2        760.0  4550596031736          0.0   20221027    202235
3        760.0  4965888050300          1.0   20221027    202235
4        760.0  4935773523011          2.0   20221027    202235
...        ...            ...          ...        ...       ...
4010     760.0  4984931248297          1.0   20251019    202534
4011     760.0  4549509909408          1.0   20251014    202534
4012     760.0  4549509167020          1.0   20251017    202534
4013     760.0    49074025182          1.0   20251014    202534
4014     760.0  4549509167013          1.0   20251018    202534

[4015 rows x 5 columns]
2025-10-24 16:15:35,379 - INFO - start odas correction improvements *******************************
2025-10-24 16:20:32,912 - INFO - processing odas correction improvements *******************************
2025-10-24 16:20:32,913 - INFO - odas correction elapsed time: 297.534 seconds
2025-10-24 16:27:10,084 - INFO - end odas Correction improvements *******************************
2025-10-24 16:27:10,085 - INFO - odas correction elapsed time: 694.706 seconds
############sales_df###############                 PRD_CD nenshudo  URI_SU TENPO_CD baika_toitsu   BAIKA  DPT  \
0        4549509798071   202226     1.0      760         1480  1280.0   69   
1        4549509798071   202232     1.0      760         1480  1280.0   69   
2        4549509798071   202237     2.0      760         1480  1280.0   69   
3        4549509798071   202238     1.0      760         1480  1480.0   69   
4        4549509798071   202242     1.0      760         1480  1480.0   69   
...                ...      ...     ...      ...          ...     ...  ...   
2958278  4549509844235   202528     1.0      760          248   248.0   34   
2958279  4549509844235   202529     1.0      760          248   248.0   34   
2958280  4549509844235   202532    12.0      760          248   248.0   34   
2958281  4549509844235   202533     2.0      760          248   248.0   34   
2958282  4549509844235   202534     6.0      760          248   248.0   34   

        line_cd cls_cd      hnmk_cd  odas_amount  URI_SU_NEW  URI_SU_NEW_org  \
0           690   6900  20024550000          0.0         1.0             1.0   
1           690   6900  20024550000          0.0         1.0             1.0   
2           690   6900  20024550000          0.0         2.0             2.0   
3           690   6900  20024550000          0.0         1.0             1.0   
4           690   6900  20024550000          0.0         1.0             1.0   
...         ...    ...          ...          ...         ...             ...   
2958278     340   3402  20021480000          0.0         1.0             1.0   
2958279     340   3402  20021480000          0.0         1.0             1.0   
2958280     340   3402  20021480000          0.0        12.0            12.0   
2958281     340   3402  20021480000          0.0         2.0             2.0   
2958282     340   3402  20021480000          0.0         6.0             6.0   

        URI_SU_NEW_OVER0_IMPLVMNT_TYPE  URI_SU_NEW_OVER0_IMPLVMNT_bk  
0                                  NaN                           1.0  
1                                  NaN                           1.0  
2                                  NaN                           2.0  
3                                  NaN                           1.0  
4                                  NaN                           1.0  
...                                ...                           ...  
2958278                            NaN                           1.0  
2958279                            NaN                           1.0  
2958280                            NaN                          12.0  
2958281                            NaN                           2.0  
2958282                            NaN                           6.0  

[2958283 rows x 15 columns]
2025-10-24 16:32:55,258 - extract_as_df - INFO - start
2025-10-24 16:32:55,530 - extract_as_df - INFO - end elapsed time: 0.2714507579803467
#############df_vx_test############                  PrdCd  WeekStartDate  PreviousYearSalesActualQuantity  \
0        4550596040417       20201221                              2.0   
1        4971544121123       20201221                              0.0   
2        4549509628255       20201221                              4.0   
3        4547204276528       20201221                              0.0   
4        4987244196798       20201221                              3.0   
...                ...            ...                              ...   
7124149  4979874110695       20251124                              0.0   
7124150  4936695979238       20251124                              0.0   
7124151  4979874110688       20251124                              0.0   
7124152  4975364054227       20251124                              2.0   
7124153  4902757466107       20251124                              0.0   

         time_leap8  SalesAmount  SalesAmount8ema  baika_toitsu        BAIKA  \
0          1.552025          2.0         1.817189         798.0   598.000000   
1          0.000000          0.0         0.000000        3780.0  3480.000000   
2          6.736326          6.0         3.192187         798.0   698.000000   
3          0.000000          0.0         0.000000        1980.0  1580.000000   
4          1.429167          0.0         1.388848         148.0   101.474359   
...             ...          ...              ...           ...          ...   
7124149    0.236087          0.0              NaN         698.0   698.000000   
7124150    0.161605          0.0              NaN         328.0   328.000000   
7124151    0.226485          0.0              NaN         698.0   698.000000   
7124152    1.006122          0.0              NaN        5280.0  5280.000000   
7124153    0.500327          0.0              NaN         498.0   498.000000   

         DPT  line_cd  cls_cd      hnmk_cd weekstartdatestamp  training_weight  
0         68      680    6801  23008420000         2020-12-21      3200.000000  
1         25      251    2518   9000012526         2020-12-21      3200.000000  
2         69      692    6941  19017240200         2020-12-21      3200.000000  
3         27      271    2751  99999992751         2020-12-21      3200.000000  
4         85      851    8511  99408018511         2020-12-21      3200.000000  
...      ...      ...     ...          ...                ...              ...  
7124149   75      753    7538  99566277538         2025-11-24     15133.333333  
7124150   15      152    1522   8001280100         2025-11-24     15133.333333  
7124151   75      753    7538  99566277538         2025-11-24     15133.333333  
7124152   92      922    9226  99999999226         2025-11-24     15133.333333  
7124153   74      743    7432  99810367432         2025-11-24     15133.333333  

[7124154 rows x 14 columns]
#################df_vx_test############                  PrdCd  WeekStartDate  PreviousYearSalesActualQuantity  \
0        4550596040417       20201221                              2.0   
1        4971544121123       20201221                              0.0   
2        4549509628255       20201221                              4.0   
3        4547204276528       20201221                              0.0   
4        4987244196798       20201221                              3.0   
...                ...            ...                              ...   
7124149  4979874110695       20251124                              0.0   
7124150  4936695979238       20251124                              0.0   
7124151  4979874110688       20251124                              0.0   
7124152  4975364054227       20251124                              2.0   
7124153  4902757466107       20251124                              0.0   

         time_leap8  SalesAmount  SalesAmount8ema  baika_toitsu        BAIKA  \
0          1.552025          2.0         1.817189         798.0   598.000000   
1          0.000000          0.0         0.000000        3780.0  3480.000000   
2          6.736326          6.0         3.192187         798.0   698.000000   
3          0.000000          0.0         0.000000        1980.0  1580.000000   
4          1.429167          0.0         1.388848         148.0   101.474359   
...             ...          ...              ...           ...          ...   
7124149    0.236087          0.0              NaN         698.0   698.000000   
7124150    0.161605          0.0              NaN         328.0   328.000000   
7124151    0.226485          0.0              NaN         698.0   698.000000   
7124152    1.006122          0.0              NaN        5280.0  5280.000000   
7124153    0.500327          0.0              NaN         498.0   498.000000   

         DPT  line_cd  cls_cd      hnmk_cd weekstartdatestamp  \
0         68      680    6801  23008420000         2020-12-21   
1         25      251    2518   9000012526         2020-12-21   
2         69      692    6941  19017240200         2020-12-21   
3         27      271    2751  99999992751         2020-12-21   
4         85      851    8511  99408018511         2020-12-21   
...      ...      ...     ...          ...                ...   
7124149   75      753    7538  99566277538         2025-11-24   
7124150   15      152    1522   8001280100         2025-11-24   
7124151   75      753    7538  99566277538         2025-11-24   
7124152   92      922    9226  99999999226         2025-11-24   
7124153   74      743    7432  99810367432         2025-11-24   

         training_weight hierarchical_model_id qty_model_type  tenpo_cd  \
0            3200.000000     760_not_small_qty          large       760   
1            3200.000000         760_small_qty          small       760   
2            3200.000000     760_not_small_qty          large       760   
3            3200.000000     760_not_small_qty         middle       760   
4            3200.000000     760_not_small_qty          large       760   
...                  ...                   ...            ...       ...   
7124149     15133.333333     760_not_small_qty         middle       760   
7124150     15133.333333     760_not_small_qty         middle       760   
7124151     15133.333333     760_not_small_qty         middle       760   
7124152     15133.333333         760_small_qty         middle       760   
7124153     15133.333333     760_not_small_qty         middle       760   

              TenpoCdPrdCd  BusyPeriodFlagNenmatsu  BusyPeriodFlagGw  \
0        760_4550596040417                       0                 0   
1        760_4971544121123                       0                 0   
2        760_4549509628255                       0                 0   
3        760_4547204276528                       0                 0   
4        760_4987244196798                       0                 0   
...                    ...                     ...               ...   
7124149  760_4979874110695                       0                 0   
7124150  760_4936695979238                       0                 0   
7124151  760_4979874110688                       0                 0   
7124152  760_4975364054227                       0                 0   
7124153  760_4902757466107                       0                 0   

         BusyPeriodFlagNewLife  BusyPeriodFlagEndRainySsn  BusyPeriodFlagObon  
0                            0                          0                   0  
1                            0                          0                   0  
2                            0                          0                   0  
3                            0                          0                   0  
4                            0                          0                   0  
...                        ...                        ...                 ...  
7124149                      0                          0                   0  
7124150                      0                          0                   0  
7124151                      0                          0                   0  
7124152                      0                          0                   0  
7124153                      0                          0                   0  

[7124154 rows x 23 columns]
2025-10-24 16:56:03,983 - INFO - data upload start
INFO:__main__:data upload start
2025-10-24 16:56:06,389 - INFO - ==data-uploaded-bq hieralchical model ===
INFO:__main__:==data-uploaded-bq hieralchical model ===
2025-10-24 16:56:09,051 - INFO - ==data-uploaded-bq===
INFO:__main__:==data-uploaded-bq===
2025-10-24 16:56:09,053 - INFO - Elapsed time: 2902.691 seconds
INFO:__main__:Elapsed time: 2902.691 seconds
2025-10-24 16:56:09,057 - INFO - Stage completed
INFO:__main__:Stage completed













########################################This is test case file stage 2 weekly pipeline
import pytest
from unittest.mock import patch, MagicMock
from io import BytesIO
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
patcher = patch("google.cloud.storage.Client", return_value=MagicMock())
patcher.start()
from main import main
from repos.cainz_demand_forecast.cainz.common import common
from repos.cainz_demand_forecast.cainz.short_term import short_term_preprocess_common
@pytest.fixture
def mock_calendar_df():
   today = datetime.now()
   week_from_ymd = (today - timedelta(days=2)).strftime('%Y-%m-%d')
   week_to_ymd = (today + timedelta(days=2)).strftime('%Y-%m-%d')
   return pd.DataFrame({
       'nenshudo': [202530, 202525, 202524, 202505, 202605],
       'nendo': [2025, 2025, 2025, 2025, 2025],
       'shudo': [30, 30, 30, 30, 30],
       'minashi_tsuki': [3, 3, 3, 3, 3],
       'week_from_ymd': [week_from_ymd, week_from_ymd, week_from_ymd, week_from_ymd, week_from_ymd],
       'week_to_ymd': [week_to_ymd, week_to_ymd, week_to_ymd, week_to_ymd, week_to_ymd],
   })

@pytest.fixture
def mock_calendar_df_extract():
   today = datetime.now()
   week_from_ymd = (today - timedelta(days=2)).strftime('%Y-%m-%d')
   week_to_ymd = (today + timedelta(days=2)).strftime('%Y-%m-%d')
   return pd.DataFrame({
       
       'nenshudo': [202530, 202525, 202524, 202505, 202605],
       'nendo': [2025, 2025, 2025, 2025, 2025],
       'shudo': [30, 30, 30, 30, 30],
       'minashi_tsuki': [3, 3, 3, 3, 3],
       'week_from_ymd': [week_from_ymd, week_from_ymd, week_from_ymd, week_from_ymd, week_from_ymd],
       'week_to_ymd': [week_to_ymd, week_to_ymd, week_to_ymd, week_to_ymd, week_to_ymd],
       "znen_week_from_ymd": [20020225, 20020304, 20020311, 20020318, 20020325], 
       "znen_week_to_ymd": [20020303, 20020310, 20020317, 20020324, 20020331], 
       'znen_nendo': [2002, 2002, 2002, 2002, 2002], 
       'znen_shudo': [1, 2, 3, 4, 5],
       
   })


@pytest.fixture
def mock_sales_df():
   return pd.DataFrame({
       "PRD_CD": [4550596138817, 4550596144238],
       "TENPO_CD": [760, 760],
       "cls_cd": [6922, 9727],
       "line_cd": [692, 972],
       "hnmk_cd": [24008460001, 24011030000],
       "baika_toitsu": [1980, 4980],
       "URI_SU": [1, 3],
       "nenshudo": [202530, 202529],
       "BAIKA": [1980, 4980],
       "DPT": [69, 97],
   })


@pytest.fixture
def mock_df_vx_test():
   return pd.DataFrame({
    "PrdCd": [4549509833536, 45495096138763],
    "WeekStartDate": [20170227, 20170227],
    "PreviousYearSalesActualQuantity": [0.0, 0.0],
    "PreviousYearEcSalesActualQuantity": [0.0, 0.0],
    "PreviousYearClassSalesActualQuantity": [0.0, 0.0],
    "SalesAmount": [0.0, 0.0],
    "SalesAmountEC": [None, None],
    "SalesAmountCLASS": [None, 2.0],
    "baika_toitsu": [1980.0,  698.0],
    "DPT": [69, 69], 
    "line_cd": [692, 692],
    "hnmk_cd": [24008460001, 24008460001], 
    "tenpo_cd": [760, 760],
    "weekstartdatestamp": ["2017-02-27", "2017-02-27"]})


@pytest.fixture
def mock_updated_sales_df():
   return pd.DataFrame({
       "PRD_CD": [4550596138817, 4550596144238],
       "TENPO_CD": [760, 760],
       "cls_cd": [6922, 9727],
       "line_cd": [692, 972],
       "hnmk_cd": [24008460001, 24011030000],
       "baika_toitsu": [1980, 4980],
       "URI_SU": [1, 3],
       "nenshudo": [202530, 202529],
       "BAIKA": [1980, 4980],
       "DPT": [69, 97],
       "URI_SU_EC": [0.0, 0.0]
   })


@pytest.fixture
def mock_odas_df():
   return pd.DataFrame({
       "PRD_CD": [200001, 200002],
       "TENPO_CD": [760, 760],
       "odas_amount": [5, 15],
       "nenshudo": [202530, 202529],
       "sales_ymd": [20240101, 20240201]
   })


@pytest.fixture
def mock_reference_store_df():
   # Use columns your pipeline expects from reference store
   return pd.DataFrame({
       "STORE": [1, 2],
       "STORE_REF": [10, 20],
       "OPEN_DATE": ["2021-01-01", "2021-01-02"],
       "OPEN_DATE_REF": ["2020-01-01", "2020-01-02"]
   })


@pytest.fixture
def mock_config():
   return {
       'path_week_master': 'dummy_path_week_master.csv',
       'bucket_name': 'dummy-bucket',
       'restrict_minmax': False,
       'restrinct_tenpo_hacchu_end': False,
       'output_6wk_2sales': False,
       'add_reference_store': True,   # <-- True so reference_store logic runs!
       'today_date_str': datetime.now().strftime('%Y-%m-%d'),
   }


def get_bytes_from_df(df):
   return df.to_csv(index=False).encode("utf-8")


def make_blob_mock_for(csv_bytes):
   blob = MagicMock()
   blob.open.return_value = BytesIO(csv_bytes)
   blob.download_as_bytes.return_value = csv_bytes
   return blob


def make_storage_client_mock(blob_mapping):
   mock_storage_client = MagicMock()
   mock_bucket = MagicMock()
   def get_blob(name):
       return blob_mapping.get(name, MagicMock())
   mock_bucket.blob.side_effect = get_blob
   mock_storage_client.bucket.return_value = mock_bucket
   return mock_storage_client


def test_main_stage2_weekly_flow(
   mock_calendar_df,
   mock_calendar_df_extract,
   mock_odas_df,
   mock_sales_df,
   mock_df_vx_test, 
   mock_reference_store_df,
   mock_updated_sales_df,
   mock_config
):
   # bytes for each GCS CSV used in your pipeline
   calendar_csv_bytes = get_bytes_from_df(mock_calendar_df)
   calendar_extract_csv_bytes = get_bytes_from_df(mock_calendar_df_extract)
   reference_csv_bytes = get_bytes_from_df(mock_reference_store_df)
   blob_mapping = {
       # Match the names your main asks for
       "Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv": make_blob_mock_for(calendar_csv_bytes),
       "Basic_Analysis_unzip_result/01_Data/37_reference_store/reference_store.csv": make_blob_mock_for(reference_csv_bytes),
       "Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv": make_blob_mock_for(calendar_extract_csv_bytes),
       
   }
   mock_storage_client = make_storage_client_mock(blob_mapping)
   with \
       patch('google.cloud.storage.Client', return_value=mock_storage_client), \
       patch('yaml.safe_load', return_value=mock_config), \
       patch('main.common.extract_as_df', return_value=mock_calendar_df) as mock_extract_as_df, \
       patch('main.extract_as_df_with_encoding', return_value=mock_calendar_df_extract) as mock_extract_as_df_with_encoding, \
       patch('main.apply_minmax_restrictions', return_value=(mock_updated_sales_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())) as mock_apply_minmax_restrictions, \
       patch('main.odas_correct', return_value=mock_odas_df) as mock_odas_correct, \
       patch('main.process_sales_data', return_value=(mock_sales_df, [])) as mock_process_sales_data, \
       patch('main.process_sales_data2', return_value=([], [])) as mock_process_sales_data2, \
       patch('main.prepare_vx_test_data', return_value= []) as mock_prepare_vx_test_data, \
       patch('main.process_kakaku_jizen_kichi_data', return_value=(mock_df_vx_test, [], [], [])) as mock_process_kakaku_jizen_kichi_data, \
       patch('main.upload_collected_sales_data') as mock_upload_collected_sales_data, \
       patch('main.process_sales_data_division', return_value=([], [], [])) as mock_process_sales_data_division, \
       patch('main.manage_cloud_function_trigger', return_value=True) as mock_manage_cloud_function_trigger:
       main()
       mock_extract_as_df.assert_called()
       mock_extract_as_df_with_encoding.assert_called()
       mock_apply_minmax_restrictions.assert_called()
       mock_odas_correct.assert_called()
       mock_process_sales_data.assert_called()
       mock_process_sales_data2.assert_called()
       mock_prepare_vx_test_data.assert_called()
       mock_process_kakaku_jizen_kichi_data.assert_called()
       mock_upload_collected_sales_data.assert_called()
       mock_process_sales_data_division.assert_called()
       mock_manage_cloud_function_trigger.assert_called()
        
def teardown_module(module):
   patcher.stop()



