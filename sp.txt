
# -*- coding: utf-8 -*-

import bisect
from google.cloud import storage
from pandas import read_csv
import pandas as pd
import numpy as np
import warnings
import sys
import datetime
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
#import swifter
import statsmodels.api as st

from io import BytesIO
import scipy
from scipy import stats

warnings.filterwarnings("ignore")

project_id = "dev-cainz-demandforecast"

def get_prd_bunrui():
    bunrui_table_id = 'M00_PRD_BUNRUI'
    bunrui_query = f"""
    SELECT PRD_CD, CLS_CD, CLS_NM, DPT_CD, DPT_NM
    FROM {dataset_id}.{bunrui_table_id}
    """

    df_bunrui = pd.read_gbq(bunrui_query, project_id, dialect='standard')
    print(df_bunrui)
    return df_bunrui


def calc_incriment_nenshudo(value_nenshudo):
    str_value_nenshudo = str(value_nenshudo)
    value_nenshudo_part = str_value_nenshudo[5:8]
    print("====value_nenshudo====",value_nenshudo_part)

    return value_nenshudo

def get_last_year_sales_amount(df):
#    df[["time_leep8"]] = 0
    df_tmp = df[["商品コード","前年週開始日付","8週平均ema"]]
    df_tmp = df_tmp.rename(columns={'8週平均ema': 'time_leap8'})
    df_tmp2 = pd.merge(df,df_tmp,on = ["商品コード","前年週開始日付"])

    return df_tmp2


def get_last_year_sales_amount_left(df):
#    df[["time_leep8"]] = 0
    df_tmp = df[["商品コード","前年週開始日付","8週平均ema"]]
    df_tmp = df_tmp.rename(columns={'8週平均ema': 'time_leap8'})
    df_tmp2 = pd.merge(df,df_tmp,on = ["商品コード","前年週開始日付"], how='left')
    
    df_tmp2['time_leap8'] = df_tmp2['time_leap8'].fillna(0)

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
    print(df_bunrui)
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

def interpolate_df(sales_df, df_calendar, add_ec_salesamount):
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

    if add_ec_salesamount:
        merged_sales_df = pd.merge(df_calendar_tmp, sales_df[["key", "URI_SU", "URI_SU_EC", "TENPO_CD", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']], how="left", on="key")
        merged_sales_df['URI_SU'] = merged_sales_df['URI_SU'].fillna(0)
        merged_sales_df['URI_SU_EC'] = merged_sales_df['URI_SU_EC'].fillna(0)
    else:
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

def odas_correct(df_calendar, tenpo_cd, use_jan_connect):
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

        print(tenpo_cd, ' df_odas_old shape:', df_odas_old.shape)
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
        
        print('***df_odas_new 0 shape:', df_odas_new.shape)
        # ここはtenpo_cdをint型にしなくてよいか？　差し替え前データの店舗コードは文字列("""")なのでよいはず
        df_odas_new = df_odas_new[df_odas_new['tenpo_cd']==int(tenpo_cd)].reset_index(drop=True)

        print('***df_odas_new 1 shape:', df_odas_new.shape)
        
        df_odas_new['prd_cd_len'] = df_odas_new['prd_cd'].apply(lambda x:len(x))
        df_odas_new = df_odas_new[df_odas_new['prd_cd_len']<=13]
        
        print('***df_odas_new 2 shape:', df_odas_new.shape)

        df_odas_new['prd_cd_isnumelic'] = df_odas_new['prd_cd'].apply(lambda x:x.isnumeric())
        df_odas_new = df_odas_new[df_odas_new['prd_cd_isnumelic']==True]
        
        print('***df_odas_new 3 shape:', df_odas_new.shape)

        df_odas_new['prd_cd'] = df_odas_new['prd_cd'].astype(int)
        
        print(tenpo_cd, ' df_odas_new shape:', df_odas_new.shape)
        
        #df_odas_new.to_csv('df_odas_new.csv')
    
    
    df_odas_new = df_odas_new[df_odas_new['tenpo_cd'] == int(tenpo_cd)].reset_index(drop=True)
    df_odas_new = df_odas_new.rename(columns={"tenpo_cd":"TENPO_CD", "prd_cd":"PRD_CD", "amount": "odas_amount","sales_date":"sales_ymd"})

    #df_odas_new.to_csv('df_odas_new.csv')
    
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
    print("===df_odas_calender_new===",df_odas_calender_new)
    
    odas_merge_df = pd.concat([df_odas_calender, df_odas_calender_new]).reset_index(drop=True)

    return odas_merge_df

def calc_nenshudo(my_nenshudo, offset):
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


def nenshudo2weekfromymd(my_nenshudo, my_df_calendar):
    for nenshudo, week_from_ymd in zip(my_df_calendar['nenshudo'], my_df_calendar['week_from_ymd']):
        if my_nenshudo == nenshudo:
            return week_from_ymd
    return None

def weekfromymd2nenshudo(my_week_from_ymd, my_df_calender):
    for nenshudo, week_from_ymd in zip(my_df_calendar['nenshudo'], my_df_calendar['week_from_ymd']):
        if my_week_from_ymd == week_from_ymd:
            return nenshudo
    return None

def ymd2nenshudo(ymd, my_df_calendar):
    prev = None
    for nenshudo, week_from_ymd in zip(my_df_calendar['nenshudo'], my_df_calendar['week_from_ymd']):
        if ymd == week_from_ymd:
            return nenshudo
        elif ymd > week_from_ymd:
            prev = nenshudo
        else:
            return prev
    return prev
    
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



cloudrunjob_mode = True

sys.path.append("repos/cainz_demand-forecast/cainz/")

from common import common

TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))
TODAY_OFFSET = int(os.environ.get("TODAY_OFFSET", 0))
OUTPUT_TABLE_SUFFIX = os.environ.get("OUTPUT_TABLE_SUFFIX", "")
CALL_NEXT_PIPELINE = int(os.environ.get("CALL_NEXT_PIPELINE", 0))
OUTPUT_COLLECTED_SALES_VALUE = int(os.environ.get("OUTPUT_COLLECTED_SALES_VALUE", 0))
OUTPUT_METRICS_VALUE = int(os.environ.get("OUTPUT_METRICS_VALUE", 0))
SEASONAL_TRAINDATA_TABLE = str(os.environ.get("SEASONAL_TRAINDATA_TABLE", ""))
TURN_BACK_YYYYMMDD = os.environ.get("TURN_BACK_YYYYMMDD", "")
THEME_MD_MODE = int(os.environ.get("THEME_MD_MODE", 0))

print('TASK_INDEX:', TASK_INDEX)
print('TASK_COUNT:', TASK_COUNT)
print('TODAY_OFFSET:', TODAY_OFFSET)
print('OUTPUT_TABLE_SUFFIX:', OUTPUT_TABLE_SUFFIX)
print('CALL_NEXT_PIPELINE:', CALL_NEXT_PIPELINE)
print('OUTPUT_METRICS_VALUE:', OUTPUT_METRICS_VALUE)
print('SEASONAL_TRAINDATA_TABLE:', SEASONAL_TRAINDATA_TABLE)
print('TURN_BACK_YYYYMMDD:', TURN_BACK_YYYYMMDD)
print('THEME_MD_MODE:', THEME_MD_MODE)

start_t = time.time()


'''
# 全店拡大20240926年末積み増し対応 239店舗(22店舗追加) + テーマMD（土鍋カセットボンベ　5店舗）　全244店舗
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 
50, 137, 154, 235, 613, 615, 617, 618, 623, 624, 741, 767, 794, 799, 808, 812, 842, 868, 875, 878, 903, 909,
51,52,908,910,932,
]
'''


# 全店拡大20250130 218店舗（古河は13週経過したので入れる）
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 910
]



if cloudrunjob_mode:
    # cloudrunjobで動かすモード
    tenpo_cd = tenpo_cd_list[TASK_INDEX]
    print('TASK_INDEX:', TASK_INDEX, 'tenpo_cd:', tenpo_cd)
    
    
    if (TASK_INDEX == 0) and (CALL_NEXT_PIPELINE == 1):
        # stage2完了チェックフォルダ配下のファイル削除
        storage_client = storage.Client()
        bucket_name = "dev-cainz-demandforecast"
        bucket = storage_client.bucket(bucket_name)

        blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_weekly_complete/completed_')
        for blob in blobs:
            print(blob.name)
            generation_match_precondition = None
            blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
            generation_match_precondition = blob.generation
            blob.delete(if_generation_match=generation_match_precondition)
            print(f"Blob {blob.name} deleted.")
    
else:
    # notebookで動かすモード
    tenpo_cd = int(sys.argv[1])
    print('tenpo_cd:', tenpo_cd)
    TODAY_OFFSET = 3
    print('TODAY_OFFSET:', TODAY_OFFSET) 
    OUTPUT_TABLE_SUFFIX = '_obon_218str'
    print('OUTPUT_TABLE_SUFFIX:', OUTPUT_TABLE_SUFFIX)
    OUTPUT_COLLECTED_SALES_VALUE = 0
    print('OUTPUT_COLLECTED_SALES_VALUE:', OUTPUT_TABLE_SUFFIX)
    OUTPUT_METRICS_VALUE = 0
    print('OUTPUT_METRICS_VALUE:', OUTPUT_TABLE_SUFFIX)
    SEASONAL_TRAINDATA_TABLE = '' #'weekly-train-seasonal-15-2024-04-09_seasonal_6tenpo'
    print('SEASONAL_TRAINDATA_TABLE:', SEASONAL_TRAINDATA_TABLE)
    TURN_BACK_YYYYMMDD = '' #'20241022' # ex:20230430
    print('TURN_BACK_YYYYMMDD:', TURN_BACK_YYYYMMDD)
    THEME_MD_MODE = 0
    print('THEME_MD_MODE:', THEME_MD_MODE)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)

## シーズン品に絞るかを指定
season_flag = False

if season_flag:
    with open('input_data/00_config_season.yaml') as file:
        config = yaml.safe_load(file.read())
else:
    with open('input_data/00_config_not_season.yaml') as file:
        config = yaml.safe_load(file.read())


model_name=config['model_name']

## 中央値特徴量の作成範囲（TSにおける派生ウィンドウと合わせる

# 必要なパスの取り出し
path_week_master = config['path_week_master']



# stage1の前日以前の過去データを使うフラグ*******
use_past_stage1_data_flag = True
#past_days_num = -1
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
    
print("******************* today ", today)
print("******************* today_date_str ", today_date_str)

    
    
my_date = today.strftime('%Y%m%d')
my_date = int(my_date)
print("******************* my_date ", my_date)


# フラグ設定 **********************************

# JAN差し替えファイルを使うかどうか指定する
use_jan_connect = True

# ODAS補正の改良を有効にする
odas_imprvmnt = True

# 目的変数を対数化
logarithmize_target_variable = False

# MinMaxにない商品、MinMax下限が0の商品を除く
restrict_minmax = True

# 店舗発注が終わっている商品を除く
restrinct_tenpo_hacchu_end = True



# 価格を事前に既知にする
kakaku_jizen_kichi = True

# 直近過去8週の補正ずみ売り数データをBQにアップする
#OUTPUT_COLLECTED_SALES_VALUE
output_collected_sales_value = True

# -----------------------------------
# 以下を全てFalseにすると予測距離は２０週となる

# 予測期間を5+15週から、5+5週にする(本番使用なし)
prediction_term_4week = False

# 予測期間を5+15週から、5+6週にする(本番使用中 2024 7/9より)
prediction_term_11week = True    ##### 年末積み増し対応やテーマMDのときはここをFalseにする 通常運用はTrueで　20240926

# 梅雨明けお盆予測用　予測距離18週
prediction_term_18week = False  # 通常運用はFalseで


# 予測期間を5+15週から、5+21週にする(テーマMD使用中)　# 通常運用はFalseで

prediction_term_26week = False

# -----------------------------------


# コンテキスト期間を変更する
contex_term_valiable = False
contex_term=4

# 評価指標データを作成する
make_metrics_data = True




# -----------------------------------


# モデル学習上の現在日時を巻き戻す
turn_back_time = False
#turn_back_yyyymmdd = 20230424
if turn_back_time:
    turn_back_yyyymmdd = int(TURN_BACK_YYYYMMDD)
    today_date_str = str(TURN_BACK_YYYYMMDD)
    print("******************* today_date_str_turnback ", today_date_str)

# 新店の参照店舗のデータを作成する
add_reference_store = True
# 新店と既存店を同じモデルにする
add_reference_store_unitedmodel = True


# モデルを販売数量で分割する
#divide_by_salesamount = False # こちらは使わないのでFalseでよい
divide_by_salesamount_v2 = True # テーマMDはOFF　20241008
# 売り数10の分割点を追加する
divide_by_salesamount_v3 = False


# 店舗出荷数量のテーブルから、ECの販売数の列を追加する
#`dev-cainz-demandforecast.dev_cainz_nssol.shipment_with_store_inventory`
add_ec_salesamount = True
# クラス波形を追加する(これをonにするときはEC販売数をonにすること)
class_wave_add = True
class_wave_mean_add = True

# **************************************************************************************
# 販売のない販売期間のweightを0にする（これは販売期間の無い期間を、ある期間の売り数で埋める処理・・・予測データでweightはいらない）
no_sales_term_weight_zero = False
# 1年実績あれば、それ以前の未販売期間を補間する
interpolate_1yearsales = False
# 販売の無い期間をクラス波形で補完する
interporate_by_class_wave = False
# **************************************************************************************


# シーズン品のデータを出力する
devide_season_items = False


# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# 直近13週中6週実績あり、平均週販2以上の提案ありのデータを対象とする(モデル切り替え！！！) テーマMDではFalse
output_6wk_2sales = True
# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx


# 直近1か月のweightを大きくして、直近の売り数変化に追従できるようにする
# 予測開始ポイントを当週からにする
startpont_this_week = False
#last_month_weight_larger = True # (学習用の処理なのでコメントのみ記載)

# メトリックスの出力先をテスト用テーブルにする
output_metrics_test_tbl = False
# 補正済みデータの出力先をテストテーブルにする
output_collected_sales_value_test_table = False

# 繁忙期フラグ
salesup_flag = True

# *********************************************


col_lag = config['col_lag']
bucket_name = config['bucket_name']
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

col_id = '商品コード'
col_target = config["target"]
col_time = config["feature_timeline"]

print("loading 週番マスター")
dfc = common.extract_as_df(path_week_master, bucket_name)
df_calendar = extract_as_df_with_encoding("Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv","dev-cainz-demandforecast","utf-8")

#tenpo_cd = sys.argv[1]


# add 20230614********************************************
dfc_tmp = df_calendar[["nenshudo", "week_from_ymd", "week_to_ymd"]]
dfc_tmp["week_from_ymd"] = dfc_tmp["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
dfc_tmp["week_to_ymd"] = dfc_tmp["week_to_ymd"].apply(lambda x : pd.to_datetime(str(x)))

df_today_nenshudo  = dfc_tmp["nenshudo"][(dfc_tmp["week_from_ymd"] <= today_date_str)&(dfc_tmp["week_to_ymd"] >= today_date_str)]
today_nenshudo = df_today_nenshudo.values[0]

max_syudo_dic = dfc[['nendo', 'shudo']].groupby(['nendo']).max()['shudo'].to_dict()
# *********************************************************

if output_6wk_2sales:
    path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-62/'+str(tenpo_cd)+"/{}_{}_time_series.csv" 
else:
    path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_time_series.csv"


tenpo_cd_ref = None
path_tran_ref = None
if add_reference_store:
    # 参照店舗の紐づけ情報を読み込む
    path_reference_store = "Basic_Analysis_unzip_result/01_Data/37_reference_store/reference_store.csv"
    reference_store_df = extract_as_df(path_reference_store, bucket_name)
    reference_store_df["OPEN_DATE"] = reference_store_df["OPEN_DATE"].apply(lambda x : pd.to_datetime(str(x)))
    reference_store_df["OPEN_DATE_REF"] = reference_store_df["OPEN_DATE_REF"].apply(lambda x : pd.to_datetime(str(x)))
    
    newstore_refstore_dict = dict(zip(reference_store_df['STORE'], reference_store_df['STORE_REF']))
    newstore_opendate_dict = dict(zip(reference_store_df['STORE'], reference_store_df['OPEN_DATE']))
    
    # 参照店舗の有無をチェックして、あればpath_tran_refを設定する
    if tenpo_cd in newstore_refstore_dict:
        tenpo_cd_ref = newstore_refstore_dict[tenpo_cd]
        print('参照店舗:', tenpo_cd_ref)      
        if output_6wk_2sales:
            path_tran_ref = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-62/'+str(tenpo_cd_ref)+"/{}_{}_time_series.csv"
        else:
            path_tran_ref = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd_ref)+"/{}_{}_time_series.csv"


if THEME_MD_MODE:
    theme_md_prdcd_list = []
    
    if 1:
        # dev-cainz-demandforecast/Basic_Analysis_unzip_result/01_Data/92_ADD_DATA_adhoc
        #path_target_prd_master = "Basic_Analysis_unzip_result/01_Data/92_ADD_DATA_adhoc/kaou_prd_master.csv"
        path_target_prd_master = "Basic_Analysis_unzip_result/01_Data/92_ADD_DATA_adhoc/kao_vmi_prdcd_list_20241129.csv"
        
        target_prd_masterdf = extract_as_df(path_target_prd_master, bucket_name)
        target_prd_masterdf['PRD_CD'] = target_prd_masterdf['PRD_CD'].astype(int)
        
        theme_md_prdcd_list = target_prd_masterdf['PRD_CD'].unique().tolist()
        this_tenpo_theme_md_prdcd_list = theme_md_prdcd_list
        
        print('theme_md_prdcd_list:', theme_md_prdcd_list)
    
    
    '''
    if 1:
        # 棚割りパターンコードから、テーマMDの店舗、商品コードを取得する
        tanawari_ptn_cd_list = [
            '094-990-187-02',
            '094-990-188-01',
            '094-990-189-01',
        ]
        
        
        path_ten_tana = "Basic_Analysis_unzip_result/01_Data/39_tanawari_ptn/30_T_090_TEN_TANA_PTN_SM.csv"
        ten_tana_df = extract_as_df(path_ten_tana, bucket_name)
        ten_tana_df['tenpo_cd'] = ten_tana_df['tenpo_cd'].astype(int)
        
        path_tanaptn_dtl = "Basic_Analysis_unzip_result/01_Data/39_tanawari_ptn/30_T_090_TANA_PTN_DTL_SM.csv"
        tanaptn_dtl_df = extract_as_df(path_tanaptn_dtl, bucket_name)
        tanaptn_dtl_df['prd_cd'] = tanaptn_dtl_df['prd_cd'].astype(int)
        
        theme_md_prdcd_list = []
        this_tenpo_theme_md_prdcd_list = []
        another_tenpo_theme_md_prdcd_list = []
        
        # 各棚割りパターンCDをチェックする
        for my_tanawari_ptn_cd in tanawari_ptn_cd_list:
            print('check tanawari_ptn_cd:', my_tanawari_ptn_cd)
            
            # 棚割りパターンの店舗コードリスト
            my_tanawari_ptn_cd_tenpocd_list = ten_tana_df[ten_tana_df['tanawari_cd']==my_tanawari_ptn_cd]['tenpo_cd'].tolist()
            # 棚割りパターンの商品コードリスト
            my_tanawari_ptn_cd_prdcd_list = tanaptn_dtl_df[tanaptn_dtl_df['tanawari_cd']==my_tanawari_ptn_cd]['prd_cd'].tolist()
            
            # この店舗は、棚割りパターン対象である
            if tenpo_cd in my_tanawari_ptn_cd_tenpocd_list:
                if len(my_tanawari_ptn_cd_prdcd_list) > 0:
                    this_tenpo_theme_md_prdcd_list = this_tenpo_theme_md_prdcd_list + my_tanawari_ptn_cd_prdcd_list
            else:
                if len(my_tanawari_ptn_cd_prdcd_list) > 0:
                    another_tenpo_theme_md_prdcd_list = another_tenpo_theme_md_prdcd_list + my_tanawari_ptn_cd_prdcd_list
        
        
        theme_md_prdcd_list = this_tenpo_theme_md_prdcd_list + another_tenpo_theme_md_prdcd_list
        print('this_tenpo_theme_md_prdcd_list:', this_tenpo_theme_md_prdcd_list)
        print('another_tenpo_theme_md_prdcd_list:', another_tenpo_theme_md_prdcd_list)
        print('theme_md_prdcd_list:', theme_md_prdcd_list)
        
        
        
        # 各棚割りパターンCDと店舗をファイル出力する
        if 0:
            tana_ten_prd_output_list = []
            for my_tanawari_ptn_cd in tanawari_ptn_cd_list:
                my_tanawari_ptn_cd_tenpocd_list = ten_tana_df[ten_tana_df['tanawari_cd']==my_tanawari_ptn_cd]['tenpo_cd'].tolist()
                my_tanawari_ptn_cd_prdcd_list = tanaptn_dtl_df[tanaptn_dtl_df['tanawari_cd']==my_tanawari_ptn_cd]['prd_cd'].tolist()

                for my_tenpo in my_tanawari_ptn_cd_tenpocd_list:
                    for my_prdcd in my_tanawari_ptn_cd_prdcd_list:
                        tana_ten_prd_output_list.append([my_tanawari_ptn_cd, my_tenpo, my_prdcd])
                        
            tana_ten_prd_output_list_df = pd.DataFrame(tana_ten_prd_output_list)
            tana_ten_prd_output_list_df.columns=['tanawari_pattern_cd', 'tenpo_cd', 'prdcd']
            tana_ten_prd_output_list_df.to_csv('tana_ten_prd_output_list_df.csv')
            print('output tana_ten_prd_output_list_df.csv')
            sys.exit()
        
        
        
        
        
    else:
        path_tehme_md = "Basic_Analysis_unzip_result/01_Data/36_theme_md/theme_md_prd_store.csv"
        theme_md_df = extract_as_df(path_tehme_md, bucket_name)
        theme_md_df['JANコード'] = theme_md_df['JANコード'].astype(int)
        theme_md_prdcd_list = theme_md_df['JANコード'].unique().tolist()
    '''    
        
    print('テーマMD商品:', theme_md_prdcd_list)
    
    # テーマMDの販売データパス設定
    path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-allprd/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
    if tenpo_cd_ref is not None:
        path_tran_ref = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-allprd/'+str(tenpo_cd_ref)+"/{}_{}_time_series.csv"
        
    
print('path_tran:', path_tran)
print('path_tran_ref:', path_tran_ref)


# 93を追加 20240304
dpt_list = [69,97,14,37,27,39,28,74,33,30,36,75,85,80,20,22,55,72,15,62,32,77,84,89,23,60,25,87,68,56,92,61,2,40,86,88,26,17,24,34,52,64,73,21,35,58,83,94,63,38,18,29,19,31,53,45,50,81,82,90,91,54,95,93]

#dpt_list = [94]
#dpt_list = [69,97,14,37,27,39,28,74,33,30,36,75,85,80,20]
    
    
# 以下のリストはつかっていない
#exclusion_dpt_list = [2,20,26,30,33,37,39,47,48,53,57,80,98]

# 花王テーマMD
#dpt_list = [22,34,64,72,73,74,83,84,85,86,87,89,93]
#dpt_list = [72, 83, 86, 87]

# 対象店舗（新店）の週次（中量品の設定の場合は中量品）データを読み込む
sales_df = pd.DataFrame()
for dpt in dpt_list:
    print(dpt)
    temp_sales_df = pd.DataFrame()
    try:
        temp_sales_df = extract_as_df_with_encoding(path_tran.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")
        #print('件数', len(temp_sales_df))
        if 'Unnamed: 0' in temp_sales_df.columns:
            temp_sales_df = temp_sales_df.drop('Unnamed: 0', axis=1)

        if THEME_MD_MODE:
            temp_sales_df = temp_sales_df[temp_sales_df['PRD_CD'].isin(theme_md_prdcd_list)]
            print('テーマMD件数', len(temp_sales_df))
            
        temp_sales_df['DPT'] = int(dpt)
        sales_df = pd.concat([sales_df, temp_sales_df], axis=0).reset_index(drop=True)
    except:
        continue
        


if not THEME_MD_MODE:
    # 参照店舗のある場合の処理
    if tenpo_cd_ref is not None:
        print('新店処理')

        # 週次データの場合
        # 1. 新店の週次データを読む
        # 2. 新店の月次データを読む
        # 3. 新店の中位データを読む
        # 4. 参照店の週次データを読む
        # 1+2+3のデータのうち、4にあるデータに限定する

        # 中量データの場合
        # 1. 新店の中位データを読む
        # 2. 新店の月次データを読む
        # 3. 新店の週次データを読む
        # 4. 参照店の中位データを読む
        # 1+2+3のデータのうち、4にあるデータに限定する

        # テーマMDデータの場合
        # 1. 新店の週次データを読む
        

        # 新店の月次データを読み込む(対象店舗では月次に分類されているかもしれないため)
        path_tran_small = "01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_monthly_series.csv"
        #sales_df = pd.DataFrame()
        for dpt in dpt_list:
            print(dpt)
            temp_sales_df = pd.DataFrame()
            try:
                temp_sales_df = extract_as_df_with_encoding(path_tran_small.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")
                #print('件数', len(temp_sales_df))
                if 'Unnamed: 0' in temp_sales_df.columns:
                    temp_sales_df = temp_sales_df.drop('Unnamed: 0', axis=1)

                temp_sales_df['DPT'] = int(dpt)
                sales_df = pd.concat([sales_df, temp_sales_df], axis=0).reset_index(drop=True)
            except:
                continue
                
                
        # 新店の中量品データを読み込む
        if output_6wk_2sales:
            path_tran_sub = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
        else:
            path_tran_sub = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-62/'+str(tenpo_cd)+"/{}_{}_time_series.csv"

        for dpt in dpt_list:
            print(dpt)
            temp_sales_df = pd.DataFrame()
            try:
                temp_sales_df = extract_as_df_with_encoding(path_tran_sub.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")
                #print('件数', len(temp_sales_df))
                if 'Unnamed: 0' in temp_sales_df.columns:
                    temp_sales_df = temp_sales_df.drop('Unnamed: 0', axis=1)

                temp_sales_df['DPT'] = int(dpt)
                sales_df = pd.concat([sales_df, temp_sales_df], axis=0).reset_index(drop=True)
            except:
                continue
                

        # 参照店舗の週次データ or 中量品データを読み込む
        sales_df_ref = pd.DataFrame()
        for dpt in dpt_list:
            print(dpt)
            temp_sales_df_ref = pd.DataFrame()
            try:
                temp_sales_df_ref = extract_as_df_with_encoding(path_tran_ref.format(dpt, str(tenpo_cd_ref)), bucket_name, "utf-8")
                #print('件数', len(temp_sales_df_ref))
                if 'Unnamed: 0' in temp_sales_df_ref.columns:
                    temp_sales_df_ref = temp_sales_df_ref.drop('Unnamed: 0', axis=1)

                if THEME_MD_MODE:
                    temp_sales_df_ref = temp_sales_df_ref[temp_sales_df_ref['PRD_CD'].isin(theme_md_prdcd_list)]
                    print('テーマMD ref件数', len(temp_sales_df_ref))

                temp_sales_df_ref['DPT'] = int(dpt)
                sales_df_ref = pd.concat([sales_df_ref, temp_sales_df_ref], axis=0).reset_index(drop=True)
            except:
                continue


        # **************************************************************************
        # 対象店舗の販売データを、参照店舗の週次データ　or 中量品データにある商品に限定する
        prdcd_list = sales_df_ref['PRD_CD'].unique().tolist()    
        sales_df = sales_df[sales_df['PRD_CD'].isin(prdcd_list)].reset_index(drop=True)
        # **************************************************************************

        sales_df['TENPO_CD'] = tenpo_cd
    
print('sales_df.shape:', sales_df.shape)   


if(len(sales_df) <= 0):
    print("=====There is no sales data, please check: stage1 has been executed======")
    sys.exit(1)

    
sales_df = sales_df[['PRD_CD', 'nenshudo', 'URI_SU','TENPO_CD', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

print('sales_df SKU:', len(sales_df['PRD_CD'].unique()))





if add_ec_salesamount:
    
    project_id = "dev-cainz-demandforecast"
    dataset_id = 'dev_cainz_nssol'
    table_id = 'shipment_with_store_inventory'
    
    my_tenpo_cd = str(tenpo_cd).zfill(4)
    #target_query = f"""  SELECT d_product_code as PRD_CD, d_ship_date_jst_of_Nendo_Weekly as NENDO, d_ship_date_jst_of_Shudo_Weekly as SHUDO, total_qnt as URI_SU_EC FROM `{dataset_id}.{table_id}` WHERE ship_place_code = '""" + my_tenpo_cd + "' order by PRD_CD, NENDO, SHUDO"
    
    target_query = f"""  select PRD_CD, NENDO, SHUDO, sum(URI_SU_EC) as URI_SU_EC
from (SELECT d_product_code as PRD_CD, d_ship_date_jst_of_Nendo_Weekly as NENDO, d_ship_date_jst_of_Shudo_Weekly as SHUDO, total_qnt as URI_SU_EC FROM `{dataset_id}.{table_id}` WHERE ship_place_code = '""" + my_tenpo_cd + "' order by PRD_CD, NENDO, SHUDO) group by PRD_CD, NENDO, SHUDO order by PRD_CD, NENDO, SHUDO"
    
    print(target_query)
    ec_sales_df = pd.read_gbq(target_query, project_id, dialect='standard')
    
    sales_df['PRD_CD'] = sales_df['PRD_CD'].astype(int)
    sales_df['nenshudo'] = sales_df['nenshudo'].astype(int)
    
    ec_sales_df['PRD_CD'] = ec_sales_df['PRD_CD'].astype(int)
    ec_sales_df['nenshudo'] = ec_sales_df['NENDO'].astype(str) + ec_sales_df['SHUDO'].astype(str)
    ec_sales_df['nenshudo'] = ec_sales_df['nenshudo'].astype(int)
    
    sales_df = pd.merge(sales_df, ec_sales_df[['PRD_CD', 'nenshudo', 'URI_SU_EC']], on=['PRD_CD', 'nenshudo'], how='left')
    sales_df['URI_SU_EC'] = sales_df['URI_SU_EC'].fillna(0.0)




if not THEME_MD_MODE:

    if restrict_minmax:    
        if 0:
            # dev-cainz-demandforecast.dev_cainz_nssol.TB_MINMAX_CHANGE_HISTORY_NB_DPT
            project_id = "dev-cainz-demandforecast"
            dataset_id = 'dev_cainz_nssol'
            table_id = 'TB_MINMAX_CHANGE_HISTORY_NB_DPT'

            my_tenpo_cd = str(tenpo_cd).zfill(4)
            target_query = f"""  SELECT * FROM {dataset_id}.{table_id} WHERE TENPO_CD = '""" + my_tenpo_cd + "' order by PRD_CD, NENSHUDO"
            print(target_query)
            minmax_df = pd.read_gbq(target_query, project_id, dialect='standard')
            minmax_df['DPT'] = minmax_df['BUMON_CD'].astype(int)
            minmax_df['BUMON_CD'] = minmax_df['BUMON_CD'].astype(int)
            minmax_df['TENPO_CD'] = minmax_df['TENPO_CD'].astype(int)
            minmax_df['PRD_CD'] = minmax_df['PRD_CD'].astype(int)
            minmax_df['NENSHUDO'] = minmax_df['NENSHUDO'].astype(int)
            minmax_df['HOJU_START_YMD'] = minmax_df['HOJU_START_YMD'].astype(int)
            minmax_df['HOJU_END_YMD'] = minmax_df['HOJU_END_YMD'].astype(int)
            minmax_df['HOJU_MAX_SU'] = minmax_df['HOJU_MAX_SU'].astype(int)
            minmax_df['HOJU_MIN_SU'] = minmax_df['HOJU_MIN_SU'].astype(int)
            minmax_df['TOROKU_YMD'] = minmax_df['TOROKU_YMD'].astype(int)
            minmax_df['TOROKU_HMS'] = minmax_df['TOROKU_HMS'].astype(int)

        else:
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
                except:
                    continue


        print('minmax_df.shape:', minmax_df.shape)         

        if len(minmax_df) > 0:
            # MinMaxレコードの製品別最新レコードを取り出す
            minmax_df['TOROKU_YMD_TOROKU_HMS'] = minmax_df['TOROKU_YMD'].astype(str) + minmax_df['TOROKU_HMS'].astype(str) + minmax_df['NENSHUDO'].astype(str)
            prdcd_nenshudomax_df = minmax_df.groupby(['PRD_CD'])['TOROKU_YMD_TOROKU_HMS'].max().reset_index()
            minmax_df_newest = pd.merge(prdcd_nenshudomax_df, minmax_df, on=['PRD_CD', 'TOROKU_YMD_TOROKU_HMS'], how='inner')

            # minmaxの無い製品番号を調査用にとっておく
            nominmax_prdcd_df = pd.DataFrame(sales_df[~sales_df['PRD_CD'].isin(minmax_df_newest['PRD_CD'].tolist())]['PRD_CD'].unique())
            nominmax_prdcd_df.columns = ['PRD_CD']
            nominmax_prdcd_df['reason'] = 'no_minmax'
            nominmax_prdcd_df['HACCHU_TO_YMD'] = ''
            # ---------------------------------------

            # minmaxデータのある製品に絞る
            sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest['PRD_CD'].tolist())]
            minmax_df_newest2 = minmax_df_newest[minmax_df_newest['PRD_CD'].isin(sales_df['PRD_CD'].tolist())]
                    #minmax_df_newest2.to_csv('wkly_minmax_df_newest2_20230912.csv', index=False)
                
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
                # 下限0意外の商品に絞る
                minmax_df_newest2_not_min0 =minmax_df_newest2[minmax_df_newest2['HOJU_MIN_SU']>0]
                sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest2_not_min0['PRD_CD'].tolist())]

            print('restrict minmax sales_df SKU:', len(sales_df['PRD_CD'].unique()))


    if restrinct_tenpo_hacchu_end:
        # 店舗別の生産発注停止情報を結合
        path_tenpo_hacchu_master = "Basic_Analysis_unzip_result/01_Data/33_tenpo_hacchu/29_TENPO_HACCHU_YMD.csv"
        store_prd_hacchu_ymd = extract_as_df(path_tenpo_hacchu_master, bucket_name)    
        store_prd_hacchu_ymd['TENPO_CD'] = store_prd_hacchu_ymd['TENPO_CD'].astype(int)
        store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD']==tenpo_cd].reset_index(drop=True)
        
        if len(store_prd_hacchu_ymd) > 0:
            # 店舗別発注終了日
            store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].fillna(99999999)
            store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].astype(int)
            # 対象店舗データに絞ってマージ
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
            hacchu_end_prdlist = store_prd_hacchu_ymd[store_prd_hacchu_ymd['HACCHU_TO_YMD'] <= my_date]['PRD_CD'].astype(int).tolist()
            sales_df = sales_df[~sales_df['PRD_CD'].isin(hacchu_end_prdlist)]
            sales_df = sales_df.reset_index(drop=True)
            print('exclude store hacchuend sales_df SKU:', len(sales_df['PRD_CD'].unique())) 
    
    
    '''
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
    
    dfm_base = load_multiple_df(path_prd_master_list, bucket_name)
    dfm_base.rename(columns={"prd_cd":"PRD_CD"})
    dfm_base['PRD_CD'] = dfm_base['PRD_CD'].astype(int)
    
    
    exclude_prdcd_df = pd.concat([nominmax_prdcd_df, minmax0_prdcd_df, hacchu_teishi_prdcd_df2])
    
    dfm_base_select = dfm_base[['PRD_CD', 'BUMON_CD', 'LINE_CD', 'CLS_CD', 'NENDO', 'HNMK_CD', 'MAINT_FROM_YMD', 'MAINT_TO_YMD', 'PRD_NM_KJ', 'TENPO_HACCHU_FROM_YMD_TOITSU', 'TENPO_HACCHU_TO_YMD_TOITSU']]
    
    exclude_prdcd_df_merged = pd.merge(exclude_prdcd_df, dfm_base_select, on='PRD_CD', how='left').reset_index(drop=True)
    
    exclude_prdcd_df_merged.to_csv('exclude_prdcd_df_merged.csv', index=False)
    '''
    
        

train_df = sales_df


#train_df.to_csv('train_df_0.csv')


df_calendar_tmp_sales = df_calendar[["nenshudo","week_from_ymd"]]


if startpont_this_week:
    train_end_nenshudo = calc_nenshudo(today_nenshudo, -1)
    print('train_end_nenshudo', train_end_nenshudo)

    target_nenshudo = today_nenshudo
    print('target_nenshudo:', target_nenshudo)
    
    if contex_term_valiable:
        start_nenshudo = calc_nenshudo(today_nenshudo, -(contex_term))
        print('start_nenshudo:', start_nenshudo)    
    else:
        start_nenshudo = calc_nenshudo(today_nenshudo, -20)
        print('start_nenshudo:', start_nenshudo)

    if prediction_term_4week: # 4週未来  トータル10週
        end_nenshudo = calc_nenshudo(today_nenshudo, 9)
        print('end_nenshudo:', end_nenshudo)
    elif prediction_term_11week: # 5wk未来 トータル11週
        end_nenshudo = calc_nenshudo(today_nenshudo, 10)
        print('end_nenshudo:', end_nenshudo)
        
    elif prediction_term_18week: # 12wk未来まで トータル18週　梅雨明けお盆
        end_nenshudo = calc_nenshudo(today_nenshudo, 17)
        print('end_nenshudo:', end_nenshudo)
        
    elif prediction_term_26week: #thmemd　トータル26週
        end_nenshudo = calc_nenshudo(today_nenshudo, 25)
        print('end_nenshudo:', end_nenshudo)
        
    else: # 14週未来　トータル20週
        end_nenshudo = calc_nenshudo(today_nenshudo, 19)
        print('end_nenshudo:', end_nenshudo)

else:
    train_end_nenshudo = calc_nenshudo(today_nenshudo, -6)
    print('train_end_nenshudo', train_end_nenshudo)

    target_nenshudo = calc_nenshudo(today_nenshudo, -5)
    print('target_nenshudo:', target_nenshudo)

    if contex_term_valiable:
        start_nenshudo = calc_nenshudo(today_nenshudo, -(5+contex_term))
        print('start_nenshudo:', start_nenshudo)    
    else:
        start_nenshudo = calc_nenshudo(today_nenshudo, -25)
        print('start_nenshudo:', start_nenshudo)

    if prediction_term_4week: # 4週未来  トータル10週
        end_nenshudo = calc_nenshudo(today_nenshudo, 4)
        print('end_nenshudo:', end_nenshudo)
    elif prediction_term_11week: # 5wk未来 トータル11週
        end_nenshudo = calc_nenshudo(today_nenshudo, 5)
        print('end_nenshudo:', end_nenshudo)
        
    elif prediction_term_18week: # 12wk未来まで トータル18週　梅雨明けお盆
        end_nenshudo = calc_nenshudo(today_nenshudo, 12)
        print('end_nenshudo:', end_nenshudo)
        
    elif prediction_term_26week: #thmemd　トータル26週
        end_nenshudo = calc_nenshudo(today_nenshudo, 20)
        print('end_nenshudo:', end_nenshudo)
    else: # 14週未来　トータル20週
        end_nenshudo = calc_nenshudo(today_nenshudo, 14)
        print('end_nenshudo:', end_nenshudo)


if output_collected_sales_value == True:
    prev_nenshudo = calc_nenshudo(today_nenshudo, -1)
    # ここで、売り数０週のレコードを追加する
    train_df = train_df[train_df['nenshudo']<=prev_nenshudo].reset_index(drop=True) # 先週まで
    train_df = interpolate_df(train_df, df_calendar, add_ec_salesamount)
    # 今週以降のカレンダー
    df_calendar_expand = df_calendar[['nenshudo','week_from_ymd']][df_calendar['nenshudo']>=today_nenshudo].reset_index(drop=True)
    df_calendar_expand = df_calendar_expand[['nenshudo','week_from_ymd']][df_calendar_expand['nenshudo']<=end_nenshudo].reset_index(drop=True) 
else:
    train_df = train_df[train_df['nenshudo']<=train_end_nenshudo].reset_index(drop=True) # 6週前まで
    # ここで、売り数０週のレコードを追加する
    train_df = interpolate_df(train_df, df_calendar, add_ec_salesamount)
    # 予測開始週（5週前）以降のカレンダー
    df_calendar_expand = df_calendar[['nenshudo','week_from_ymd']][df_calendar['nenshudo']>=target_nenshudo].reset_index(drop=True)
    df_calendar_expand = df_calendar_expand[['nenshudo','week_from_ymd']][df_calendar_expand['nenshudo']<=end_nenshudo].reset_index(drop=True) 



train_df = pd.merge(train_df, df_calendar_tmp_sales, on ="nenshudo", how ="left").reset_index(drop=True)

if add_ec_salesamount:
    train_df = train_df.rename(columns={'PRD_CD':'商品コード', 'week_from_ymd':'週開始日付', 'URI_SU':'売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'nenshudo':'年週度'})
else:
    train_df = train_df.rename(columns={'PRD_CD':'商品コード', 'week_from_ymd':'週開始日付', 'URI_SU':'売上実績数量', 'nenshudo':'年週度'})
    

#if make_metrics_data:
if OUTPUT_METRICS_VALUE:
    ##################################################
    # 補正前のmetrics
    ##################################################

    # 1年前の年週度
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
    
    
    # 欠損週のカウント/直近1年/補正なし/
    train_df_on_sales_last1year_zero  = train_df_on_sales[(train_df_on_sales['年週度']>=last1yer_nenshudo)&(train_df_on_sales['売上実績数量']<0.001)][['商品コード', '売上実績数量']]
    
    if len(train_df_on_sales_last1year_zero) > 0:    
        year1_nosales_weekcount = train_df_on_sales_last1year_zero.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.count()).rename(columns={'売上実績数量':'欠損週数_直近1年_補正無し'})
    else:
        year1_nosales_weekcount = train_df_on_sales_last1year_zero.rename(columns={'売上実績数量':'欠損週数_直近1年_補正無し'})
        
        
    
    #標準偏差/直近1年（0含む）/直近1年/補正なし
    train_df_on_sales_last1year = train_df_on_sales[train_df_on_sales['年週度']>=last1yer_nenshudo][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_last1year) > 0:
        year1_std = train_df_on_sales_last1year.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正無し'})
    else:
        year1_std = train_df_on_sales_last1year.rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正無し'})
        
    
    #標準偏差/直近2年（0含む）/直近2年/補正なし
    train_df_on_sales_last2year = train_df_on_sales[train_df_on_sales['年週度']>=last2yer_nenshudo][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_last2year) > 0:
        year2_std = train_df_on_sales_last2year.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正無し'})
    else:
        year2_std = train_df_on_sales_last2year.rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正無し'})
    
    # 直近13週データ
    #基本統計量/実績平均/直近13週/補正なし
    #基本統計量/実績中央値/直近13週/補正なし
    #基本統計量/実績最大/直近13週/補正なし
    #基本統計量/実績最小/直近13週/補正なし
    #基本統計量/実績4分位点/直近13週/補正なし
    #基本統計量/実績標準偏差/直近13週/補正なし
    
    train_df_on_sales_last13wk = train_df_on_sales[train_df_on_sales['年週度']>=last13week_nenshudo][['商品コード', '売上実績数量']]
    
    if len(train_df_on_sales_last13wk) > 0:    
        week13_metrics = train_df_on_sales_last13wk.groupby('商品コード').describe()['売上実績数量'].drop('count', axis=1).rename(columns={'mean':'売上実績数量_直近13週実績平均_補正無し', 'std':'売上実績数量_直近13週実績std_補正無し', 'min':'売上実績数量_直近13週実績最小_補正無し', '25%':'売上実績数量_直近13週実績25%_補正無し', '50%':'売上実績数量_直近13週実績50%_補正無し', '75%':'売上実績数量_直近13週実績75%_補正無し', 'max':'売上実績数量_直近13週実績最大_補正無し'})
        #'count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'

        week13_median = train_df_on_sales_last13wk.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.median()).rename(columns={'売上実績数量':'売上実績数量_直近13週実績平均_中央値'})
    else:
        week13_metrics = pd.DataFrame(columns=['商品コード', 'count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'])
        week13_metrics = week13_metrics.rename(columns={'mean':'売上実績数量_直近13週実績平均_補正無し', 'std':'売上実績数量_直近13週実績std_補正無し', 'min':'売上実績数量_直近13週実績最小_補正無し', '25%':'売上実績数量_直近13週実績25%_補正無し', '50%':'売上実績数量_直近13週実績50%_補正無し', '75%':'売上実績数量_直近13週実績75%_補正無し', 'max':'売上実績数量_直近13週実績最大_補正無し'})
        
        
        week13_median = train_df_on_sales_last13wk.rename(columns={'売上実績数量':'売上実績数量_直近13週実績平均_中央値'})
        

    # 売り数０を除外
    train_df_on_sales = train_df_on_sales[train_df_on_sales['売上実績数量']>=0.001]
    #標準偏差/直近1年（0除外）/直近1年/補正なし
    
    train_df_on_sales_last1year =  train_df_on_sales[train_df_on_sales['年週度']>=last1yer_nenshudo][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_last1year) > 0:
        year1_std_exclude0 = train_df_on_sales_last1year.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正無し_0除外'})
    else:
        year1_std_exclude0 = train_df_on_sales_last1year.rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正無し_0除外'})
        
    
    #標準偏差/直近2年（0除外）/直近2年/補正なし
    train_df_on_sales_last2year = train_df_on_sales[train_df_on_sales['年週度']>=last2yer_nenshudo][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_last2year) > 0:
        year2_std_exclude0 = train_df_on_sales_last2year.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正無し_0除外'})
    else:
        year2_std_exclude0 = train_df_on_sales_last2year.rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正無し_0除外'})
    
    metrics_result = pd.merge(year1_std, year2_std, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year1_std_exclude0, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year2_std_exclude0, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, week13_median, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, week13_metrics, on='商品コード', how='left')
    metrics_result = pd.merge(metrics_result, year1_nosales_weekcount, on='商品コード', how='left')
    
    
    
train_df_prd = train_df[['商品コード']]
# 来週以降のカレンダーをTrainデータに結合
df_calendar_expand = df_calendar_expand.assign(join_key=0).drop_duplicates().reset_index(drop=True)
train_df_prd = train_df_prd.assign(join_key=0).drop_duplicates().reset_index(drop=True)
temp_df = pd.merge(df_calendar_expand, train_df_prd, on="join_key",how='outer').drop('join_key', axis=1).reset_index(drop=True)
temp_df[['売上実績数量']] = 0
temp_df = temp_df.rename(columns={'week_from_ymd':'週開始日付', 'nenshudo':'年週度'})
train_df = pd.concat([train_df, temp_df], axis=0).reset_index(drop=True)
train_df[['店舗コード']] = tenpo_cd

if add_ec_salesamount:
    #train_df = train_df[['商品コード','週開始日付','店舗コード','売上実績数量', '売上実績数量EC', '前年EC売上実績数量', '年週度', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    train_df = train_df[['商品コード','週開始日付','店舗コード','売上実績数量', '売上実績数量EC', '年週度', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
else:
    train_df = train_df[['商品コード','週開始日付','店舗コード','売上実績数量','年週度', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

#train_df.to_csv('train_df_2.csv')


target_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == target_nenshudo].values[0]
start_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == start_nenshudo].values[0]
context_end_nenshudo = target_nenshudo
context_end_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == context_end_nenshudo].values[0]
end_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == end_nenshudo].values[0]


if output_collected_sales_value == False:
    train_df = train_df[train_df['週開始日付'] >= start_week_from_ymd].reset_index(drop=True)



train_df = train_df.sort_values('年週度').reset_index(drop=True)


#train_df.to_csv('train_df_3.csv') # 売上0の週レコードあり。前週まではDPTなどは入っているが、次週以降ははいってない



dftarget = train_df
dftarget['週開始日付_予測対象'] = dftarget['週開始日付']
dftarget2 = dftarget[['商品コード','週開始日付_予測対象', '店舗コード', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]



if output_collected_sales_value == False:
    dftarget2 = dftarget2[dftarget2['週開始日付_予測対象'] >= start_week_from_ymd].reset_index(drop=True)

df_calendar = extract_as_df_with_encoding("Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv","dev-cainz-demandforecast","utf-8")

if 0:
    df_zen_calendar = df_calendar[["week_from_ymd", "znen_week_from_ymd","nenshudo"]]
    df_zen_calendar = df_zen_calendar.rename(columns={'week_from_ymd': '週開始日付_予測対象', 'znen_week_from_ymd':'前年週開始日付'})

    # 週度に換算する
    dftarget2 = pd.merge(dftarget2, df_zen_calendar, on="週開始日付_予測対象")
    dftarget2[["zen_nenshudo"]] = dftarget2[["nenshudo"]] - 100

else:
    # 20230802修正
    df_zen_calendar = df_calendar[["week_from_ymd", "znen_week_from_ymd", "nenshudo", 'znen_nendo', 'znen_shudo']]
    df_zen_calendar = df_zen_calendar.rename(columns={'week_from_ymd': '週開始日付_予測対象', 'znen_week_from_ymd':'前年週開始日付'})
    
    # 週度に換算する
    dftarget2 = pd.merge(dftarget2, df_zen_calendar, on="週開始日付_予測対象")
    dftarget2["zen_nenshudo"] = dftarget2["znen_nendo"] * 100 + dftarget2["znen_shudo"]
    dftarget2 = dftarget2.drop('znen_nendo', axis=1)
    dftarget2 = dftarget2.drop('znen_shudo', axis=1)


print("==============週開始日付_予測対象================")


if add_ec_salesamount:
    sales_df = sales_df[['PRD_CD', 'nenshudo', 'URI_SU', 'URI_SU_EC', 'TENPO_CD', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
else:
    sales_df = sales_df[['PRD_CD', 'nenshudo', 'URI_SU','TENPO_CD', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    
sales_df = sales_df.drop_duplicates().reset_index(drop=True)



'''
if interpolate_1yearsales:
# odas処理の後に移動
    
'''

df_odas_calender = odas_correct(df_calendar, tenpo_cd, use_jan_connect=use_jan_connect)

'''
# 参照店舗のODASデータを使いたい場合はこちらで
if tenpo_cd_ref is None:
    df_odas_calender = odas_correct(df_calendar, tenpo_cd, use_jan_connect=use_jan_connect)
else:
    df_odas_calender = odas_correct(df_calendar, tenpo_cd_ref, use_jan_connect=use_jan_connect)
    df_odas_calender['TENPO_CD'] = tenpo_cd
'''

df_odas_calender = df_odas_calender.groupby(['TENPO_CD', 'PRD_CD', 'nenshudo'], as_index=False).agg({"odas_amount":'sum', 'sales_ymd':'max'})
sales_df = pd.merge(sales_df, df_odas_calender, on =["PRD_CD", "nenshudo", "TENPO_CD"], how="left")
sales_df = sales_df.drop('sales_ymd', axis=1)
sales_df["odas_amount"] = sales_df["odas_amount"].fillna(0)


#sales_df1 = copy.deepcopy(sales_df)


if odas_imprvmnt == False:
    print('odas_imprvmnt == False')
    sales_df["URI_SU"] = sales_df["URI_SU"] - sales_df["odas_amount"]
else:
    print('odas_imprvmnt == True')
    
    odas_correction_start_t = time.time()
    print('start odas correction improvements *******************************')
    

    sales_df["URI_SU_NEW"] = sales_df["URI_SU"] - sales_df["odas_amount"]
    
    # 0以下を0にする
    sales_df["URI_SU_NEW_org"] = sales_df["URI_SU_NEW"]
    sales_df["URI_SU_NEW"][sales_df["URI_SU_NEW"]<0] = 0
    
    
    # まずは極端なピークを除外した平均値を見たい***********************************************************
    # ここで8週最頻値±2σの範囲のデータを作成、外れる箇所は線形補間する
    sales_df['URI_SU_NEW_STD'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW'].transform(lambda x:x.std())    
    sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW'].transform(lambda x:x.rolling(window=8).apply(lambda y: np_mode2(y)))    
    sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_8MODE'].transform(lambda x: x.interpolate(limit_direction='both'))

    # 8週MODE±2σを超える値にnanを設定して、
    sales_df["URI_SU_NEW_2SIGMA_LOWER"] = sales_df['URI_SU_NEW_8MODE'] - 2*sales_df["URI_SU_NEW_STD"]
    sales_df["URI_SU_NEW_2SIGMA_UPPER"] = sales_df['URI_SU_NEW_8MODE'] + 2*sales_df["URI_SU_NEW_STD"]

    sales_df["URI_SU_NEW_2SIGMA"] = sales_df["URI_SU_NEW"]
    sales_df["URI_SU_NEW_2SIGMA"][
        (sales_df["URI_SU_NEW"] < sales_df["URI_SU_NEW_2SIGMA_LOWER"])
        |(sales_df["URI_SU_NEW"] > sales_df["URI_SU_NEW_2SIGMA_UPPER"])                       
    ] = np.nan
    
    #sales_df['URI_SU_NEW_2SIGMA_BK'] = sales_df['URI_SU_NEW_2SIGMA']
    
    # nanを線形補完する
    sales_df['URI_SU_NEW_2SIGMA'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_2SIGMA'].transform(lambda x: x.interpolate(limit_direction='both'))
    
    # 外れ値を補間しなおした売り数の8週平均をとる
    sales_df['URI_SU_NEW_2SIGMA_8EMA'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_2SIGMA'].transform(lambda x: x.ewm(span=8).mean())
    
    
    print('processing odas correction improvements *******************************')
    odas_correction_end_t = time.time()
    elapsed_time = odas_correction_end_t - odas_correction_start_t
    print(f"odas correction elapsed time: {elapsed_time:.3f} seconds")
    
    # ここから補正の本処理　****************************************************************************************
    # ODAS補正後にマイナスとなったものは、2σの8週平均で補完する
    sales_df["URI_SU_NEW_OVER0"] = sales_df["URI_SU_NEW_org"]
    sales_df["URI_SU_NEW_OVER0"][sales_df["URI_SU_NEW_OVER0"]<0] = sales_df['URI_SU_NEW_2SIGMA_8EMA'][sales_df["URI_SU_NEW_OVER0"]<0]
    # ここであらためて0以下は0に置換
    sales_df["URI_SU_NEW_OVER0"][sales_df["URI_SU_NEW_OVER0"]<0] = 0

    # ODAS補正後がマイナスで、前後n週内に客数値に近い売りがあれば、nanをセットして線形補間していく  
    sales_df['URI_SU_NEW_OVER0_8MODE'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_OVER0'].transform(lambda x:x.rolling(window=8).apply(lambda y: np_mode2(y)))
    sales_df['URI_SU_NEW_OVER0_8MODE'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_8MODE'].transform(lambda x: x.interpolate(limit_direction='both'))
    sales_df['URI_SU_NEW_OVER0_STD'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_OVER0'].transform(lambda x:x.std())
    
    # MODE+2σをODASがあるときのスパイク補正の閾値とする
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
    
    #myprdlist = list(sales_df[sales_df['URI_SU_NEW_OVER0_IMPLVMNT_bk'].isna()]['PRD_CD'].unique())
    #sales_df_ex = sales_df[sales_df['PRD_CD'].isin(myprdlist)]
    
    #sales_df_ex.to_csv('sales_df_ex.csv')
    
    #print('test exit')
    #sys.exit()
    
    
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
    
    print('end odas Correction improvements *******************************')
    odas_correction_end_t = time.time()
    elapsed_time = odas_correction_end_t - odas_correction_start_t
    print(f"odas correction elapsed time: {elapsed_time:.3f} seconds")


sales_df = sales_df.drop('odas_amount', axis=1)
sales_df = sales_df.drop_duplicates().reset_index(drop=True)


#sales_df2 = copy.deepcopy(sales_df)


#sales_df.to_csv('sales_df_1.csv')
# （売り数も直近まで入っている、売り数０の週のレコードは無し・・・）


if class_wave_add:
    sales_df['URI_SU_CLASS'] = sales_df.groupby(["cls_cd", "nenshudo"], as_index=False)['URI_SU'].transform(lambda x: x.sum())
    if class_wave_mean_add:
        sales_df['URI_SU_CLASS8ema'] = sales_df.groupby("PRD_CD", as_index=False)['URI_SU_CLASS'].transform(lambda x: x.ewm(span=8).mean())
    
    
    
#if make_metrics_data:
if OUTPUT_METRICS_VALUE:
    ##################################################
    # 補正後のmetrics
    ##################################################

    # テストデータ期間に絞っている *********************************************************************
    train_df =  copy.deepcopy(sales_df)
    # ******************************************************************************************************

    if add_ec_salesamount:
        if class_wave_add:
            if class_wave_mean_add:
                train_df = train_df.rename(columns={'PRD_CD':'商品コード', 'nenshudo':'年週度', 'URI_SU':'売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'URI_SU_CLASS':'売上実績数量CLASS', 'URI_SU_CLASS8ema':'売上実績数量CLASS8ema'})           
            else:
                train_df = train_df.rename(columns={'PRD_CD':'商品コード', 'nenshudo':'年週度', 'URI_SU':'売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'URI_SU_CLASS':'売上実績数量CLASS'})
        else:
            train_df = train_df.rename(columns={'PRD_CD':'商品コード', 'nenshudo':'年週度', 'URI_SU':'売上実績数量', 'URI_SU_EC': '売上実績数量EC'})
    else:
        train_df = train_df.rename(columns={'PRD_CD':'商品コード', 'nenshudo':'年週度', 'URI_SU':'売上実績数量'})
                    
    
    # 1年前の年週度
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
    train_df_on_sales_last1year = train_df_on_sales[train_df_on_sales['年週度']>=last1yer_nenshudo][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_last1year) > 0:
        year1_std_hosei = train_df_on_sales_last1year.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正あり'})
    else:
        year1_std_hosei = train_df_on_sales_last1year.rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正あり'})

    #標準偏差/直近2年（0含む）/直近2年/補正あり
    train_df_on_sales_last2year = train_df_on_sales[train_df_on_sales['年週度']>=last2yer_nenshudo][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_last2year) > 0:
        year2_std_hosei = train_df_on_sales_last2year.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正あり'})
    else:
        year2_std_hosei = train_df_on_sales_last2year.rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正あり'})
        
    # 売り数０を除外
    train_df_on_sales = train_df_on_sales[train_df_on_sales['売上実績数量']>=0.001]

    #標準偏差/直近1年（0除外）/直近1年/補正あり
    train_df_on_sales_last1year = train_df_on_sales[train_df_on_sales['年週度']>=last1yer_nenshudo][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_last1year) > 0:
        year1_std_exclude0_hosei = train_df_on_sales_last1year.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正あり_0除外'})
    else:
        year1_std_exclude0_hosei = train_df_on_sales_last1year.rename(columns={'売上実績数量':'売上実績数量_直近1年std_補正あり_0除外'})
        
        
    #標準偏差/直近2年（0除外）/直近2年/補正あり
    train_df_on_sales_last2year = train_df_on_sales[train_df_on_sales['年週度']>=last2yer_nenshudo][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_last2year) > 0:
        year2_std_exclude0_hosei = train_df_on_sales_last2year.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正あり_0除外'})
    else:
        year2_std_exclude0_hosei = train_df_on_sales_last2year.rename(columns={'売上実績数量':'売上実績数量_直近2年std_補正あり_0除外'})
    
        
    #標準偏差/直近3年（当週±前後5週)（0含む）/補正あり
    prev5_nenshudo = calc_nenshudo2(today_nenshudo, -6, dfc_tmp)
    after5_nenshudo = calc_nenshudo2(today_nenshudo, 6, dfc_tmp)
    
    prev1year_nenshudo = calc_nenshudo2(today_nenshudo, -52, dfc_tmp)
    prev5_prev1year_nenshudo = calc_nenshudo2(prev1year_nenshudo, -6, dfc_tmp)
    after5_prev1year_nenshudo = calc_nenshudo2(prev1year_nenshudo, 6, dfc_tmp)
    
    prev2year_nenshudo = calc_nenshudo2(prev1year_nenshudo, -52, dfc_tmp)
    prev5_prev2year_nenshudo = calc_nenshudo2(prev2year_nenshudo, -6, dfc_tmp)
    after5_prev2year_nenshudo = calc_nenshudo2(prev2year_nenshudo, 6, dfc_tmp)

    
    train_df_on_sales_ba6wk = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']]
    
    if len(train_df_on_sales_ba6wk) > 0:
        year3_beforeafter6wk_std_hosei = train_df_on_sales_ba6wk.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週std_補正あり'})
    else:
        year3_beforeafter6wk_std_hosei = train_df_on_sales_ba6wk.rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週std_補正あり'})


    
    #標準偏差/直近3年 (当月±前後1か月)（0含む）/補正あり
    train_df_on_sales = pd.merge(train_df_on_sales, dfc[['nenshudo', 'minashi_tsuki']].rename(columns={'nenshudo':'年週度'}), on='年週度', how='left')
    prev1_nenshudo = calc_nenshudo2(today_nenshudo, -1, dfc_tmp)
    #this_minashi_tsuki = train_df_on_sales[train_df_on_sales['年週度']==prev1_nenshudo].reset_index()['minashi_tsuki'][0]
    this_minashi_tsuki = dfc[dfc['nenshudo']==prev1_nenshudo]['minashi_tsuki'].reset_index(drop=True)[0]
    
    prev_minashi_tsuki = this_minashi_tsuki - 1
    if prev_minashi_tsuki < 0:
        prev_minashi_tsuki = 12
    after_minashi_tsuki = this_minashi_tsuki + 1
    if after_minashi_tsuki > 12:
        after_minashi_tsuki = 1
    prev3year_nenshudo = calc_nenshudo2(today_nenshudo, -(52*3+10), dfc_tmp)
    
    train_df_on_sales_ba1month = train_df_on_sales[
        (train_df_on_sales['年週度']>=prev3year_nenshudo)
        &(train_df_on_sales['minashi_tsuki'].isin([prev_minashi_tsuki, this_minashi_tsuki, after_minashi_tsuki]))
                                       ][['商品コード', '売上実績数量']]
    if len(train_df_on_sales_ba1month) > 0:
        year3_beforeafter1month_std_hosei = train_df_on_sales_ba1month.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後1ヶ月std_補正あり'})
    else:
        year3_beforeafter1month_std_hosei = train_df_on_sales_ba1month.rename(columns={'売上実績数量':'売上実績数量_直近3年前後1ヶ月std_補正あり'})
        


    # 中央値
    
    train_df_on_sales_ba6wk = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']]
    
    if len(train_df_on_sales_ba6wk) > 0:
        year3_beforeafter6wk_median_hosei = train_df_on_sales_ba6wk.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.median()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週median_補正あり'})    
    else:
        year3_beforeafter6wk_median_hosei = train_df_on_sales_ba6wk.rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週median_補正あり'})  
        
    # 中央絶対偏差(median absolute deviation)
    
    train_df_on_sales_ba6wk = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']]
    
    if len(train_df_on_sales_ba6wk) > 0:
        year3_beforeafter6wk_mad_hosei = train_df_on_sales_ba6wk.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:st.robust.scale.mad(x)).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週mad_補正あり'})
    else:
        year3_beforeafter6wk_mad_hosei = train_df_on_sales_ba6wk.rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週mad_補正あり'})
    
    # 平均
    train_df_on_sales_ba6wk = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']]
    
    if len(train_df_on_sales_ba6wk) > 0:
        year3_beforeafter6wk_mean_hosei = train_df_on_sales_ba6wk.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.mean()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週mean_補正あり'})
    else:
        year3_beforeafter6wk_mean_hosei = train_df_on_sales_ba6wk.rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週mean_補正あり'})

    train_df_on_sales_ba6wk = train_df_on_sales[
        ((prev5_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_nenshudo))
        | ((prev5_prev1year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev1year_nenshudo))
        | ((prev5_prev2year_nenshudo<=train_df_on_sales['年週度'])&(train_df_on_sales['年週度']<=after5_prev2year_nenshudo))
    ][['商品コード', '売上実績数量']]    
        
    # 変動係数(coefficient of variation)
    if len(train_df_on_sales_ba6wk) > 0:
        year3_beforeafter6wk_cov_hosei = train_df_on_sales_ba6wk.groupby('商品コード', as_index=False)['売上実績数量'].apply(lambda x:x.std()/x.mean()).rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週cov_補正あり'})
    else:
        year3_beforeafter6wk_cov_hosei = train_df_on_sales_ba6wk.rename(columns={'売上実績数量':'売上実績数量_直近3年前後6週cov_補正あり'})
        
        
    
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
    
    if output_6wk_2sales:
        metrics_result['MODEL_TYPE'] = 'medium'
    else:
        metrics_result['MODEL_TYPE'] = 'weekly'
    
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
    
    metrics_result['ActualSalesAmount_1YearStd'] = metrics_result['ActualSalesAmount_1YearStd'].fillna(0.0)
    metrics_result['ActualSalesAmount_2YearStd'] = metrics_result['ActualSalesAmount_2YearStd'].fillna(0.0)
    metrics_result['ActualSalesAmount_1YearStd_Exclude0'] = metrics_result['ActualSalesAmount_1YearStd_Exclude0'].fillna(0.0)
    metrics_result['ActualSalesAmount_2YearStd_Exclude0'] = metrics_result['ActualSalesAmount_2YearStd_Exclude0'].fillna(0.0)
    
    metrics_result['ActualSalesAmount_13WeekStd'] = metrics_result['ActualSalesAmount_13WeekStd'].fillna(0.0)
    
    metrics_result['ActualSalesAmount_1YearStd_Corrected'] = metrics_result['ActualSalesAmount_1YearStd_Corrected'].fillna(0.0)
    metrics_result['ActualSalesAmount_2YearStd_Corrected'] = metrics_result['ActualSalesAmount_2YearStd_Corrected'].fillna(0.0)
    metrics_result['ActualSalesAmount_1YearStd_Corrected_Exclude0'] = metrics_result['ActualSalesAmount_1YearStd_Corrected_Exclude0'].fillna(0.0)
    metrics_result['ActualSalesAmount_2YearStd_Corrected_Exclude0'] = metrics_result['ActualSalesAmount_2YearStd_Corrected_Exclude0'].fillna(0.0)
    
    
    metrics_result['ActualSalesAmount_3Yearba6weekStd_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekStd_Corrected'].fillna(0.0)
    metrics_result['ActualSalesAmount_3Yearba1monthStd_Corrected'] = metrics_result['ActualSalesAmount_3Yearba1monthStd_Corrected'].fillna(0.0)
    
    
    metrics_result['ActualSalesAmount_3Yearba6weekMedian_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekMedian_Corrected'].fillna(0.0)
    metrics_result['ActualSalesAmount_3Yearba6weekMad_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekMad_Corrected'].fillna(0.0)
    metrics_result['ActualSalesAmount_3Yearba6weekMean_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekMean_Corrected'].fillna(0.0)
    metrics_result['ActualSalesAmount_3Yearba6weekCov_Corrected'] = metrics_result['ActualSalesAmount_3Yearba6weekCov_Corrected'].fillna(0.0)
    
    
    # BQに列を追加するときはアップロード前に以下ようにカラムを追加するクエリを実行しておく
    # ALTER TABLE `dev-cainz-demandforecast.cainz_shortterm_predicted_value_for_statistics.metrics_stage2`
    # ADD COLUMN ActualSalesAmount_3Yearba6weekStd_Corrected FLOAT64,
    # ADD COLUMN ActualSalesAmount_3Yearba1monthStd_Corrected FLOAT64;
    
    
    from google.cloud import bigquery
    if output_metrics_test_tbl:
        table_id = "dev-cainz-demandforecast.cainz_shortterm_predicted_value_for_statistics.metrics_stage2_218strdebug"
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
            print('errtype:', str(type(e)))
            print('err:', str(e))
            print('data upload retry')
            time.sleep(20)
            
    
if logarithmize_target_variable:
    print('start logarithmize_target_variable')
    sales_df['URI_SU'] = np.log1p(sales_df['URI_SU'])
    print('end logarithmize_target_variable')


# 20230823 上からこちらに移動（補正後の値をとっておくため）
# ここで、URI_SUをセーブして、あとで使ってる　******************************************************
sales_df_saved = sales_df
# ***************************************************************************************************
# 売り数も直近まで入っているが、売り数０のレコードは無い

if 1:    
    # *********************************************************************************************
    # ここから、前年売り数、前年売り数に関連する特徴量を作成する処理開始
    # *********************************************************************************************
    # 前年売り数 (ここはinner joinしてるので、元々あるレコードのみ、売り数０のレコードは無い)
    sales_df = pd.merge(sales_df, df_calendar, on="nenshudo")
    # 前週まで入っている
    if add_ec_salesamount:
        if class_wave_add:
            if class_wave_mean_add:
                sales_df = sales_df.rename(columns={'URI_SU':'前年売上実績数量', 'URI_SU_EC':'前年EC売上実績数量', 'URI_SU_CLASS':'前年CLASS売上実績数量', 'URI_SU_CLASS8ema':'前年CLASS売上実績数量8ema', 'PRD_CD':'商品コード', 'week_from_ymd': '前年週開始日付'})
            else:
                sales_df = sales_df.rename(columns={'URI_SU':'前年売上実績数量', 'URI_SU_EC':'前年EC売上実績数量', 'URI_SU_CLASS':'前年CLASS売上実績数量', 'PRD_CD':'商品コード', 'week_from_ymd': '前年週開始日付'})            
        else:
            sales_df = sales_df.rename(columns={'URI_SU':'前年売上実績数量', 'URI_SU_EC':'前年EC売上実績数量', 'PRD_CD':'商品コード', 'week_from_ymd': '前年週開始日付'})
    else:
        sales_df = sales_df.rename(columns={'URI_SU':'前年売上実績数量', 'PRD_CD':'商品コード', 'week_from_ymd': '前年週開始日付'})
    
    sales_df = sales_df.drop('nenshudo', axis=1).reset_index(drop=True)

    # ここでは23年46まである（売り数も直近まで入っているが、売り数０のレコードは無い）
    ######################################################################################
    # dftarget2（商品番号×カレンダーだけのデータ）に、売り数等のデータを結合（DPTなど）
    # ここで、売り数０の週レコードが作成される
    ######################################################################################
    if add_ec_salesamount:
        if class_wave_add:
            if class_wave_mean_add:
                dftarget3 = pd.merge(dftarget2.rename(columns={'店舗コード':'TENPO_CD'}), sales_df[['商品コード', '前年週開始日付', '前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema']], on=['商品コード', '前年週開始日付'], how='left')                   
            else:
                dftarget3 = pd.merge(dftarget2.rename(columns={'店舗コード':'TENPO_CD'}), sales_df[['商品コード', '前年週開始日付', '前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量']], on=['商品コード', '前年週開始日付'], how='left')        
        else:
            dftarget3 = pd.merge(dftarget2.rename(columns={'店舗コード':'TENPO_CD'}), sales_df[['商品コード', '前年週開始日付', '前年売上実績数量', '前年EC売上実績数量']], on=['商品コード', '前年週開始日付'], how='left')
        
    else:
        dftarget3 = pd.merge(dftarget2.rename(columns={'店舗コード':'TENPO_CD'}), sales_df[['商品コード', '前年週開始日付', '前年売上実績数量']], on=['商品コード', '前年週開始日付'], how='left')   

    ### ここで補間が必要
    dftarget3 = interpolate_df2(dftarget3)
    dftarget3['TENPO_CD'] = tenpo_cd
    dftarget3 = dftarget3.fillna(0).reset_index(drop=True)
    
    if add_ec_salesamount:
        if class_wave_add:
            if class_wave_mean_add:
                dftarget3 = dftarget3[["商品コード","週開始日付_予測対象","前年週開始日付","前年売上実績数量",
                                       "前年EC売上実績数量","前年CLASS売上実績数量","前年CLASS売上実績数量8ema",
                                       "TENPO_CD","nenshudo", 'baika_toitsu', 'BAIKA', 'DPT', 
                                       'line_cd', 'cls_cd', 'hnmk_cd']]
                
            else:
                dftarget3 = dftarget3[["商品コード","週開始日付_予測対象","前年週開始日付","前年売上実績数量","前年EC売上実績数量","前年CLASS売上実績数量","TENPO_CD","nenshudo", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
        else:
            dftarget3 = dftarget3[["商品コード","週開始日付_予測対象","前年週開始日付","前年売上実績数量","前年EC売上実績数量","TENPO_CD","nenshudo", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    else:
        dftarget3 = dftarget3[["商品コード","週開始日付_予測対象","前年週開始日付","前年売上実績数量","TENPO_CD","nenshudo", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
        

    dftarget3 = dftarget3.drop_duplicates().reset_index(drop=True)
    dftarget3['time_leap8'] = dftarget3.groupby('商品コード',as_index=False)['前年売上実績数量'].transform(lambda x: x.ewm(span=8).mean())
    
    if add_ec_salesamount:
        if class_wave_add:
            if class_wave_mean_add:
                df_vx_test = dftarget3[['商品コード','週開始日付_予測対象','前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema', 'time_leap8','nenshudo', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]                            
            else:
                df_vx_test = dftarget3[['商品コード','週開始日付_予測対象','前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', 'time_leap8','nenshudo', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]            
        else:
            df_vx_test = dftarget3[['商品コード','週開始日付_予測対象','前年売上実績数量', '前年EC売上実績数量', 'time_leap8','nenshudo', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    else:
        df_vx_test = dftarget3[['商品コード','週開始日付_予測対象','前年売上実績数量', 'time_leap8','nenshudo', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]


    sales_df_saved_tenpo = sales_df_saved[sales_df_saved['TENPO_CD'] == tenpo_cd].reset_index(drop=True)
    
    
    if add_ec_salesamount:
        if class_wave_add:
            if class_wave_mean_add:
                sales_df_saved_tenpo = sales_df_saved_tenpo[['PRD_CD','nenshudo','URI_SU', 'URI_SU_EC', 'URI_SU_CLASS', 'URI_SU_CLASS8ema']]                
            else:
                sales_df_saved_tenpo = sales_df_saved_tenpo[['PRD_CD','nenshudo','URI_SU', 'URI_SU_EC', 'URI_SU_CLASS']]
        else:
            sales_df_saved_tenpo = sales_df_saved_tenpo[['PRD_CD','nenshudo','URI_SU', 'URI_SU_EC']]
    else:
        sales_df_saved_tenpo = sales_df_saved_tenpo[['PRD_CD','nenshudo','URI_SU']]
    
    
    sales_df_saved_tenpo = sales_df_saved_tenpo.rename(columns={'PRD_CD':'商品コード'})


    # df_vx_test　　　　　：（前年URI_SU）、売り数０のレコードあり
    #sales_df_saved_tenpo：（当年URI_SU）、売り数０のレコードは無し
    sales_df_saved = pd.merge(df_vx_test, sales_df_saved_tenpo, on=['商品コード','nenshudo'],how='left')
    sales_df_saved['URI_SU'] = sales_df_saved['URI_SU'].fillna(0).reset_index(drop=True)

    
if add_ec_salesamount:
    if class_wave_add:
        if class_wave_mean_add:
            df_vx_test = sales_df_saved.rename(columns={'商品コード': 'PrdCd', '週開始日付_予測対象':'WeekStartDate', 
                '割引率':'DiscountRate','前年売上実績数量':'PreviousYearSalesActualQuantity', 
                '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', 
                '前年CLASS売上実績数量':'PreviousYearClassSalesActualQuantity',
                '前年CLASS売上実績数量8ema':'PreviousYearClassSalesActualQuantity8ema',
                'URI_SU':'SalesAmount', 'URI_SU_EC':'SalesAmountEC', 
                'URI_SU_CLASS':'SalesAmountCLASS',
                'URI_SU_CLASS8ema':'SalesAmountCLASS8ema'})

            df_vx_test = df_vx_test[['PrdCd','WeekStartDate','PreviousYearSalesActualQuantity','PreviousYearEcSalesActualQuantity','PreviousYearClassSalesActualQuantity','PreviousYearClassSalesActualQuantity8ema', 'time_leap8', 'SalesAmount', 'SalesAmountEC', 'SalesAmountCLASS','SalesAmountCLASS8ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

            
            
        else:
            df_vx_test = sales_df_saved.rename(columns={'商品コード': 'PrdCd', '週開始日付_予測対象':'WeekStartDate', '割引率':'DiscountRate','前年売上実績数量':'PreviousYearSalesActualQuantity', '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', '前年CLASS売上実績数量':'PreviousYearClassSalesActualQuantity', 'URI_SU':'SalesAmount', 'URI_SU_EC':'SalesAmountEC', 'URI_SU_CLASS':'SalesAmountCLASS'})

            df_vx_test = df_vx_test[['PrdCd', 'WeekStartDate', 'PreviousYearSalesActualQuantity', 'PreviousYearEcSalesActualQuantity', 'PreviousYearClassSalesActualQuantity', 'time_leap8', 'SalesAmount', 'SalesAmountEC', 'SalesAmountCLASS', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

            # sales_df_savedにはURI_SUはあるが、URI_SU_ECが無いので追加する
        
    else:
        df_vx_test = sales_df_saved.rename(columns={'商品コード': 'PrdCd', '週開始日付_予測対象':'WeekStartDate', '割引率':'DiscountRate','前年売上実績数量':'PreviousYearSalesActualQuantity', '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', 'URI_SU':'SalesAmount', 'URI_SU_EC':'SalesAmountEC'})

        df_vx_test = df_vx_test[['PrdCd', 'WeekStartDate', 'PreviousYearSalesActualQuantity', 'PreviousYearEcSalesActualQuantity', 'time_leap8', 'SalesAmount', 'SalesAmountEC', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

        # sales_df_savedにはURI_SUはあるが、URI_SU_ECが無いので追加する
    

else:
    df_vx_test = sales_df_saved.rename(columns={'商品コード': 'PrdCd', '週開始日付_予測対象':'WeekStartDate', '割引率':'DiscountRate','前年売上実績数量':'PreviousYearSalesActualQuantity','URI_SU':'SalesAmount'})   
    
    df_vx_test = df_vx_test[['PrdCd', 'WeekStartDate', 'PreviousYearSalesActualQuantity',  'time_leap8', 'SalesAmount', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    
    
df_vx_test['weekstartdatestamp'] = pd.to_datetime(df_vx_test['WeekStartDate'], format = '%Y%m%d')

df_vx_test = df_vx_test.dropna(subset=['PreviousYearSalesActualQuantity']).reset_index(drop=True)
df_vx_test = df_vx_test.dropna(subset=['time_leap8']).reset_index(drop=True)


if output_collected_sales_value == False:
    if kakaku_jizen_kichi == False:
        df_vx_test = df_vx_test.drop('WeekStartDate', axis=1)

df_vx_test['PreviousYearSalesActualQuantity'] = df_vx_test['PreviousYearSalesActualQuantity'].astype(float)
df_vx_test['SalesAmount'] = df_vx_test['SalesAmount'].astype(float)

if add_ec_salesamount:
    df_vx_test['PreviousYearEcSalesActualQuantity'] = df_vx_test['PreviousYearEcSalesActualQuantity'].astype(float)
    df_vx_test['SalesAmountEC'] = df_vx_test['SalesAmountEC'].astype(float)
    
if class_wave_add:
    df_vx_test['SalesAmountCLASS'] = df_vx_test['SalesAmountCLASS'].astype(float)
    df_vx_test['PreviousYearClassSalesActualQuantity'] = df_vx_test['PreviousYearClassSalesActualQuantity'].astype(float)
if class_wave_mean_add:
    df_vx_test['SalesAmountCLASS8ema'] = df_vx_test['SalesAmountCLASS8ema'].astype(float)
    df_vx_test['PreviousYearClassSalesActualQuantity8ema'] = df_vx_test['PreviousYearClassSalesActualQuantity8ema'].astype(float)

    
df_vx_test['tenpo_cd'] = tenpo_cd

df_vx_test = df_vx_test.drop_duplicates().reset_index(drop=True)
df_vx_test['PrdCd'] = df_vx_test['PrdCd'].astype(int)

df_vx_test['DPT'] = df_vx_test['DPT'].astype(int)
df_vx_test['line_cd'] = df_vx_test['line_cd'].astype(int)
df_vx_test['cls_cd'] = df_vx_test['cls_cd'].astype(int)
df_vx_test['hnmk_cd'] = df_vx_test['hnmk_cd'].astype(int)
df_vx_test['TenpoCdPrdCd'] = str(tenpo_cd) + '_' + df_vx_test['PrdCd'].astype(str)


df_vx_test = df_vx_test[df_vx_test['PrdCd'] > 0]

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
    
    # 後始末
    df_vx_test = df_vx_test.drop('1stsalesweekstartdatestamp', axis=1)
    del df_vx_test_exist_sales2
    del prdcd_1stsalesweekstartdatestamp_dict
    
    
if kakaku_jizen_kichi == True:   
    
    # ********************************************************************************************
    # 売価を事前に既知にする
    # ********************************************************************************************
    # プライスライン変更予定
    path_pliceline = "Basic_Analysis_unzip_result/01_Data/35_pliceline/pliceline_shuusei_20240404.csv"
    pliceline_df = extract_as_df(path_pliceline, bucket_name)
    if len(pliceline_df) > 0:
        df_vx_test = pd.merge(df_vx_test, pliceline_df[['PRD_CD', 'MAINT_FROM_YMD', 'BAIKA']].rename(columns={'PRD_CD':'PrdCd', 'BAIKA':'PLICELINE_BAIKA'}),
                on='PrdCd', how='left')
    
        df_vx_test['BAIKA'][(df_vx_test['WeekStartDate']>=df_vx_test['MAINT_FROM_YMD'])] =  df_vx_test['PLICELINE_BAIKA']
        df_vx_test = df_vx_test.drop(columns=['MAINT_FROM_YMD', 'PLICELINE_BAIKA']).reset_index(drop=True)
    
    path_kikaku_master = "Basic_Analysis_unzip_result/02_DM/NBKikaku_prd_ten_test20240115/kikaku_inf_"
    # 企画マスターは、SQLで店舗別出力の追加が必要
    path_longs = "Basic_Analysis_unzip_result/02_DM/NBLongs_prd/longs_"

    kikaku_master = pd.DataFrame()
    list_price_df = pd.DataFrame()
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


    # 販促名のマスターをロード
    path_kikaku_type = "Basic_Analysis_unzip_result/01_Data/90_ADD_DATA/M010KIKAKU_TYP.csv"
    kikaku_type = extract_as_df(path_kikaku_type, bucket_name)

    # 店別売価マスターをロード
    #list_price_df = load_price_data(
    #    path_price_list,
    #    tenpo_cd_list,
    #    bucket_name
    #)

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


    '''
    list_price_df = short_term_preprocess_common.rewrite_jan_code(
        subject_jan_master,
        newest_jan_list,
        list_price_df,
        "PRD_CD"
    )
    '''
    '''
    list_price_df = pd.merge(list_price_df, jan_df[['old_jan', 'latest_jan']].rename(columns={'old_jan':'prd_cd'}), on='prd_cd', how='left')
    list_price_df.loc[~list_price_df['latest_jan'].isnull(), 'prd_cd'] = list_price_df['latest_jan']
    list_price_df = list_price_df.drop(['latest_jan'], axis=1)
    list_price_df = list_price_df[~kikaku_master['prd_cd'].isna()].reset_index(drop=True)
    list_price_df['prd_cd'] = list_price_df['prd_cd'].astype(int)
    '''

    '''
    # 店別単品履歴なので不要
    # 店別売価データを結合可能な状態に加工
    df_vx_test = short_term_preprocess_weekly.cleansing_list_price_df(
        df_vx_test,
        list_price_df,
        dfc
    )
    '''

    # 商品マスター変更予約
    path_dfm_yoyaku = "Basic_Analysis_unzip_result/01_Data/29_PRD_YOYAKU/23_M_090_PRD_YOYAKU.csv"
    prd_yoyaku = extract_as_df(path_dfm_yoyaku, bucket_name)

    #店別売価変更予約
    path_list_price_yoyaku = "Basic_Analysis_unzip_result/01_Data/30_TEN_TNPN_YOYAKU/24_M030PRD_TEN_TNPN_INF_YOYAKU.csv"
    list_price_yoyaku = common.extract_as_df(path_list_price_yoyaku, bucket_name)
    list_price_yoyaku = list_price_yoyaku[list_price_yoyaku['TENPO_CD']==tenpo_cd].reset_index(drop=True)
    

    # df_vx_test, kikaku_master, df_calを渡す
    # 最終的に最小値を取り出す行を格納するリストを作る
    check_baika_list = ['baika_toitsu', 'BAIKA']

    # 未来の予約データに関してあれば売価統一を変更する
    if len(prd_yoyaku)!=0:
        prd_yoyaku['prd_cd'] = prd_yoyaku['prd_cd'].astype(int)
        prd_yoyaku['koshin_ymd'] = prd_yoyaku['koshin_ymd'].astype(int)
        prd_yoyaku['baika_toitsu'] = prd_yoyaku['baika_toitsu'].astype(float)
        for i in range(len(prd_yoyaku)):
            df_vx_test.loc[(df_vx_test['PrdCd']==prd_yoyaku['prd_cd'][i])
                            &(df_vx_test['WeekStartDate']>=prd_yoyaku['koshin_ymd'][i]), 'baika_toitsu'] = prd_yoyaku['baika_toitsu'][i]

    # 店別売価の予約データを商品コードで制限
    temp_list_price_yoyaku = list_price_yoyaku.loc[list_price_yoyaku['PRD_CD'].isin(df_vx_test['PrdCd'].unique().tolist())].reset_index(drop=True)

    # 制限した店別売価の予約データが有れば書き換えを行う
    if len(temp_list_price_yoyaku)!=0:
        for i in range(len(temp_list_price_yoyaku)):
            df_vx_test.loc[(df_vx_test['PrdCd']==temp_list_price_yoyaku['PRD_CD'][i])
                              &
                              (df_vx_test['WeekStartDate']>=temp_list_price_yoyaku['MAINT_FROM_YMD'][i]),
                              'BAIKA']=temp_list_price_yoyaku['BAIKA'][i]
            
            
    # kikaku_masterのkikaku_type_cdでループを回す
    for kikaku in kikaku_master['KIKAKU_TYP_CD'].unique().tolist():
        # 企画番号の文字列をリストに格納
        df_vx_test[str(kikaku)] = None
        check_baika_list.append(str(kikaku))
        # 対象企画のみ+終了日がholdout以降に絞る
        temp_kikaku_df = kikaku_master.loc[(kikaku_master['KIKAKU_TYP_CD'] == kikaku)
                                           &
                                           (kikaku_master['HANBAI_TO_YMD']>=target_week_from_ymd)].reset_index(drop=True)
        
        if 1:
            temp_kikaku_df = temp_kikaku_df.sort_values(['PRD_CD', 'HANBAI_FROM_YMD'])
            while len(temp_kikaku_df) > 0:
                dup = temp_kikaku_df['PRD_CD'].duplicated()
                temp_kikaku_df_1 = temp_kikaku_df[~dup]
                temp_kikaku_df = temp_kikaku_df[dup]

                df_vx_test = pd.merge(df_vx_test, temp_kikaku_df_1[['PRD_CD', 'HANBAI_FROM_YMD', 'HANBAI_TO_YMD', 'KIKAKU_BAIKA']].rename(columns={'PRD_CD':'PrdCd'}), on = 'PrdCd', how='left')

                df_vx_test[str(kikaku)][(df_vx_test['WeekStartDate']>=df_vx_test['HANBAI_FROM_YMD'])&(df_vx_test['WeekStartDate']<=df_vx_test['HANBAI_TO_YMD'])] =  df_vx_test['KIKAKU_BAIKA']

                df_vx_test = df_vx_test.drop(columns=['HANBAI_FROM_YMD', 'HANBAI_TO_YMD', 'KIKAKU_BAIKA']).reset_index(drop=True)
        else:
            # 企画マスターの行でループ
            for i in range(len(temp_kikaku_df)):
                # 特徴量名を機革命にし、その期間に売価を入れていく（なければnullになる）
                df_vx_test.loc[(df_vx_test['PrdCd']==temp_kikaku_df['PRD_CD'][i])
                                  &
                                  (df_vx_test['WeekStartDate']>=temp_kikaku_df['HANBAI_FROM_YMD'][i])
                                  &
                                  (df_vx_test['WeekStartDate']<=temp_kikaku_df['HANBAI_TO_YMD'][i]), str(kikaku)] = temp_kikaku_df['KIKAKU_BAIKA'][i]
            
        del temp_kikaku_df     

    longs_df['HANBAI_TO_YMD'] = longs_df['HANBAI_TO_YMD'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    longs_df = longs_df[~longs_df['HANBAI_TO_YMD'].isna()]
    longs_df['HANBAI_TO_YMD'] = longs_df['HANBAI_TO_YMD'].astype(int)    

    
    longs_df = longs_df.loc[longs_df['HANBAI_TO_YMD']>target_week_from_ymd].reset_index(drop=True)
    if len(longs_df)!=0:
        df_vx_test['店舗売変'] = None
        check_baika_list.append('店舗売変')
        
        if 1:
            longs_df = longs_df.sort_values(['PRD_CD', 'HANBAI_FROM_YMD'])
            while len(longs_df) > 0:
                dup = longs_df['PRD_CD'].duplicated()
                longs_df_1 = longs_df[~dup]
                longs_df = longs_df[dup]

                df_vx_test = pd.merge(df_vx_test, longs_df_1[['PRD_CD', 'HANBAI_FROM_YMD', 'HANBAI_TO_YMD', 'KIKAKU_BAIKA']].rename(columns={'PRD_CD':'PrdCd'}), on='PrdCd', how='left')

                df_vx_test['店舗売変'][(df_vx_test['WeekStartDate']>=df_vx_test['HANBAI_FROM_YMD'])&(df_vx_test['WeekStartDate']<=df_vx_test['HANBAI_TO_YMD'])] =  df_vx_test['KIKAKU_BAIKA']

                df_vx_test = df_vx_test.drop(columns=['HANBAI_FROM_YMD', 'HANBAI_TO_YMD', 'KIKAKU_BAIKA']).reset_index(drop=True)
            
        else:
            for i in range(len(longs_df)):
                df_vx_test.loc[(df_vx_test['PrdCd']==longs_df['PRD_CD'][i])
                                  &
                                  (df_vx_test['WeekStartDate']>=longs_df['HANBAI_FROM_YMD'][i])
                                  &
                                  (df_vx_test['WeekStartDate']<=longs_df['HANBAI_TO_YMD'][i]), '店舗売変'] = longs_df['KIKAKU_BAIKA'][i]

            
    # 行方向に対してminをとりそれを店頭売価とする
    df_vx_test['BAIKA_NEW'] = df_vx_test[check_baika_list].min(axis=1)
    
    df_vx_test['BAIKA'][df_vx_test['WeekStartDate'] >= target_week_from_ymd] = df_vx_test[df_vx_test['WeekStartDate'] >= target_week_from_ymd]['BAIKA_NEW']
    
    check_baika_list.append('BAIKA_NEW')
    check_baika_list.remove('baika_toitsu')
    check_baika_list.remove('BAIKA')
    
    # holdout後のもののTANKA, 割引率、割引額をdrop
    df_vx_test = df_vx_test.drop(check_baika_list, axis=1)
    if output_collected_sales_value == False:
        df_vx_test = df_vx_test.drop('WeekStartDate', axis=1)


if OUTPUT_COLLECTED_SALES_VALUE:
    
    extract_start_nenshudo = calc_nenshudo(today_nenshudo, -13)
    extract_start_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == extract_start_nenshudo].values[0]
    today_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == today_nenshudo].values[0]

    # 13週前から先週までのデータを抽出
    df_vx_test_colected_sales_value =  df_vx_test[(extract_start_week_from_ymd <= df_vx_test['WeekStartDate'])&(df_vx_test['WeekStartDate'] < today_week_from_ymd)].reset_index(drop=True)
    
    df_vx_test_colected_sales_value = df_vx_test_colected_sales_value[['PrdCd', 'WeekStartDate', 'PreviousYearSalesActualQuantity', 'time_leap8', 'SalesAmount', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd', 'weekstartdatestamp', 'tenpo_cd', 'TenpoCdPrdCd']]
    
    df_vx_test_colected_sales_value['NENSHUDO'] = today_nenshudo
    
    
    if output_6wk_2sales:
        df_vx_test_colected_sales_value['MODEL_TYPE'] = 'MEDIUM_QTY'
    else:
        df_vx_test_colected_sales_value['MODEL_TYPE'] = 'WEEKLY'
    
    from google.cloud import bigquery

    if output_collected_sales_value_test_table == True:
        table_id = "dev-cainz-demandforecast.cainz_shortterm_predicted_value_for_statistics.corrected_sales_values_all_218strdebug"    
    else:
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
            job.result()

            upload_complete = True

        except Exception as e:
            print('errtype:', str(type(e)))
            print('err:', str(e))
            print('data upload retry')
            time.sleep(20)
    
    

if salesup_flag:
    
    def get_salesup_prdcd_list(my_salesup_table, my_flag_col_name, tenpo_cd):
        project_id = "dev-cainz-demandforecast"
        dataset_id = 'short_term_cloudrunjobs'
        target_query = f"""  SELECT DISTINCT PrdCd FROM `{dataset_id}.{my_salesup_table}` WHERE tenpo_cd = """ + str(tenpo_cd) \
                        + f""" AND {my_flag_col_name} > 0"""    
        print(target_query)
        salesup_prdcd_df = pd.read_gbq(target_query, project_id, dialect='standard')
        salesup_prdcd_list = list(salesup_prdcd_df['PrdCd'].astype(int))
        return salesup_prdcd_list
    
    if output_6wk_2sales:
        salesup_train_table_name_list = [
                              'weekly-train-2025-06-10_obon_218str_wk6sls20div',
                              'weekly-train-2025-06-10_obon_218str_wk6sls230div',
                            ]
        
    else:
        salesup_train_table_name_list = [
                              'weekly-train-2025-06-10_obon_218str0div',
                              'weekly-train-2025-06-10_obon_218str30div',
                            ]
    
    
    flag_col_name_list = ['BusyPeriodFlagNenmatsu',
                          'BusyPeriodFlagGw',
                          'BusyPeriodFlagNewLife',
                          'BusyPeriodFlagEndRainySsn',
                          'BusyPeriodFlagObon']
    
    for my_flag_col_name in flag_col_name_list:
        df_vx_test[my_flag_col_name] = 0
        for my_salesup_table in salesup_train_table_name_list:
            my_salesup_prdcd_list = get_salesup_prdcd_list(my_salesup_table, my_flag_col_name, tenpo_cd)
            df_vx_test[my_flag_col_name][df_vx_test['PrdCd'].isin(my_salesup_prdcd_list)] = 1



            
if devide_season_items:

    project_id = "dev-cainz-demandforecast"
    dataset_id = 'short_term_cloudrunjobs'
    table_id = SEASONAL_TRAINDATA_TABLE

    target_query = f"""  SELECT DISTINCT PrdCd FROM `{dataset_id}.{table_id}` WHERE tenpo_cd = """ + str(tenpo_cd)
    print(target_query)
    seasonal_prdcd_df = pd.read_gbq(target_query, project_id, dialect='standard')
    seasonal_prdcd_list = list(seasonal_prdcd_df['PrdCd'].astype(int))

    df_vx_test_seasonal = df_vx_test[df_vx_test['PrdCd'].isin(seasonal_prdcd_list)]
    
    # テストデータ期間に絞っている *********************************************************************
    df_vx_test_seasonal =  df_vx_test_seasonal[df_vx_test_seasonal['WeekStartDate'] >= start_week_from_ymd].reset_index(drop=True)
    df_vx_test_seasonal =  df_vx_test_seasonal[df_vx_test_seasonal['WeekStartDate'] <= end_week_from_ymd].reset_index(drop=True)
    df_vx_test_seasonal['SalesAmount'][df_vx_test_seasonal['WeekStartDate'] >= target_week_from_ymd] = np.nan
    # ******************************************************************************************************
    df_vx_test_seasonal = df_vx_test_seasonal.drop('WeekStartDate', axis=1)
    

    table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-test-seasonal-" + str(today)  + str(OUTPUT_TABLE_SUFFIX)

    
    upload_complete = False
    while upload_complete == False:
        try:
            client = BigqueryClient()
            job = client.load_table_from_dataframe(df_vx_test_seasonal, table_id)
            job.result()
            print("==data-uploaded-bq===")
            
            upload_complete = True

        except Exception as e:
            print('errtype:', str(type(e)))
            print('err:', str(e))
            print('data upload retry')
            time.sleep(20)
            
            
            
    
    # df_vx_testをシーズン品以外のデータとする
    df_vx_test = df_vx_test[~df_vx_test['PrdCd'].isin(seasonal_prdcd_list)]


if divide_by_salesamount_v2:     
    
    def set_div_points(x, div_points):
        if x is None:
            return 0
        prev_dp = 0
        for dp in div_points:
            if x > dp:
                prev_dp = dp
            else:
                return prev_dp
        return None
    
    div_points = [0, 30, 999999]
    
    if divide_by_salesamount_v3:
        div_points = [0, 10, 30, 999999]
    

    if turn_back_time:
        # todayを巻き戻し日にする
        #today = datetime.datetime.now(JST)
    
        today_turn_back = pd.to_datetime(today_date_str, format='%Y%m%d')
        #today_turn_back = datetime.date(today.year, today.month, today.day)
        
        last13week_date = today_turn_back - datetime.timedelta(days=int(4.3*13)) - datetime.timedelta(days=7)
        last13week_date = pd.Timestamp(last13week_date)
        prev_week_date = today_turn_back - datetime.timedelta(days=7)
        
        
    else:
        last13week_date = today - datetime.timedelta(days=int(4.3*13)) - datetime.timedelta(days=7)
        last13week_date = pd.Timestamp(last13week_date)
        prev_week_date = today - datetime.timedelta(days=7)
    
    
    #df_vx_test_last13week = df_vx_test[df_vx_test['weekstartdatestamp'] >= last13week_date]
    df_vx_test_last13week = df_vx_test[(last13week_date <= df_vx_test['weekstartdatestamp'])&(df_vx_test['weekstartdatestamp'] < pd.Timestamp(prev_week_date))]
    
    df_vx_test_last13week['URISU_AVE'] = df_vx_test_last13week.groupby("PrdCd",as_index=False)['SalesAmount'].transform(lambda x:x.mean())
    df_vx_test_last13week['DivPoint'] = df_vx_test_last13week['URISU_AVE'].apply(lambda x:set_div_points(x, div_points))
    
    #df_vx_test_last13week.to_csv('df_vx_test_last13week.csv')
    
    df_vx_test_last13week = df_vx_test_last13week[['PrdCd', 'URISU_AVE', 'DivPoint']].drop_duplicates().reset_index(drop=True)
    df_vx_test = pd.merge(df_vx_test, df_vx_test_last13week, on='PrdCd', how='left').reset_index(drop=True)
    
    #df_vx_test.to_csv('df_vx_test.csv')
    
    
    # テストデータ期間に絞っている *********************************************************************
    df_vx_test =  df_vx_test[df_vx_test['WeekStartDate'] >= start_week_from_ymd].reset_index(drop=True)
    df_vx_test =  df_vx_test[df_vx_test['WeekStartDate'] <= end_week_from_ymd].reset_index(drop=True)
    df_vx_test['SalesAmount'][df_vx_test['WeekStartDate'] >= target_week_from_ymd] = np.nan
    # ******************************************************************************************************
    df_vx_test = df_vx_test.drop('WeekStartDate', axis=1)
    
    prediction_table_name_list = []
    for dp in div_points:
        if dp != div_points[-1]:# 分割店リストの最後の要素は反映されていないのでスキップする        
            df_vx_test_tmp = df_vx_test[df_vx_test['DivPoint']==dp]       
    
            table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-test-" + str(today)  + str(OUTPUT_TABLE_SUFFIX) + '-' + str(dp) + 'div'
        
            prediction_table_name_list.append("weekly-test-" + str(today)  + str(OUTPUT_TABLE_SUFFIX) + '-' + str(dp) + 'div')
    
            if len(df_vx_test_tmp):
                upload_complete = False
                while upload_complete == False:
                    try:    
                        client = BigqueryClient()
                        job = client.load_table_from_dataframe(df_vx_test_tmp, table_id)
                        job.result()

                        print("==data-uploaded-bq===")

                        end_t = time.time()
                        elapsed_time = end_t - start_t
                        print(f"Elapsed time: {elapsed_time:.3f} seconds")
                        upload_complete = True

                    except:
                        print('data upload retry')
                        time.sleep(20)
                        
                        
    prediction_table_name_small = prediction_table_name_list[0]
    prediction_table_name_large = prediction_table_name_list[1]
    
else:
    if THEME_MD_MODE: 
        # テストデータ期間に絞っている *********************************************************************
        df_vx_test =  df_vx_test[df_vx_test['WeekStartDate'] >= start_week_from_ymd].reset_index(drop=True)
        df_vx_test =  df_vx_test[df_vx_test['WeekStartDate'] <= end_week_from_ymd].reset_index(drop=True)
        df_vx_test['SalesAmount'][df_vx_test['WeekStartDate'] >= target_week_from_ymd] = np.nan
        # ******************************************************************************************************
        df_vx_test = df_vx_test.drop('WeekStartDate', axis=1)
        
        if 1:
            df_vx_test['theme_md_div'] = 0
            df_vx_test['theme_md_div'][df_vx_test['PrdCd'].isin(this_tenpo_theme_md_prdcd_list)] = 1
        else:
            df_vx_test = pd.merge(df_vx_test, theme_md_df[['店番', 'JANコード', 'テーマMD種別']].rename(columns={'テーマMD種別':'theme_md_div'}), left_on=['tenpo_cd', 'PrdCd'], right_on=['店番', 'JANコード'], how='left').reset_index(drop=True)

            df_vx_test['is_theme_md'] = 0
            df_vx_test['is_theme_md'][~df_vx_test['theme_md_div'].isna()] = 1

            df_vx_test = df_vx_test.drop('店番', axis=1)
            df_vx_test = df_vx_test.drop('JANコード', axis=1)
        

    prediction_table_name = "weekly-test-" + str(today)  + str(OUTPUT_TABLE_SUFFIX)
    
    if add_reference_store_unitedmodel:
        table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-test-" + str(today)  + str(OUTPUT_TABLE_SUFFIX)
    else:
        if tenpo_cd_ref is None:      
            table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-test-" + str(today)  + str(OUTPUT_TABLE_SUFFIX)
        else:
            table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-test-new-store-" + str(today)  + str(OUTPUT_TABLE_SUFFIX)

    
    if len(df_vx_test) > 0:
        upload_complete = False
        while upload_complete == False:
            try:    
                client = BigqueryClient()
                job = client.load_table_from_dataframe(df_vx_test, table_id)
                job.result()

                print("==data-uploaded-bq===")

                end_t = time.time()
                elapsed_time = end_t - start_t
                print(f"Elapsed time: {elapsed_time:.3f} seconds")
                upload_complete = True

            except:
                print('data upload retry')
                time.sleep(20)

    else:
        print('test data len=0 fin tenpo_cd:', tenpo_cd)


from google.cloud import bigquery, storage
import requests
import google.auth
import google.auth.transport.requests
import google.oauth2.id_token
 

# 週次モデル ***************************************************************************************************
# cloud functions name  :vertex-pipeline-shortterm1-weekly
# https://console.cloud.google.com/functions/details/asia-northeast1/vertex-pipeline-shortterm1-weekly?env=gen2&authuser=0&project=dev-cainz-demandforecast
#
# functions url:
# https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/vertex-pipeline-shortterm1-weekly

#def trigger_cloud_function_weekly(prediction_table_name):
def trigger_cloud_function_weekly(prediction_table_name_large, prediction_table_name_small):
    #url = 'https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/vertex-pipeline-shortterm1-weekly'
    url = 'https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/vertex-pipeline-shortterm1-weekly2'
    data = {
        #'prediction_table': prediction_table_name,        
        'prediction_table_large':prediction_table_name_large,
        'prediction_table_small':prediction_table_name_small,
    }
    
    if 0:
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
            print('Function response:', response_data)
        except Exception as error:
            print('Error calling Cloud Function:', error)

    else:
        try:
            response = requests.post(url, json=data)  # POST リクエストを送信
            # GETリクエストの場合は、データをクエリパラメーターとしてURLに追加

            if response.status_code != 200:
                raise Exception(f'HTTP error! status: {response.status_code}')

            response_data = response.json()
            print('Function response:', response_data)
        except Exception as error:
            print('Error calling Cloud Function:', error)
            
            
def trigger_cloud_function_medium(prediction_table_name_large, prediction_table_name_small):
    url = 'https://asia-northeast1-dev-cainz-demandforecast.cloudfunctions.net/vertex-pipeline-shortterm1-midium'
    
    data = {    
        'prediction_table_midqty_large':prediction_table_name_large,
        'prediction_table_midqty_small':prediction_table_name_small,
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
        print('Function response:', response_data)
    except Exception as error:
        print('Error calling Cloud Function:', error)

            
            
            


if cloudrunjob_mode and CALL_NEXT_PIPELINE:
        
    # stage2完了チェックフォルダ配下への完了ファイルアップロード
    if output_6wk_2sales:
        path_upload_blob = "vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_medium_complete/completed_" + str(tenpo_cd) + ".csv"
        blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_medium_complete/completed_')
    else:
        path_upload_blob = "vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_weekly_complete/completed_" + str(tenpo_cd) + ".csv"
        blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_weekly_complete/completed_')
    tmp_fname = str(tenpo_cd) + ".csv"
    my_df = pd.DataFrame([[tenpo_cd]])
    my_df.to_csv(tmp_fname, index=False)

    blob = bucket.blob(path_upload_blob)
    blob.upload_from_filename(tmp_fname)


    # 全店分のstage2が終了しているかチェックする
    complete_task_count = sum([1 for blob in blobs])

    if TASK_COUNT == complete_task_count:
        #trigger_cloud_function_weekly(prediction_table_name)
        
        if output_6wk_2sales:
            trigger_cloud_function_medium(prediction_table_name_large, prediction_table_name_small)
        else:
            trigger_cloud_function_weekly(prediction_table_name_large, prediction_table_name_small)
        
        print('call batch prediction and post process pipeline complete*******************')

        
        
        if output_6wk_2sales:
            # stage2完了チェックフォルダ配下のファイル削除
            blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_medium_complete/completed_')

        else:
            # stage2完了チェックフォルダ配下のファイル削除
            blobs = storage_client.list_blobs(bucket, prefix='vertex_pipelines/pipeline/pipeline_shortterm1/check_stage2_weekly_complete/completed_')
            
            
        for blob in blobs:
            print(blob.name)
            generation_match_precondition = None
            blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
            generation_match_precondition = blob.generation
            blob.delete(if_generation_match=generation_match_precondition)
            print(f"Blob {blob.name} deleted.")
