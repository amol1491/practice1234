import os
from google.cloud.bigquery import Client as BigqueryClient
import pandas as pd
import sys
from google.cloud import storage
import datetime
from datetime import date,timedelta
from io import BytesIO
import datetime
from datetime import timedelta
import numpy as np
import copy
import bisect
import time
import logging


cloudrunjob_mode = True

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
OUTPUT_TABLE_SUFFIX = os.environ.get("OUTPUT_TABLE_SUFFIX", "")
TURN_BACK_YYYYMMDD = os.environ.get("TURN_BACK_YYYYMMDD", "")

# print('TASK_INDEX:', TASK_INDEX)
# print('TASK_COUNT:', TASK_COUNT)
# print('TODAY_OFFSET:', TODAY_OFFSET)
# print('OUTPUT_TABLE_SUFFIX:', OUTPUT_TABLE_SUFFIX)
# print('TURN_BACK_YYYYMMDD:', TURN_BACK_YYYYMMDD)

logger.info(f'TASK_INDEX: {TASK_INDEX}')
logger.info(f'TASK_COUNT: {TASK_COUNT}')
logger.info(f'TODAY_OFFSET: {TODAY_OFFSET}')
logger.info(f'OUTPUT_TABLE_SUFFIX: {OUTPUT_TABLE_SUFFIX}')
logger.info(f'TURN_BACK_YYYYMMDD: {TURN_BACK_YYYYMMDD}')

start_t = time.time()

# tenpo_cd_list = [760, 294, 753, 809, 814, 836]
# 50店舗
# tenpo_cd_list = [760, 294, 753, 809, 814, 836, 156, 165, 231, 244, 256, 259, 268, 273, 274, 275, 277, 282, 284, 286, 287, 288, 289, 292, 296, 34, 730, 731, 734, 735, 737, 738, 743, 744, 750, 756, 764, 766, 768, 769, 775, 792, 793, 803, 813, 822, 827, 828, 832, 96] 


# 48店舗拡大 20230915
# tenpo_cd_list=[
# 760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828
# ]

# 48店舗拡大 + 鶴ヶ島＋鶴ヶ島資材館+その他資材館　20230920　54店舗
# tenpo_cd_list=[
# 760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, #252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, #284, 618, 253, 613, 615, 612]

# 48店舗拡大
# tenpo_cd_list=[
# 760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828]


# 48店舗拡大+小規模6店舗：54店舗
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242]


# 全店　246店
# tenpo_cd_list=[
# 760, 814, 294, 836, 809, 753, 20, 28, 31, 34, 47, 48, 50, 51, 52, 67, 69, 89, 96, 98, 102, 120, 132, 133, 134, 135, 136, 137, 139, 140, 143, 147, 151, 154, 155, 156, 157, 158, 162, 164, 165, 166, 167, 168, 230, 231, 232, 233, 234, 235, 236, 237, 238, 240, 242, 243, 244, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 295, 296, 612, 613, 615, 617, 618, 623, 664, 681, 682, 683, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 755, 756, 757, 758, 759, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 802, 803, 804, 806, 807, 808, 810, 811, 812, 813, 815, 816, 817, 818, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 837, 838, 839, 840, 842, 843, 844, 845, 848, 849, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 866, 867, 869, 870, 871, 872, 873, 875, 876, 877, 878, 879, 624, 684, 868, 874
# ]


# 全店246店から2021年以降開店を除く229店（stage2以降に使用）
# tenpo_cd_list=[
# 760, 814, 294, 836, 809, 753, 20, 28, 31, 34, 47, 48, 50, 51, 52, 67, 69, 89, 96, 98, 102, 120, 132, 133, 134, 135, 136, 137, 139, 140, 143, 147, 151, 154, 155, 156, 157, 158, 162, 164, 165, 166, 167, 168, 230, 231, 232, 233, 234, 235, 236, 237, 238, 240, 242, 243, 244, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 295, 296, 612, 613, 615, 617, 618, 623, 664, 681, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 755, 756, 757, 758, 759, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 802, 803, 804, 806, 807, 808, 810, 811, 812, 813, 815, 816, 817, 818, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 837, 838, 839, 840, 842, 843, 844, 845, 848, 849, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 871]


# 全店246店から2021年以降開店を除く236店（stage2以降に使用, 新店10店舗追加）              (870:ＳＦららぽーと立川立飛 872:ＳＦららぽーと湘南平塚 削除)
# tenpo_cd_list=[
# 760, 814, 294, 836, 809, 753, 20, 28, 31, 34, 47, 48, 50, 51, 52, 67, 69, 89, 96, 98, 102, 120, 132, 133, 134, 135, 136, 137, 139, 140, 143, 147, 151, 154, 155, 156, 157, 158, 162, 164, 165, 166, 167, 168, 230, 231, 232, 233, 234, 235, 236, 237, 238, 240, 242, 243, 244, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 295, 296, 612, 613, 615, 617, 618, 623, 664, 681, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 755, 756, 757, 758, 759, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 802, 803, 804, 806, 807, 808, 810, 811, 812, 813, 815, 816, 817, 818, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 837, 838, 839, 840, 842, 843, 844, 845, 848, 849, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 871,
# 869, 871, 873, 874, 875, 876, 877, 878
# ]


# 全店拡大20240813 209店舗
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
]


'''
# 全店拡大20240919 217店舗
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 
]
'''

'''
# 全店拡大20240926年末積み増し対応 239店舗(22店舗追加)
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 
50, 137, 154, 235, 613, 615, 617, 618, 623, 624, 741, 767, 794, 799, 808, 812, 842, 868, 875, 878, 903, 909,
]
'''

# 全店拡大20250130 218店舗（古河は13週経過したので入れる）
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 910
]


bucket_name = "dev-cainz-demandforecast"

# stage1の前日以前の過去データを使うフラグ
use_past_stage1_data_flag = True
past_days_num = int(TODAY_OFFSET)

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, 'JST')

# if use_past_stage1_data_flag:
#    today = datetime.date.today() - timedelta(days=past_days_num)    
# else:
#    today = datetime.date.today()

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

logger.info(f'************ today: {today}')

# flag設定 *************************************************************
# JAN差し替えファイルを使うかどうか指定する
use_jan_connect = True

# 実績値が0以下を0に補正する
correct_sales_minus = False

# 目的変数を対数化
logarithmize_target_variable = False

# ODAS補正の改良を有効にする
odas_correct_imprv = True

# 階層モデルを使う
# hierarchical_model = True

# 爆買い補正を使う
bakugai_hosei = True

# モデル2分割(3分割のときはFalseにする)
model_devide_2 = False

# MinMaxにない商品、MinMax下限が0の商品を除く
restrict_minmax = True

# 店舗発注が終わっている商品を除く
restrinct_tenpo_hacchu_end = True

# 販売のない販売期間に販売のある期間の売り数をコピーする
no_sales_term_copy_existsales = False
# さらに、販売数0のところに平均値をセットする
no_sales_term_set_ave = False

# 販売のない販売期間のweightを0にする(20240722シーズン品モデルで本番化)
no_sales_term_weight_zero = True

# さらに、販売のある期間で販売数が０の週のweightを半分にする
# sales0_on_salesterm_weight_half = False
# sales0_on_salesterm_weight_half_coef = 0.1

# 年末のweightを0にする
# nenmatsu_weight_zero = False

# 価格を事前に既知にする(Trainデータ作成時は特に処理変更は無し)
# kakaku_jizen_kichi = True


# 月別で予測するデータを出力する
# monthly_data_pred = False(処理不要により削除　7/4)

# シーズン品の分離
devide_season_items = True

# モデル学習上の現在日時を巻き戻す
turn_back_time = False
#turn_back_yyyymmdd = 20230425
if turn_back_time:
    turn_back_yyyymmdd = int(TURN_BACK_YYYYMMDD)

# 新店の場合、参照店舗の全データを使ってモデルを作成する（新店にない商品のデータも入る）
add_reference_store = True
# 新店の場合、新店にある商品番号のみで参照店舗の過去データを使ってモデルを作成する
# (そもそも新店の商品に現地しなくてよい、ここはfalseでよい)
add_reference_store_by_prdcd = False

# 繁忙期フラグ
salesup_flag = True

# 93を追加 20240304
dpt_list = [69,97,14,37,27,39,28,74,33,30,36,75,85,80,20,22,55,72,15,62,32,77,84,89,23,60,25,87,68,56,92,61,2,40,86,88,26,17,24,34,52,64,73,21,35,58,83,94,63,38,18,29,19,31,53,45,50,81,82,90,91,54,95,93]

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)



def extract_as_df_with_encoding(SOURCE_BLOB_NAME, BUCKET_NAME, encoding):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(SOURCE_BLOB_NAME)
    with blob.open(mode="rb") as f:
        df = pd.read_csv(f, encoding=encoding)
    return df


def get_last_year_sales_amount(df):
    df_tmp = df[["商品コード","週開始日付","8週平均ema"]]
    df_tmp = df_tmp.rename(columns={'週開始日付': '前年週開始日付','8週平均ema': 'time_leap8'})
    df_tmp2 = pd.merge(df,df_tmp,on = ["商品コード","前年週開始日付"])
    return df_tmp2


def get_last_year_sales_amount_ahead(df_train_data, df_calendar):
    after_one = datetime.timedelta(weeks=+1)
    after_two = datetime.timedelta(weeks=+2)
    after_three = datetime.timedelta(weeks=+3)
    after_four = datetime.timedelta(weeks=+4)
    after_five = datetime.timedelta(weeks=+5)
    after_six = datetime.timedelta(weeks=+6)
    after_seven = datetime.timedelta(weeks=+7)

    df_train_data_tmp = df_train_data[['商品コード','週開始日付', '売上実績数量']]
    df_train_data_tmp['週開始日付'] = pd.to_datetime(df_train_data_tmp['週開始日付'].astype(str), format="%Y-%m-%d")
    df_train_data_tmp = df_train_data_tmp.drop_duplicates().reset_index(drop=True)

    df_train_data_tmp2 = df_train_data[['商品コード','週開始日付', '売上実績数量']]
    df_train_data_tmp2['週開始日付'] = pd.to_datetime(df_train_data_tmp2['週開始日付'].astype(str), format="%Y-%m-%d")
    df_train_data_tmp2 = df_train_data_tmp2.drop_duplicates().reset_index(drop=True)

    df_train_data_tmp3 = df_train_data[['商品コード','週開始日付', '売上実績数量']]
    df_train_data_tmp3['週開始日付'] = pd.to_datetime(df_train_data_tmp3['週開始日付'].astype(str), format="%Y-%m-%d")
    df_train_data_tmp3 = df_train_data_tmp3.drop_duplicates().reset_index(drop=True)

    df_train_data_tmp4 = df_train_data[['商品コード','週開始日付', '売上実績数量']]
    df_train_data_tmp4['週開始日付'] = pd.to_datetime(df_train_data_tmp4['週開始日付'].astype(str), format="%Y-%m-%d")
    df_train_data_tmp4 = df_train_data_tmp4.drop_duplicates().reset_index(drop=True)

    df_train_data_tmp5 = df_train_data[['商品コード','週開始日付', '売上実績数量']]
    df_train_data_tmp5['週開始日付'] = pd.to_datetime(df_train_data_tmp5['週開始日付'].astype(str), format="%Y-%m-%d")
    df_train_data_tmp5 = df_train_data_tmp5.drop_duplicates().reset_index(drop=True)

    df_train_data_tmp6 = df_train_data[['商品コード','週開始日付', '売上実績数量']]
    df_train_data_tmp6['週開始日付'] = pd.to_datetime(df_train_data_tmp6['週開始日付'].astype(str), format="%Y-%m-%d")
    df_train_data_tmp6 = df_train_data_tmp6.drop_duplicates().reset_index(drop=True)

    df_train_data_tmp7 = df_train_data[['商品コード','週開始日付', '売上実績数量']]
    df_train_data_tmp7['週開始日付'] = pd.to_datetime(df_train_data_tmp7['週開始日付'].astype(str), format="%Y-%m-%d")
    df_train_data_tmp7 = df_train_data_tmp7.drop_duplicates().reset_index(drop=True)

    df_train_data['前年週開始日付'] = pd.to_datetime(df_train_data['前年週開始日付'].astype(str), format="%Y-%m-%d")

    df_train_data['前年週開始日付oneweeklater'] = df_train_data['前年週開始日付'] + pd.offsets.Week(n=1, weekday=0)
    df_train_data['前年週開始日付twoweeklater'] = df_train_data['前年週開始日付'] + pd.offsets.Week(n=2, weekday=0)
    df_train_data['前年週開始日付threeweeklater'] = df_train_data['前年週開始日付'] + pd.offsets.Week(n=3, weekday=0)
    df_train_data['前年週開始日付fourweeklater'] = df_train_data['前年週開始日付'] + pd.offsets.Week(n=4, weekday=0)
    df_train_data['前年週開始日付fiveweeklater'] = df_train_data['前年週開始日付'] + pd.offsets.Week(n=5, weekday=0)
    df_train_data['前年週開始日付sixweeklater'] = df_train_data['前年週開始日付'] + pd.offsets.Week(n=6, weekday=0)
    df_train_data['前年週開始日付sevenweeklater'] = df_train_data['前年週開始日付'] + pd.offsets.Week(n=7, weekday=0)

    df_train_data2 = df_train_data[['商品コード','売上実績数量','前年売上実績数量', '前年週開始日付', '週開始日付','前年週開始日付oneweeklater', '前年週開始日付twoweeklater', '前年週開始日付threeweeklater','前年週開始日付fourweeklater', '前年週開始日付fiveweeklater','前年週開始日付sixweeklater','前年週開始日付sevenweeklater']]
    df_train_data2 = df_train_data2.rename(columns={'売上実績数量': '売上実績数量saved'})
    df_train_data2 = df_train_data2.drop_duplicates().reset_index(drop=True)

    df_train_data_tmp = df_train_data_tmp.rename(columns={'週開始日付': '前年週開始日付oneweeklater'})
    df_train_data_tmp2 = df_train_data_tmp2.rename(columns={'週開始日付': '前年週開始日付twoweeklater'})
    df_train_data_tmp3 = df_train_data_tmp3.rename(columns={'週開始日付': '前年週開始日付threeweeklater'})
    df_train_data_tmp4 = df_train_data_tmp4.rename(columns={'週開始日付': '前年週開始日付fourweeklater'})
    df_train_data_tmp5 = df_train_data_tmp5.rename(columns={'週開始日付': '前年週開始日付fiveweeklater'})
    df_train_data_tmp6 = df_train_data_tmp6.rename(columns={'週開始日付': '前年週開始日付sixweeklater'})    
    df_train_data_tmp7 = df_train_data_tmp7.rename(columns={'週開始日付': '前年週開始日付sevenweeklater'})

    df_train_data2['前年週開始日付oneweeklater'] = df_train_data2['前年週開始日付oneweeklater'].apply(lambda x: x.value)
    df_train_data2['前年週開始日付twoweeklater'] = df_train_data2['前年週開始日付twoweeklater'].apply(lambda x: x.value) 
    df_train_data2['前年週開始日付threeweeklater'] = df_train_data2['前年週開始日付threeweeklater'].apply(lambda x: x.value)
    df_train_data2['前年週開始日付fourweeklater'] = df_train_data2['前年週開始日付fourweeklater'].apply(lambda x: x.value)
    df_train_data2['前年週開始日付fiveweeklater'] = df_train_data2['前年週開始日付fiveweeklater'].apply(lambda x: x.value)
    df_train_data2['前年週開始日付sixweeklater'] = df_train_data2['前年週開始日付sixweeklater'].apply(lambda x: x.value)
    df_train_data2['前年週開始日付sevenweeklater'] = df_train_data2['前年週開始日付sevenweeklater'].apply(lambda x: x.value)

    df_train_data_tmp['前年週開始日付oneweeklater'] = df_train_data_tmp['前年週開始日付oneweeklater'].apply(lambda x: x.value)
    df_train_data_tmp2['前年週開始日付twoweeklater'] = df_train_data_tmp2['前年週開始日付twoweeklater'].apply(lambda x: x.value) 
    df_train_data_tmp3['前年週開始日付threeweeklater'] = df_train_data_tmp3['前年週開始日付threeweeklater'].apply(lambda x: x.value) 
    df_train_data_tmp4['前年週開始日付fourweeklater'] = df_train_data_tmp4['前年週開始日付fourweeklater'].apply(lambda x: x.value) 
    df_train_data_tmp5['前年週開始日付fiveweeklater'] = df_train_data_tmp5['前年週開始日付fiveweeklater'].apply(lambda x: x.value) 
    df_train_data_tmp6['前年週開始日付sixweeklater'] = df_train_data_tmp6['前年週開始日付sixweeklater'].apply(lambda x: x.value) 
    df_train_data_tmp7['前年週開始日付sevenweeklater'] = df_train_data_tmp7['前年週開始日付sevenweeklater'].apply(lambda x: x.value) 

    df_train_data2 = pd.merge(df_train_data2, df_train_data_tmp, on=['商品コード','前年週開始日付oneweeklater']).drop('前年週開始日付oneweeklater', axis=1)
    df_train_data2 = df_train_data2.rename(columns={'売上実績数量': '前年売上実績数量oneweeklater'})

    df_train_data2 = pd.merge(df_train_data2, df_train_data_tmp2, on=['商品コード','前年週開始日付twoweeklater']).drop('前年週開始日付twoweeklater', axis=1)
    df_train_data2 = df_train_data2.rename(columns={'売上実績数量': '前年売上実績数量twoweeklater'})

    # あとは拡張する
    df_train_data2 = pd.merge(df_train_data2, df_train_data_tmp3, on=['商品コード','前年週開始日付threeweeklater']).drop('前年週開始日付threeweeklater', axis=1)
    df_train_data2 = df_train_data2.rename(columns={'売上実績数量': '前年売上実績数量threeweeklater'})

    df_train_data2 = pd.merge(df_train_data2, df_train_data_tmp4, on=['商品コード','前年週開始日付fourweeklater']).drop('前年週開始日付fourweeklater', axis=1)
    df_train_data2 = df_train_data2.rename(columns={'売上実績数量': '前年売上実績数量fourweeklater'})

    df_train_data2 = pd.merge(df_train_data2, df_train_data_tmp5, on=['商品コード','前年週開始日付fiveweeklater']).drop('前年週開始日付fiveweeklater', axis=1)
    df_train_data2 = df_train_data2.rename(columns={'売上実績数量': '前年売上実績数量fiveweeklater'})

    df_train_data2 = pd.merge(df_train_data2, df_train_data_tmp6, on=['商品コード','前年週開始日付sixweeklater']).drop('前年週開始日付sixweeklater', axis=1)
    df_train_data2 = df_train_data2.rename(columns={'売上実績数量': '前年売上実績数量sixweeklater'})

    df_train_data2 = pd.merge(df_train_data2, df_train_data_tmp7, on=['商品コード','前年週開始日付sevenweeklater']).drop('前年週開始日付sevenweeklater', axis=1)
    df_train_data2 = df_train_data2.rename(columns={'売上実績数量': '前年売上実績数量sevenweeklater'})

    df_train_data2['time_leap8'] = (df_train_data2['前年売上実績数量'] + df_train_data2['前年売上実績数量oneweeklater'] + df_train_data2['前年売上実績数量twoweeklater'] + df_train_data2['前年売上実績数量threeweeklater'] + df_train_data2['前年売上実績数量fourweeklater'] + df_train_data2['前年売上実績数量fiveweeklater'] + df_train_data2['前年売上実績数量sixweeklater'] + df_train_data2['前年売上実績数量sevenweeklater']) / 8

    df_train_data2 = df_train_data2.rename(columns={'売上実績数量saved': '売上実績数量'})
    df_train_data3 = df_train_data2[['商品コード','週開始日付','time_leap8']]
    df_train_data3 = df_train_data3.drop_duplicates().reset_index(drop=True)

    return df_train_data3


def get_last_year_sales_amount_ahead2(df_train_data, df_calendar):
    after_one = datetime.timedelta(weeks=+1)
    after_two = datetime.timedelta(weeks=+2)
    after_three = datetime.timedelta(weeks=+3)
    after_four = datetime.timedelta(weeks=+4)
    after_five = datetime.timedelta(weeks=+5)
    after_six = datetime.timedelta(weeks=+6)
    after_seven = datetime.timedelta(weeks=+7)    

    df_train_data['前年週開始日付'] = pd.to_datetime(df_train_data['前年週開始日付'], format = '%Y%m%d')
    df_train_data['週開始日付'] = pd.to_datetime(df_train_data['週開始日付'], format = '%Y%m%d')

    # print(df_train_data['週開始日付'])
    logger.info(f"df_train_data['週開始日付']: {df_train_data['週開始日付']}")

    df_train_data2 = df_train_data

    for ii in range(len(df_train_data)):
        prd_cd = df_train_data['商品コード'][ii]
        week_start_date = df_train_data['前年週開始日付'][ii]
        sales_amout_zen = df_train_data['前年売上実績数量'][ii]

        week_start_one = (week_start_date + after_one).strftime('%Y-%m-%d')
        week_start_two = (week_start_date + after_two).strftime('%Y-%m-%d')
        week_start_three = (week_start_date + after_three).strftime('%Y-%m-%d')
        week_start_four = (week_start_date + after_four).strftime('%Y-%m-%d')
        week_start_five = (week_start_date + after_five).strftime('%Y-%m-%d')
        week_start_six = (week_start_date + after_six).strftime('%Y-%m-%d')
        week_start_seven = (week_start_date + after_seven).strftime('%Y-%m-%d')

        # print("===week_start_one====",week_start_one)
        logger.info(f"===week_start_one==== {week_start_one}")

        # n週後を取得する
        sales_amount_one = df_train_data2['前年売上実績数量'][(df_train_data2['前年週開始日付'] == week_start_one) & (df_train_data2['商品コード'] == prd_cd)].values[0]
        sales_amount_two = df_train_data2['前年売上実績数量'][(df_train_data2['前年週開始日付'] == week_start_two) & (df_train_data2['商品コード'] == prd_cd)].values[0]
        sales_amount_three = df_train_data2['前年売上実績数量'][(df_train_data2['前年週開始日付'] == week_start_three) & (df_train_data2['商品コード'] == prd_cd)].values[0]
        sales_amount_four = df_train_data2['前年売上実績数量'][(df_train_data2['前年週開始日付'] == week_start_four) & (df_train_data2['商品コード'] == prd_cd)].values[0]
        sales_amount_five = df_train_data2['前年売上実績数量'][(df_train_data2['前年週開始日付'] == week_start_five) & (df_train_data2['商品コード'] == prd_cd)].values[0]
        sales_amount_six = df_train_data2['前年売上実績数量'][(df_train_data2['前年週開始日付'] == week_start_six) & (df_train_data2['商品コード'] == prd_cd)].values[0]
        sales_amount_seven = df_train_data2['前年売上実績数量'][(df_train_data2['前年週開始日付'] == week_start_seven) & (df_train_data2['商品コード'] == prd_cd)].values[0]

        sales_amount_mean = (sales_amout_zen + sales_amount_one + sales_amount_two +  sales_amount_three + sales_amount_four + sales_amount_five + sales_amount_six + sales_amount_seven) / 8

        # print("====sales_amount_mean====",prd_cd,sales_amount_mean)
        logger.info(f'====sales_amount_mean==== {prd_cd} {sales_amount_mean}')

    return df_train_data


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

    #merged_sales_df = pd.merge(df_calendar_tmp, sales_df[["key", "URI_SU", "TENPO_CD"]], how="left", on="key")
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
  

    sales_df["key"] = sales_df["nenshudo"].astype(str).str.cat(sales_df["PRD_CD"].astype(str), sep='-')
    df_calendar_tmp["key"] = df_calendar_tmp["nenshudo"].astype(str).str.cat(df_calendar_tmp["PRD_CD"].astype(str), sep='-')

    #merged_sales_df = pd.merge(df_calendar_tmp, sales_df[["key", "URI_SU", "TENPO_CD"]], how="left", on="key")
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


def correct_customer_order(
    df,
    df_odas_old,
    upper_th,
    end_train_nenshudo
):
    median_col_name = "現在の地点までの中央値(0以外)"
    df = pd.merge(
        df,
        df_odas_old.rename(columns={"店番":"TENPO_CD", "JAN":"PRD_CD", "売上計上日":"week_from_ymd"}), 
        how="left",
        on=["TENPO_CD","PRD_CD","week_from_ymd"]
    )
    df['数量'] = df['数量'].fillna(0)
    df.loc[df['URI_SU']>0, 'URI_SU'] = df['URI_SU'] - df['数量']
    df = df.drop('数量', axis=1)
    
    df = df.sort_values("week_from_ymd").reset_index(drop=True)
    df[median_col_name] = df["URI_SU"]
    df.loc[df[median_col_name]<=0, median_col_name] = np.nan
    df[median_col_name] =\
        df.groupby(["TENPO_CD", "PRD_CD"])[median_col_name].transform(lambda x: x.shift(1).expanding(0).median())
    df.loc[df[median_col_name].isnull(), median_col_name] = df['URI_SU']
    df.loc[df["URI_SU"]<0, "URI_SU"] = df[median_col_name]
    
    return df


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


def odas_correct(df_calendar, tenpo_cd, use_jan_connect=True):
    
    if use_jan_connect:
        # old odas 
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
        df_odas_old = load_multiple_df(
            path_odas_list,
            bucket_name
        )
        
        # インデント変更20230803
        df_odas_old = df_odas_old.loc[~df_odas_old['JAN'].isnull()]
        df_odas_old = df_odas_old.loc[df_odas_old['店番'] == int(tenpo_cd)]

        # 集計処理に必要なマスターのロードと加工
        path_week_master = "short_term_train/01_Data/10_week_m/WEEK_MST.csv"
        dfc = extract_as_df(path_week_master, bucket_name)
        df_cal = get_df_cal_out_calender(
            dfc,
            dfc['nenshudo'].min(),
            dfc['nenshudo'].max(),
        )

        # 売上計上日を2分探索でweek_from_ymdに変換する（2分探索でないほうが高速化できそうだが・・要改善）
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
        # ここまで20230803

    df_odas_old = df_odas_old.loc[df_odas_old['店番'] == int(tenpo_cd)].reset_index(drop=True)    
    df_odas_old = df_odas_old.rename(columns={"店番":"TENPO_CD", "JAN":"PRD_CD", "数量": "odas_amount","売上計上日":"sales_ymd"})

    # 年週度を設定しているが、ループする必要ないのでは・・・？要改善
    df_odas_calender = pd.DataFrame()
    for wfy,wty,nsd in zip(df_calendar["week_from_ymd"],df_calendar["week_to_ymd"],df_calendar["nenshudo"]):
        tmp = df_odas_old[(df_odas_old["sales_ymd"]>=wfy)&(df_odas_old["sales_ymd"]<=wty)]
        tmp["nenshudo"] = nsd
        df_odas_calender = pd.concat([df_odas_calender,tmp])
        df_odas_calender = df_odas_calender.reset_index(drop=True)

    df_odas_calender = df_odas_calender.dropna(subset=['PRD_CD']).reset_index(drop=True)
    if use_jan_connect:
        # new odas 
        path_new_odas = "01_short_term/60_cached_data/08_odas_new/ODAS_new.csv"
        df_odas_new = extract_as_df(path_new_odas, bucket_name, "utf-8", ["tenpo_cd","prd_cd","amount","sales_date"])
    else:
        path_odas_list = ["Basic_Analysis_unzip_result/01_Data/21_ODAS/Odas_Order_Detail__c.csv"]
        
        df_odas_new = load_multiple_df(
            path_odas_list,
            bucket_name
        )
    
        # インデント変更20230803
        df_odas_new = df_odas_new[df_odas_new['tenpo_cd']==int(tenpo_cd)].reset_index(drop=True)

        df_odas_new['prd_cd_len'] = df_odas_new['prd_cd'].apply(lambda x:len(x))
        df_odas_new = df_odas_new[df_odas_new['prd_cd_len']<=13]

        df_odas_new['prd_cd_isnumelic'] = df_odas_new['prd_cd'].apply(lambda x:x.isnumeric())
        df_odas_new = df_odas_new[df_odas_new['prd_cd_isnumelic']==True]

        df_odas_new['prd_cd'] = df_odas_new['prd_cd'].astype(int)
        # ここまで20230803      
    
    df_odas_new = df_odas_new[df_odas_new['tenpo_cd'] == int(tenpo_cd)].reset_index(drop=True)
    df_odas_new = df_odas_new.rename(columns={"tenpo_cd":"TENPO_CD", "prd_cd":"PRD_CD", "amount": "odas_amount","sales_date":"sales_ymd"})

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
        
    odas_merge_df = pd.concat([df_odas_calender, df_odas_calender_new]).reset_index(drop=True)

    # 週ごと品番ごとにサマリする
    # 新旧Odasのデータが合わさっている部分を削除する

    return odas_merge_df


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


def nenshudo2weekfromymd(my_nenshudo, df_calendar):
    for nenshudo, week_from_ymd in zip(df_calendar['nenshudo'], df_calendar['week_from_ymd']):
        if my_nenshudo == nenshudo:
            return week_from_ymd
    return None


def weekfromymd2nenshudo(my_week_from_ymd, df_calender):
    for nenshudo, week_from_ymd in zip(df_calendar['nenshudo'], df_calendar['week_from_ymd']):
        if my_week_from_ymd == week_from_ymd:
            return nenshudo
    return None


def main():
    TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
    TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))
    TODAY_OFFSET = int(os.environ.get("TODAY_OFFSET", 0))
    OUTPUT_TABLE_SUFFIX = os.environ.get("OUTPUT_TABLE_SUFFIX", "")
    TURN_BACK_YYYYMMDD = os.environ.get("TURN_BACK_YYYYMMDD", "")
    tenpo_cd_ref = None
    path_tran_ref = None    
    counter = 0
    
    if cloudrunjob_mode:
        # cloudrunjobで動かすモード
        tenpo_cd = tenpo_cd_list[TASK_INDEX]
        # print('TASK_INDEX:', TASK_INDEX, 'tenpo_cd:', tenpo_cd)
        logger.info(f'TASK_INDEX: {TASK_INDEX}, tenpo_cd: {tenpo_cd}')
    else:
        # notebookで動かすモード
        tenpo_cd = int(sys.argv[1])
        # print('tenpo_cd:', tenpo_cd)
        logger.info(f'tenpo_cd: {tenpo_cd}')
        TODAY_OFFSET = -1
        # print('TODAY_OFFSET:', TODAY_OFFSET)
        logger.info(f'TODAY_OFFSET: {TODAY_OFFSET}')
        OUTPUT_TABLE_SUFFIX = '_217newstr_nenmatsuzero'

    df_calendar = extract_as_df_with_encoding("Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv","dev-cainz-demandforecast","utf-8")
    df_zen_calendar = df_calendar[["nenshudo","week_from_ymd", "znen_week_from_ymd"]]
    df_zen_calendar = df_zen_calendar.rename(columns={'week_from_ymd': '週開始日付', 'znen_week_from_ymd':'前年週開始日付'})
    dfc_tmp = df_calendar[["nenshudo", "week_from_ymd", "week_to_ymd"]]
    dfc_tmp["week_from_ymd"] = dfc_tmp["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    dfc_tmp["week_to_ymd"] = dfc_tmp["week_to_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    
    df_today_nenshudo  = dfc_tmp["nenshudo"][(dfc_tmp["week_from_ymd"] <= today_date_str)&(dfc_tmp["week_to_ymd"] >= today_date_str)]
    today_nenshudo = df_today_nenshudo.values[0]
    # print('today_nenshudo:', today_nenshudo)
    logger.info(f'today_nenshudo: {today_nenshudo}')
    #max_syudo_dic = dfc[['nendo', 'shudo']].groupby(['nendo']).max()['shudo'].to_dict()
    max_syudo_dic = df_calendar[['nendo', 'shudo']].groupby(['nendo']).max()['shudo'].to_dict()
    path_tran ="01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_monthly_series.csv"
    if add_reference_store_by_prdcd:
        if add_reference_store:
            path_reference_store = "Basic_Analysis_unzip_result/01_Data/37_reference_store/reference_store.csv"
            reference_store_df = extract_as_df(path_reference_store, bucket_name)
            reference_store_df["OPEN_DATE"] = reference_store_df["OPEN_DATE"].apply(lambda x : pd.to_datetime(str(x)))
            reference_store_df["OPEN_DATE_REF"] = reference_store_df["OPEN_DATE_REF"].apply(lambda x : pd.to_datetime(str(x)))

            newstore_refstore_dict = dict(zip(reference_store_df['STORE'], reference_store_df['STORE_REF']))
            newstore_opendate_dict = dict(zip(reference_store_df['STORE'], reference_store_df['OPEN_DATE']))

            if tenpo_cd in newstore_refstore_dict:
                tenpo_cd_ref = newstore_refstore_dict[tenpo_cd]
                logger.info(f'参照店舗: {tenpo_cd_ref}')        
                #path_tran_ref = "01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd_ref)+"/{}_{}_time_series.csv"
                path_tran_ref = "01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd_ref)+"/{}_{}_monthly_series.csv"
    else:
        tenpo_cd_ref = None
        if add_reference_store:
            path_reference_store = "Basic_Analysis_unzip_result/01_Data/37_reference_store/reference_store.csv"
            reference_store_df = extract_as_df(path_reference_store, bucket_name)
            reference_store_df["OPEN_DATE"] = reference_store_df["OPEN_DATE"].apply(lambda x : pd.to_datetime(str(x)))
            reference_store_df["OPEN_DATE_REF"] = reference_store_df["OPEN_DATE_REF"].apply(lambda x : pd.to_datetime(str(x)))

            newstore_refstore_dict = dict(zip(reference_store_df['STORE'], reference_store_df['STORE_REF']))
            newstore_opendate_dict = dict(zip(reference_store_df['STORE'], reference_store_df['OPEN_DATE']))

            if tenpo_cd in newstore_refstore_dict:
                tenpo_cd_ref = newstore_refstore_dict[tenpo_cd]
                logger.info(f'参照店舗: {tenpo_cd_ref}')        
                path_tran = "01_short_term/01_stage1_result/02_monthly/"+str(today)+'-6/'+str(tenpo_cd_ref)+"/{}_{}_monthly_series.csv"


    if add_reference_store_by_prdcd:
        sales_df = pd.DataFrame()
        for dpt in dpt_list:
            # print(dpt)
            # print(path_tran.format(dpt, str(tenpo_cd)))
            logger.info(f'Department: {dpt}')
            logger.info(f'Transaction Path: {path_tran.format(dpt, str(tenpo_cd))}')
            temp_df = pd.DataFrame()
            try:
                temp_df = extract_as_df_with_encoding(path_tran.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")
                if 'Unnamed: 0' in temp_df.columns:
                    temp_df = temp_df.drop('Unnamed: 0', axis=1)

                temp_df['DPT'] = int(dpt)
                sales_df = pd.concat([sales_df, temp_df], axis=0).reset_index(drop=True)
            except:
                continue
                
        if tenpo_cd_ref is not None:
            #sales_df_ref = pd.DataFrame()
            for dpt in dpt_list:
                # print(dpt)
                logger.info(f'Department: {dpt}')
                temp_df_ref = pd.DataFrame()
                try:
                #if 1:
                    temp_df_ref = extract_as_df_with_encoding(path_tran_ref.format(dpt, str(tenpo_cd_ref)), bucket_name, "utf-8")
                    if 'Unnamed: 0' in temp_df_ref.columns:
                        temp_df_ref = temp_df_ref.drop('Unnamed: 0', axis=1)

                    temp_df_ref['DPT'] = int(dpt)
                    sales_df_ref = pd.concat([sales_df_ref, temp_df_ref], axis=0).reset_index(drop=True)
                except:
                    continue

            prdcd_list = sales_df['PRD_CD'].unique().tolist()    
            sales_df = sales_df_ref[sales_df_ref['PRD_CD'].isin(prdcd_list)].reset_index(drop=True)
            sales_df['TENPO_CD'] = tenpo_cd       
    else:
        sales_df = pd.DataFrame()
        for dpt in dpt_list:
            # print(dpt)
            logger.info(f'Department: {dpt}')
            temp_df = pd.DataFrame()
            try:
                if tenpo_cd_ref is None:
                    #print('新店ではありません')
                    # print(path_tran.format(dpt, str(tenpo_cd))
                    temp_df = extract_as_df_with_encoding(path_tran.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")
                else:
                    #print('新店です')
                    #print(path_tran.format(dpt, str(tenpo_cd_ref)))
                    temp_df = extract_as_df_with_encoding(path_tran.format(dpt, str(tenpo_cd_ref)), bucket_name, "utf-8")
                    #print('件数:', len(temp_df))

                if 'Unnamed: 0' in temp_df.columns:
                    temp_df = temp_df.drop('Unnamed: 0', axis=1)

                temp_df['DPT'] = int(dpt)
                #print('temp_df.shape:', temp_df.shape)

                sales_df = pd.concat([sales_df, temp_df], axis=0).reset_index(drop=True)
            except:
                continue

 
    if(len(sales_df) <= 0):
        # print("=====There is no sales data, please check: stage1 has been executed======")
        # print("tenpo_cd:", tenpo_cd)
        # print("path_tran:", path_tran)
        logger.info("=====There is no sales data, please check: stage1 has been executed======")
        logger.info(f"tenpo_cd: {tenpo_cd}")
        logger.info(f"path_tran: {path_tran}")
        sys.exit(1)

    sales_df = sales_df[['PRD_CD', 'nenshudo', 'URI_SU', 'TENPO_CD', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    # print('sales_df SKU:', len(sales_df['PRD_CD'].unique()))
    logger.info(f"sales_df SKU: {len(sales_df['PRD_CD'].unique())}")

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
            except:
                continue

        # print('minmax_df.shape:', minmax_df.shape)
        logger.info(f'minmax_df.shape: {minmax_df.shape}')
        # そもそもMinMax設定がなければ、以下の処理はしない
        if len(minmax_df) > 0:
            # MinMaxレコードの製品別最新レコードを取り出す
            minmax_df['TOROKU_YMD_TOROKU_HMS'] = minmax_df['TOROKU_YMD'].astype(str) + minmax_df['TOROKU_HMS'].astype(str) + minmax_df['NENSHUDO'].astype(str)
            prdcd_nenshudomax_df = minmax_df.groupby(['PRD_CD'])['TOROKU_YMD_TOROKU_HMS'].max().reset_index()
            minmax_df_newest = pd.merge(prdcd_nenshudomax_df, minmax_df, on=['PRD_CD', 'TOROKU_YMD_TOROKU_HMS'], how='inner')

            # minmaxデータのある製品に絞る
            sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest['PRD_CD'].tolist())]
            minmax_df_newest2 = minmax_df_newest[minmax_df_newest['PRD_CD'].isin(sales_df['PRD_CD'].tolist())]
                    #minmax_df_newest2.to_csv('wkly_minmax_df_newest2_20230912.csv', index=False)
            if 1:
                # MaxMinx0のみ除外
                minmax_df_newest2_not_minmax0 =minmax_df_newest2[~((minmax_df_newest2['HOJU_MIN_SU']==0)&(minmax_df_newest2['HOJU_MAX_SU']==0))]
                sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest2_not_minmax0['PRD_CD'].tolist())]
                sales_df = sales_df.reset_index(drop=True)

            else:
                # 下限0意外の商品に絞る
                minmax_df_newest2_not_min0 =minmax_df_newest2[minmax_df_newest2['HOJU_MIN_SU']>0]
                sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest2_not_min0['PRD_CD'].tolist())]
                sales_df = sales_df.reset_index(drop=True)
            # print('restrict minmax sales_df SKU:', len(sales_df['PRD_CD'].unique()))
            logger.info(f"restrict minmax sales_df SKU: {len(sales_df['PRD_CD'].unique())}")

    if restrinct_tenpo_hacchu_end:
        # 店舗別の生産発注停止情報を結合
        path_tenpo_hacchu_master = "Basic_Analysis_unzip_result/01_Data/33_tenpo_hacchu/29_TENPO_HACCHU_YMD.csv"
        store_prd_hacchu_ymd = extract_as_df(path_tenpo_hacchu_master, bucket_name)
        if len(store_prd_hacchu_ymd) > 0:
            store_prd_hacchu_ymd['TENPO_CD'] = store_prd_hacchu_ymd['TENPO_CD'].astype(int)
            store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD']==tenpo_cd].reset_index(drop=True)

            # 店舗別発注終了日
            store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].fillna(99999999)
            store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].astype(int)
            # 対象店舗データに絞ってマージ
            store_prd_hacchu_ymd = store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD']==int(tenpo_cd)].reset_index(drop=True)

            # 発注停止以外の商品に絞る
            hacchu_end_prdlist = store_prd_hacchu_ymd[store_prd_hacchu_ymd['HACCHU_TO_YMD'] <= my_date]['PRD_CD'].astype(int).tolist()
            sales_df = sales_df[~sales_df['PRD_CD'].isin(hacchu_end_prdlist)]
            sales_df = sales_df.reset_index(drop=True)
            # print('exclude store hacchuend sales_df SKU:', len(sales_df['PRD_CD'].unique()))
            logger.info(f"exclude store hacchuend sales_df SKU: {len(sales_df['PRD_CD'].unique())}")

    # 欠損データ補間
    sales_df = interpolate_df(sales_df, df_calendar)

    if no_sales_term_copy_existsales:
        ################################################################################
        # 販売の無い（販売開始前）期間に、販売のある期間の最初の同じ週度の売り数をコピーする
        ################################################################################

        # 前年年週度を結合
        df_calendar['nenshudo'] = df_calendar['nendo'] * 100 + df_calendar['shudo']
        df_calendar['znen_nenshudo'] = df_calendar['znen_nendo'] * 100 + df_calendar['znen_shudo']

        sales_df = pd.merge(sales_df, df_calendar[['nendo', 'shudo', 'nenshudo', 'znen_nenshudo']], left_on='nenshudo', right_on='nenshudo', how='left')

        # 販売期間のある最初の年週度をとってくる
        sales_df_exist_sales = sales_df[sales_df['URI_SU'] >= 0.001]
        sales_df_exist_sales['nenshudo_exist_uri_su_min'] = sales_df_exist_sales.groupby("PRD_CD", as_index=False)['nenshudo'].transform(lambda x: x.min())


        sales_df_exist_sales2 = sales_df_exist_sales[['PRD_CD', 'nenshudo_exist_uri_su_min']].drop_duplicates()
        #del sales_df_exist_sales

        # 製品番号と、販売期間のある最初の年週度の辞書
        prdcd_1stsalesnenshudo_dict = dict(zip(sales_df_exist_sales2['PRD_CD'], sales_df_exist_sales2['nenshudo_exist_uri_su_min']))


        # 販売実績最初の週の列を作成
        sales_df['1stsales_nenshudo'] = sales_df['PRD_CD'].apply(lambda x:prdcd_1stsalesnenshudo_dict[x])

        # 実績販売期間のデータ
        sales_df_existsales = sales_df[sales_df['nenshudo'] >= sales_df['1stsales_nenshudo']]

        # 実績販売データが1年分あるものに限定する
        sales_df_existsales['jisseki_uriage_wk_count'] = sales_df_existsales.groupby(["PRD_CD"], as_index=False)['nendo'].transform(lambda x:len(x))

        # 
        if no_sales_term_set_ave:
            sales_df_existsales_under1yer = sales_df_existsales[sales_df_existsales['jisseki_uriage_wk_count'] < 52]

        if 1:
            sales_df_existsales = sales_df_existsales[sales_df_existsales['jisseki_uriage_wk_count'] >= 52]

            # 製品別で、週ごとに実績販売最初の年度を出す
            sales_df_existsales['sales_exist_nendo_min_for_shudo'] = sales_df_existsales.groupby(["PRD_CD", "shudo"], as_index=False)['nendo'].transform(lambda x: x.min())    

            # 販売実績ありの期間で、最初の1年分だけ抽出する
            sales_df_existsales2 = sales_df_existsales[(sales_df_existsales['sales_exist_nendo_min_for_shudo']==sales_df_existsales['nendo'])]

            # 最初の1年分の販売データで、販売の無かった期間を埋める
            sales_df_existsales2 = sales_df_existsales2.rename(columns={'URI_SU':'URI_SU_1ST'})

            sales_df2 = pd.merge(sales_df, sales_df_existsales2[['PRD_CD', 'shudo', 'URI_SU_1ST', 'jisseki_uriage_wk_count']], left_on=['PRD_CD', 'shudo'], right_on=['PRD_CD', 'shudo'], how='left')

            #sales_df2['URI_SU_ORG'] = sales_df2['URI_SU']
            sales_df2['URI_SU'][sales_df2['nenshudo'] < sales_df2['1stsales_nenshudo']] = sales_df2['URI_SU_1ST'][sales_df2['nenshudo'] < sales_df2['1stsales_nenshudo']]


        if no_sales_term_set_ave:

            # 売りのある期間の売り数平均値を出す
            sales_df3 = sales_df2[sales_df2['nenshudo']>=sales_df2['1stsales_nenshudo']].reset_index(drop=True)
            sales_df3['URI_SU_ave'] = sales_df3.groupby(["PRD_CD"], as_index=False)['URI_SU'].transform(lambda x: x.mean())
            prdcd_urisuave_dict = dict(zip(sales_df3['PRD_CD'], sales_df3['URI_SU_ave']))
            sales_df2['URI_SU_ave'] = sales_df2['PRD_CD'].apply(lambda x:prdcd_urisuave_dict.get(x, 0)) 

            #sales_df2.to_csv('sales_df2_before_setave.csv')

            # 売りのある期間が1年以下の商品について、売りの無かった期間の売り数に「売り数平均値」をセットする
            prdcd_list_sales_udr1year = sales_df_existsales_under1yer['PRD_CD'].unique().tolist()
            sales_df2['URI_SU'][
                (sales_df2['PRD_CD'].isin(prdcd_list_sales_udr1year))
                &(sales_df2['nenshudo'] < sales_df2['1stsales_nenshudo']
                  )] = sales_df2['URI_SU_ave']

            #sales_df2.to_csv('sales_df2_after_setave.csv')         
            sales_df2 = sales_df2.drop('URI_SU_ave', axis=1)

        sales_df = sales_df2.reset_index(drop=True)

        # 後始末
        sales_df = sales_df.drop('nendo', axis=1)
        sales_df = sales_df.drop('shudo', axis=1)
        sales_df = sales_df.drop('znen_nenshudo', axis=1)
        sales_df = sales_df.drop('1stsales_nenshudo', axis=1)
        sales_df = sales_df.drop('URI_SU_1ST', axis=1)
        sales_df = sales_df.drop('jisseki_uriage_wk_count', axis=1)

        del sales_df2
        del prdcd_1stsalesnenshudo_dict
        del sales_df_existsales
        del sales_df_existsales2

    df_odas_calender = pd.DataFrame()
    df_odas_calender = odas_correct(df_calendar, tenpo_cd, use_jan_connect=use_jan_connect)
    # 20230324追加
    df_odas_calender = df_odas_calender.groupby(['TENPO_CD', 'PRD_CD', 'nenshudo'], as_index=False).agg({"odas_amount":'sum'})
    sales_df = pd.merge(sales_df, df_odas_calender, on =["PRD_CD", "nenshudo", "TENPO_CD"], how="left")
    sales_df["odas_amount"] = sales_df["odas_amount"].fillna(0)

    # ODAS補正
    if odas_correct_imprv == False:
        sales_df["URI_SU"] = sales_df["URI_SU"] - sales_df["odas_amount"]
    else:
        odas_correction_start_t = time.time()
        logger.info('start odas correction improvements *******************************')


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


        logger.info('processing odas correction improvements *******************************')
        odas_correction_end_t = time.time()
        elapsed_time = odas_correction_end_t - odas_correction_start_t
        # print(f"odas correction elapsed time: {elapsed_time:.3f} seconds")
        logger.info(f'odas correction elapsed time: {elapsed_time:.3f} seconds')

        # ここから補正の本処理　****************************************************************************************
        # ODAS補正後にマイナスとなったものは、2σの8週平均で補完する
        #   ここでURI_SU_NEW_org(マイナス値入りの最初の値をもってきていることに注意)
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
        &(sales_df['URI_SU_NEW_OVER0'] >= 100)] = np.nan
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_TYPE'][
        (sales_df['odas_amount_ROLLMAX'] == 0)
        &(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] > sales_df['URI_SU_NEW_OVER0_TH2'])
        &(sales_df['URI_SU_NEW_OVER0'] > sales_df['URI_SU_NEW_OVER0_TH2'])
        &(sales_df['URI_SU_NEW_OVER0'] > (sales_df['URI_SU_NEW_OVER0_8MODE'] + 1) * 8)
        &(sales_df['URI_SU_NEW_OVER0'] >= 100)] = '_+spike_without_odas'
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
        # print(f"odas correction elapsed time: {elapsed_time:.3f} seconds")
        logger.info(f'odas correction elapsed time: {elapsed_time:.3f} seconds')


    sales_df = sales_df.drop('odas_amount', axis=1)
    sales_df = sales_df.drop_duplicates().reset_index(drop=True)

    # 目的変数の対数化
    if logarithmize_target_variable:
        #print('start logarithmize_target_variable')
        sales_df['URI_SU'] = np.log1p(sales_df['URI_SU'])
        #print('end logarithmize_target_variable')    

    #　前年売り数を作成
    # 20230721修正
    df_znen_cal = df_calendar[['nenshudo', 'znen_nendo', 'znen_shudo']]
    df_znen_cal['zen_nenshdo'] = df_znen_cal['znen_nendo'] * 100 + df_znen_cal['znen_shudo']
    sales_df = pd.merge(sales_df, df_znen_cal[['nenshudo', 'zen_nenshdo']], on='nenshudo', how='left')

    sales_df_last_year = copy.deepcopy(sales_df)
    sales_df_last_year = sales_df_last_year.drop('zen_nenshdo', axis=1)
    sales_df_last_year = sales_df_last_year.drop('baika_toitsu', axis=1)
    sales_df_last_year = sales_df_last_year.drop('BAIKA', axis=1)
    sales_df_last_year = sales_df_last_year.drop('DPT', axis=1)
    sales_df_last_year = sales_df_last_year.drop('line_cd', axis=1)
    sales_df_last_year = sales_df_last_year.drop('cls_cd', axis=1)
    sales_df_last_year = sales_df_last_year.drop('hnmk_cd', axis=1)
    sales_df_last_year = sales_df_last_year.drop('TENPO_CD', axis=1)
    sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量'})

    df_merged_sales = pd.merge(sales_df, sales_df_last_year, on = ['PRD_CD','zen_nenshdo'])

    del sales_df
    del sales_df_last_year

    df_merged_sales2 = pd.merge(df_merged_sales, df_zen_calendar, on = 'nenshudo')
    del df_merged_sales

    df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'PRD_CD':'商品コード'}).reset_index(drop=True)

    train_tmp = df_merged_sales2[['商品コード','売上実績数量','前年売上実績数量','週開始日付','前年週開始日付', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    del df_merged_sales2

    train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')

    if bakugai_hosei:
        # 高速化バージョン
        while True:
            counter = counter + 1
            train_tmp['8週平均ema'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).mean())
            train_tmp['指数加重移動標準偏差'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).std())
            # 標準偏差 * 1.725 + 8ema
            train_tmp["指数加重移動標準偏差2"] = train_tmp["指数加重移動標準偏差"] * 1.725 + train_tmp["8週平均ema"]

            # 追加 **********************************************************************
            train_tmp = train_tmp.sort_values(by=["商品コード","週開始日付"])
            train_tmp['8週平均ema_前週'] = train_tmp.groupby("商品コード",as_index=False)['8週平均ema'].shift(1)
            train_tmp['8週平均ema_前週'] = train_tmp.groupby(["商品コード"])['8週平均ema_前週'].transform(lambda x: x.interpolate(limit_direction='both'))
            # 追加 **********************************************************************

            # 8週ema + 1.725σ　を超えているデータを抽出（補正対象）
            df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量','8週平均ema','指数加重移動標準偏差2','前年売上実績数量','8週平均ema_前週', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

            # 8週ema + 1.725σ　以下のデータを抽出（補正対象外）
            train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量','8週平均ema','指数加重移動標準偏差2','前年売上実績数量','8週平均ema_前週', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"]]

            df_output["爆買い補正"] = counter
            df_output["補正前売上実績数量"] = df_output["売上実績数量"]
            df_output['売上実績数量'] =  df_output['8週平均ema_前週']
            df_output['売上実績数量'] = df_output['売上実績数量'].fillna(0).astype('int')
            train_tmp = pd.concat([train_tmp_remain, df_output], axis=0).reset_index()

            if(counter >= 5):
                break

            if(df_output['売上実績数量'].max() < 200):
                break

    else: # bakugai_hosei
        train_tmp = train_tmp.drop('前年週開始日付', axis=1)

    train_tmp['8週平均ema'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).mean())
    train_tmp = train_tmp.drop('週開始日付tmp', axis=1)
    df_merged = copy.deepcopy(train_tmp)
    del train_tmp
    df_merged = pd.merge(df_merged,df_zen_calendar,on='週開始日付')
    df_merged2 = df_merged[['商品コード','売上実績数量','前年売上実績数量','前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    del df_merged
    df = get_last_year_sales_amount(df_merged2)
    del df_merged2
    df_vx_train = df[['商品コード','売上実績数量','週開始日付','前年売上実績数量','8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    del df

    df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount','前年売上実績数量':'PreviousYearSalesActualQuantity','8週平均ema':'SalesAmount8ema'})
    df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'WeekStartDate','PreviousYearSalesActualQuantity', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    df_vx_train['PreviousYearSalesActualQuantity'] = df_vx_train['PreviousYearSalesActualQuantity'].astype(float)
    # ここでNULLを落としている
    df_vx_train = df_vx_train.dropna(how='any').reset_index(drop=True)

    if turn_back_time:
        # モデル学習上の現在日時を巻き戻す
        # turn_back_time = True
        # turn_back_yyyymmdd = 20230424
        # ***********************************************
        train_start_nenshudo = calc_nenshudo2(turn_back_yyyymmdd, -52*3 - 5, dfc_tmp)
        train_start_week_from_ymd = nenshudo2weekfromymd(train_start_nenshudo, df_calendar)
        # print('train_start_nenshudo:', train_start_nenshudo)
        # print('train_start_week_from_ymd:', train_start_week_from_ymd)
        logger.info(f'train_start_nenshudo: {train_start_nenshudo}')
        logger.info(f'train_start_week_from_ymd: {train_start_week_from_ymd}')
        df_vx_train =  df_vx_train[df_vx_train['WeekStartDate'] <= turn_back_yyyymmdd].reset_index(drop=True)
    else:
        train_start_nenshudo = calc_nenshudo2(today_nenshudo, -52*3 - 5, dfc_tmp)
        train_start_week_from_ymd = nenshudo2weekfromymd(train_start_nenshudo, df_calendar)
        # print('train_start_nenshudo:', train_start_nenshudo)
        # print('train_start_week_from_ymd:', train_start_week_from_ymd)
        logger.info(f'train_start_nenshudo: {train_start_nenshudo}')
        logger.info(f'train_start_week_from_ymd: {train_start_week_from_ymd}')

    # 学習期間を絞る*************************************************************************
    df_vx_train = df_vx_train[df_vx_train['WeekStartDate']>=train_start_week_from_ymd].reset_index(drop=True)
    # **************************************************************************************
    df_vx_train['weekstartdatestamp'] = pd.to_datetime(df_vx_train['WeekStartDate'], format = '%Y%m%d')
    df_vx_train['tenpo_cd'] = tenpo_cd
    df_vx_train = df_vx_train.drop('WeekStartDate', axis=1)
    df_vx_train['PrdCd'] = df_vx_train['PrdCd'].astype(int)
    df_vx_train['DPT'] = df_vx_train['DPT'].astype(int)
    df_vx_train['line_cd'] = df_vx_train['line_cd'].astype(int)
    df_vx_train['cls_cd'] = df_vx_train['cls_cd'].astype(int)
    df_vx_train['hnmk_cd'] = df_vx_train['hnmk_cd'].astype(int)

    # 売上が0以下を0に補正する
    if correct_sales_minus:
        df_vx_train['SalesAmount'][df_vx_train['SalesAmount'] < 0.0] = 0.0

    # add 20230301
    df_vx_train['TenpoCdPrdCd'] = str(tenpo_cd) + '_' + df_vx_train['PrdCd'].astype(str)
    # 重みのデフォルト値設定
    df_vx_train['training_weight'] = 7000

    if salesup_flag:
        ## 繁忙期フラグとweightの設定
        #df_vx_train['training_weight'] = 5000
        df_vx_train['training_weight'] = 2000 # デフォルト設定値
        # 販売期間のある最初の週のカラムを設定する
        weekstartdatestamp_min = df_vx_train['weekstartdatestamp'].min()
        df_vx_train_exist_sales = df_vx_train[df_vx_train['SalesAmount'] >= 0.001]
        table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs_temp." + "monthly-train-tmp-" + str(today) + "-" + str(tenpo_cd) + str(OUTPUT_TABLE_SUFFIX)
        client = BigqueryClient()
        job = client.load_table_from_dataframe(df_vx_train_exist_sales, table_id)
        job.result()
        project_id = "dev-cainz-demandforecast"
        target_query = f"""  SELECT PrdCd, min(weekstartdatestamp) AS weekstartdatestamp_exist_sales_min FROM `{table_id}` group by PrdCd"""
        df_temp = pd.read_gbq(target_query, project_id, dialect='standard')
        df_vx_train = pd.merge(df_vx_train, df_temp, on='PrdCd', how='left')
        df_vx_train['weekstartdatestamp_exist_sales_min'] = df_vx_train['weekstartdatestamp_exist_sales_min'].fillna(df_vx_train['weekstartdatestamp'].min())
        df_vx_train['1stsalesweekstartdatestamp'] = df_vx_train['weekstartdatestamp_exist_sales_min']

        def set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 
                            start_nenshudo_before, end_nenshudo_before, 
                            start_nenshudo_after, end_nenshudo_after, 
                            up_diff_th, up_per_th,
                            flag_column_name,
                            weight_columns_name,
                            weight_value
                           ):

            # 繁忙期間前
            start_ymd_b = str(nenshudo2weekfromymd(start_nenshudo_before, df_calendar))
            end_ymd_b = str(nenshudo2weekfromymd(end_nenshudo_before, df_calendar))
            # 繁忙期間後
            start_ymd_a = str(nenshudo2weekfromymd(start_nenshudo_after, df_calendar))
            end_ymd_a = str(nenshudo2weekfromymd(end_nenshudo_after, df_calendar))

            dft_before = df_vx_train[(pd.to_datetime(start_ymd_b)<= df_vx_train['weekstartdatestamp'])
                                     &(df_vx_train['weekstartdatestamp']<=pd.to_datetime(end_ymd_b))]

            dft_after = df_vx_train[(pd.to_datetime(start_ymd_a)<= df_vx_train['weekstartdatestamp'])
                                     &(df_vx_train['weekstartdatestamp']<=pd.to_datetime(end_ymd_a))]

            if len(dft_before) > 0 and len(dft_after) > 0:
                dft_before['BEFORE_MEAN'] = dft_before.groupby(["PrdCd"], as_index=False)['SalesAmount'].transform(lambda x: x.mean())
                dft_after['AFTER_MEAN'] = dft_after.groupby(["PrdCd"], as_index=False)['SalesAmount'].transform(lambda x: x.mean())

                compare_df = pd.merge(dft_before[['PrdCd', 'BEFORE_MEAN']].drop_duplicates().reset_index(),
                         dft_after[['PrdCd', 'AFTER_MEAN']].drop_duplicates().reset_index(), on = 'PrdCd', how='inner')

                compare_df['upsales_diff'] = compare_df['AFTER_MEAN'] - compare_df['BEFORE_MEAN']
                compare_df['upsales_percentage'] = compare_df['AFTER_MEAN'] / compare_df['BEFORE_MEAN']

                # 販売数平均差が閾値以上、かつ販売数平均増加率が閾値以上の商品を抽出
                flag_target_df = compare_df[(compare_df['upsales_diff']>=up_diff_th)&(compare_df['upsales_percentage']>=up_per_th)]
                flag_target_df_prdcd_list = flag_target_df['PrdCd'].unique().tolist()
            else:
                flag_target_df_prdcd_list = []

            # さらに1年前の伸長をチェックする
            start_nenshudo_before2 = start_nenshudo_before - 100
            end_nenshudo_before2 = end_nenshudo_before - 100
            start_nenshudo_after2 = start_nenshudo_after - 100
            end_nenshudo_after2 = end_nenshudo_after - 100     

            logger.info(f"start_nenshudo_before2: {start_nenshudo_before2}")
            logger.info(f"end_nenshudo_before2: {end_nenshudo_before2}")
            logger.info(f"start_nenshudo_after2: {start_nenshudo_after2}")
            logger.info(f"end_nenshudo_after2: {end_nenshudo_after2}")

            # 繁忙期間前
            start_ymd_b2 = str(nenshudo2weekfromymd(start_nenshudo_before2, df_calendar))
            end_ymd_b2 = str(nenshudo2weekfromymd(end_nenshudo_before2, df_calendar))
            # 繁忙期間後
            start_ymd_a2 = str(nenshudo2weekfromymd(start_nenshudo_after2, df_calendar))
            end_ymd_a2 = str(nenshudo2weekfromymd(end_nenshudo_after2, df_calendar))

            logger.info(f"start_ymd_b2: {start_ymd_b2}")
            logger.info(f"end_ymd_b2: {end_ymd_b2}")
            logger.info(f"start_ymd_a2: {start_ymd_a2}")
            logger.info(f"end_ymd_a2: {end_ymd_a2}")

            # 1stsalesweekstartdatestamp より前は見ないようにする
            df_vx_train2 = df_vx_train[df_vx_train['weekstartdatestamp'] >= df_vx_train['1stsalesweekstartdatestamp']]

            dft_before2 = df_vx_train2[(pd.to_datetime(start_ymd_b2)<= df_vx_train2['weekstartdatestamp'])
                                     &(df_vx_train2['weekstartdatestamp']<=pd.to_datetime(end_ymd_b2))]

            dft_after2 = df_vx_train2[(pd.to_datetime(start_ymd_a2)<= df_vx_train2['weekstartdatestamp'])
                                     &(df_vx_train2['weekstartdatestamp']<=pd.to_datetime(end_ymd_a2))]

            if len(dft_before2) > 0 and len(dft_after2) > 0:
                dft_before2['BEFORE_MEAN'] = dft_before2.groupby(["PrdCd"], as_index=False)['SalesAmount'].transform(lambda x: x.mean())
                dft_after2['AFTER_MEAN'] = dft_after2.groupby(["PrdCd"], as_index=False)['SalesAmount'].transform(lambda x: x.mean())

                compare_df2 = pd.merge(dft_before2[['PrdCd', 'BEFORE_MEAN']].drop_duplicates().reset_index(),
                         dft_after2[['PrdCd', 'AFTER_MEAN']].drop_duplicates().reset_index(), on = 'PrdCd', how='inner')

                compare_df2['upsales_diff'] = compare_df2['AFTER_MEAN'] - compare_df2['BEFORE_MEAN']
                compare_df2['upsales_percentage'] = compare_df2['AFTER_MEAN'] / compare_df2['BEFORE_MEAN']

                # 販売数平均差が閾値以上、かつ販売数平均増加率が閾値以上の商品を抽出
                flag_target_df2 = compare_df2[(compare_df2['upsales_diff']>=up_diff_th)&(compare_df2['upsales_percentage']>=up_per_th)]
                #flag_target_df2.to_csv('flag_target_df2' + '_' + flag_column_name + '_'+ str(tenpo_cd) + '.csv')
                flag_target_df2_prdcd_list = flag_target_df2['PrdCd'].unique().tolist()
            else:    
                flag_target_df2_prdcd_list = []


            flag_target_prdcd_list = list(set(flag_target_df_prdcd_list) & set(flag_target_df2_prdcd_list))

            flag_target_prdcd_list_df = pd.DataFrame(flag_target_prdcd_list)

            # 閾値以上の商品にフラグをつける（全期間にフラグを立てる）
            df_vx_train[flag_column_name] = 0
            df_vx_train[flag_column_name][df_vx_train['PrdCd'].isin(flag_target_prdcd_list)] = 1

            # training_weightの設定（繁忙期間のみ重みを大きくする）
            df_vx_train[weight_columns_name][df_vx_train['PrdCd'].isin(flag_target_prdcd_list)
                                        &(pd.to_datetime(start_ymd_a)<= df_vx_train['weekstartdatestamp'])
                                        &(df_vx_train['weekstartdatestamp']<=pd.to_datetime(end_ymd_a))
                                      ] = weight_value

            def calc_prev_year_nenshudo(my_nenshudo, n):
                my_year = int(my_nenshudo / 100)
                my_shudo = int(my_nenshudo % 100)
                return (my_year - n) * 100 + my_shudo

            # 4年前までの伸長後週のweightを大きくする
            for prev_year in [1, 2, 3, 4]:
                prev_year_start_nenshudo_a = calc_prev_year_nenshudo(start_nenshudo_after, prev_year)
                prev_year_end_nenshudo_a = calc_prev_year_nenshudo(end_nenshudo_after, prev_year)
                
                logger.info(f'start_nenshudo_after: {start_nenshudo_after}')
                logger.info(f'end_nenshudo_after: {end_nenshudo_after}')
                logger.info(f'{prev_year_start_nenshudo_a} - {prev_year_end_nenshudo_a} の伸長後週weightを設定: {weight_value}')

                if (prev_year_start_nenshudo_a is not None) and (prev_year_end_nenshudo_a is not None):
                    #print('設定します')
                    start_ymd_a = str(nenshudo2weekfromymd(prev_year_start_nenshudo_a, df_calendar))
                    end_ymd_a = str(nenshudo2weekfromymd(prev_year_end_nenshudo_a, df_calendar))
                    #training_weightの設定（繁忙期間のみ重みを大きくする, ただし1stsalesweekstartdatestampより後のみとする）
                    df_vx_train[weight_columns_name][df_vx_train['PrdCd'].isin(flag_target_df['PrdCd'].tolist())
                                                &(pd.to_datetime(start_ymd_a)<= df_vx_train['weekstartdatestamp'])
                                                &(df_vx_train['weekstartdatestamp']<=pd.to_datetime(end_ymd_a))
                                                &(df_vx_train['1stsalesweekstartdatestamp'] <= df_vx_train['weekstartdatestamp'])
                                              ] = weight_value

            return df_vx_train


        ## 本番用 (伸長条件を4.0, 1.4にして、1年前の伸長も条件に入れる)、伸長判定週をまきもどさない設定にする
        # 年末　202437-40週 / 41週-44週 を比較して、平均販売数の差が2以上かつ120%以上
        df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202437, 202440, 202441, 202444, 4.0, 1.4, 'BusyPeriodFlagNenmatsu', 'training_weight', 10000)

        # 4半期（2024年3,4,5月　販売ランクS, Aにおいて）、＊＊モデル学習時は商品ランクを見ない＊＊
        # GW  202407-08週 (4/8-4/21)/ 09-10週(4/22-5/5) を比較して、平均販売数の差が2以上かつ120%以上
        df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202407, 202408, 202409, 202410, 4.0, 1.4, 'BusyPeriodFlagGw', 'training_weight', 10000)

        # 新生活 202403-04週 / 05-06週                   を比較して、平均販売数の差が2以上かつ120%以上
        df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202503, 202504, 202505, 202506, 4.0, 1.4, 'BusyPeriodFlagNewLife', 'training_weight', 10000)
        
        
        ###########Newly Added########################

        # 梅雨明け夏休み 202419-20週 / 21-22週                   を比較して、平均販売数の差が2以上かつ120%以上
        df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202419, 202420, 202421, 202422, 4.0, 1.4, 'BusyPeriodFlagEndRainySsn', 'training_weight', 10000)

        # お盆 202422-23週 / 24-25週                   を比較して、平均販売数の差が2以上かつ120%以上
        df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202422, 202423, 202424, 202425, 4.0, 1.4, 'BusyPeriodFlagObon', 'training_weight', 10000)
        
        ###########Newly Added########################

        # 後始末
        df_vx_train = df_vx_train.drop('1stsalesweekstartdatestamp', axis=1)
        #del df_vx_train_exist_sales2
        #del prdcd_1stsalesweekstartdatestamp_dict

    if no_sales_term_weight_zero:
        # SKU別にみて、最初に販売の無い期間はウェイトを0にしておく
        # デフォルト値設定
        #df_vx_train['training_weight'] = 10000
        # 販売期間のある最初の週をとってくる
        df_vx_train_exist_sales = df_vx_train[df_vx_train['SalesAmount'] >= 0.001]
        df_vx_train['training_weight'][df_vx_train['weekstartdatestamp'] < df_vx_train['weekstartdatestamp_exist_sales_min']] = 0.0
        df_vx_train = df_vx_train.drop('weekstartdatestamp_exist_sales_min', axis=1)

    # 階層モデルID 20230331
    #if hierarchical_model:
    # 階層モデル版
    threshold_small_wave_nendo=2020
    df_vx_train['SalesAmount_Mean'] = df_vx_train.groupby("PrdCd")["SalesAmount"].transform(lambda x: x.mean())
    df_vx_train['hierarchical_model_id'] = np.nan
    df_vx_train['hierarchical_model_id'][df_vx_train['SalesAmount_Mean'] < df_vx_train['SalesAmount_Mean'].median()] = str(tenpo_cd) + '_small_qty'
    df_vx_train['hierarchical_model_id'][df_vx_train['SalesAmount_Mean'] >= df_vx_train['SalesAmount_Mean'].median()] =  str(tenpo_cd) + '_not_small_qty'            
    df_vx_train = df_vx_train.drop('SalesAmount_Mean', axis=1)


    #データ分割列を使用すると、トレーニング、検証、テストに使用する特定の行を選択できます。
    #トレーニング データを作成する場合、列を追加して、そこに次のいずれかの値（大文字小文字の区別あり）
    #を含めることができます。
    #TEST     5wk     train_start_nenshudo
    #TRAIN    ---
    #VALIDATE 52wk
    test_end_nenshudo = calc_nenshudo2(train_start_nenshudo, 4, dfc_tmp) # 最初の5週を評価期間にする
    test_end_ymd = nenshudo2weekfromymd(test_end_nenshudo, df_calendar)
    test_end_stamp = pd.to_datetime(str(test_end_ymd))
    logger.info(f'test_end_stamp: {test_end_stamp}')

    validate_start_nenshudo = calc_nenshudo2(today_nenshudo, -52, dfc_tmp)
    validate_start_ymd = nenshudo2weekfromymd(validate_start_nenshudo, df_calendar)
    validate_start_stamp = pd.to_datetime(str(validate_start_ymd))
    logger.info(f'validate_start_stamp: {validate_start_stamp}')

    df_vx_train['split'] = 'TRAIN'
    df_vx_train['split'][df_vx_train['weekstartdatestamp'] <= test_end_stamp] = 'TEST'
    df_vx_train['split'][df_vx_train['weekstartdatestamp'] >= validate_start_stamp] = 'VALIDATE'

    # シーズン品の分離
    if devide_season_items:
        # 細工するので、元データをとっておく
        df_vx_train_bk = copy.deepcopy(df_vx_train)
        df_vx_train['prd_mean'] = df_vx_train.groupby(['PrdCd'], as_index=False)['SalesAmount'].transform(lambda x: x.mean())
        df_vx_train['prd_std'] = df_vx_train.groupby(['PrdCd'], as_index=False)['SalesAmount'].transform(lambda x: x.std())

        # 販売期間のある最初の週をとってくる
        df_vx_train_exist_sales = df_vx_train[df_vx_train['SalesAmount'] >= 0.001]
        df_vx_train_exist_sales['weekstartdatestamp_exist_sales_min'] = df_vx_train_exist_sales.groupby("PrdCd", as_index=False)['weekstartdatestamp'].transform(lambda x: x.min())

        df_vx_train_exist_sales2 = df_vx_train_exist_sales[['PrdCd', 'weekstartdatestamp_exist_sales_min']].drop_duplicates()
        del df_vx_train_exist_sales

        weekstartdatestamp_max = df_vx_train['weekstartdatestamp'].max()

        prdcd_1stsalesweekstartdatestamp_dict = dict(zip(df_vx_train_exist_sales2['PrdCd'], df_vx_train_exist_sales2['weekstartdatestamp_exist_sales_min']))

        df_vx_train['1stsalesweekstartdatestamp'] = df_vx_train['PrdCd'].apply(lambda x:prdcd_1stsalesweekstartdatestamp_dict.get(x, weekstartdatestamp_max))
        # 販売開始前の期間には、平均値を設定しておく（売り数０の影響を避けるため）

        df_vx_train['SalesAmount'][df_vx_train['weekstartdatestamp'] < df_vx_train['1stsalesweekstartdatestamp']] = df_vx_train['prd_mean']
        # 月番号
        df_vx_train['month'] = df_vx_train["weekstartdatestamp"].apply(lambda x : x.month)

        season_pattern_list = [

            # 20240404add
            #[[3,4,5,6,7,8], [9,10,11,12,1,2]],
            #[[4,5,6,7,8,9], [10,11,12,1,2,3]],
            #[[5,6,7,8,9,10], [11,12,1,2,3,4]],
            #[[6,7,8,9,10,11], [12,1,2,3,4,5]],
            #[[7,8,9,10,11,12], [1,2,3,4,5,6]],
            #[[8,9,10,11,12,1], [2,3,4,5,6,7]],

            #[[1,2,3,4], [5,6,7,8],[9,10,11,12]],
            #[[2,3,4,5], [6,7,8,9],[10,11,12,1]],
            #[[3,4,5,6], [7,8,9,10],[11,12,1,2]],
            #[[4,5,6,7], [8,9,10,11],[12,1,2,3]],

            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]],
            [[2, 3, 4], [5, 6, 7], [8, 9, 10], [11, 12, 1]],
            [[3, 4, 5], [6, 7, 8], [9, 10, 11], [12, 1, 2]],

            [[12, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10, 11]],
            [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10], [11, 12]],

        ]

        for i, season_pattern in enumerate(season_pattern_list):

            df_vx_train[f'season_{i}' ] = np.nan
            for j, month_list in enumerate(season_pattern):
                df_vx_train[f'season_{i}'][df_vx_train['month'].isin(month_list)] = j

            df_vx_train[f'prd_season_mean_{i}'] = df_vx_train.groupby(['PrdCd', f'season_{i}'], as_index=False)['SalesAmount'].transform(lambda x: x.mean())

            df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_{i}'] = (df_vx_train[f'prd_season_mean_{i}'] - df_vx_train["prd_mean"]) / df_vx_train["prd_std"]

            df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_max_{i}'] =  df_vx_train.groupby(['PrdCd'], as_index=False)[f'prd_season_mean_minus_prd_mean_per_std_{i}'].transform(lambda x: x.max())

            df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_min_{i}'] =  df_vx_train.groupby(['PrdCd'], as_index=False)[f'prd_season_mean_minus_prd_mean_per_std_{i}'].transform(lambda x: x.min())

            df_vx_train[f'gap_{i}'] = df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_max_{i}'] - df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_min_{i}']


            df_vx_train[f"is_season_{i}"] = 0
            #df_vx_train[f"is_season_{i}"][df_vx_train[f"gap_{i}"]>=1.5] = 1
            df_vx_train[f"is_season_{i}"][df_vx_train[f"gap_{i}"]>=1.2] = 1


        #df_vx_train['gap_mean'] = df_vx_train[['gap_0', 'gap_1', 'gap_2', 'gap_3', 'gap_4', 'gap_5', 'gap_6', 'gap_7', 'gap_8', 'gap_9', 'gap_10', 'gap_11', 'gap_12', 'gap_13', 'gap_14']].apply(lambda x:x.mean(), axis=1)
        df_vx_train['gap_mean'] = df_vx_train[['gap_0', 'gap_1', 'gap_2', 'gap_3', 'gap_4']].apply(lambda x:x.mean(), axis=1)


        #df_vx_train['gap_max'] = df_vx_train[['gap_0', 'gap_1', 'gap_2', 'gap_3', 'gap_4', 'gap_5', 'gap_6', 'gap_7', 'gap_8', 'gap_9', 'gap_10', 'gap_11', 'gap_12', 'gap_13', 'gap_14']].apply(lambda x:x.max(), axis=1)
        df_vx_train['gap_max'] = df_vx_train[['gap_0', 'gap_1', 'gap_2', 'gap_3', 'gap_4']].apply(lambda x:x.max(), axis=1)

        #df_vx_train['is_season_sum'] = df_vx_train[['is_season_0', 'is_season_1', 'is_season_2', 'is_season_3', 'is_season_4', 'is_season_5', 'is_season_6', 'is_season_7', 'is_season_8', 'is_season_9', 'is_season_10', 'is_season_11', 'is_season_12', 'is_season_13', 'is_season_14']].apply(lambda x:x.sum(), axis=1)
        df_vx_train['is_season_sum'] = df_vx_train[['is_season_0', 'is_season_1', 'is_season_2', 'is_season_3', 'is_season_4']].apply(lambda x:x.sum(), axis=1)
        #df_vx_train.to_csv('df_vx_train_season.csv')
        df_vx_train_season_tmp = copy.deepcopy(df_vx_train)
        seasonal_prdcd_list = list(set(df_vx_train_season_tmp[df_vx_train_season_tmp['gap_max'] >= 1.2]['PrdCd']))    
        df_vx_train_seasonal = df_vx_train_bk[df_vx_train_bk['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)

        table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "monthly-train-seasonal-12-" + str(today) + str(OUTPUT_TABLE_SUFFIX)

        if len(df_vx_train_seasonal):
            upload_complete = False
            while upload_complete == False:
                try:
                    # データアップロード時にコメントアウトを外す
                    client = BigqueryClient()
                    job = client.load_table_from_dataframe(df_vx_train_seasonal, table_id)
                    job.result()
                    logger.info("==data-uploaded-bq-seasonal===")

                    upload_complete = True

                except Exception as e:
                    logger.info(f'errtype: {str(type(e))}')
                    logger.info(f'err: {str(e)}')
                    logger.info('data upload retry')
                    time.sleep(20)

        # シーズン品以外の商品
        df_vx_train = df_vx_train_bk[~df_vx_train_bk['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)
        
    df_vx_train = df_vx_train.drop('hierarchical_model_id', axis=1)


    # モデル分割版
    if model_devide_2:
        # 非少量/少量品でデータを分割
        threshold_small_wave_nendo=2020
        df_vx_train['SalesAmount_Mean'] = df_vx_train.groupby("PrdCd")["SalesAmount"].transform(lambda x: x.mean())

        df_vx_train_not_small = df_vx_train[df_vx_train['SalesAmount_Mean'] >= df_vx_train['SalesAmount_Mean'].median()]

        df_vx_train_small = df_vx_train[df_vx_train['SalesAmount_Mean'] < df_vx_train['SalesAmount_Mean'].median()]         

        df_vx_train = df_vx_train.drop('SalesAmount_Mean', axis=1)
        df_vx_train_not_small = df_vx_train_not_small.drop('SalesAmount_Mean', axis=1)
        df_vx_train_small = df_vx_train_small.drop('SalesAmount_Mean', axis=1)    

        if cloudrunjob_mode:
            table_id1 = "dev-cainz-demandforecast.short_term_cloudrunjobs.monthly-train-not-small" + str(today)  + str(OUTPUT_TABLE_SUFFIX)
            table_id2 = "dev-cainz-demandforecast.short_term_cloudrunjobs.monthly-train-small" + str(today)  + str(OUTPUT_TABLE_SUFFIX)
        else:
            table_id1 = "dev-cainz-demandforecast.short_term_cloudrunjobs.monthly-train-not-small" + str(today)  + str(OUTPUT_TABLE_SUFFIX)
            table_id2 = "dev-cainz-demandforecast.short_term_cloudrunjobs.monthly-train-small" + str(today)  + str(OUTPUT_TABLE_SUFFIX)

        client = BigqueryClient()
        job = client.load_table_from_dataframe(df_vx_train_not_small, table_id1)
        job.result()

        client = BigqueryClient()
        job = client.load_table_from_dataframe(df_vx_train_small, table_id2)
        job.result()

    else:
        # 製品別の販売数平均値
        df_vx_train['SalesAmount_Mean'] = df_vx_train.groupby("PrdCd")["SalesAmount"].transform(lambda x: x.mean())

        # データを3分割
        def get_33_percentile(x):
            return np.percentile(x, q=[33])[0]

        def get_66_percentile(x):
            return np.percentile(x, q=[66])[0]

        df_vx_train['SalesAmount_Mean_33'] = get_33_percentile(df_vx_train['SalesAmount_Mean'])
        df_vx_train['SalesAmount_Mean_66'] = get_66_percentile(df_vx_train['SalesAmount_Mean'])

        df_vx_train_percentile = copy.deepcopy(df_vx_train)

        df_vx_train_large = df_vx_train[df_vx_train['SalesAmount_Mean'] >= df_vx_train['SalesAmount_Mean_66']]

        df_vx_train_middle = df_vx_train[  (df_vx_train['SalesAmount_Mean_66'] > df_vx_train['SalesAmount_Mean'])
                                         & (df_vx_train['SalesAmount_Mean'] >= df_vx_train['SalesAmount_Mean_33'])]
        df_vx_train_small = df_vx_train[df_vx_train['SalesAmount_Mean'] < df_vx_train['SalesAmount_Mean_33']]

        df_vx_train = df_vx_train.drop('SalesAmount_Mean', axis=1)
        df_vx_train = df_vx_train.drop('SalesAmount_Mean_33', axis=1)
        df_vx_train = df_vx_train.drop('SalesAmount_Mean_66', axis=1)                                 

        df_vx_train_large = df_vx_train_large.drop('SalesAmount_Mean', axis=1)
        df_vx_train_middle = df_vx_train_middle.drop('SalesAmount_Mean', axis=1)
        df_vx_train_small = df_vx_train_small.drop('SalesAmount_Mean', axis=1)

        df_vx_train_large = df_vx_train_large.drop('SalesAmount_Mean_33', axis=1)
        df_vx_train_middle = df_vx_train_middle.drop('SalesAmount_Mean_33', axis=1)
        df_vx_train_small = df_vx_train_small.drop('SalesAmount_Mean_33', axis=1)                           

        df_vx_train_large = df_vx_train_large.drop('SalesAmount_Mean_66', axis=1)    
        df_vx_train_middle = df_vx_train_middle.drop('SalesAmount_Mean_66', axis=1)   
        df_vx_train_small = df_vx_train_small.drop('SalesAmount_Mean_66', axis=1)   

        df_vx_train_middlesmall = pd.concat([df_vx_train_middle, df_vx_train_small])
        table_id1 = "dev-cainz-demandforecast.short_term_cloudrunjobs.monthly-train-large" + str(today) + str(OUTPUT_TABLE_SUFFIX)
        table_id2 = "dev-cainz-demandforecast.short_term_cloudrunjobs.monthly-train-middlesmall" + str(today) + str(OUTPUT_TABLE_SUFFIX)

        client = BigqueryClient()
        job = client.load_table_from_dataframe(df_vx_train_large, table_id1)
        job.result()


        client = BigqueryClient()
        job = client.load_table_from_dataframe(df_vx_train_middlesmall, table_id2)
        job.result()

        logger.info(f"Table ID 1: {table_id1}")
        logger.info(f"Table ID 2: {table_id2}")


    logger.info("==data-uploaded-bq divided model ===")
    end_t = time.time()
    elapsed_time = end_t - start_t
    logger.info(f"Elapsed time: {elapsed_time:.3f} seconds")
    logger.info("Stage completed")

    
    
if __name__ == "__main__":
    main()
            
