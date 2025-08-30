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
import scipy
from scipy import stats
import logging

# col_id = "商品コード"
# col_time = "週開始日付"
# window_size = 8

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

# BQテーブル名のサフィックス
OUTPUT_TABLE_SUFFIX = os.environ.get("OUTPUT_TABLE_SUFFIX", "")

# 日付を巻き戻す機能を使う場合の、巻き戻し指定年月日
TURN_BACK_YYYYMMDD = os.environ.get("TURN_BACK_YYYYMMDD", "")

# テーマMD機能を使うときは1にする
THEME_MD_MODE = int(os.environ.get("THEME_MD_MODE", 0))


################################
# OLD CODE
'''
print('TASK_INDEX:', TASK_INDEX)
print('TASK_COUNT:', TASK_COUNT)
print('TODAY_OFFSET:', TODAY_OFFSET)
print('OUTPUT_TABLE_SUFFIX:', OUTPUT_TABLE_SUFFIX)
print('THEME_MD_MODE:', THEME_MD_MODE)
print('TURN_BACK_YYYYMMDD:', TURN_BACK_YYYYMMDD)
'''

###############################
# REFACTORED CODE
logger.info(f'TASK_INDEX: {TASK_INDEX}')
logger.info(f'TASK_COUNT: {TASK_COUNT}')
logger.info(f'TODAY_OFFSET: {TODAY_OFFSET}')
logger.info(f'OUTPUT_TABLE_SUFFIX: {OUTPUT_TABLE_SUFFIX}')
logger.info(f'THEME_MD_MODE: {THEME_MD_MODE}')
logger.info(f'TURN_BACK_YYYYMMDD: {TURN_BACK_YYYYMMDD}')

###########copied to main
#df_calendar = extract_as_df_with_encoding("Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv","dev-cainz-demandforecast","utf-8")
#df_zen_calendar = df_calendar[["nenshudo","week_from_ymd", "znen_week_from_ymd"]]
# df_zen_calendar = df_zen_calendar.rename(columns={'week_from_ymd': '週開始日付', 'znen_week_from_ymd':'前年週開始日##付'})

#dfc_tmp = df_calendar[["nenshudo", "week_from_ymd", "week_to_ymd"]]
#dfc_tmp["week_from_ymd"] = dfc_tmp["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
#dfc_tmp["week_to_ymd"] = dfc_tmp["week_to_ymd"].apply(lambda x : pd.to_datetime(str(x)))


#tenpo_cd_list = [760, 294, 753, 809, 814, 836]
# 50店舗
#tenpo_cd_list = [760, 294, 753, 809, 814, 836, 156, 165, 231, 244, 256, 259, 268, 273, 274, 275, 277, 282, 284, 286, 287, 288, 289, 292, 296, 34, 730, 731, 734, 735, 737, 738, 743, 744, 750, 756, 764, 766, 768, 769, 775, 792, 793, 803, 813, 822, 827, 828, 832, 96] 

# 48店舗拡大 20230915
#tenpo_cd_list=[
#760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, #164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828
#]

# 48店舗拡大 + 鶴ヶ島＋鶴ヶ島資材館+その他資材館　20230920　54店舗
#tenpo_cd_list=[
#760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 284, 618, 253, 613, 615, 612]

# 48店舗拡大(リリース中)
#tenpo_cd_list=[
#760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828]


# 48店舗拡大(リリース中+6店舗ー＞54店舗)
# tenpo_cd_list=[
# 760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242]


# 全店　246店
#tenpo_cd_list=[
#760, 814, 294, 836, 809, 753, 20, 28, 31, 34, 47, 48, 50, 51, 52, 67, 69, 89, 96, 98, 102, 120, 132, 133, 134, 135, 136, 137, 139, 140, 143, 147, 151, 154, 155, 156, 157, 158, 162, 164, 165, 166, 167, 168, 230, 231, 232, 233, 234, 235, 236, 237, 238, 240, 242, 243, 244, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 295, 296, 612, 613, 615, 617, 618, 623, 664, 681, 682, 683, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 755, 756, 757, 758, 759, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 802, 803, 804, 806, 807, 808, 810, 811, 812, 813, 815, 816, 817, 818, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 837, 838, 839, 840, 842, 843, 844, 845, 848, 849, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 866, 867, 869, 870, 871, 872, 873, 875, 876, 877, 878, 879, 624, 684, 868, 874
#]


# 全店246店から2021年以降開店を除く228店（stage2以降に使用）テーマMDでも使用(776除く）
#tenpo_cd_list=[
#760, 814, 294, 836, 809, 753, 20, 28, 31, 34, 47, 48, 50, 51, 52, 67, 69, 89, 96, 98, 102, 120, 132, 133, 134, 135, 136, 137, 139, 140, 143, 147, 151, 154, 155, 156, 157, 158, 162, 164, 165, 166, 167, 168, 230, 231, 232, 233, 234, 235, 236, 237, 238, 240, 242, 243, 244, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 295, 296, 612, 613, 615, 617, 618, 623, 664, 681, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 755, 756, 757, 758, 759, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 802, 803, 804, 806, 807, 808, 810, 811, 812, 813, 815, 816, 817, 818, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 837, 838, 839, 840, 842, 843, 844, 845, 848, 849, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 871
#]


# 全店246店から2021年以降開店を除く238店（stage2以降に使用, 新店10店舗追加）
#tenpo_cd_list=[
#760, 814, 294, 836, 809, 753, 20, 28, 31, 34, 47, 48, 50, 51, 52, 67, 69, 89, 96, 98, 102, 120, 132, 133, 134, 135, 136, 137, 139, 140, 143, 147, 151, 154, 155, 156, 157, 158, 162, 164, 165, 166, 167, 168, 230, 231, 232, 233, 234, 235, 236, 237, 238, 240, 242, 243, 244, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 295, 296, 612, 613, 615, 617, 618, 623, 664, 681, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 755, 756, 757, 758, 759, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 802, 803, 804, 806, 807, 808, 810, 811, 812, 813, 815, 816, 817, 818, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 837, 838, 839, 840, 842, 843, 844, 845, 848, 849, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 871,
#869, 870, 871, 872, 873, 874, 875, 876, 877, 878
#]

# 776(index158)　豊野赤沼　閉店
# 818 伊豆高原　MinMax出ていなかった


# 全店246店から2021年以降開店を除く236店（stage2以降に使用, 新店10店舗追加）              (870:ＳＦららぽーと立川立飛 872:ＳＦららぽーと湘南平塚 削除)
#tenpo_cd_list=[
#760, 814, 294, 836, 809, 753, 20, 28, 31, 34, 47, 48, 50, 51, 52, 67, 69, 89, 96, 98, 102, 120, 132, 133, 134, 135, 136, 137, 139, 140, 143, 147, 151, 154, 155, 156, 157, 158, 162, 164, 165, 166, 167, 168, 230, 231, 232, 233, 234, 235, 236, 237, 238, 240, 242, 243, 244, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 295, 296, 612, 613, 615, 617, 618, 623, 664, 681, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 755, 756, 757, 758, 759, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 802, 803, 804, 806, 807, 808, 810, 811, 812, 813, 815, 816, 817, 818, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 837, 838, 839, 840, 842, 843, 844, 845, 848, 849, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 871,
#869, 871, 873, 874, 875, 876, 877, 878
#]

# 新店のみ、8店舗（SFのぞく）
#tenpo_cd_list=[
#869, 871, 873, 874, 875, 876, 877, 878
#]


# 全店拡大20240813 209店舗
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
]

'''
# 全店拡大20240926年末積み増し対応 239店舗(22店舗追加)
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 
50, 137, 154, 235, 613, 615, 617, 618, 623, 624, 741, 767, 794, 799, 808, 812, 842, 868, 875, 878, 903, 909,
]
'''
# 参照店舗あり17店舗
#tenpo_cd_list=[
#869, 871, 873, 874, 876, 877, 879, 900, 907, 902, 904, 624, 868, 875, 878, 903, 909, 
#]

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

# Green_東日本の店舗
'''
tenpo_cd_list=[265 ,791 ,820 ,832 ,292 ,268 ,827 ,798 ,797 ,253 ,746 ,287 ,878 ,813 ,748 ,254 ,135 ,733 ,852 ,761 ,234 ,261 ,796 ,288 ,876 ,803 ,859 ,281 ,102 ,133 ,266 ,296 ,137 ,779 ,833 ,871 ,166 ,766 ,20 ,750 ,291 ,738 ,903 ,900 ,140 ,98 ,238 ,162 ,243 ,69 ,263 ,804 ,272 ,232 ,856 ,742 ,753 ,866 ,168 ,96 ,814 ,277 ,279 ,806 ,132 ,293 ,262 ,861 ,800 ,247 ,816 ,736 ,264 ,873 ,151 ,136 ,867 ,747 ,143 ,756 ,276 ,778 ,34 ,807 ,755 ,745 ,233 ,904 ,793 ,817 ,773 ,821 ,120 ,28 ,164 ,809 ,259 ,295 ,157 ,845 ,771 ,289 ,759 ,165 ,244 ,251 ,31 ,770 ,662 ,282 ,252 ,792 ,48 ,258 ,47 ,844 ,865 ,851 ,848 ,236 ,730 ,242 ,775 ,839 ,284 ,89 ,664 ,50 ,760 ,836 ,134 ,269 ,790 ,777 ,255]
'''



'''
# 全店拡大20250109 217店舗（古河は13週経過しないので入れていない）
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 
]
'''


# 全店拡大20250130 218店舗（古河は13週経過したので入れる）
tenpo_cd_list=[
760, 814, 294, 836, 809, 753, 287, 845, 269, 281, 277, 766, 168, 292, 797, 807, 265, 738, 791, 792, 166, 288, 247, 730, 252, 755, 164, 804, 263, 289, 261, 793, 156, 273, 231, 835, 278, 286, 823, 825, 732, 763, 274, 822, 743, 843, 737, 828, 28, 96, 98, 102, 157, 242,    
20, 31, 34, 47, 48, 67, 89, 120, 132, 133, 134, 135, 136, 139, 140, 143, 147, 151, 155, 158, 162, 165, 167, 230, 232, 233, 234, 236, 237, 238, 240, 243, 244, 246, 248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 262, 264, 266, 267, 268, 270, 271, 272, 275, 276, 279, 280, 282, 283, 284, 285, 290, 291, 293, 295, 296, 612, 664, 731, 733, 734, 735, 736, 739, 740, 742, 744, 745, 746, 747, 748, 749, 750, 751, 752, 754, 756, 757, 758, 759, 761, 762, 764, 765, 768, 769, 770, 771, 772, 773, 774, 775, 777, 778, 779, 790, 795, 796, 798, 800, 802, 803, 806, 810, 811, 813, 815, 816, 817, 818, 820, 821, 824, 826, 827, 829, 830, 831, 832, 833, 834, 837, 838, 839, 840, 844, 848, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 867, 869, 871, 873, 
874, 876, 877, 879, 900, 907, 902, 904, 910
]


if cloudrunjob_mode:
    tenpo_cd = tenpo_cd_list[TASK_INDEX]
    # print('TASK_INDEX:', TASK_INDEX, 'tenpo_cd:', tenpo_cd)
    logger.info(f"TASK_INDEX: {TASK_INDEX}, tenpo_cd: {tenpo_cd}")
else:
    # notebookで動かすモード
    tenpo_cd = int(sys.argv[1])
    # print('tenpo_cd:', tenpo_cd)
    logger.info(f"tenpo_cd: {tenpo_cd}")
    TODAY_OFFSET = 28
    # print('TODAY_OFFSET:', TODAY_OFFSET)
    logger.info(f"TODAY_OFFSET: {TODAY_OFFSET}")
    OUTPUT_TABLE_SUFFIX = '_salesupflg_20241119back_6str_narrow_criteria_2000-10000w_2nd'

    # print('OUTPUT_TABLE_SUFFIX:', OUTPUT_TABLE_SUFFIX)
    logger.info(f"OUTPUT_TABLE_SUFFIX: {OUTPUT_TABLE_SUFFIX}")
    TURN_BACK_YYYYMMDD = '20241119'
    #TURN_BACK_YYYYMMDD = ''
    # print('TURN_BACK_YYYYMMDD:', TURN_BACK_YYYYMMDD)
    logger.info(f"TURN_BACK_YYYYMMDD: {TURN_BACK_YYYYMMDD}")
    THEME_MD_MODE = 0
    # print('THEME_MD_MODE:', THEME_MD_MODE)
    logger.info(f"THEME_MD_MODE: {THEME_MD_MODE}")



# 既存6店舗でテスト用
#tenpo_cd_list = [760, 294, 753, 809, 814, 836]    

bucket_name = "dev-cainz-demandforecast"
#tenpo_cd = sys.argv[1]

# stage1の前日以前の過去データを使うフラグ
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


#if use_past_stage1_data_flag:
#    today = datetime.date.today() - timedelta(days=past_days_num)    
#else:
#    today = datetime.date.today()
    
my_date = today.strftime('%Y%m%d')
my_date = int(my_date)

# print('************ today:', today)
logger.info(f"************ today: {today}")



# flag設定 *************************************************************
# JAN差し替えファイルを使うかどうか指定する
use_jan_connect = True

# ODAS補正の改良を有効にする
odas_imprvmnt = True
odas_hosei_under_threshold = 100 # 通常
#odas_hosei_under_threshold = 50
#odas_hosei_under_threshold = 20 # 花王AVIだとこっち



# 爆買い補正を有効にする
bakugai_hosei = True


# 目的変数を対数化
logarithmize_target_variable = False

# MinMaxにない商品、MinMax下限が0の商品を除く
restrict_minmax = True

# 店舗発注が終わっている商品を除く
restrinct_tenpo_hacchu_end = True


# 販売のない販売期間に販売のある期間の売り数をコピーし、販売無い期間のweightを0にする
# (週次モデルではこの機能はあまり効かないのでOFFにしておく, 補正のしすぎになるかも)
# ****************************
# 処理時間がかかる・・・？
# テーマMDのときはデータが少ないのでonでよい
# ****************************
no_sales_term_weight_zero = True

# クラス波形で売上の無い期間を補間する
interporate_by_class_wave = False

# 1年実績あれば、それ以前の未販売期間を補間する
interpolate_1yearsales = False

# 1年以上販売の無い商品を除く
exclude_salesless1year = False


# 価格を事前に既知にする(フラグのみで処理追加なしｗ)
kakaku_jizen_kichi = True

# 予測期間を5+15週から、5+4週にする
# (trainデータ作成時はとくに処理の変更はないのでコメントにしておく)
#prediction_term_4week = True


# モデル学習上の現在日時を巻き戻す
turn_back_time = False
if turn_back_time:
    turn_back_yyyymmdd = int(TURN_BACK_YYYYMMDD)
    
    
# 新店の場合、参照店舗の全データを使ってモデルを作成する（新店にない商品のデータも入る）
add_reference_store = True
# 新店と既存店のモデルを分けない
add_reference_store_unitedmodel = True


# 新店の場合、新店週次モデルにある商品番号のみ、参照店舗の過去データを使ってモデルを作成する
# (そもそも新店は週次モデルに分類される商品が少ないのでよろしくないので、ここはfalseでよい)
#add_reference_store_by_prdcd = False


# モデルを販売数量で分割する（細かく分割して階層モデル）
#divide_by_salesamount = False # これはもう使わないのでFalseでよい。

# モデルを販売数量で分割する（売り数30で分割して分割モデル）
divide_by_salesamount_v2 = True # テーマMD, シーズンモデルはOFF　20241008


# モデルを販売数量で分割する（売り数分割を10, 30にする）　これを使うときはv2もTrueにすること いまのところOFFで
divide_by_salesamount_v3 = False

# 店休補正
store_closed_correction = False


# 予測期間を5+15週から、5+21週にする
# (trainデータ作成時はとくに処理の変更はないのでコメントにしておく)
#prediction_term_26week = True

# 学習期間、検証期間、評価期間の列を追加する
add_split_column = True

# 店舗出荷数量のテーブルから、ECの販売数の列を追加する
#`dev-cainz-demandforecast.dev_cainz_nssol.shipment_with_store_inventory`
add_ec_salesamount = True


# クラス波形を追加する(これをonにするときはEC販売数をonにすること)
class_wave_add = True
# クラス波形平均値
class_wave_mean_add = True

# シーズン品の分離(定番品とシーズン品を完全にわける)
devide_season_items = False

# 予測距離を5週＋6週の合計11週にする（trainデータは特に処理変更なし）

# --------------------------------------------------------------------------------
# 直近13週中6週実績あり、平均週販2以上の提案ありのデータを対象とする**切り替え必要＊＊
output_6wk_2sales = True
# --------------------------------------------------------------------------------

# 直近1か月のweightを大きくして、直近の売り数変化に追従できるようにする
last_month_weight_larger = False


# 繁忙期フラグ
salesup_flag = True

# 93を追加 20240304
dpt_list = [69,97,14,37,27,39,28,74,33,30,36,75,85,80,20,22,55,72,15,62,32,77,84,89,23,60,25,87,68,56,92,61,2,40,86,88,26,17,24,34,52,64,73,21,35,58,83,94,63,38,18,29,19,31,53,45,50,81,82,90,91,54,95,93]


# # 花王テーマMDのDPT設定
#dpt_list = [22,34,64,72,73,74,83,84,85,86,87,89,93]
#dpt_list = [72, 83, 86, 87]


# GreenのDPT設定
#dpt_list = [28]



# これは使っていない
exclusion_dpt_list = [2,20,26,30,33,37,39,47,48,53,57,80,98]

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



def get_last_year_sales_amount_left(df):
    df_tmp = df[["商品コード","週開始日付","8週平均ema"]]
    df_tmp = df_tmp.rename(columns={'週開始日付': '前年週開始日付','8週平均ema': 'time_leap8'})
    

    df_tmp2 = pd.merge(df,df_tmp,on = ["商品コード","前年週開始日付"], how='left')
    
    df_tmp2['time_leap8'] = df_tmp2['time_leap8'].fillna(0)

    return df_tmp2



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


def interpolate_df(sales_df, df_calendar, add_ec_salesamount):
    
    # 販売データの年週度のmin maxを取得
    max_nenshudo = sales_df["nenshudo"].max()
    min_nenshudo = sales_df["nenshudo"].min()
    # min max期間内のカレンダーを作成する
    df_calendar_tmp = df_calendar[["nenshudo"]]
    df_calendar_tmp = df_calendar_tmp[df_calendar_tmp["nenshudo"] <= max_nenshudo].reset_index(drop=True)
    df_calendar_tmp = df_calendar_tmp[df_calendar_tmp["nenshudo"] >= min_nenshudo].reset_index(drop=True)
    prd_cd_list = sales_df['PRD_CD'].values.tolist()
    prd_cd_list = list(set(prd_cd_list))

    # 商品コード×カレンダーのマトリクスを生成する
    for prd_cd in prd_cd_list:
        df_calendar_tmp[prd_cd] = 0

    df_calendar_tmp = pd.melt(df_calendar_tmp, id_vars=["nenshudo"], var_name="PRD_CD", value_name="delete")
    df_calendar_tmp = df_calendar_tmp[["nenshudo", "PRD_CD"]]

    sales_df["key"] = sales_df["nenshudo"].astype(str).str.cat(sales_df["PRD_CD"].astype(str), sep='-')
    df_calendar_tmp["key"] = df_calendar_tmp["nenshudo"].astype(str).str.cat(df_calendar_tmp["PRD_CD"].astype(str), sep='-')

    # 商品コード×カレンダーのマトリクスに、販売データを結合する

    if add_ec_salesamount:
        merged_sales_df = pd.merge(df_calendar_tmp, sales_df[["key", "URI_SU", "URI_SU_EC", "TENPO_CD", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']], how="left", on="key")
        merged_sales_df['URI_SU'] = merged_sales_df['URI_SU'].fillna(0)
        merged_sales_df['URI_SU_EC'] = merged_sales_df['URI_SU_EC'].fillna(0)            
    else:
        merged_sales_df = pd.merge(df_calendar_tmp, sales_df[["key", "URI_SU", "TENPO_CD", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']], how="left", on="key")
        merged_sales_df['URI_SU'] = merged_sales_df['URI_SU'].fillna(0)
        
        
    # 1回目の補完をする　********************************************************
    #　売価統一の補完
    merged_sales_df[merged_sales_df['baika_toitsu'] == 0]['baika_toitsu'] = np.nan
    merged_sales_df['baika_toitsu'] = merged_sales_df['baika_toitsu'].astype(float)
    merged_sales_df['baika_toitsu'] = merged_sales_df.groupby(["PRD_CD"])['baika_toitsu'].transform(lambda x: x.interpolate(limit_direction='both'))
    
    # 売価の補完
    merged_sales_df['BAIKA'] = merged_sales_df['BAIKA'].astype(float)
    merged_sales_df['BAIKA'] = merged_sales_df.groupby(["PRD_CD"])['BAIKA'].transform(lambda x: x.interpolate(limit_direction='both'))
    
    # DPTの補完
    merged_sales_df['DPT'] = merged_sales_df['DPT'].astype(float)
    merged_sales_df['DPT'] = merged_sales_df.groupby(["PRD_CD"])['DPT'].transform(lambda x: x.interpolate(limit_direction='both'))
    merged_sales_df['DPT'] = merged_sales_df['DPT'].astype(int)
    
    # LINEコードの補完
    merged_sales_df['line_cd'] = merged_sales_df['line_cd'].astype(float)
    merged_sales_df['line_cd'] = merged_sales_df.groupby(["PRD_CD"])['line_cd'].transform(lambda x: x.interpolate(limit_direction='both'))
    merged_sales_df['line_cd'] = merged_sales_df['line_cd'].astype(int)
    
    # クラスコードの補完
    merged_sales_df['cls_cd'] = merged_sales_df['cls_cd'].astype(float)
    merged_sales_df['cls_cd'] = merged_sales_df.groupby(["PRD_CD"])['cls_cd'].transform(lambda x: x.interpolate(limit_direction='both'))    
    merged_sales_df['cls_cd'] = merged_sales_df['cls_cd'].astype(int)
    
    # 品目コードの補完
    merged_sales_df['hnmk_cd'] = merged_sales_df['hnmk_cd'].astype(float)
    merged_sales_df['hnmk_cd'] = merged_sales_df.groupby(["PRD_CD"])['hnmk_cd'].transform(lambda x: x.interpolate(limit_direction='both'))    
    merged_sales_df['hnmk_cd'] = merged_sales_df['hnmk_cd'].astype(int)
    
    merged_sales_df = merged_sales_df.fillna(0)
    
    
    
    # 上の処理と同じのを繰り返し・・？　不要っぽい
  

    # カレンダーとマージ**************************************************************************************
    sales_df["key"] = sales_df["nenshudo"].astype(str).str.cat(sales_df["PRD_CD"].astype(str), sep='-')
    df_calendar_tmp["key"] = df_calendar_tmp["nenshudo"].astype(str).str.cat(df_calendar_tmp["PRD_CD"].astype(str), sep='-')

    if add_ec_salesamount:
        merged_sales_df = pd.merge(df_calendar_tmp, sales_df[["key", "URI_SU", "URI_SU_EC", "TENPO_CD", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']], how="left", on="key")
    else:
        merged_sales_df = pd.merge(df_calendar_tmp, sales_df[["key", "URI_SU", "TENPO_CD", 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']], how="left", on="key") 

    
    # カレンダーとマージしたので(NULL行が発生する)補間する *************************************************************
    # 売り数は0で補完する
    merged_sales_df['URI_SU'] = merged_sales_df['URI_SU'].fillna(0)
    
    # 売価統一の補完
    merged_sales_df[merged_sales_df['baika_toitsu'] == 0]['baika_toitsu'] = np.nan
    merged_sales_df['baika_toitsu'] = merged_sales_df['baika_toitsu'].astype(float)
    merged_sales_df['baika_toitsu'] = merged_sales_df.groupby(["PRD_CD"])['baika_toitsu'].transform(lambda x: x.interpolate(limit_direction='both'))
    
    # 売価の補完
    merged_sales_df['BAIKA'] = merged_sales_df['BAIKA'].astype(float)
    merged_sales_df['BAIKA'] = merged_sales_df.groupby(["PRD_CD"])['BAIKA'].transform(lambda x: x.interpolate(limit_direction='both'))
    
    # DPTの補完
    merged_sales_df['DPT'] = merged_sales_df['DPT'].astype(float)
    merged_sales_df['DPT'] = merged_sales_df.groupby(["PRD_CD"])['DPT'].transform(lambda x: x.interpolate(limit_direction='both'))
    merged_sales_df['DPT'] = merged_sales_df['DPT'].astype(int)
    
    # ラインコードの補完
    merged_sales_df['line_cd'] = merged_sales_df['line_cd'].astype(float)
    merged_sales_df['line_cd'] = merged_sales_df.groupby(["PRD_CD"])['line_cd'].transform(lambda x: x.interpolate(limit_direction='both'))
    merged_sales_df['line_cd'] = merged_sales_df['line_cd'].astype(int)
    
    # クラスコードの補完
    merged_sales_df['cls_cd'] = merged_sales_df['cls_cd'].astype(float)
    merged_sales_df['cls_cd'] = merged_sales_df.groupby(["PRD_CD"])['cls_cd'].transform(lambda x: x.interpolate(limit_direction='both'))    
    merged_sales_df['cls_cd'] = merged_sales_df['cls_cd'].astype(int)

    # 品目コードの補完
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
    # old odas 
    if use_jan_connect:
        path_odas_list = "01_short_term/60_cached_data/07_odas_old/ODAS_old.csv"
        #path_odas_list = "01_short_term/60_cached_data_bk20230224/07_odas_old/ODAS_old.csv"
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

        # print(tenpo_cd, ' df_odas_old shape:', df_odas_old.shape)
        logger.info(f"{tenpo_cd} df_odas_old shape: {df_odas_old.shape}")
        #df_odas_old.to_csv('df_odas_old.csv')

    
    
    # print("===df_odas_old['店番'].dtype====",df_odas_old['店番'].dtype)
    logger.info(f"===df_odas_old['店番'].dtype==== {df_odas_old['店番'].dtype}")
    
    df_odas_old = df_odas_old.loc[df_odas_old['店番'] == int(tenpo_cd)].reset_index(drop=True)    
    df_odas_old = df_odas_old.rename(columns={"店番":"TENPO_CD", "JAN":"PRD_CD", "数量": "odas_amount","売上計上日":"sales_ymd"})

    df_odas_calender = pd.DataFrame()

    for wfy,wty,nsd in zip(df_calendar["week_from_ymd"],df_calendar["week_to_ymd"],df_calendar["nenshudo"]):
        tmp = df_odas_old[(df_odas_old["sales_ymd"]>=wfy)&(df_odas_old["sales_ymd"]<=wty)]
        tmp["nenshudo"] = nsd
        df_odas_calender = pd.concat([df_odas_calender,tmp])
        df_odas_calender = df_odas_calender.reset_index(drop=True)

    df_odas_calender = df_odas_calender.dropna(subset=['PRD_CD']).reset_index(drop=True)
    
    #df_odas_calender.to_csv('df_odas_calender_0.csv')

    
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
        
        # print('***df_odas_new 0 shape:', df_odas_new.shape)
        logger.info(f"***df_odas_new 0 shape: {df_odas_new.shape}")
        # ここはtenpo_cdをint型にしなくてよいか？　差し替え前データの店舗コードは文字列("""")なのでよいはず
        df_odas_new = df_odas_new[df_odas_new['tenpo_cd']==int(tenpo_cd)].reset_index(drop=True)

        # print('***df_odas_new 1 shape:', df_odas_new.shape)
        logger.info(f"***df_odas_new 1 shape: {df_odas_new.shape}")
        
        df_odas_new['prd_cd_len'] = df_odas_new['prd_cd'].apply(lambda x:len(x))
        df_odas_new = df_odas_new[df_odas_new['prd_cd_len']<=13]
        
        # print('***df_odas_new 2 shape:', df_odas_new.shape)
        logger.info(f"***df_odas_new 2 shape: {df_odas_new.shape}")

        df_odas_new['prd_cd_isnumelic'] = df_odas_new['prd_cd'].apply(lambda x:x.isnumeric())
        df_odas_new = df_odas_new[df_odas_new['prd_cd_isnumelic']==True]
        
        # print('***df_odas_new 3 shape:', df_odas_new.shape)
        logger.info(f"***df_odas_new 3 shape: {df_odas_new.shape}")

        df_odas_new['prd_cd'] = df_odas_new['prd_cd'].astype(int)
        
        # print(tenpo_cd, ' df_odas_new shape:', df_odas_new.shape)
        logger.info(f"{tenpo_cd} df_odas_new shape: {df_odas_new.shape}")
        
        #df_odas_new.to_csv('df_odas_new.csv')

 
    # print("==df_odas_new['tenpo_cd']==",df_odas_new['tenpo_cd'].dtype)
    logger.info(f"==df_odas_new['tenpo_cd']== {df_odas_new['tenpo_cd'].dtype}")
    df_odas_new = df_odas_new[df_odas_new['tenpo_cd'] == int(tenpo_cd)].reset_index(drop=True)
    # print("====df_odas_new===",df_odas_new)
    logger.info(f"====df_odas_new=== {df_odas_new}")
    
    # print('***df_odas_new 4 shape:', df_odas_new.shape)
    logger.info(f"***df_odas_new 4 shape: {df_odas_new.shape}")

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

    # print('***df_odas_new 5 shape:', df_odas_new.shape)
    logger.info(f"***df_odas_new 5 shape: {df_odas_new.shape}")
    
    df_odas_calender_new = pd.DataFrame()

    for wfy,wty,nsd in zip(df_calendar["week_from_ymd"],df_calendar["week_to_ymd"],df_calendar["nenshudo"]):
        tmp = df_odas_new[(df_odas_new["sales_ymd"]>=wfy)&(df_odas_new["sales_ymd"]<=wty)]
        tmp["nenshudo"] = nsd
        df_odas_calender_new = pd.concat([df_odas_calender_new,tmp])
        df_odas_calender_new = df_odas_calender_new.reset_index(drop=True)

    df_odas_calender_new = df_odas_calender_new.dropna(subset=['PRD_CD']).reset_index(drop=True)
    # print("===df_odas_calender_new===",df_odas_calender_new)
    logger.info(f"===df_odas_calender_new=== {df_odas_calender_new}")
    #df_odas_calender_new.to_csv('df_odas_calender_new.csv')
    
    
    #df_odas_calender.to_csv('df_odas_calender.csv')
    #df_odas_calender_new.to_csv('df_odas_calender_new.csv')
    
    odas_merge_df = pd.concat([df_odas_calender, df_odas_calender_new]).reset_index(drop=True)
    #odas_merge_df.to_csv('odas_merge_df.csv')

    # 週ごと品番ごとにサマリする
    # 新旧Odasのデータが合わさっている部分を削除する
    #odas_merge_df.to_csv('odas_merge_df.csv')    
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

        
def load_tenkyu_master(
    tenpo_cd,
    calendar
):
    #dev-cainz-demandforecast/Basic_Analysis_unzip_result/01_Data/32_tenkyu/28_TENKYU.csv
    bucket_name = 'dev-cainz-demandforecast'
    path_tenkyu = "Basic_Analysis_unzip_result/01_Data/32_tenkyu/28_TENKYU.csv"
    tenkyu = extract_as_df(path_tenkyu, bucket_name)
    
    tenkyu = tenkyu.loc[tenkyu['tenpo_cd']==tenpo_cd].reset_index(drop=True)
    tenkyu['taiymd'] = pd.to_datetime(tenkyu['taiymd'], format='%Y%m%d')
    
    tenkyu_master = pd.DataFrame()

    temp_calendar = calendar.reset_index(drop=True).copy()
    
    if 1:
        for i in range(len(temp_calendar)):
            tt = tenkyu[(tenkyu['taiymd']>=temp_calendar['week_from_ymd'][i])&(tenkyu['taiymd']<=temp_calendar['week_to_ymd'][i])]['tenkyu_flg'].sum()
            temp_calendar.loc[i, '店休日数'] = tt
            
    else:
        for i in range(len(temp_calendar)):
            temp_calendar.loc[i, '店休日数'] = tenkyu.loc[
                (tenkyu['taiymd']>=temp_calendar['week_from_ymd'][i]) &\
                (tenkyu['taiymd']<=temp_calendar['week_to_ymd'][i])
            ]['tenkyu_flg'].sum()

    temp_calendar['TENPO_CD'] = tenpo_cd
    tenkyu_master = pd.concat([
        tenkyu_master,
        temp_calendar[['nenshudo', 'TENPO_CD', '店休日数']]],
        axis=0
    ).reset_index(drop=True)
        
    return tenkyu_master




################################
# REFACTORED CODE
def process_sales_data(dpt_list, tenpo_cd, tenpo_cd_ref, path_tran, bucket_name, THEME_MD_MODE, add_ec_salesamount,
                       restrict_minmax, restrinct_tenpo_hacchu_end, my_date,
                       project_id="dev-cainz-demandforecast", dataset_id='dev_cainz_nssol',
                       table_id='shipment_with_store_inventory'):
    """
    Processes sales data for a given list of departments (dpt_list).

    Args:
      dpt_list (list): List of department codes.
      tenpo_cd (int): Store code.
      tenpo_cd_ref (int, None): Reference store code (for new stores), or None.
      path_tran (str): Path to transaction data file. Should accept two format arguments: department and store code.
      bucket_name (str): Name of the cloud storage bucket.
      THEME_MD_MODE (bool): Flag for theme-based merchandising mode.
      add_ec_salesamount (bool): Flag to add e-commerce sales amounts.
      restrict_minmax (bool): Flag to restrict data based on MinMax values.
      restrinct_tenpo_hacchu_end (bool): Flag to restrict data based on store-specific order end dates.
      my_date (int): Current date (used for order end date restriction).
      project_id (str): Google Cloud project ID. Default is "dev-cainz-demandforecast".
      dataset_id (str): BigQuery dataset ID. Default is 'dev_cainz_nssol'.
      table_id (str): BigQuery table ID. Default is 'shipment_with_store_inventory'.

    Returns:
      pandas.DataFrame: Processed sales data DataFrame.
    """

    sales_df = pd.DataFrame()

    for dpt in dpt_list:
        temp_df = pd.DataFrame()
        try:
            # Determine the correct tenpo_cd to use
            current_tenpo_cd = str(tenpo_cd_ref) if tenpo_cd_ref is not None else str(tenpo_cd)
            # Call extract_as_df_with_encoding (now assumed to be globally available/imported)
            temp_df = extract_as_df_with_encoding(path_tran.format(dpt, current_tenpo_cd), bucket_name, "utf-8")

            # Drop Unnamed column
            if 'Unnamed: 0' in temp_df.columns:
                temp_df = temp_df.drop('Unnamed: 0', axis=1)

            if THEME_MD_MODE:
                # Calculate PRD_CD prefix once
                temp_df['PRD_CD_PREFIX'] = (temp_df['PRD_CD'] / 1000000).astype(int)

                # Extract Kao products
                kao_prd_prefixes = [4901301, 4973167]
                temp_df_ex1 = temp_df[temp_df['PRD_CD_PREFIX'].isin(kao_prd_prefixes)].reset_index(drop=True)

                if not temp_df_ex1.empty:
                    cls_cd_list1 = temp_df_ex1['cls_cd'].unique().tolist()

                    # Extract P&G and Lion products in the same class
                    pg_lion_prd_prefixes = [4902430, 4987176, 4903301]
                    temp_df_ex2 = temp_df[
                        temp_df['cls_cd'].isin(cls_cd_list1) & temp_df['PRD_CD_PREFIX'].isin(pg_lion_prd_prefixes)
                        ]

                    if not temp_df_ex2.empty:
                        temp_df = pd.concat([temp_df_ex1, temp_df_ex2]).reset_index(drop=True)
                    else:
                        temp_df = temp_df_ex1

            temp_df['DPT'] = int(dpt)
            sales_df = pd.concat([sales_df, temp_df], axis=0).reset_index(drop=True)
        except:
            continue

    sales_df['TENPO_CD'] = tenpo_cd

    logger.info(f"sales_df.shape: {sales_df.shape}")

    if sales_df.empty:
        logger.info("=====There is no sales data, please check: stage1 has been executed======")
        sys.exit(1)

    sales_df = sales_df[['PRD_CD', 'nenshudo', 'URI_SU', 'TENPO_CD', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    logger.info(f"sales_df SKU: {len(sales_df['PRD_CD'].unique())}")

    if add_ec_salesamount:
#         my_tenpo_cd = str(tenpo_cd).zfill(4)

#         # Retaining the original BigQuery query structure including the placeholder FROM `.`
#         # and WHERE ship_place_code = '' to ensure no functional changes.
#         target_query = f"""
#         SELECT PRD_CD, NENDO, SHUDO, SUM(URI_SU_EC) AS URI_SU_EC
#         FROM (
#             SELECT d_product_code AS PRD_CD, d_ship_date_jst_of_Nendo_Weekly AS NENDO,
#                 d_ship_date_jst_of_Shudo_Weekly AS SHUDO, total_qnt AS URI_SU_EC
#             FROM `.`
#             WHERE ship_place_code = ''
#         )
#         GROUP BY PRD_CD, NENDO, SHUDO
#         ORDER BY PRD_CD, NENDO, SHUDO
#         """

#         logger.info(f"{target_query}")
# #         try:
# #             ec_sales_df = pd.read_gbq(target_query, project_id, dialect='standard')
# #             sales_df['PRD_CD'] = sales_df['PRD_CD'].astype(int)
# #             sales_df['nenshudo'] = sales_df['nenshudo'].astype(int)

# #             ec_sales_df['PRD_CD'] = ec_sales_df['PRD_CD'].astype(int)
# #             ec_sales_df['nenshudo'] = (ec_sales_df['NENDO'].astype(str) + ec_sales_df['SHUDO'].astype(str)).astype(int)

# #             sales_df = pd.merge(sales_df, ec_sales_df[['PRD_CD', 'nenshudo', 'URI_SU_EC']], on=['PRD_CD', 'nenshudo'], how='left')
# #             sales_df['URI_SU_EC'] = sales_df['URI_SU_EC'].fillna(0.0)
            
# #         except Exception as e:
# #             logger.error(f"Error reading from BigQuery: {e}")

#         ec_sales_df = pd.read_gbq(target_query, project_id, dialect='standard')
#         sales_df['PRD_CD'] = sales_df['PRD_CD'].astype(int)
#         sales_df['nenshudo'] = sales_df['nenshudo'].astype(int)
#         ec_sales_df['PRD_CD'] = ec_sales_df['PRD_CD'].astype(int)
#         ec_sales_df['nenshudo'] = ec_sales_df['NENDO'].astype(str) + ec_sales_df['SHUDO'].astype(str)
#         ec_sales_df['nenshudo'] = ec_sales_df['nenshudo'].astype(int)

#         # Merge dataframes
#         sales_df = pd.merge(sales_df, ec_sales_df[['PRD_CD', 'nenshudo', 'URI_SU_EC']], on=['PRD_CD', 'nenshudo'], how='left')
#         sales_df['URI_SU_EC'] = sales_df['URI_SU_EC'].fillna(0.0)

        my_tenpo_cd = str(tenpo_cd).zfill(4)

        # Construct the query
        target_query = f"""
            SELECT 
                PRD_CD, 
                NENDO, 
                SHUDO, 
                SUM(URI_SU_EC) AS URI_SU_EC
            FROM (
                SELECT 
                    d_product_code AS PRD_CD, 
                    d_ship_date_jst_of_Nendo_Weekly AS NENDO, 
                    d_ship_date_jst_of_Shudo_Weekly AS SHUDO, 
                    total_qnt AS URI_SU_EC 
                FROM `{dataset_id}.{table_id}` 
                WHERE ship_place_code = '{my_tenpo_cd}' 
                ORDER BY PRD_CD, NENDO, SHUDO
            ) 
            GROUP BY PRD_CD, NENDO, SHUDO 
            ORDER BY PRD_CD, NENDO, SHUDO
        """

        logger.info(f"{target_query}")
        ec_sales_df = pd.read_gbq(target_query, project_id, dialect='standard')

        # Data type consistency
        sales_df['PRD_CD'] = sales_df['PRD_CD'].astype(int)
        sales_df['nenshudo'] = sales_df['nenshudo'].astype(int)

        ec_sales_df['PRD_CD'] = ec_sales_df['PRD_CD'].astype(int)
        ec_sales_df['nenshudo'] = ec_sales_df['NENDO'].astype(str) + ec_sales_df['SHUDO'].astype(str)
        ec_sales_df['nenshudo'] = ec_sales_df['nenshudo'].astype(int)

        # Merge dataframes
        sales_df = pd.merge(sales_df, ec_sales_df[['PRD_CD', 'nenshudo', 'URI_SU_EC']], on=['PRD_CD', 'nenshudo'], how='left')
        sales_df['URI_SU_EC'] = sales_df['URI_SU_EC'].fillna(0.0)

        
        
        
    if not THEME_MD_MODE:
        if restrict_minmax:
            # Load MinMax information from CSV
            path_minmax = "Basic_Analysis_unzip_result/02_DM/NBMinMax_ten_prd_ten/min_max_{}_{}_000000000000.csv"
            minmax_df = pd.DataFrame()

            for dpt in dpt_list:
                try:
                    # Call extract_as_df_with_encoding (now assumed to be globally available/imported)
                    temp_df = extract_as_df_with_encoding(path_minmax.format(dpt, str(tenpo_cd)), bucket_name, "utf-8")

                    if 'Unnamed: 0' in temp_df.columns:
                        temp_df = temp_df.drop('Unnamed: 0', axis=1)

                    temp_df['DPT'] = int(dpt)
                    logger.info(f"DPT: {dpt} minmax件数: {len(temp_df)}")
                    minmax_df = pd.concat([minmax_df, temp_df], axis=0).reset_index(drop=True)

                except Exception as e:
                    logger.error(f"Error processing minmax for dpt : ", exc_info=True)
                    continue

            logger.info(f"minmax_df.shape: {minmax_df.shape}")

            if not minmax_df.empty:
                # Get the newest MinMax record per product
                minmax_df['TOROKU_YMD_TOROKU_HMS'] = minmax_df['TOROKU_YMD'].astype(str) + minmax_df['TOROKU_HMS'].astype(str) + minmax_df['NENSHUDO'].astype(str)
                prdcd_nenshudomax_df = minmax_df.groupby(['PRD_CD'])['TOROKU_YMD_TOROKU_HMS'].max().reset_index()
                minmax_df_newest = pd.merge(prdcd_nenshudomax_df, minmax_df, on=['PRD_CD', 'TOROKU_YMD_TOROKU_HMS'], how='inner')

                # Filter sales_df to only include products with minmax data
                sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest['PRD_CD'].tolist())]
                minmax_df_newest2 = minmax_df_newest[minmax_df_newest['PRD_CD'].isin(sales_df['PRD_CD'].tolist())]

                # Exclude MaxMinx0 only
                minmax_df_newest2_not_minmax0 = minmax_df_newest2[~((minmax_df_newest2['HOJU_MIN_SU'] == 0) & (minmax_df_newest2['HOJU_MAX_SU'] == 0))]
                sales_df = sales_df[sales_df['PRD_CD'].isin(minmax_df_newest2_not_minmax0['PRD_CD'].tolist())]
                sales_df = sales_df.reset_index(drop=True)

                logger.info(f"restrict minmax sales_df SKU: {len(sales_df['PRD_CD'].unique())}")

        if restrinct_tenpo_hacchu_end:
            # Combine store-specific production order stop information
            path_tenpo_hacchu_master = "Basic_Analysis_unzip_result/01_Data/33_tenpo_hacchu/29_TENPO_HACCHU_YMD.csv"
            # Call extract_as_df (now assumed to be globally available/imported)
            store_prd_hacchu_ymd = extract_as_df(path_tenpo_hacchu_master, bucket_name)
            store_prd_hacchu_ymd['TENPO_CD'] = store_prd_hacchu_ymd['TENPO_CD'].astype(int)
            store_prd_hacchu_ymd = store_prd_hacchu_ymd[store_prd_hacchu_ymd['TENPO_CD'] == tenpo_cd].reset_index(drop=True)

            if not store_prd_hacchu_ymd.empty:
                # Store-specific order end date
                store_prd_hacchu_ymd['HACCHU_TO_YMD'] = store_prd_hacchu_ymd['HACCHU_TO_YMD'].fillna(99999999).astype(int)

                # Filter for products that are not order stops
                hacchu_end_prdlist = store_prd_hacchu_ymd[store_prd_hacchu_ymd['HACCHU_TO_YMD'] <= my_date]['PRD_CD'].astype(int).tolist()
                sales_df = sales_df[~sales_df['PRD_CD'].isin(hacchu_end_prdlist)]
                sales_df = sales_df.reset_index(drop=True)
                logger.info(f"exclude store hacchuend sales_df SKU: {len(sales_df['PRD_CD'].unique())}")
    return sales_df
            
            

#############################
# REFACTORED CODE
def interpolate_one_year_sales(sales_df, df_calendar, interpolate_1yearsales, exclude_salesless1year):
    """
    Interpolates sales data for products with less than one year of sales history.

    Args:
        sales_df (pd.DataFrame): Sales data DataFrame.
        df_calendar (pd.DataFrame): Calendar DataFrame with nendo, shudo, and znen_nendo/shudo information.
        interpolate_1yearsales (bool): Flag to enable sales data interpolation.
        exclude_salesless1year (bool): Flag to exclude products with less than 1 year of sales data.

    Returns:
        pd.DataFrame: Updated sales_df with interpolated sales data.
    """

    if interpolate_1yearsales:
        # Combine year and week for current and previous year
        df_calendar['nenshudo'] = df_calendar['nendo'] * 100 + df_calendar['shudo']
        df_calendar['znen_nenshudo'] = df_calendar['znen_nendo'] * 100 + df_calendar['znen_shudo']

        # Merge with calendar data
        sales_df = pd.merge(sales_df, df_calendar[['nendo', 'shudo', 'nenshudo', 'znen_nenshudo']],
                             left_on='nenshudo', right_on='nenshudo', how='left')

        # Find the first week with sales for each product
        sales_df_exist_sales = sales_df[sales_df['URI_SU'] >= 0.001].copy() #create copy to avoid warning
        sales_df_exist_sales['nenshudo_exist_uri_su_min'] = sales_df_exist_sales.groupby("PRD_CD")['nenshudo'].transform('min')
        sales_df_exist_sales2 = sales_df_exist_sales[['PRD_CD', 'nenshudo_exist_uri_su_min']].drop_duplicates()
        prdcd_1stsalesnenshudo_dict = dict(zip(sales_df_exist_sales2['PRD_CD'], sales_df_exist_sales2['nenshudo_exist_uri_su_min']))

        # Set the first sales week for each product
        nenshudo_max = sales_df['nenshudo'].max()
        sales_df['1stsales_nenshudo'] = sales_df['PRD_CD'].apply(lambda x: prdcd_1stsalesnenshudo_dict.get(x, nenshudo_max))

        # Filter data to only include sales period
        sales_df_existsales = sales_df[sales_df['nenshudo'] >= sales_df['1stsales_nenshudo']].copy() #create copy to avoid warning
        # Filter data to only include products with at least 52 weeks of sales data
        sales_df_existsales['jisseki_uriage_wk_count'] = sales_df_existsales.groupby("PRD_CD")['nendo'].transform('size') #transform('len') -> transform('size')
        sales_df_existsales = sales_df_existsales[sales_df_existsales['jisseki_uriage_wk_count'] >= 52]

        prdcd_list_sales_over1year = sales_df_existsales['PRD_CD'].unique().tolist()

        if not sales_df_existsales.empty:
            # Get the earliest nendo value for each PRD_CD and shudo combination
            sales_df_existsales['sales_exist_nendo_min_for_shudo'] = sales_df_existsales.groupby(["PRD_CD", "shudo"])['nendo'].transform('min')

            # Extract data for the first year of sales
            sales_df_existsales2 = sales_df_existsales[sales_df_existsales['sales_exist_nendo_min_for_shudo'] == sales_df_existsales['nendo']].copy()

            # Rename URI_SU to URI_SU_1ST
            sales_df_existsales2 = sales_df_existsales2.rename(columns={'URI_SU': 'URI_SU_1ST'})

            # Merge with existing sales data
            sales_df2 = pd.merge(sales_df, sales_df_existsales2[['PRD_CD', 'shudo', 'URI_SU_1ST', 'jisseki_uriage_wk_count']],
                                 left_on=['PRD_CD', 'shudo'], right_on=['PRD_CD', 'shudo'], how='left')

            # Interpolate URI_SU values based on URI_SU_1ST for periods before 1stsales_nenshudo
            mask = (sales_df2['nenshudo'] < sales_df2['1stsales_nenshudo']) & (~sales_df2['URI_SU_1ST'].isna())
            #sales_df2.loc[mask, 'URI_SU'] = sales_df2.loc[mask, 'URI_SU_1ST']
            sales_df2['URI_SU'] = sales_df2.apply(lambda row: row['URI_SU_1ST'] if (row['nenshudo'] < row['1stsales_nenshudo']) and (not pd.isna(row['URI_SU_1ST'])) else row['URI_SU'], axis=1)
            sales_df = sales_df2.copy()

            if exclude_salesless1year:
                # Filter data to only include products with sales for more than 1 year
                sales_df = sales_df[sales_df['PRD_CD'].isin(prdcd_list_sales_over1year)].copy() #removed reset_index

            # Clean up columns and DataFrames
            sales_df = sales_df.drop(['URI_SU_1ST', 'jisseki_uriage_wk_count'], axis=1, errors='ignore') #added errors='ignore' to prevent keyError if column name is not found
            del sales_df2
            del sales_df_existsales2

        # Final cleanup
        sales_df = sales_df.drop(['nendo', 'shudo', 'znen_nenshudo', '1stsales_nenshudo'], axis=1, errors='ignore') #added errors='ignore' to prevent keyError if column name is not found
        if 'prdcd_1stsalesnenshudo_dict' in locals():
            del prdcd_1stsalesnenshudo_dict
        if 'sales_df_existsales' in locals():
            del sales_df_existsales

    return sales_df


#df_odas_calender = pd.DataFrame()

#if tenpo_cd_ref is None:
#   df_odas_calender = odas_correct(df_calendar, tenpo_cd, use_jan_connect=use_jan_connect)
#else:
#    df_odas_calender = odas_correct(df_calendar, tenpo_cd_ref, use_jan_connect=use_jan_connect)
#    df_odas_calender['TENPO_CD'] = tenpo_cd

    
############################
# OLD CODE
'''
if 1:
    # 20230324追加
    df_odas_calender = df_odas_calender.groupby(['TENPO_CD', 'PRD_CD', 'nenshudo'], as_index=False).agg({"odas_amount":'sum'})
    sales_df = pd.merge(sales_df, df_odas_calender, on =["PRD_CD", "nenshudo", "TENPO_CD"], how="left")
else:
    sales_df = pd.merge(sales_df, df_odas_calender, on =["PRD_CD", "nenshudo", "TENPO_CD"], how="left")
    # 20230324削除
    sales_df = sales_df.drop('sales_ymd', axis=1)


if 1:
    sales_df["odas_amount"] = sales_df["odas_amount"].fillna(0)
else:
    # ODAS補正を無効にする場合はこちらで
    sales_df["odas_amount"] = 0.0

# ODAS補正
if odas_imprvmnt == False:
    sales_df["URI_SU"] = sales_df["URI_SU"] - sales_df["odas_amount"]
else:

    odas_correction_start_t = time.time()
    print('start odas correction improvements *******************************')
    

    sales_df["URI_SU_NEW"] = sales_df["URI_SU"] - sales_df["odas_amount"]
    
    # 0以下を0にする
    sales_df["URI_SU_NEW_org"] = sales_df["URI_SU_NEW"]
    sales_df["URI_SU_NEW"][sales_df["URI_SU_NEW"]<0] = 0
    
    
    
    if 1:
        
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

        
    else:
        # ここから花王VMIで調整した新しい処理（MODEではなくてMDEIANを使う、odas_hosei_under_thresholdの値を可変）
        # *******************************************
        # 商品別に販売のある期間の平均をセットする
        sales_df_exist_sales = sales_df[sales_df['URI_SU_NEW'] >= 0.001]

        #sales_df_exist_sales['URI_SU_MEAN'] = sales_df_exist_sales.groupby(["PRD_CD"], as_index=False)['URI_SU_NEW'].transform(lambda x: x.mean())
        #sales_df_exist_sales4 = sales_df_exist_sales[['PRD_CD', 'URI_SU_MEAN']].drop_duplicates()
        #prdcd_mean_dict = dict(zip(sales_df_exist_sales4['PRD_CD'], sales_df_exist_sales4['URI_SU_MEAN']))
        #sales_df['URI_SU_MEAN'] = sales_df['PRD_CD'].apply(lambda x:prdcd_mean_dict.get(x, 0))

        sales_df_exist_sales['URI_SU_MEDIAN'] = sales_df_exist_sales.groupby(["PRD_CD"], as_index=False)['URI_SU_NEW'].transform(lambda x: x.median())
        sales_df_exist_sales4 = sales_df_exist_sales[['PRD_CD', 'URI_SU_MEDIAN']].drop_duplicates()
        prdcd_median_dict = dict(zip(sales_df_exist_sales4['PRD_CD'], sales_df_exist_sales4['URI_SU_MEDIAN']))
        sales_df['URI_SU_MEDIAN'] = sales_df['PRD_CD'].apply(lambda x:prdcd_median_dict.get(x, 0))
        # *******************************************

        # まずは極端なピークを除外した平均値を見たい***********************************************************
        # ここで8週最頻値±2σの範囲のデータを作成、外れる箇所は線形補間する
        sales_df['URI_SU_NEW_STD'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW'].transform(lambda x:x.std())    
        if 0:
            sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW'].transform(lambda x:x.rolling(window=8).apply(lambda y: np_mode2(y))) 
        else:
            ###########################################################################
            sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW'].transform(lambda x:x.rolling(window=8).apply(lambda y:y.median()))    
            ###########################################################################

        sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_8MODE'].transform(lambda x: x.interpolate(limit_direction='both'))

        # 8週MODE±2σを超える値にnanを設定して、
        sales_df["URI_SU_NEW_2SIGMA_LOWER"] = sales_df['URI_SU_NEW_8MODE'] - 2*sales_df["URI_SU_NEW_STD"]
        sales_df["URI_SU_NEW_2SIGMA_UPPER"] = sales_df['URI_SU_NEW_8MODE'] + 2*sales_df["URI_SU_NEW_STD"]

        # 新しくつくる売り数NEW
        sales_df["URI_SU_NEW_2SIGMA"] = sales_df["URI_SU_NEW"]

        sales_df["URI_SU_NEW_2SIGMA"][
            (sales_df["URI_SU_NEW"] < sales_df["URI_SU_NEW_2SIGMA_LOWER"])
            |(sales_df["URI_SU_NEW"] > sales_df["URI_SU_NEW_2SIGMA_UPPER"])                       
        ] = np.nan

        # ***********************************************************************************
        # 中央値の100値にnanを設定して (標準偏差が異常値)
        sales_df["URI_SU_NEW_2SIGMA"][
            (sales_df["URI_SU_NEW_2SIGMA"] > (sales_df["URI_SU_MEDIAN"] * 100.0))
            &(sales_df["URI_SU_MEDIAN"] >= 1.0)
        ] = np.nan
        # ***********************************************************************************


        # ***********************************************************************************
        sales_df["URI_SU_NEW_2SIGMA"][sales_df["URI_SU_NEW_2SIGMA"]<0] = np.nan
        # ***********************************************************************************

        # nanを線形補完する
        sales_df['URI_SU_NEW_2SIGMA'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_2SIGMA'].transform(lambda x: x.interpolate(limit_direction='both'))

        #sales_df.to_csv('sales_df_odashoseiimpl_1.csv')
    
    

        print('processing odas correction improvements *******************************')
        odas_correction_end_t = time.time()
        elapsed_time = odas_correction_end_t - odas_correction_start_t
        print(f"odas correction elapsed time: {elapsed_time:.3f} seconds")

        # ここから補正の本処理　****************************************************************************************
        # ODAS補正後にマイナスとなったものは、2σの8週平均で補完する

        # ****************************************************************************
        ##############sales_df["URI_SU_NEW_OVER0"] = sales_df["URI_SU_NEW_org"]
        sales_df["URI_SU_NEW_OVER0"] = sales_df["URI_SU_NEW_2SIGMA"]
        # ****************************************************************************

        # 外れ値を補間しなおした売り数の8週平均をとる
        sales_df['URI_SU_NEW_2SIGMA_8EMA'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_2SIGMA'].transform(lambda x: x.ewm(span=8).mean())

        sales_df["URI_SU_NEW_OVER0"][sales_df["URI_SU_NEW_OVER0"]<0] = sales_df['URI_SU_NEW_2SIGMA_8EMA'][sales_df["URI_SU_NEW_OVER0"]<0]
        # ここであらためて0以下は0に置換
        sales_df["URI_SU_NEW_OVER0"][sales_df["URI_SU_NEW_OVER0"]<0] = 0

        # ODAS補正後がマイナスで、前後n週内に客数値に近い売りがあれば、nanをセットして線形補間していく  
        if 0:
            sales_df['URI_SU_NEW_OVER0_8MODE'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_OVER0'].transform(lambda x:x.rolling(window=8).apply(lambda y: np_mode2(y)))
        else:
            sales_df['URI_SU_NEW_OVER0_8MODE'] = sales_df.groupby("PRD_CD",as_index=False)['URI_SU_NEW_OVER0'].transform(lambda x:x.rolling(window=8).apply(lambda y: y.median()))



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


        #sales_df.to_csv('sales_df_odashoseiimpl_2.csv')



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
        #&(sales_df['URI_SU_NEW_OVER0'] >= 100)
        &(sales_df['URI_SU_NEW_OVER0'] >= odas_hosei_under_threshold)


        ] = np.nan
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_TYPE'][
        (sales_df['odas_amount_ROLLMAX'] == 0)
        &(sales_df['URI_SU_NEW_OVER0_ROLLMAX'] > sales_df['URI_SU_NEW_OVER0_TH2'])
        &(sales_df['URI_SU_NEW_OVER0'] > sales_df['URI_SU_NEW_OVER0_TH2'])

        &(sales_df['URI_SU_NEW_OVER0'] > (sales_df['URI_SU_NEW_OVER0_8MODE'] + 1) * 8)
        #&(sales_df['URI_SU_NEW_OVER0'] >= 100)
        &(sales_df['URI_SU_NEW_OVER0'] >= odas_hosei_under_threshold)

        ] = '_+spike_without_odas'

        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_bk'] = sales_df['URI_SU_NEW_OVER0_IMPLVMNT']

        sales_df['URI_SU_NEW_OVER0_IMPLVMNT'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_IMPLVMNT'].transform(lambda x: x.interpolate(limit_direction='both'))

        #myprdlist = list(sales_df[sales_df['URI_SU_NEW_OVER0_IMPLVMNT_bk'].isna()]['PRD_CD'].unique())
        #sales_df_ex = sales_df[sales_df['PRD_CD'].isin(myprdlist)]

        #sales_df_ex.to_csv('sales_df_ex.csv')

        #print('test exit')
        #sys.exit()


        #sales_df.to_csv('sales_df_odashoseiimpl_3.csv')


        sales_df['URI_SU'] = sales_df['URI_SU_NEW_OVER0_IMPLVMNT']

        #sales_df = sales_df.drop('URI_SU_NEW_8EMA', axis=1)

        sales_df = sales_df.drop('URI_SU_MEDIAN', axis=1)

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
'''
    
##############################
# REFACTORED CODE
def correct_odas_impact(sales_df, df_odas_calender, odas_imprvmnt, odas_hosei_under_threshold):
    """
    Corrects the sales data for the impact of ODAS (Online Distribution and Allocation System).

    Args:
        sales_df (pd.DataFrame): The sales data DataFrame.
        df_odas_calender (pd.DataFrame): DataFrame containing ODAS amount information.
        odas_imprvmnt (bool): Flag to enable the ODAS correction improvements.
        odas_hosei_under_threshold (float): Threshold for sales values below which ODAS correction is applied.
        

    Returns:
        pd.DataFrame: The sales data DataFrame corrected for ODAS impact.
    """

    # Merge ODAS data
    df_odas_calender = df_odas_calender.groupby(['TENPO_CD', 'PRD_CD', 'nenshudo'], as_index=False).agg({"odas_amount": 'sum'})
    sales_df = pd.merge(sales_df, df_odas_calender, on=["PRD_CD", "nenshudo", "TENPO_CD"], how="left")

    # Fill NaN ODAS amounts with 0
    sales_df["odas_amount"] = sales_df["odas_amount"].fillna(0)

    # ODAS Correction
    if not odas_imprvmnt:
        sales_df["URI_SU"] = sales_df["URI_SU"] - sales_df["odas_amount"]
    else:
        odas_correction_start_t = time.time()
        logger.info('start odas correction improvements *******************************')

        sales_df["URI_SU_NEW"] = sales_df["URI_SU"] - sales_df["odas_amount"]

        # Set values below 0 to 0
        sales_df["URI_SU_NEW_org"] = sales_df["URI_SU_NEW"].copy()  # Keep original for reference
        sales_df["URI_SU_NEW"] = sales_df["URI_SU_NEW"].clip(lower=0)  # More efficient than boolean indexing

        # Calculate rolling mode, standard deviation, and thresholds
        sales_df['URI_SU_NEW_STD'] = sales_df.groupby("PRD_CD")['URI_SU_NEW'].transform(lambda x: x.std())
        sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby("PRD_CD")['URI_SU_NEW'].transform(lambda x: x.rolling(window=8).apply(lambda y: np_mode2(y)))
        sales_df['URI_SU_NEW_8MODE'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_8MODE'].transform(lambda x: x.interpolate(limit_direction='both'))

        # Calculate upper and lower bounds for outlier detection
        sales_df["URI_SU_NEW_2SIGMA_LOWER"] = sales_df['URI_SU_NEW_8MODE'] - 2 * sales_df["URI_SU_NEW_STD"]
        sales_df["URI_SU_NEW_2SIGMA_UPPER"] = sales_df['URI_SU_NEW_8MODE'] + 2 * sales_df["URI_SU_NEW_STD"]

        # Cap outliers at 2 sigma
        sales_df["URI_SU_NEW_2SIGMA"] = sales_df["URI_SU_NEW"].copy()
        outlier_mask = (sales_df["URI_SU_NEW"] < sales_df["URI_SU_NEW_2SIGMA_LOWER"]) | (sales_df["URI_SU_NEW"] > sales_df["URI_SU_NEW_2SIGMA_UPPER"])
        sales_df.loc[outlier_mask, "URI_SU_NEW_2SIGMA"] = np.nan  # Set outliers to NaN
        sales_df['URI_SU_NEW_2SIGMA'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_2SIGMA'].transform(lambda x: x.interpolate(limit_direction='both'))

        # Calculate exponentially weighted moving average
        sales_df['URI_SU_NEW_2SIGMA_8EMA'] = sales_df.groupby("PRD_CD")['URI_SU_NEW_2SIGMA'].transform(lambda x: x.ewm(span=8).mean())

        logger.info('processing odas correction improvements *******************************')
        odas_correction_end_t = time.time()
        elapsed_time = odas_correction_end_t - odas_correction_start_t
        logger.info(f"odas correction elapsed time: {elapsed_time:.3f} seconds")

        # Impute negative values using the 2-sigma 8-week EMA
        sales_df["URI_SU_NEW_OVER0"] = sales_df["URI_SU_NEW_org"].copy()
        negative_mask = sales_df["URI_SU_NEW_OVER0"] < 0
        sales_df.loc[negative_mask, "URI_SU_NEW_OVER0"] = sales_df['URI_SU_NEW_2SIGMA_8EMA'][negative_mask]
        sales_df["URI_SU_NEW_OVER0"] = sales_df["URI_SU_NEW_OVER0"].clip(lower=0)  # Set values below 0 to 0 again

        # Rolling statistics for outlier detection and ODAS impact
        sales_df['URI_SU_NEW_OVER0_8MODE'] = sales_df.groupby("PRD_CD")['URI_SU_NEW_OVER0'].transform(lambda x: x.rolling(window=8).apply(lambda y: y.median()))
        sales_df['URI_SU_NEW_OVER0_8MODE'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_8MODE'].transform(lambda x: x.interpolate(limit_direction='both'))
        sales_df['URI_SU_NEW_OVER0_STD'] = sales_df.groupby("PRD_CD")['URI_SU_NEW_OVER0'].transform(lambda x: x.std())

        sales_df['URI_SU_NEW_OVER0_TH'] = sales_df['URI_SU_NEW_OVER0_8MODE'] + 2 * sales_df['URI_SU_NEW_OVER0_STD']
        sales_df['URI_SU_NEW_OVER0_TH2'] = sales_df['URI_SU_NEW_OVER0_8MODE'] + 2 * sales_df['URI_SU_NEW_OVER0_STD']

        sales_df['URI_SU_NEW_OVER0_ROLLMAX'] = sales_df.groupby("PRD_CD")['URI_SU_NEW_OVER0'].transform(lambda x: x.rolling(7).max()).shift(-4)
        sales_df['URI_SU_NEW_OVER0_ROLLMAX'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_ROLLMAX'].transform(lambda x: x.interpolate(limit_direction='both'))

        sales_df['URI_SU_NEW_OVER0_ROLLMIN'] = sales_df.groupby("PRD_CD")['URI_SU_NEW_OVER0'].transform(lambda x: x.rolling(7).min()).shift(-4)
        sales_df['URI_SU_NEW_OVER0_ROLLMIN'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_ROLLMIN'].transform(lambda x: x.interpolate(limit_direction='both'))

        sales_df['odas_amount_ROLLMAX'] = sales_df.groupby("PRD_CD")['odas_amount'].transform(lambda x: x.rolling(7).max()).shift(-4)
        sales_df['odas_amount_ROLLMAX'] = sales_df.groupby(["PRD_CD"])['odas_amount_ROLLMAX'].transform(lambda x: x.interpolate(limit_direction='both'))

        # Correct for spikes and ODAS impact
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT'] = sales_df['URI_SU_NEW_OVER0'].copy()
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_TYPE'] = np.nan

        # + Spike remaining
        spike_plus_mask = (sales_df['odas_amount_ROLLMAX'] > 0) & \
                         (sales_df['URI_SU_NEW_OVER0_ROLLMAX'] > sales_df['URI_SU_NEW_OVER0_TH']) & \
                         (sales_df['URI_SU_NEW_OVER0_ROLLMAX'] >= (sales_df['odas_amount_ROLLMAX'] * 0.45)) & \
                         (sales_df['URI_SU_NEW_OVER0'] > sales_df['URI_SU_NEW_OVER0_TH'])
        sales_df.loc[spike_plus_mask, 'URI_SU_NEW_OVER0_IMPLVMNT'] = np.nan
        sales_df.loc[spike_plus_mask, 'URI_SU_NEW_OVER0_IMPLVMNT_TYPE'] = '_+spike'

        # - Spike remaining
        spike_minus_mask = (sales_df['URI_SU_NEW_OVER0'] < 0) & \
                          (sales_df['odas_amount_ROLLMAX'] > 0) & \
                          (sales_df['URI_SU_NEW_OVER0_ROLLMIN'] < (sales_df['URI_SU_NEW_OVER0_TH'] * (-1)))
        sales_df.loc[spike_minus_mask, 'URI_SU_NEW_OVER0_IMPLVMNT'] = np.nan
        sales_df.loc[spike_minus_mask, 'URI_SU_NEW_OVER0_IMPLVMNT_TYPE'] = '_-spike'

        # No ODAS value, but spike is present
        no_odas_spike_mask = (sales_df['odas_amount_ROLLMAX'] == 0) & \
                             (sales_df['URI_SU_NEW_OVER0_ROLLMAX'] > sales_df['URI_SU_NEW_OVER0_TH2']) & \
                             (sales_df['URI_SU_NEW_OVER0'] > sales_df['URI_SU_NEW_OVER0_TH2']) & \
                             (sales_df['URI_SU_NEW_OVER0'] > (sales_df['URI_SU_NEW_OVER0_8MODE'] + 1) * 8) & \
                             (sales_df['URI_SU_NEW_OVER0'] >= odas_hosei_under_threshold)
        sales_df.loc[no_odas_spike_mask, 'URI_SU_NEW_OVER0_IMPLVMNT'] = np.nan
        sales_df.loc[no_odas_spike_mask, 'URI_SU_NEW_OVER0_IMPLVMNT_TYPE'] = '_+spike_without_odas'

        # Interpolate to fill in missing values
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT_bk'] = sales_df['URI_SU_NEW_OVER0_IMPLVMNT'].copy()  # Backup before interpolation
        sales_df['URI_SU_NEW_OVER0_IMPLVMNT'] = sales_df.groupby(["PRD_CD"])['URI_SU_NEW_OVER0_IMPLVMNT'].transform(lambda x: x.interpolate(limit_direction='both'))

        sales_df['URI_SU'] = sales_df['URI_SU_NEW_OVER0_IMPLVMNT']

        # Drop intermediate columns
        cols_to_drop = ['URI_SU_NEW_STD', 'URI_SU_NEW_8MODE', 'URI_SU_NEW_2SIGMA_LOWER',
                        'URI_SU_NEW_2SIGMA_UPPER', 'URI_SU_NEW_2SIGMA', 'URI_SU_NEW_2SIGMA_8EMA',
                        'URI_SU_NEW_OVER0', 'URI_SU_NEW_OVER0_8MODE', 'URI_SU_NEW_OVER0_STD',
                        'URI_SU_NEW_OVER0_TH', 'URI_SU_NEW_OVER0_TH2', 'URI_SU_NEW_OVER0_ROLLMAX',
                        'URI_SU_NEW_OVER0_ROLLMIN', 'odas_amount_ROLLMAX', 'URI_SU_NEW_OVER0_IMPLVMNT']
        sales_df = sales_df.drop(columns=cols_to_drop, errors='ignore')

        logger.info("end odas Correction improvements *******************************")
        odas_correction_end_t = time.time()
        elapsed_time = odas_correction_end_t - odas_correction_start_t
        logger.info(f"odas correction elapsed time: {elapsed_time:.3f} seconds")

    return sales_df
    


#################
# OLD CODE (COPIED IN MAIN)
'''
sales_df = sales_df.drop('odas_amount', axis=1)


# ここでodas補正後のdrop_duplicatesで帳尻合わせしているが・・・(上のgroupbyで対処)
sales_df = sales_df.drop_duplicates().reset_index(drop=True)



#sales_df_bk4 = copy.deepcopy(sales_df)      




if class_wave_add:
    sales_df['URI_SU_CLASS'] = sales_df.groupby(["cls_cd", "nenshudo"], as_index=False)['URI_SU'].transform(lambda x: x.sum())
    if class_wave_mean_add:
        sales_df['URI_SU_CLASS8ema'] = sales_df.groupby("PRD_CD", as_index=False)['URI_SU_CLASS'].transform(lambda x: x.ewm(span=8).mean())
'''   

############################
# OLD CODE
'''    
if interporate_by_class_wave:    
""
    # 商品別に販売のある最初の年週度をとってきてdictを作成する
    sales_df_exist_sales = sales_df[sales_df['URI_SU'] >= 0.001]
    
    # 商品別に販売のある最初の年週度をとってきてdictを作成する
    sales_df_exist_sales['nenshudo_exist_uri_su_min'] = sales_df_exist_sales.groupby("PRD_CD", as_index=False)['nenshudo'].transform(lambda x: x.min())
    # 商品別の最初の販売週を2週後にずらす
    sales_df_exist_sales['nenshudo_exist_uri_su_min_2wklater'] = sales_df_exist_sales['nenshudo_exist_uri_su_min'].apply(lambda x:calc_nenshudo2(x, 2, dfc_tmp)  )
    # 最初の販売週から2週後以降のデータに限定
    sales_df_exist_sales = sales_df_exist_sales[sales_df_exist_sales['nenshudo'] >= sales_df_exist_sales['nenshudo_exist_uri_su_min_2wklater']].reset_index(drop=True)
    
    
    #sales_df_exist_sales['nenshudo_exist_uri_su_min'] = sales_df_exist_sales.groupby("PRD_CD", as_index=False)['nenshudo'].transform(lambda x: x.min())
    sales_df_exist_sales['nenshudo_exist_uri_su_min_2wklater'] = sales_df_exist_sales.groupby("PRD_CD", as_index=False)['nenshudo'].transform(lambda x: x.min())
    
    #sales_df_exist_sales2 = sales_df_exist_sales[['PRD_CD', 'nenshudo_exist_uri_su_min']].drop_duplicates()
    #prdcd_1stsalesnenshudo_dict = dict(zip(sales_df_exist_sales2['PRD_CD'], sales_df_exist_sales2['nenshudo_exist_uri_su_min']))

    sales_df_exist_sales2 = sales_df_exist_sales[['PRD_CD', 'nenshudo_exist_uri_su_min_2wklater']].drop_duplicates()
    prdcd_1stsalesnenshudo_dict = dict(zip(sales_df_exist_sales2['PRD_CD'], sales_df_exist_sales2['nenshudo_exist_uri_su_min_2wklater']))
    
    
    # 商品別の最初の販売週をセットする
    nenshudo_max = sales_df['nenshudo'].max()
    sales_df['1stsales_nenshudo'] = sales_df['PRD_CD'].apply(lambda x:prdcd_1stsalesnenshudo_dict.get(x, nenshudo_max))
    
    
    # 商品別に販売のある期間の標準偏差をセットする
    sales_df_exist_sales['URI_SU_STD'] = sales_df_exist_sales.groupby(["PRD_CD"], as_index=False)['URI_SU'].transform(lambda x: x.std())    
    sales_df_exist_sales3 = sales_df_exist_sales[['PRD_CD', 'URI_SU_STD']].drop_duplicates()
    prdcd_std_dict = dict(zip(sales_df_exist_sales3['PRD_CD'], sales_df_exist_sales3['URI_SU_STD']))

    sales_df['URI_SU_STD'] = sales_df['PRD_CD'].apply(lambda x:prdcd_std_dict.get(x, 0))
    
    # 商品別に販売のある期間の平均をセットする
    sales_df_exist_sales['URI_SU_MEAN'] = sales_df_exist_sales.groupby(["PRD_CD"], as_index=False)['URI_SU'].transform(lambda x: x.mean())
    sales_df_exist_sales4 = sales_df_exist_sales[['PRD_CD', 'URI_SU_MEAN']].drop_duplicates()
    prdcd_mean_dict = dict(zip(sales_df_exist_sales4['PRD_CD'], sales_df_exist_sales4['URI_SU_MEAN']))
    
    sales_df['URI_SU_MEAN'] = sales_df['PRD_CD'].apply(lambda x:prdcd_mean_dict.get(x, 0))
    
    
        
    # クラス波形を標準化する
    sales_df['URI_SU_CLASS_STANDARDIZED'] = sales_df.groupby(["PRD_CD"], as_index=False)['URI_SU_CLASS'].transform(lambda x:scipy.stats.zscore(x))

    sales_df['URI_SU_CLASS_STD'] = sales_df.groupby(["PRD_CD"], as_index=False)['URI_SU_CLASS'].transform(lambda x: x.std())
    sales_df['URI_SU_CLASS_MEAN'] = sales_df.groupby(["PRD_CD"], as_index=False)['URI_SU_CLASS'].transform(lambda x: x.mean())    
    sales_df['URI_SU_CLASS_INVERSE'] = sales_df['URI_SU_CLASS_STANDARDIZED'] * sales_df['URI_SU_STD'] + sales_df['URI_SU_MEAN']
    # 標準化を戻したときマイナスになることがあるので0にする
    sales_df['URI_SU_CLASS_INVERSE'][sales_df['URI_SU_CLASS_INVERSE']<0] = 0
   

    # 販売開始前の期間をクラス波形で補完する
    sales_df['URI_SU'][ (sales_df['nenshudo'] < sales_df['1stsales_nenshudo'])
                        &(~sales_df['URI_SU_CLASS_INVERSE'].isna())] \
        = sales_df['URI_SU_CLASS_INVERSE'][ (sales_df['nenshudo'] < sales_df['1stsales_nenshudo'])
                                  &(~sales_df['URI_SU_CLASS_INVERSE'].isna())]
    

    #sales_df = sales_df2.reset_index(drop=True)
    #sales_df_bk_x2 = copy.deepcopy(sales_df)
    #sales_df_bk_x2.to_csv('sales_df_bk2_inved.csv')
    

    
    
    # 後始末
    sales_df = sales_df.drop('1stsales_nenshudo', axis=1)
    #sales_df = sales_df.drop('URI_SU_1ST', axis=1)
    sales_df = sales_df.drop('URI_SU_STD', axis=1)
    sales_df = sales_df.drop('URI_SU_MEAN', axis=1)
    sales_df = sales_df.drop('URI_SU_CLASS_STANDARDIZED', axis=1)
    sales_df = sales_df.drop('URI_SU_CLASS_STD', axis=1)
    sales_df = sales_df.drop('URI_SU_CLASS_MEAN', axis=1)
    sales_df = sales_df.drop('URI_SU_CLASS_INVERSE', axis=1)
    
    #del sales_df_existsales2
    #del sales_df_existsales3
    #del sales_df_existsales4
    del prdcd_1stsalesnenshudo_dict
    del prdcd_std_dict
    del prdcd_mean_dict
'''
    
###############################
# REFACTORED CODE
def process_sales_data1(sales_df, interporate_by_class_wave, dfc_tmp):
    """
    Processes sales data by interpolating missing values using class wave patterns.

    Args:
        sales_df (pd.DataFrame): The sales data DataFrame.
        interporate_by_class_wave (bool): A flag indicating whether to perform interpolation.
        dfc_tmp: dfc_tmp

    Returns:
        pd.DataFrame: The processed sales data DataFrame.
    """
    if interporate_by_class_wave:
        # 商品別に販売のある最初の年週度をとってきてdictを作成する
        sales_df_exist_sales = sales_df[sales_df['URI_SU'] >= 0.001].copy()

        # 商品別に販売のある最初の年週度をとってきてdictを作成する
        sales_df_exist_sales['nenshudo_exist_uri_su_min'] = sales_df_exist_sales.groupby("PRD_CD", as_index=False)['nenshudo'].transform(lambda x: x.min())
        # 商品別の最初の販売週を2週後にずらす
        sales_df_exist_sales['nenshudo_exist_uri_su_min_2wklater'] = sales_df_exist_sales['nenshudo_exist_uri_su_min'].apply(lambda x:calc_nenshudo2(x, 2, dfc_tmp) )
        # 最初の販売週から2週後以降のデータに限定
        sales_df_exist_sales = sales_df_exist_sales[sales_df_exist_sales['nenshudo'] >= sales_df_exist_sales['nenshudo_exist_uri_su_min_2wklater']].reset_index(drop=True)


        #sales_df_exist_sales['nenshudo_exist_uri_su_min'] = sales_df_exist_sales.groupby("PRD_CD", as_index=False)['nenshudo'].transform(lambda x: x.min())
        sales_df_exist_sales['nenshudo_exist_uri_su_min_2wklater'] = sales_df_exist_sales.groupby("PRD_CD", as_index=False)['nenshudo'].transform(lambda x: x.min())

        #sales_df_exist_sales2 = sales_df_exist_sales[['PRD_CD', 'nenshudo_exist_uri_su_min']].drop_duplicates()
        #prdcd_1stsalesnenshudo_dict = dict(zip(sales_df_exist_sales2['PRD_CD'], sales_df_exist_sales2['nenshudo_exist_uri_su_min']))

        sales_df_exist_sales2 = sales_df_exist_sales[['PRD_CD', 'nenshudo_exist_uri_su_min_2wklater']].drop_duplicates()
        prdcd_1stsalesnenshudo_dict = dict(zip(sales_df_exist_sales2['PRD_CD'], sales_df_exist_sales2['nenshudo_exist_uri_su_min_2wklater']))


        # 商品別の最初の販売週をセットする
        nenshudo_max = sales_df['nenshudo'].max()
        sales_df['1stsales_nenshudo'] = sales_df['PRD_CD'].apply(lambda x:prdcd_1stsalesnenshudo_dict.get(x, nenshudo_max))


        # 商品別に販売のある期間の標準偏差をセットする
        sales_df_exist_sales['URI_SU_STD'] = sales_df_exist_sales.groupby(["PRD_CD"], as_index=False)['URI_SU'].transform(lambda x: x.std())
        sales_df_exist_sales3 = sales_df_exist_sales[['PRD_CD', 'URI_SU_STD']].drop_duplicates()
        prdcd_std_dict = dict(zip(sales_df_exist_sales3['PRD_CD'], sales_df_exist_sales3['URI_SU_STD']))

        sales_df['URI_SU_STD'] = sales_df['PRD_CD'].apply(lambda x:prdcd_std_dict.get(x, 0))

        # 商品別に販売のある期間の平均をセットする
        sales_df_exist_sales['URI_SU_MEAN'] = sales_df_exist_sales.groupby(["PRD_CD"], as_index=False)['URI_SU'].transform(lambda x: x.mean())
        sales_df_exist_sales4 = sales_df_exist_sales[['PRD_CD', 'URI_SU_MEAN']].drop_duplicates()
        prdcd_mean_dict = dict(zip(sales_df_exist_sales4['PRD_CD'], sales_df_exist_sales4['URI_SU_MEAN']))

        sales_df['URI_SU_MEAN'] = sales_df['PRD_CD'].apply(lambda x:prdcd_mean_dict.get(x, 0))


        # クラス波形を標準化する
        sales_df['URI_SU_CLASS_STANDARDIZED'] = sales_df.groupby(["PRD_CD"], as_index=False)['URI_SU_CLASS'].transform(lambda x:scipy.stats.zscore(x))

        sales_df['URI_SU_CLASS_STD'] = sales_df.groupby(["PRD_CD"], as_index=False)['URI_SU_CLASS'].transform(lambda x: x.std())
        sales_df['URI_SU_CLASS_MEAN'] = sales_df.groupby(["PRD_CD"], as_index=False)['URI_SU_CLASS'].transform(lambda x: x.mean())
        sales_df['URI_SU_CLASS_INVERSE'] = sales_df['URI_SU_CLASS_STANDARDIZED'] * sales_df['URI_SU_STD'] + sales_df['URI_SU_MEAN']
        # 標準化を戻したときマイナスになることがあるので0にする
        sales_df.loc[sales_df['URI_SU_CLASS_INVERSE']<0, 'URI_SU_CLASS_INVERSE'] = 0


        # 販売開始前の期間をクラス波形で補完する
        sales_df.loc[(sales_df['nenshudo'] < sales_df['1stsales_nenshudo']) & (~sales_df['URI_SU_CLASS_INVERSE'].isna()), 'URI_SU'] = \
        sales_df['URI_SU_CLASS_INVERSE'][(sales_df['nenshudo'] < sales_df['1stsales_nenshudo']) & (~sales_df['URI_SU_CLASS_INVERSE'].isna())]

        #sales_df = sales_df2.reset_index(drop=True)
        #sales_df_bk_x2 = copy.deepcopy(sales_df)
        #sales_df_bk_x2.to_csv('sales_df_bk2_inved.csv')


        # 後始末
        sales_df = sales_df.drop('1stsales_nenshudo', axis=1)
        #sales_df = sales_df.drop('URI_SU_1ST', axis=1)
        sales_df = sales_df.drop('URI_SU_STD', axis=1)
        sales_df = sales_df.drop('URI_SU_MEAN', axis=1)
        sales_df = sales_df.drop('URI_SU_CLASS_STANDARDIZED', axis=1)
        sales_df = sales_df.drop('URI_SU_CLASS_STD', axis=1)
        sales_df = sales_df.drop('URI_SU_CLASS_MEAN', axis=1)
        sales_df = sales_df.drop('URI_SU_CLASS_INVERSE', axis=1)

        #del sales_df_existsales2
        #del sales_df_existsales3
        #del sales_df_existsales4
        del prdcd_1stsalesnenshudo_dict
        del prdcd_std_dict
        del prdcd_mean_dict

    return sales_df
    
##############copied to main    
#if logarithmize_target_variable:
#    print('start logarithmize_target_variable')
#    sales_df['URI_SU'] = np.log1p(sales_df['URI_SU'])
#    print('end logarithmize_target_variable')


################################
# OLD CODE
'''
if 0:
    #　前年売り数を作成(ここで202053週と2021年52週が落ちる)
    sales_df_last_year = sales_df
    sales_df[['zen_nenshdo']] = sales_df[['nenshudo']] - 100

    sales_df_last_year = sales_df_last_year.drop('zen_nenshdo', axis=1)
    sales_df_last_year = sales_df_last_year.drop('baika_toitsu', axis=1)
    sales_df_last_year = sales_df_last_year.drop('BAIKA', axis=1)
    sales_df_last_year = sales_df_last_year.drop('DPT', axis=1)
    sales_df_last_year = sales_df_last_year.drop('line_cd', axis=1)
    sales_df_last_year = sales_df_last_year.drop('cls_cd', axis=1)
    sales_df_last_year = sales_df_last_year.drop('hnmk_cd', axis=1)

    sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量'})

    # ここでinnner joinだから202053は前年201953週が無いから落ちるよね・・・
    df_merged_sales = pd.merge(sales_df, sales_df_last_year, on = ['PRD_CD','zen_nenshdo'])
else:
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
    
    
    if add_ec_salesamount:
        if class_wave_add:
            
            if class_wave_mean_add:
                sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量', 'URI_SU_EC': '前年EC売上実績数量', 'URI_SU_CLASS':'前年CLASS売上実績数量', 'URI_SU_CLASS8ema':'前年CLASS売上実績数量8ema'})               
            else:
                sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量', 'URI_SU_EC': '前年EC売上実績数量', 'URI_SU_CLASS':'前年CLASS売上実績数量'})

        else:
            sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量', 'URI_SU_EC': '前年EC売上実績数量'})
    else:
        sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量'})
    
    
    
    
    if THEME_MD_MODE:
        df_merged_sales = pd.merge(sales_df, sales_df_last_year, on = ['PRD_CD','zen_nenshdo'], how='left')
        
        df_merged_sales['前年売上実績数量'] = df_merged_sales['前年売上実績数量'].fillna(0)
        df_merged_sales['前年EC売上実績数量'] = df_merged_sales['前年EC売上実績数量'].fillna(0)
        df_merged_sales['前年CLASS売上実績数量'] = df_merged_sales['前年CLASS売上実績数量'].fillna(0)
        df_merged_sales['前年CLASS売上実績数量8ema'] = df_merged_sales['前年CLASS売上実績数量8ema'].fillna(0)
        #df_merged_sales[''] = df_merged_sales[''].fillna(0)
        
    else:
        df_merged_sales = pd.merge(sales_df, sales_df_last_year, on = ['PRD_CD','zen_nenshdo'])


# 20240620 comment 不要のため
##################　sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量'})

df_merged_sales2 = pd.merge(df_merged_sales, df_zen_calendar, on = 'nenshudo')

if add_ec_salesamount:
    if class_wave_add:
        
        if class_wave_mean_add:
            df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'URI_SU_CLASS':'売上実績数量CLASS', 'URI_SU_CLASS8ema':'売上実績数量CLASS8ema', 'PRD_CD':'商品コード'}).reset_index(drop=True)
            
            train_tmp = df_merged_sales2[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '売上実績数量CLASS8ema', 
                                          '前年売上実績数量',
                                          '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema',
                                          '週開始日付','前年週開始日付', 'baika_toitsu',
                                          'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
            
            train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')
 
        else:
            df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'URI_SU_CLASS':'売上実績数量CLASS', 'PRD_CD':'商品コード'}).reset_index(drop=True)
            train_tmp = df_merged_sales2[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '週開始日付','前年週開始日付', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
            train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')
    
    else:
        df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'PRD_CD':'商品コード'}).reset_index(drop=True)
        train_tmp = df_merged_sales2[['商品コード','売上実績数量', '売上実績数量EC','前年売上実績数量', '前年EC売上実績数量', '週開始日付','前年週開始日付', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
        train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')

else:
    df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'PRD_CD':'商品コード'}).reset_index(drop=True)
    train_tmp = df_merged_sales2[['商品コード','売上実績数量','前年売上実績数量','週開始日付','前年週開始日付', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')

    
    
    
if bakugai_hosei:
    
    counter = 0
    while True:
        counter = counter + 1

        train_tmp['8週平均ema'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).mean())
        train_tmp['指数加重移動標準偏差'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).std())
        # 8週移動平均に標準偏差*1.725を足す
        train_tmp["指数加重移動標準偏差2"] = train_tmp["指数加重移動標準偏差"] * 1.725 + train_tmp["8週平均ema"]

        # 8週移動平均に標準偏差*1.725を超える値を抽出
        #df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量','8週平均ema','指数加重移動標準偏差2','前年売上実績数量']][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]


        if add_ec_salesamount:
            if class_wave_add:

                if class_wave_mean_add:
                    df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', 
                                           '売上実績数量CLASS', '売上実績数量CLASS8ema', '8週平均ema','指数加重移動標準偏差2',
                                           '前年売上実績数量', 
                                           '前年EC売上実績数量', '前年CLASS売上実績数量','前年CLASS売上実績数量8ema', 
                                           'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd'
                                          ]][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

                    train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', 
                                                  '売上実績数量CLASS', '売上実績数量CLASS8ema', '8週平均ema','指数加重移動標準偏差2',
                                                  '前年売上実績数量', 
                                                  '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema',
                                                  'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd'
                                                 ]][(train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"])
                                                    |(train_tmp["指数加重移動標準偏差2"].isna())
                                                   ]

                else:
                    df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', 
                                           '売上実績数量CLASS', '8週平均ema','指数加重移動標準偏差2','前年売上実績数量', 
                                           '前年EC売上実績数量', '前年CLASS売上実績数量', 
                                           'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd'
                                          ]][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

                    train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', 
                                                  '売上実績数量CLASS', '8週平均ema','指数加重移動標準偏差2','前年売上実績数量', 
                                                  '前年EC売上実績数量', '前年CLASS売上実績数量', 
                                                  'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd'
                                                 ]][(train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"])
                                                    |(train_tmp["指数加重移動標準偏差2"].isna())]

            else:
                df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', '8週平均ema','指数加重移動標準偏差2','前年売上実績数量', '前年EC売上実績数量', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

                train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', '8週平均ema','指数加重移動標準偏差2','前年売上実績数量', '前年EC売上実績数量', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"]]

        else:
            df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量','8週平均ema','指数加重移動標準偏差2','前年売上実績数量', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

            train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量','8週平均ema','指数加重移動標準偏差2','前年売上実績数量', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"]]


        df_output["爆買い補正"] = counter
        df_output["補正前売上実績数量"] = df_output["売上実績数量"]

        df_output = df_output.drop('売上実績数量', axis=1).reset_index(drop=True)

        df_output['売上実績数量'] = 0 

        for index in range(df_output.shape[0]):
            product_code = df_output.商品コード.values[index]

            # 8週移動平均に標準偏差*1.725を超える値は、前週の'8週平均emaに置き換える
            last_week_start_date = df_output['週開始日付tmp'][index]
            last_week_start_date = last_week_start_date + datetime.timedelta(days=-7)
            df_train_tmp_ema = train_tmp['8週平均ema'][(train_tmp['商品コード'] == product_code) & (train_tmp['週開始日付tmp'] == last_week_start_date)]
            if(len(df_train_tmp_ema) > 0):
                train_tmp_ema = df_train_tmp_ema.values[0]
                if(train_tmp_ema  > 0):
                    df_output.売上実績数量.values[index] = train_tmp_ema

        df_output['売上実績数量'] = df_output['売上実績数量'].fillna(0).astype('int')




        train_tmp = pd.concat([train_tmp_remain, df_output], axis=0).reset_index()

        # ループは最大５回まで
        if(counter >= 5):
            break

        # 8週移動平均に標準偏差*1.725を超える値のmaxが200以下であればbreak
        if(df_output['売上実績数量'].max() < 200):
            break
'''

#########################
# REFACTORED CODE
def process_sales_data_internal(sales_df, df_calendar, add_ec_salesamount, class_wave_add, class_wave_mean_add,
                                  THEME_MD_MODE, bakugai_hosei, df_zen_calendar):
    """
    Internal function to merge sales data with calendar and last year's data,
    and apply bakugai (burst buying) correction if specified.

    Args:
        sales_df (pd.DataFrame): Sales data.
        df_calendar (pd.DataFrame): Calendar data with 'nenshudo', 'znen_nendo', 'znen_shudo', '週開始日付', '前年週開始日付' columns.
        add_ec_salesamount (bool): Flag to indicate presence of EC sales data.
        class_wave_add (bool): Flag for class wave addition.
        class_wave_mean_add (bool): Flag for class wave mean addition.
        THEME_MD_MODE (bool): Flag for theme MD mode.
        bakugai_hosei (bool): Flag to apply bakugai correction.

    Returns:
        pd.DataFrame: Processed sales data (train_tmp).
    """
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
    
    
    if add_ec_salesamount:
        if class_wave_add:
            
            if class_wave_mean_add:
                sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量', 'URI_SU_EC': '前年EC売上実績数量', 'URI_SU_CLASS':'前年CLASS売上実績数量', 'URI_SU_CLASS8ema':'前年CLASS売上実績数量8ema'})               
            else:
                sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量', 'URI_SU_EC': '前年EC売上実績数量', 'URI_SU_CLASS':'前年CLASS売上実績数量'})

        else:
            sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量', 'URI_SU_EC': '前年EC売上実績数量'})
    else:
        sales_df_last_year = sales_df_last_year.rename(columns={'nenshudo': 'zen_nenshdo', 'URI_SU': '前年売上実績数量'})    
    
    if THEME_MD_MODE:
        df_merged_sales = pd.merge(sales_df, sales_df_last_year, on = ['PRD_CD','zen_nenshdo'], how='left')
        
        df_merged_sales['前年売上実績数量'] = df_merged_sales['前年売上実績数量'].fillna(0)
        df_merged_sales['前年EC売上実績数量'] = df_merged_sales['前年EC売上実績数量'].fillna(0)
        df_merged_sales['前年CLASS売上実績数量'] = df_merged_sales['前年CLASS売上実績数量'].fillna(0)
        df_merged_sales['前年CLASS売上実績数量8ema'] = df_merged_sales['前年CLASS売上実績数量8ema'].fillna(0)
        #df_merged_sales[''] = df_merged_sales[''].fillna(0)
        
    else:
        df_merged_sales = pd.merge(sales_df, sales_df_last_year, on = ['PRD_CD','zen_nenshdo'])

    df_merged_sales2 = pd.merge(df_merged_sales, df_zen_calendar, on = 'nenshudo')

    if add_ec_salesamount:
        if class_wave_add:
            if class_wave_mean_add:
                df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'URI_SU_CLASS':'売上実績数量CLASS', 'URI_SU_CLASS8ema':'売上実績数量CLASS8ema', 'PRD_CD':'商品コード'}).reset_index(drop=True)

                train_tmp = df_merged_sales2[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '売上実績数量CLASS8ema', 
                                              '前年売上実績数量',
                                              '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema',
                                              '週開始日付','前年週開始日付', 'baika_toitsu',
                                              'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

                train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')

            else:
                df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'URI_SU_CLASS':'売上実績数量CLASS', 'PRD_CD':'商品コード'}).reset_index(drop=True)
                train_tmp = df_merged_sales2[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '週開始日付','前年週開始日付', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
                train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')

        else:
            df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'URI_SU_EC': '売上実績数量EC', 'PRD_CD':'商品コード'}).reset_index(drop=True)
            train_tmp = df_merged_sales2[['商品コード','売上実績数量', '売上実績数量EC','前年売上実績数量', '前年EC売上実績数量', '週開始日付','前年週開始日付', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
            train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')

    else:
        df_merged_sales2 = df_merged_sales2.rename(columns={'URI_SU': '売上実績数量', 'PRD_CD':'商品コード'}).reset_index(drop=True)
        train_tmp = df_merged_sales2[['商品コード','売上実績数量','前年売上実績数量','週開始日付','前年週開始日付', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
        train_tmp['週開始日付tmp'] = pd.to_datetime(train_tmp['週開始日付'], format = '%Y%m%d')


    if bakugai_hosei:    
        counter = 0
        while True:
            counter = counter + 1

            train_tmp['8週平均ema'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).mean())
            train_tmp['指数加重移動標準偏差'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).std())
            # 8週移動平均に標準偏差*1.725を足す
            train_tmp["指数加重移動標準偏差2"] = train_tmp["指数加重移動標準偏差"] * 1.725 + train_tmp["8週平均ema"]

            # 8週移動平均に標準偏差*1.725を超える値を抽出
            #df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量','8週平均ema','指数加重移動標準偏差2','前年売上実績数量']][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]


            if add_ec_salesamount:
                if class_wave_add:

                    if class_wave_mean_add:
                        df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', 
                                               '売上実績数量CLASS', '売上実績数量CLASS8ema', '8週平均ema','指数加重移動標準偏差2',
                                               '前年売上実績数量', 
                                               '前年EC売上実績数量', '前年CLASS売上実績数量','前年CLASS売上実績数量8ema', 
                                               'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd'
                                              ]][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

                        train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', 
                                                      '売上実績数量CLASS', '売上実績数量CLASS8ema', '8週平均ema','指数加重移動標準偏差2',
                                                      '前年売上実績数量', 
                                                      '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema',
                                                      'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd'
                                                     ]][(train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"])
                                                        |(train_tmp["指数加重移動標準偏差2"].isna())
                                                       ]

                    else:
                        df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', 
                                               '売上実績数量CLASS', '8週平均ema','指数加重移動標準偏差2','前年売上実績数量', 
                                               '前年EC売上実績数量', '前年CLASS売上実績数量', 
                                               'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd'
                                              ]][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

                        train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', 
                                                      '売上実績数量CLASS', '8週平均ema','指数加重移動標準偏差2','前年売上実績数量', 
                                                      '前年EC売上実績数量', '前年CLASS売上実績数量', 
                                                      'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd'
                                                     ]][(train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"])
                                                        |(train_tmp["指数加重移動標準偏差2"].isna())]

                else:
                    df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', '8週平均ema','指数加重移動標準偏差2','前年売上実績数量', '前年EC売上実績数量', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

                    train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量', '売上実績数量EC', '8週平均ema','指数加重移動標準偏差2','前年売上実績数量', '前年EC売上実績数量', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"]]

            else:
                df_output = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量','8週平均ema','指数加重移動標準偏差2','前年売上実績数量', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] > train_tmp["指数加重移動標準偏差2"]]

                train_tmp_remain = train_tmp[['商品コード','週開始日付','週開始日付tmp','売上実績数量','8週平均ema','指数加重移動標準偏差2','前年売上実績数量', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']][train_tmp["売上実績数量"] <= train_tmp["指数加重移動標準偏差2"]]


            df_output["爆買い補正"] = counter
            df_output["補正前売上実績数量"] = df_output["売上実績数量"]

            df_output = df_output.drop('売上実績数量', axis=1).reset_index(drop=True)

            df_output['売上実績数量'] = 0 

            for index in range(df_output.shape[0]):
                product_code = df_output.商品コード.values[index]

                # 8週移動平均に標準偏差*1.725を超える値は、前週の'8週平均emaに置き換える
                last_week_start_date = df_output['週開始日付tmp'][index]
                last_week_start_date = last_week_start_date + datetime.timedelta(days=-7)
                df_train_tmp_ema = train_tmp['8週平均ema'][(train_tmp['商品コード'] == product_code) & (train_tmp['週開始日付tmp'] == last_week_start_date)]
                if(len(df_train_tmp_ema) > 0):
                    train_tmp_ema = df_train_tmp_ema.values[0]
                    if(train_tmp_ema  > 0):
                        df_output.売上実績数量.values[index] = train_tmp_ema

            df_output['売上実績数量'] = df_output['売上実績数量'].fillna(0).astype('int')




            train_tmp = pd.concat([train_tmp_remain, df_output], axis=0).reset_index()

            # ループは最大５回まで
            if(counter >= 5):
                break

            # 8週移動平均に標準偏差*1.725を超える値のmaxが200以下であればbreak
            if(df_output['売上実績数量'].max() < 200):
                break
    return train_tmp

        
#sales_df_bk5 = copy.deepcopy(sales_df)      

########################
#copied to main
#tmp_df_calendar = copy.deepcopy(df_calendar)
#tmp_df_calendar['week_from_ymd'] = pd.to_datetime(tmp_df_calendar['week_from_ymd'], format='%Y%m%d')
#tmp_df_calendar['week_to_ymd'] = pd.to_datetime(tmp_df_calendar['week_to_ymd'], format='%Y%m%d')
#tenkyu_master = load_tenkyu_master(tenpo_cd, tmp_df_calendar)

#tenkyu_master = pd.merge(tenkyu_master, df_calendar[['nenshudo', 'week_from_ymd']].rename(columns={'week_from_ymd':'週開始日付'}), how='left')


#店舗改装期間の補正
#train_tmp = pd.merge(train_tmp, tenkyu_master[['nenshudo', '週開始日付', '店休日数']], on=['週開始日付'], how='left')


#nenshudo_list = train_tmp.loc[train_tmp['店休日数'] > 1]['nenshudo'].unique().tolist()
#remodeling_nenshudo_list = []
#for nenshudo in nenshudo_list:
#    remodeling_nenshudo_list.append(nenshudo)
#    for i in range(1, 5):
#        remodeling_nenshudo_list.append(nenshudo + i)
#        remodeling_nenshudo_list.append(nenshudo - i)
#train_tmp.loc[train_tmp['nenshudo'].isin(remodeling_nenshudo_list), 
#       "補正対象Flag"] = 1

#window_size = 13
#train_tmp["売上実績数量（3か月平均）"] = train_tmp.groupby('商品コード')["売上実績数量"].transform(lambda x:x.shift(1).rolling(window_size, #min_periods=1).mean())

# 対象範囲を補正
#train_tmp.loc[(train_tmp["補正対象Flag"]==1) & (~train_tmp["売上実績数量（3か月平均）"].isnull()), "売上実績数量"] = train_tmp["売上実績数量（3か月平均）"]
        
#train_tmp = train_tmp.drop(['補正対象Flag', '売上実績数量（3か月平均）'], axis=1)
#train_tmp.loc[train_tmp['店休日数'] > 1, '店休日数'] = 0

        
#train_tmp['8週平均ema'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).mean())
#train_tmp = train_tmp.drop('週開始日付tmp', axis=1)

#df_merged = train_tmp




#if '前年週開始日付' not in df_merged.columns.tolist(): # 爆買い補正をoffにすると前年週開始日付が残った状態になっているのでifだけを追加
#    df_merged = pd.merge(df_merged,df_zen_calendar,on='週開始日付')
    
    
# 予算特徴量は、多店舗展開まで、一旦コメントアウト
# path_budget = "01_short_term/60_cached_data/09_budget/budget.csv"
# budget_df =  extract_as_df(path_budget, bucket_name, "utf-8")
# budget_df = budget_df.rename(columns={'week_from_ymd': '週開始日付','tenpo_cd':'店舗コード'}).reset_index(drop=True)
# df_merged = pd.merge(df_merged, budget_df, on=["店舗コード","週開始日付"], how="left").reset_index(drop=True)

############################
# OLD CODE
'''
if add_ec_salesamount:
    if class_wave_add:
        
        if class_wave_mean_add:
            
            df_merged2 = df_merged[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '売上実績数量CLASS8ema', '前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema', '前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
            
            if THEME_MD_MODE:
                df = get_last_year_sales_amount_left(df_merged2)
            else:
                df = get_last_year_sales_amount(df_merged2)
            
            df_vx_train = df[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '売上実績数量CLASS8ema', '週開始日付','前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema', '8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

            df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount', '売上実績数量EC':'SalesAmountEC', '売上実績数量CLASS':'SalesAmountCLASS', '売上実績数量CLASS8ema':'SalesAmountCLASS8ema', '前年売上実績数量':'PreviousYearSalesActualQuantity', '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', '前年CLASS売上実績数量':'PreviousYearClassSalesActualQuantity', '前年CLASS売上実績数量8ema':'PreviousYearClassSalesActualQuantity8ema', '8週平均ema':'SalesAmount8ema'})

            df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'SalesAmountEC', 'SalesAmountCLASS', 'SalesAmountCLASS8ema', 'WeekStartDate','PreviousYearSalesActualQuantity', 'PreviousYearEcSalesActualQuantity', 'PreviousYearClassSalesActualQuantity', 'PreviousYearClassSalesActualQuantity8ema', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

            
            
        else:
        
            df_merged2 = df_merged[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

            if THEME_MD_MODE:
                df = get_last_year_sales_amount_left(df_merged2)
            else:
                df = get_last_year_sales_amount(df_merged2)

            df_vx_train = df[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '週開始日付','前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

            df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount', '売上実績数量EC':'SalesAmountEC', '売上実績数量CLASS':'SalesAmountCLASS', '前年売上実績数量':'PreviousYearSalesActualQuantity', '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', '前年CLASS売上実績数量':'PreviousYearClassSalesActualQuantity', '8週平均ema':'SalesAmount8ema'})

            df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'SalesAmountEC', 'SalesAmountCLASS', 'WeekStartDate','PreviousYearSalesActualQuantity', 'PreviousYearEcSalesActualQuantity', 'PreviousYearClassSalesActualQuantity', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
        
    else:
    
        df_merged2 = df_merged[['商品コード','売上実績数量', '売上実績数量EC','前年売上実績数量', '前年EC売上実績数量', '前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

        if THEME_MD_MODE:
            df = get_last_year_sales_amount_left(df_merged2)
        else:
            df = get_last_year_sales_amount(df_merged2)

        df_vx_train = df[['商品コード','売上実績数量', '売上実績数量EC', '週開始日付','前年売上実績数量', '前年EC売上実績数量', '8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

        df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount', '売上実績数量EC':'SalesAmountEC', '前年売上実績数量':'PreviousYearSalesActualQuantity', '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', '8週平均ema':'SalesAmount8ema'})

        df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'SalesAmountEC', 'WeekStartDate','PreviousYearSalesActualQuantity', 'PreviousYearEcSalesActualQuantity', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    
else:
    df_merged2 = df_merged[['商品コード','売上実績数量','前年売上実績数量','前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    if THEME_MD_MODE:
        df = get_last_year_sales_amount_left(df_merged2)
    else:
        df = get_last_year_sales_amount(df_merged2)
    
    
    
    

    df_vx_train = df[['商品コード','売上実績数量','週開始日付','前年売上実績数量','8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]


    df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount','前年売上実績数量':'PreviousYearSalesActualQuantity','8週平均ema':'SalesAmount8ema'})

    #df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'WeekStartDate','PreviousYearSalesActualQuantity', 'SalesAmount8ema','time_leap8']]
    df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'WeekStartDate','PreviousYearSalesActualQuantity', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]


df_vx_train['PreviousYearSalesActualQuantity'] = df_vx_train['PreviousYearSalesActualQuantity'].astype(float)
if add_ec_salesamount:
    df_vx_train['PreviousYearEcSalesActualQuantity'] = df_vx_train['PreviousYearEcSalesActualQuantity'].astype(float)
if class_wave_add:
    df_vx_train['PreviousYearClassSalesActualQuantity'] = df_vx_train['PreviousYearClassSalesActualQuantity'].astype(float)
if class_wave_mean_add:
    df_vx_train['PreviousYearClassSalesActualQuantity8ema'] = df_vx_train['PreviousYearClassSalesActualQuantity8ema'].astype(float)
    
    
    


# ここでNULLを落としている
df_vx_train = df_vx_train.dropna(how='any').reset_index(drop=True)



if turn_back_time:
    # モデル学習上の現在日時を巻き戻す
    # turn_back_time = True
    # turn_back_yyyymmdd = 20230424
    # ***********************************************
    #df_vx_train = df_vx_train[df_vx_train['WeekStartDate']>=20191219].reset_index(drop=True)
    #df_vx_train =  df_vx_train[df_vx_train['WeekStartDate'] <= turn_back_yyyymmdd].reset_index(drop=True)
    
    turnback_nenshudo = ymd2nenshudo(pd.to_datetime(str(turn_back_yyyymmdd)), dfc_tmp)
    train_start_nenshudo = calc_nenshudo2(turnback_nenshudo, -52*3 - 5, dfc_tmp)
    train_start_week_from_ymd = nenshudo2weekfromymd(train_start_nenshudo, df_calendar)
    print('train_start_nenshudo:', train_start_nenshudo)
    print('train_start_week_from_ymd:', train_start_week_from_ymd)
    df_vx_train =  df_vx_train[df_vx_train['WeekStartDate'] < turn_back_yyyymmdd].reset_index(drop=True)
else:
    train_start_nenshudo = calc_nenshudo2(today_nenshudo, -52*3 - 5, dfc_tmp)
    train_start_week_from_ymd = nenshudo2weekfromymd(train_start_nenshudo, df_calendar)
    print('train_start_nenshudo:', train_start_nenshudo)
    print('train_start_week_from_ymd:', train_start_week_from_ymd)
    
    
# 学習期間を絞る*************************************************************************
#df_vx_train = df_vx_train[df_vx_train['WeekStartDate']>=20201219].reset_index(drop=True)
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
df_vx_train['TenpoCdPrdCd'] = str(tenpo_cd) + '_' + df_vx_train['PrdCd'].astype(str)
'''
############################
# REFACTORED CODE
def process_data(df_merged, add_ec_salesamount, class_wave_add, class_wave_mean_add, THEME_MD_MODE,
                   get_last_year_sales_amount, get_last_year_sales_amount_left, turn_back_time,
                   turn_back_yyyymmdd, dfc_tmp, df_calendar, today_nenshudo, tenpo_cd):

    """
    Processes and transforms the input DataFrame based on various conditions
    related to sales data, time, and product categories.

    Args:
        df_merged (pd.DataFrame): Input DataFrame containing sales data.
        add_ec_salesamount (bool): Flag indicating whether to include EC sales data.
        class_wave_add (bool): Flag indicating whether to include CLASS wave data.
        class_wave_mean_add (bool): Flag indicating whether to include CLASS wave mean data.
        THEME_MD_MODE (bool): Flag for theme MD mode.
        get_last_year_sales_amount (function): Function to retrieve last year's sales amount.
        get_last_year_sales_amount_left (function): Function to retrieve last year's sales amount (left join version).
        turn_back_time (bool): Flag to indicate if time needs to be turned back for model training.
        turn_back_yyyymmdd (int): Date to turn back to in YYYYMMDD format.
        dfc_tmp (pd.DataFrame): Temporary DataFrame for calendar calculations.
        df_calendar (pd.DataFrame): DataFrame containing calendar information.
        today_nenshudo (int): Today's nenshudo.
        train_start_week_from_ymd (int): Start week for training from YYYYMMDD.
        tenpo_cd (int): Store code.


    Returns:
        pd.DataFrame: Processed and transformed DataFrame.
    """
    if add_ec_salesamount:
        if class_wave_add:

            if class_wave_mean_add:

                df_merged2 = df_merged[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '売上実績数量CLASS8ema', '前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema', '前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

                if THEME_MD_MODE:
                    df = get_last_year_sales_amount_left(df_merged2)
                else:
                    df = get_last_year_sales_amount(df_merged2)

                df_vx_train = df[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '売上実績数量CLASS8ema', '週開始日付','前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '前年CLASS売上実績数量8ema', '8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

                df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount', '売上実績数量EC':'SalesAmountEC', '売上実績数量CLASS':'SalesAmountCLASS', '売上実績数量CLASS8ema':'SalesAmountCLASS8ema', '前年売上実績数量':'PreviousYearSalesActualQuantity', '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', '前年CLASS売上実績数量':'PreviousYearClassSalesActualQuantity', '前年CLASS売上実績数量8ema':'PreviousYearClassSalesActualQuantity8ema', '8週平均ema':'SalesAmount8ema'})

                df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'SalesAmountEC', 'SalesAmountCLASS', 'SalesAmountCLASS8ema', 'WeekStartDate','PreviousYearSalesActualQuantity', 'PreviousYearEcSalesActualQuantity', 'PreviousYearClassSalesActualQuantity', 'PreviousYearClassSalesActualQuantity8ema', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]



            else:

                df_merged2 = df_merged[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

                if THEME_MD_MODE:
                    df = get_last_year_sales_amount_left(df_merged2)
                else:
                    df = get_last_year_sales_amount(df_merged2)

                df_vx_train = df[['商品コード','売上実績数量', '売上実績数量EC', '売上実績数量CLASS', '週開始日付','前年売上実績数量', '前年EC売上実績数量', '前年CLASS売上実績数量', '8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

                df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount', '売上実績数量EC':'SalesAmountEC', '売上実績数量CLASS':'SalesAmountCLASS', '前年売上実績数量':'PreviousYearSalesActualQuantity', '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', '前年CLASS売上実績数量':'PreviousYearClassSalesActualQuantity', '8週平均ema':'SalesAmount8ema'})

                df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'SalesAmountEC', 'SalesAmountCLASS', 'WeekStartDate','PreviousYearSalesActualQuantity', 'PreviousYearEcSalesActualQuantity', 'PreviousYearClassSalesActualQuantity', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

        else:

            df_merged2 = df_merged[['商品コード','売上実績数量', '売上実績数量EC','前年売上実績数量', '前年EC売上実績数量', '前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

            if THEME_MD_MODE:
                df = get_last_year_sales_amount_left(df_merged2)
            else:
                df = get_last_year_sales_amount(df_merged2)

            df_vx_train = df[['商品コード','売上実績数量', '売上実績数量EC', '週開始日付','前年売上実績数量', '前年EC売上実績数量', '8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

            df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount', '売上実績数量EC':'SalesAmountEC', '前年売上実績数量':'PreviousYearSalesActualQuantity', '前年EC売上実績数量':'PreviousYearEcSalesActualQuantity', '8週平均ema':'SalesAmount8ema'})

            df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'SalesAmountEC', 'WeekStartDate','PreviousYearSalesActualQuantity', 'PreviousYearEcSalesActualQuantity', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    else:
        df_merged2 = df_merged[['商品コード','売上実績数量','前年売上実績数量','前年週開始日付','週開始日付','8週平均ema', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

        if THEME_MD_MODE:
            df = get_last_year_sales_amount_left(df_merged2)
        else:
            df = get_last_year_sales_amount(df_merged2)





        df_vx_train = df[['商品コード','売上実績数量','週開始日付','前年売上実績数量','8週平均ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]


        df_vx_train = df_vx_train.rename(columns={'商品コード': 'PrdCd', '週開始日付':'WeekStartDate', '売上実績数量':'SalesAmount','前年売上実績数量':'PreviousYearSalesActualQuantity','8週平均ema':'SalesAmount8ema'})

        #df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'WeekStartDate','PreviousYearSalesActualQuantity', 'SalesAmount8ema','time_leap8']]
        df_vx_train = df_vx_train[['PrdCd', 'SalesAmount', 'WeekStartDate','PreviousYearSalesActualQuantity', 'SalesAmount8ema','time_leap8', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]


    df_vx_train['PreviousYearSalesActualQuantity'] = df_vx_train['PreviousYearSalesActualQuantity'].astype(float)
    if add_ec_salesamount:
        df_vx_train['PreviousYearEcSalesActualQuantity'] = df_vx_train['PreviousYearEcSalesActualQuantity'].astype(float)
    if class_wave_add:
        df_vx_train['PreviousYearClassSalesActualQuantity'] = df_vx_train['PreviousYearClassSalesActualQuantity'].astype(float)
    if class_wave_mean_add:
        df_vx_train['PreviousYearClassSalesActualQuantity8ema'] = df_vx_train['PreviousYearClassSalesActualQuantity8ema'].astype(float)

    # ここでNULLを落としている
    df_vx_train = df_vx_train.dropna(how='any').reset_index(drop=True)

    # Time rollback logic
    if turn_back_time:
        turnback_nenshudo = ymd2nenshudo(pd.to_datetime(str(turn_back_yyyymmdd)), dfc_tmp)
        train_start_nenshudo = calc_nenshudo2(turnback_nenshudo, -52*3 - 5, dfc_tmp)
        train_start_week_from_ymd = nenshudo2weekfromymd(train_start_nenshudo, df_calendar)
        logger.info(f'train_start_nenshudo: {train_start_nenshudo}')
        logger.info(f'train_start_week_from_ymd: {train_start_week_from_ymd}')

        df_vx_train = df_vx_train[df_vx_train['WeekStartDate'] < turn_back_yyyymmdd].reset_index(drop=True)
    else:
        train_start_nenshudo = calc_nenshudo2(today_nenshudo, -52*3 - 5, dfc_tmp)
        train_start_week_from_ymd = nenshudo2weekfromymd(train_start_nenshudo, df_calendar)
        logger.info(f'train_start_nenshudo: {train_start_nenshudo}')
        logger.info(f'train_start_week_from_ymd: {train_start_week_from_ymd}')


    # Filter by training period
    df_vx_train = df_vx_train[df_vx_train['WeekStartDate']>=train_start_week_from_ymd].reset_index(drop=True)

    # Final transformations
    df_vx_train['weekstartdatestamp'] = pd.to_datetime(df_vx_train['WeekStartDate'], format = '%Y%m%d')
    df_vx_train['tenpo_cd'] = tenpo_cd
    df_vx_train = df_vx_train.drop('WeekStartDate', axis=1)
    df_vx_train['PrdCd'] = df_vx_train['PrdCd'].astype(int)
    df_vx_train['DPT'] = df_vx_train['DPT'].astype(int)
    df_vx_train['line_cd'] = df_vx_train['line_cd'].astype(int)
    df_vx_train['cls_cd'] = df_vx_train['cls_cd'].astype(int)
    df_vx_train['hnmk_cd'] = df_vx_train['hnmk_cd'].astype(int)
    df_vx_train['TenpoCdPrdCd'] = str(tenpo_cd) + '_' + df_vx_train['PrdCd'].astype(str)
    df_vx_train['training_weight'] = 7000

    return df_vx_train, train_start_nenshudo, train_start_week_from_ymd


#sales_df_bk6 = copy.deepcopy(df_vx_train)     


# 重みのデフォルト値設定
# df_vx_train['training_weight'] = 7000



##############################
# REFACTORED CODE
def process_sales_data2(salesup_flag, tenpo_cd, df_vx_train, df_calendar):
    """
    Processes sales data to identify upselling opportunities, sets flags,
    and adjusts training weights.

    Args:
        salesup_flag (bool): Flag indicating whether to perform upselling analysis.
        tenpo_cd: Store code (used for logging).
        df_vx_train (pd.DataFrame): Training data.
        df_calendar (pd.DataFrame): Calendar data for year/week conversions.

    Returns:
        pd.DataFrame: Modified df_vx_train with added flags and adjusted weights.
    """

    if not salesup_flag:
        logger.info("salesup_flag is False. Skipping sales data processing.")
        return df_vx_train

    logger.info("Starting sales data processing...")

    # 繁忙期フラグとweightの設定
    df_vx_train['training_weight'] = 2000

    # 販売期間のある最初の週のカラムを設定する
    weekstartdatestamp_min = df_vx_train['weekstartdatestamp'].min()
    df_vx_train_exist_sales = df_vx_train[df_vx_train['SalesAmount'] >= 0.001]

    if len(df_vx_train_exist_sales) > 0:
        df_vx_train_exist_sales['weekstartdatestamp_exist_sales_min'] = df_vx_train_exist_sales.groupby("PrdCd", as_index=False)['weekstartdatestamp'].transform(lambda x: x.min())

        df_vx_train_exist_sales2 = df_vx_train_exist_sales[['PrdCd', 'weekstartdatestamp_exist_sales_min']].drop_duplicates()
        del df_vx_train_exist_sales

        prdcd_1stsalesweekstartdatestamp_dict = dict(zip(df_vx_train_exist_sales2['PrdCd'], df_vx_train_exist_sales2['weekstartdatestamp_exist_sales_min']))

        df_vx_train['1stsalesweekstartdatestamp'] = df_vx_train['PrdCd'].apply(lambda x:prdcd_1stsalesweekstartdatestamp_dict.get(x, weekstartdatestamp_min))
        logger.info("1stsalesweekstartdatestamp calculated based on sales data.")

    else:
        df_vx_train['1stsalesweekstartdatestamp'] = weekstartdatestamp_min
        logger.info("No sales data found. 1stsalesweekstartdatestamp set to min weekstartdatestamp.")

    # --- Nested Function: set_upsale_flag ---
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
        
        #flag_target_df.to_csv('flag_target_df' + '_' + flag_column_name + '_'+ str(tenpo_cd) + '.csv')


        # さらに1年前の伸長をチェックする
        start_nenshudo_before2 = start_nenshudo_before - 100
        end_nenshudo_before2 = end_nenshudo_before - 100
        start_nenshudo_after2 = start_nenshudo_after - 100
        end_nenshudo_after2 = end_nenshudo_after - 100     
        
        # 繁忙期間前
        start_ymd_b2 = str(nenshudo2weekfromymd(start_nenshudo_before2, df_calendar))
        end_ymd_b2 = str(nenshudo2weekfromymd(end_nenshudo_before2, df_calendar))
        # 繁忙期間後
        start_ymd_a2 = str(nenshudo2weekfromymd(start_nenshudo_after2, df_calendar))
        end_ymd_a2 = str(nenshudo2weekfromymd(end_nenshudo_after2, df_calendar))
        

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
        #flag_target_prdcd_list_df.to_csv('flag_target_prdcd_list_df' + '_' + flag_column_name + '_'+ str(tenpo_cd) + '.csv')
        
        
        
        # 閾値以上の商品にフラグをつける（全期間にフラグを立てる）
        df_vx_train[flag_column_name] = 0
        df_vx_train[flag_column_name][df_vx_train['PrdCd'].isin(flag_target_prdcd_list)] = 1
        
        # training_weightの設定（繁忙期間のみ重みを大きくする）
        df_vx_train[weight_columns_name][df_vx_train['PrdCd'].isin(flag_target_prdcd_list)
                                    &(pd.to_datetime(start_ymd_a)<= df_vx_train['weekstartdatestamp'])
                                    &(df_vx_train['weekstartdatestamp']<=pd.to_datetime(end_ymd_a))
                                  ] = weight_value
        
        
        # 他の施策案
        # 1. weightの差をもっと大きくする　7000/10000  -> 5000 / 10000
        # 2. 伸長期間だけのフラグを追加する（事前に既知）
        # 3. 伸長度合を数字にする                      → 前年波形があるのでそれでよい気がする
        # 4. 学習パラメータをRMSLE -> RMSEにする
        # 5. 伸長フラグのある商品だけでモデルをつくる
        
        #過去年度の週もweightを大きくしてみる
        # 繁忙期間後
        
        def calc_prev_year_nenshudo(my_nenshudo, n):
            my_year = int(my_nenshudo / 100)
            my_shudo = int(my_nenshudo % 100)
            
            return (my_year - n) * 100 + my_shudo
            
        # 4年前までの伸長後週のweightを大きくする
        for prev_year in [1, 2, 3, 4]:
            prev_year_start_nenshudo_a = calc_prev_year_nenshudo(start_nenshudo_after, prev_year)
            prev_year_end_nenshudo_a = calc_prev_year_nenshudo(end_nenshudo_after, prev_year)

            if (prev_year_start_nenshudo_a is not None) and (prev_year_end_nenshudo_a is not None):
                start_ymd_a = str(nenshudo2weekfromymd(prev_year_start_nenshudo_a, df_calendar))
                end_ymd_a = str(nenshudo2weekfromymd(prev_year_end_nenshudo_a, df_calendar))
                #training_weightの設定（繁忙期間のみ重みを大きくする, ただし1stsalesweekstartdatestampより後のみとする）
                df_vx_train[weight_columns_name][df_vx_train['PrdCd'].isin(flag_target_df['PrdCd'].tolist())
                                            &(pd.to_datetime(start_ymd_a)<= df_vx_train['weekstartdatestamp'])
                                            &(df_vx_train['weekstartdatestamp']<=pd.to_datetime(end_ymd_a))
                                            &(df_vx_train['1stsalesweekstartdatestamp'] <= df_vx_train['weekstartdatestamp'])
                                          ] = weight_value
        
        return df_vx_train
        
    '''
    ## TEST1       
    # 年末　202437-40週 / 41週-44週 を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202337, 202340, 202341, 202344, 2.0, 1.2, 'BusyPeriodFlagNenmatsu', 'training_weight', 10000)

    # 4半期（2024年3,4,5月　販売ランクS, Aにおいて）、＊＊モデル学習時は商品ランクを見ない＊＊
    # GW  202407-08週 (4/8-4/21)/ 09-10週(4/22-5/5) を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202407, 202408, 202409, 202410, 2.0, 1.2, 'BusyPeriodFlagGw', 'training_weight', 10000)

    # 新生活 202403-04週 / 05-06週                   を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202403, 202404, 202405, 202406, 2.0, 1.2, 'BusyPeriodFlagNewLife', 'training_weight', 10000)
    '''
    '''
    ## TEST2 (伸長条件を4.0, 1.4にして、1年前の伸長も条件に入れる)
    # 年末　202437-40週 / 41週-44週 を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202337, 202340, 202341, 202344, 4.0, 1.4, 'BusyPeriodFlagNenmatsu', 'training_weight', 10000)

    # 4半期（2024年3,4,5月　販売ランクS, Aにおいて）、＊＊モデル学習時は商品ランクを見ない＊＊
    # GW  202407-08週 (4/8-4/21)/ 09-10週(4/22-5/5) を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202407, 202408, 202409, 202410, 4.0, 1.4, 'BusyPeriodFlagGw', 'training_weight', 10000)

    # 新生活 202403-04週 / 05-06週                   を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202403, 202404, 202405, 202406, 4.0, 1.4, 'BusyPeriodFlagNewLife', 'training_weight', 10000)
    '''
    
    ## 本番用 (伸長条件を4.0, 1.4にして、1年前の伸長も条件に入れる)、伸長判定週をまきもどさない設定にする
    # 年末　202437-40週 / 41週-44週 を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202437, 202440, 202441, 202444, 4.0, 1.4, 'BusyPeriodFlagNenmatsu', 'training_weight', 10000)

    # 4半期（2024年3,4,5月　販売ランクS, Aにおいて）、＊＊モデル学習時は商品ランクを見ない＊＊
    # GW  202407-08週 (4/8-4/21)/ 09-10週(4/22-5/5) を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202407, 202408, 202409, 202410, 4.0, 1.4, 'BusyPeriodFlagGw', 'training_weight', 10000)

    # 新生活 202403-04週 / 05-06週                   を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202503, 202504, 202505, 202506, 4.0, 1.4, 'BusyPeriodFlagNewLife', 'training_weight', 10000)
    
    
    
    ########Newly Added#############################
    
    # 梅雨明け夏休み 202419-20週 / 21-22週                   を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202419, 202420, 202421, 202422, 4.0, 1.4, 'BusyPeriodFlagEndRainySsn', 'training_weight', 10000)
    
    # お盆 202422-23週 / 24-25週                   を比較して、平均販売数の差が2以上かつ120%以上
    df_vx_train = set_upsale_flag(tenpo_cd, df_vx_train, df_calendar, 202422, 202423, 202424, 202425, 4.0, 1.4, 'BusyPeriodFlagObon', 'training_weight', 10000)
    
    ########Newly Added#############################
    
    
    
    # 後始末
    df_vx_train = df_vx_train.drop('1stsalesweekstartdatestamp', axis=1)
    del df_vx_train_exist_sales2
    del prdcd_1stsalesweekstartdatestamp_dict

    logger.info("Finished sales data processing.")
    return df_vx_train


#########################
# # OLD CODE

# if no_sales_term_weight_zero:
#     # SKU別にみて、最初に販売の無い期間はウェイトを0にしておく
    
#     # デフォルト値設定
#     #df_vx_train['training_weight'] = 10000
#     # 販売期間のある最初の週をとってくる
#     df_vx_train_exist_sales = df_vx_train[df_vx_train['SalesAmount'] >= 0.001]
    
    
#     if len(df_vx_train_exist_sales) > 0:
    
#         df_vx_train_exist_sales['weekstartdatestamp_exist_sales_min'] = df_vx_train_exist_sales.groupby("PrdCd", as_index=False)['weekstartdatestamp'].transform(lambda x: x.min())

#         df_vx_train_exist_sales2 = df_vx_train_exist_sales[['PrdCd', 'weekstartdatestamp_exist_sales_min']].drop_duplicates()
#         del df_vx_train_exist_sales

#         prdcd_1stsalesweekstartdatestamp_dict = dict(zip(df_vx_train_exist_sales2['PrdCd'], df_vx_train_exist_sales2['weekstartdatestamp_exist_sales_min']))

#         weekstartdatestamp_min = df_vx_train['weekstartdatestamp'].min()
#         df_vx_train['1stsalesweekstartdatestamp'] = df_vx_train['PrdCd'].apply(lambda x:prdcd_1stsalesweekstartdatestamp_dict.get(x, weekstartdatestamp_min))


#         df_vx_train['training_weight'][df_vx_train['weekstartdatestamp'] < df_vx_train['1stsalesweekstartdatestamp']] = 0.0

#         # 後始末
#         df_vx_train = df_vx_train.drop('1stsalesweekstartdatestamp', axis=1)
#         del df_vx_train_exist_sales2
#         del prdcd_1stsalesweekstartdatestamp_dict
        

    
# # 直近1か月のweightを大きくして、直近の売り数変化に追従できるようにする    
# elif last_month_weight_larger:
#     large_weight_start_nenshudo = calc_nenshudo2(today_nenshudo, -4, dfc_tmp)
#     large_weight_start_ymd = nenshudo2weekfromymd(large_weight_start_nenshudo, df_calendar)
#     large_weight_start_stamp = pd.to_datetime(str(large_weight_start_ymd))
    
#     # デフォルト値設定
#     df_vx_train['training_weight'] = 5000
#     # 販売期間のある最初の週をとってくる
#     df_vx_train['training_weight'][df_vx_train['weekstartdatestamp'] >= large_weight_start_stamp] = 10000
    
# else:
#     # 重みはデフォルトのまま（重みは使わない）
#     pass
#     # 学習時の重み
#     #df_vx_train['training_weight'] = df_vx_train['weekstartdatestamp'].apply(lambda x:(x.year-2019)*1000.0*2.4 + (x.month/12)*800)

    
    
# if add_split_column:
#     '''
#     # データ分割列を使用すると、トレーニング、検証、テストに使用する特定の行を選択できます。
#     # トレーニング データを作成する場合、列を追加して、そこに次のいずれかの値（大文字小文字の区別あり）
#     # を含めることができます。
#     # TEST     5wk     train_start_nenshudo
#     # TRAIN    ---
#     #VALIDATE 52wk

#     #######UNASSIGNED
#     '''
#     test_end_nenshudo = calc_nenshudo2(train_start_nenshudo, 4, dfc_tmp) # 最初の5週を評価期間にする
#     test_end_ymd = nenshudo2weekfromymd(test_end_nenshudo, df_calendar)
#     test_end_stamp = pd.to_datetime(str(test_end_ymd))
#     print('test_end_stamp:', test_end_stamp)
    
#     if turn_back_time:
#         validate_start_nenshudo = calc_nenshudo2(turnback_nenshudo, -52, dfc_tmp)
#         validate_start_ymd = nenshudo2weekfromymd(validate_start_nenshudo, df_calendar)
#         validate_start_stamp = pd.to_datetime(str(validate_start_ymd))
        
#     else:
#         validate_start_nenshudo = calc_nenshudo2(today_nenshudo, -52, dfc_tmp)
#         validate_start_ymd = nenshudo2weekfromymd(validate_start_nenshudo, df_calendar)
#         validate_start_stamp = pd.to_datetime(str(validate_start_ymd))
#     print('validate_start_stamp:', validate_start_stamp)
    
#     df_vx_train['split'] = 'TRAIN'
#     df_vx_train['split'][df_vx_train['weekstartdatestamp'] <= test_end_stamp] = 'TEST'
#     df_vx_train['split'][df_vx_train['weekstartdatestamp'] >= validate_start_stamp] = 'VALIDATE'
# 
    
###################
# REFACTORED CODE
def process_data1(df_merged, add_ec_salesamount, class_wave_add, class_wave_mean_add, THEME_MD_MODE,
                   get_last_year_sales_amount, get_last_year_sales_amount_left, turn_back_time,
                   turn_back_yyyymmdd, dfc_tmp, df_calendar, today_nenshudo, tenpo_cd,
                   no_sales_term_weight_zero, last_month_weight_larger, add_split_column, df_vx_train, train_start_nenshudo):

    """
    Processes and transforms the input DataFrame based on various conditions
    related to sales data, time, product categories, training weights, and data splitting.

    Args:
        df_merged (pd.DataFrame): Input DataFrame containing sales data.
        add_ec_salesamount (bool): Flag indicating whether to include EC sales data.
        class_wave_add (bool): Flag indicating whether to include CLASS wave data.
        class_wave_mean_add (bool): Flag indicating whether to include CLASS wave mean data.
        THEME_MD_MODE (bool): Flag for theme MD mode.
        get_last_year_sales_amount (function): Function to retrieve last year's sales amount.
        get_last_year_sales_amount_left (function): Function to retrieve last year's sales amount (left join version).
        turn_back_time (bool): Flag to indicate if time needs to be turned back for model training.
        turn_back_yyyymmdd (int): Date to turn back to in YYYYMMDD format.
        dfc_tmp (pd.DataFrame): Temporary DataFrame for calendar calculations.
        df_calendar (pd.DataFrame): DataFrame containing calendar information.
        today_nenshudo (int): Today's nenshudo.
        train_start_week_from_ymd (int): Start week for training from YYYYMMDD.
        tenpo_cd (int): Store code.
        no_sales_term_weight_zero (bool): Flag to set training weight to 0 for periods with no sales.
        last_month_weight_larger (bool): Flag to increase the training weight for the last month.
        add_split_column (bool): Flag to add a data split column (TRAIN, VALIDATE, TEST).

    Returns:
        pd.DataFrame: Processed and transformed DataFrame.
    """
    # --- Training Weight Logic ---
    if no_sales_term_weight_zero:
        # SKU別にみて、最初に販売の無い期間はウェイトを0にしておく

        # 販売期間のある最初の週をとってくる
        df_vx_train_exist_sales = df_vx_train[df_vx_train['SalesAmount'] >= 0.001].copy() # Added .copy()
        if len(df_vx_train_exist_sales) > 0:
            df_vx_train_exist_sales['weekstartdatestamp_exist_sales_min'] = df_vx_train_exist_sales.groupby("PrdCd", as_index=False)['weekstartdatestamp'].transform(lambda x: x.min())
            df_vx_train_exist_sales2 = df_vx_train_exist_sales[['PrdCd', 'weekstartdatestamp_exist_sales_min']].drop_duplicates()
            prdcd_1stsalesweekstartdatestamp_dict = dict(zip(df_vx_train_exist_sales2['PrdCd'], df_vx_train_exist_sales2['weekstartdatestamp_exist_sales_min']))
            weekstartdatestamp_min = df_vx_train['weekstartdatestamp'].min()
            df_vx_train['1stsalesweekstartdatestamp'] = df_vx_train['PrdCd'].apply(lambda x:prdcd_1stsalesweekstartdatestamp_dict.get(x, weekstartdatestamp_min))

            # Avoid SettingWithCopyWarning
            df_vx_train['training_weight'] = 10000 # Default value
            df_vx_train.loc[df_vx_train['weekstartdatestamp'] < df_vx_train['1stsalesweekstartdatestamp'], 'training_weight'] = 0.0

            # Cleanup
            df_vx_train = df_vx_train.drop('1stsalesweekstartdatestamp', axis=1)
            del df_vx_train_exist_sales, df_vx_train_exist_sales2, prdcd_1stsalesweekstartdatestamp_dict

    elif last_month_weight_larger:
        large_weight_start_nenshudo = calc_nenshudo2(today_nenshudo, -4, dfc_tmp)
        large_weight_start_ymd = nenshudo2weekfromymd(large_weight_start_nenshudo, df_calendar)
        large_weight_start_stamp = pd.to_datetime(str(large_weight_start_ymd))

        df_vx_train['training_weight'] = 5000  # Default value
        df_vx_train.loc[df_vx_train['weekstartdatestamp'] >= large_weight_start_stamp, 'training_weight'] = 10000

    else:
        # 重みはデフォルトのまま（重みは使わない）
        pass
        # 学習時の重み
        #df_vx_train['training_weight'] = df_vx_train['weekstartdatestamp'].apply(lambda x:(x.year-2019)*1000.0*2.4 + (x.month/12)*800)

    # --- Data Splitting Logic ---
    if add_split_column:
        test_end_nenshudo = calc_nenshudo2(train_start_nenshudo, 4, dfc_tmp) # 最初の5週を評価期間にする
        test_end_ymd = nenshudo2weekfromymd(test_end_nenshudo, df_calendar)
        test_end_stamp = pd.to_datetime(str(test_end_ymd))
        logger.info(f'test_end_stamp: {test_end_stamp}')

        if turn_back_time:
            validate_start_nenshudo = calc_nenshudo2(turnback_nenshudo, -52, dfc_tmp)
            validate_start_ymd = nenshudo2weekfromymd(validate_start_nenshudo, df_calendar)
            validate_start_stamp = pd.to_datetime(str(validate_start_ymd))
        else:
            validate_start_nenshudo = calc_nenshudo2(today_nenshudo, -52, dfc_tmp)
            validate_start_ymd = nenshudo2weekfromymd(validate_start_nenshudo, df_calendar)
            validate_start_stamp = pd.to_datetime(str(validate_start_ymd))
        logger.info(f'validate_start_stamp: {validate_start_stamp}')

        df_vx_train['split'] = 'TRAIN'
        df_vx_train.loc[df_vx_train['weekstartdatestamp'] <= test_end_stamp, 'split'] = 'TEST'
        df_vx_train.loc[df_vx_train['weekstartdatestamp'] >= validate_start_stamp, 'split'] = 'VALIDATE'

    return df_vx_train

    
#sales_df_bk7 = copy.deepcopy(df_vx_train)      

#print('aaaaa')
#sys.exit()

#######################
# # OLD CODE
# '''
# # シーズン品の分離
# if devide_season_items:
    
#     # 細工するので、元データをとっておく
#     df_vx_train_bk = copy.deepcopy(df_vx_train)
    
#     df_vx_train['prd_mean'] = df_vx_train.groupby(['PrdCd'], as_index=False)['SalesAmount'].transform(lambda x: x.mean())
#     df_vx_train['prd_std'] = df_vx_train.groupby(['PrdCd'], as_index=False)['SalesAmount'].transform(lambda x: x.std())
    
    
#     # 販売期間のある最初の週をとってくる
#     df_vx_train_exist_sales = df_vx_train[df_vx_train['SalesAmount'] >= 0.001]
#     df_vx_train_exist_sales['weekstartdatestamp_exist_sales_min'] = df_vx_train_exist_sales.groupby("PrdCd", as_index=False)['weekstartdatestamp'].transform(lambda x: x.min())

#     df_vx_train_exist_sales2 = df_vx_train_exist_sales[['PrdCd', 'weekstartdatestamp_exist_sales_min']].drop_duplicates()
#     del df_vx_train_exist_sales

#     weekstartdatestamp_max = df_vx_train['weekstartdatestamp'].max()
    
#     prdcd_1stsalesweekstartdatestamp_dict = dict(zip(df_vx_train_exist_sales2['PrdCd'], df_vx_train_exist_sales2['weekstartdatestamp_exist_sales_min']))

#     df_vx_train['1stsalesweekstartdatestamp'] = df_vx_train['PrdCd'].apply(lambda x:prdcd_1stsalesweekstartdatestamp_dict.get(x, weekstartdatestamp_max))
    
#     # 販売開始前の期間には、平均値を設定しておく（売り数０の影響を避けるため）
#     df_vx_train['SalesAmount'][df_vx_train['weekstartdatestamp'] < df_vx_train['1stsalesweekstartdatestamp']] = df_vx_train['prd_mean']

#     # 月番号
#     df_vx_train['month'] = df_vx_train["weekstartdatestamp"].apply(lambda x : x.month)
    
#     season_pattern_list = [        
#         [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]],
#         [[2, 3, 4], [5, 6, 7], [8, 9, 10], [11, 12, 1]],
#         [[3, 4, 5], [6, 7, 8], [9, 10, 11], [12, 1, 2]],

#         [[12, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10, 11]],
#         [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10], [11, 12]],
#     ]


    
#     for i, season_pattern in enumerate(season_pattern_list):
    
#         df_vx_train[f'season_{i}' ] = np.nan
#         for j, month_list in enumerate(season_pattern):
#             df_vx_train[f'season_{i}'][df_vx_train['month'].isin(month_list)] = j

#         df_vx_train[f'prd_season_mean_{i}'] = df_vx_train.groupby(['PrdCd', f'season_{i}'], as_index=False)['SalesAmount'].transform(lambda x: x.mean())


#         # df_season[f"{col}_std"] = (df_season[col]-df_season["mean"])/df_season["std"]
#         df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_{i}'] = (df_vx_train[f'prd_season_mean_{i}'] - df_vx_train["prd_mean"]) / df_vx_train["prd_std"]

#         #df_season["gap"] = df_season[[col + "_std" for col in season_cols]].T.max() - \
#         #    df_season[[col + "_std" for col in season_cols]].T.min()

#         df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_max_{i}'] =  df_vx_train.groupby(['PrdCd'], as_index=False)[f'prd_season_mean_minus_prd_mean_per_std_{i}'].transform(lambda x: x.max())

#         df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_min_{i}'] =  df_vx_train.groupby(['PrdCd'], as_index=False)[f'prd_season_mean_minus_prd_mean_per_std_{i}'].transform(lambda x: x.min())

#         df_vx_train[f'gap_{i}'] = df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_max_{i}'] - df_vx_train[f'prd_season_mean_minus_prd_mean_per_std_min_{i}']


#         df_vx_train[f"is_season_{i}"] = 0
#         df_vx_train[f"is_season_{i}"][df_vx_train[f"gap_{i}"]>=1.5] = 1
    

#     df_vx_train['gap_mean'] = df_vx_train[['gap_0', 'gap_1', 'gap_2', 'gap_3', 'gap_4']].apply(lambda x:x.mean(), axis=1)
#     df_vx_train['gap_max'] = df_vx_train[['gap_0', 'gap_1', 'gap_2', 'gap_3', 'gap_4']].apply(lambda x:x.max(), axis=1)
#     df_vx_train['is_season_sum'] = df_vx_train[['is_season_0', 'is_season_1', 'is_season_2', 'is_season_3', 'is_season_4']].apply(lambda x:x.sum(), axis=1)
    
#     df_vx_train_season_tmp = copy.deepcopy(df_vx_train)
    
#     '''
#     # seasonal_prdcd_list = list(set(df_vx_train_season_tmp[df_vx_train_season_tmp['gap_max'] >= 1.6]['PrdCd']))    
#     # df_vx_train_seasonal = df_vx_train_bk[df_vx_train_bk['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)
#     # table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-train-seasonal-16-" + str(today) + #str(OUTPUT_TABLE_SUFFIX)
#     # try:
#         # データアップロード時にコメントアウトを外す
#         # client = BigqueryClient()
#         # job = client.load_table_from_dataframe(df_vx_train_seasonal, table_id)
#         # job.result()
#         # print("==data-uploaded-bq-seasonal===")

#     # except Exception as e:
#         # print('errtype:', str(type(e)))
#         # print('err:', str(e))
#     '''

#     seasonal_prdcd_list = list(set(df_vx_train_season_tmp[df_vx_train_season_tmp['gap_max'] >= 1.5]['PrdCd']))    
#     df_vx_train_seasonal = df_vx_train_bk[df_vx_train_bk['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)
    
#     if len(df_vx_train_seasonal) > 0:    
#         table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-train-seasonal-15-" + str(today) + str(OUTPUT_TABLE_SUFFIX)
#         try:
#             # データアップロード時にコメントアウトを外す
#             client = BigqueryClient()
#             job = client.load_table_from_dataframe(df_vx_train_seasonal, table_id)
#             job.result()
#             print("==data-uploaded-bq-seasonal===")

#         except Exception as e:
#             print('errtype:', str(type(e)))
#             print('err:', str(e))

    
#     # df_vx_trainをもとにもどす
#     #df_vx_train = copy.deepcopy(df_vx_train_bk)
    
#     # シーズン品以外をdf_vx_trainとする
#     df_vx_train = df_vx_train_bk[~df_vx_train_bk['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)
            
#     #print('devide season items sysexit')
#     #sys.exit()

    
# if THEME_MD_MODE:
    
    
#     if 1:
#         df_vx_train['theme_md_div'] = 0
#         df_vx_train['theme_md_div'][df_vx_train['PrdCd'].isin(this_tenpo_theme_md_prdcd_list)] = 1
#     else:
#         df_vx_train = pd.merge(df_vx_train, theme_md_df[['店番', 'JANコード', 'テーマMD種別']].rename(columns={'テーマMD種別':'theme_md_div'}), left_on=['tenpo_cd', 'PrdCd'], right_on=['店番', 'JANコード'], how='left').reset_index(drop=True)

#         df_vx_train['is_theme_md'] = 0
#         df_vx_train['is_theme_md'][~df_vx_train['theme_md_div'].isna()] = 1

#         df_vx_train = df_vx_train.drop('店番', axis=1)
#         df_vx_train = df_vx_train.drop('JANコード', axis=1)
    
#     #print('THEME MD sysexit')
#     #sys.exit()
# '''    

########################
# REFACTORED CODE
def process_training_data(df_vx_train, devide_season_items, THEME_MD_MODE, this_tenpo_theme_md_prdcd_list, today, OUTPUT_TABLE_SUFFIX,BigqueryClient):
    """
    Processes the training data by handling seasonal items and theme merchandise.

    Args:
        df_vx_train (pd.DataFrame): The main training dataframe.
        devide_season_items (bool): Flag to indicate if seasonal item division is needed.
        THEME_MD_MODE (bool): Flag to indicate if theme merchandise mode is enabled.
        this_tenpo_theme_md_prdcd_list (list): List of product codes for theme merchandise.
        theme_md_df (pd.DataFrame): DataFrame containing theme merchandise information.
        today: Today's date (used in table ID).
        OUTPUT_TABLE_SUFFIX (str): Suffix for the output table name.
        BigqueryClient: Bigquery client for uploading dataframe

    Returns:
        pd.DataFrame: The processed training dataframe.
    """
    
    if devide_season_items:
        logger.info("Processing seasonal items...")

        # Create a backup of the original data
        df_vx_train_bk = copy.deepcopy(df_vx_train)

        # Calculate product mean and standard deviation of sales amount
        df_vx_train['prd_mean'] = df_vx_train.groupby(['PrdCd'], as_index=False)['SalesAmount'].transform('mean')
        df_vx_train['prd_std'] = df_vx_train.groupby(['PrdCd'], as_index=False)['SalesAmount'].transform('std')

        # Find the first week with sales for each product
        df_vx_train_exist_sales = df_vx_train[df_vx_train['SalesAmount'] >= 0.001].copy() # avoid settting on slice
        df_vx_train_exist_sales.loc[:, 'weekstartdatestamp_exist_sales_min'] = df_vx_train_exist_sales.groupby("PrdCd")['weekstartdatestamp'].transform('min')

        df_vx_train_exist_sales2 = df_vx_train_exist_sales[['PrdCd', 'weekstartdatestamp_exist_sales_min']].drop_duplicates()
        del df_vx_train_exist_sales

        weekstartdatestamp_max = df_vx_train['weekstartdatestamp'].max()

        prdcd_1stsalesweekstartdatestamp_dict = dict(zip(df_vx_train_exist_sales2['PrdCd'], df_vx_train_exist_sales2['weekstartdatestamp_exist_sales_min']))

        df_vx_train['1stsalesweekstartdatestamp'] = df_vx_train['PrdCd'].apply(lambda x: prdcd_1stsalesweekstartdatestamp_dict.get(x, weekstartdatestamp_max))

        # Replace SalesAmount with prd_mean before the first sales week
        df_vx_train.loc[df_vx_train['weekstartdatestamp'] < df_vx_train['1stsalesweekstartdatestamp'], 'SalesAmount'] = df_vx_train['prd_mean']

        # Month number
        df_vx_train['month'] = df_vx_train["weekstartdatestamp"].apply(lambda x: x.month)

        season_pattern_list = [
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]],
            [[2, 3, 4], [5, 6, 7], [8, 9, 10], [11, 12, 1]],
            [[3, 4, 5], [6, 7, 8], [9, 10, 11], [12, 1, 2]],
            [[12, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10, 11]],
            [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10], [11, 12]],
        ]

        for i, season_pattern in enumerate(season_pattern_list):
            season_col = f'season_'
            df_vx_train[season_col] = np.nan
            for j, month_list in enumerate(season_pattern):
                df_vx_train.loc[df_vx_train['month'].isin(month_list), season_col] = j

            prd_season_mean_col = f'prd_season_mean_'
            df_vx_train[prd_season_mean_col] = df_vx_train.groupby(['PrdCd', season_col], as_index=False)['SalesAmount'].transform('mean')

            prd_season_mean_minus_prd_mean_per_std_col = f'prd_season_mean_minus_prd_mean_per_std_'
            df_vx_train[prd_season_mean_minus_prd_mean_per_std_col] = (df_vx_train[prd_season_mean_col] - df_vx_train["prd_mean"]) / df_vx_train["prd_std"]

            prd_season_mean_minus_prd_mean_per_std_max_col = f'prd_season_mean_minus_prd_mean_per_std_max_'
            df_vx_train[prd_season_mean_minus_prd_mean_per_std_max_col] = df_vx_train.groupby(['PrdCd'])[prd_season_mean_minus_prd_mean_per_std_col].transform('max')

            prd_season_mean_minus_prd_mean_per_std_min_col = f'prd_season_mean_minus_prd_mean_per_std_min_'
            df_vx_train[prd_season_mean_minus_prd_mean_per_std_min_col] = df_vx_train.groupby(['PrdCd'])[prd_season_mean_minus_prd_mean_per_std_col].transform('min')

            gap_col = f'gap_'
            df_vx_train[gap_col] = df_vx_train[prd_season_mean_minus_prd_mean_per_std_max_col] - df_vx_train[prd_season_mean_minus_prd_mean_per_std_min_col]

            is_season_col = f"is_season_"
            df_vx_train[is_season_col] = 0
            df_vx_train.loc[df_vx_train[gap_col] >= 1.5, is_season_col] = 1

        gap_cols = [f'gap_' for i in range(len(season_pattern_list))]
        is_season_cols = [f'is_season_' for i in range(len(season_pattern_list))]

        df_vx_train['gap_mean'] = df_vx_train[gap_cols].mean(axis=1)
        df_vx_train['gap_max'] = df_vx_train[gap_cols].max(axis=1)
        df_vx_train['is_season_sum'] = df_vx_train[is_season_cols].sum(axis=1)

        df_vx_train_season_tmp = copy.deepcopy(df_vx_train)
        seasonal_prdcd_list = list(set(df_vx_train_season_tmp[df_vx_train_season_tmp['gap_max'] >= 1.5]['PrdCd']))

        df_vx_train_seasonal = df_vx_train_bk[df_vx_train_bk['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)

        if len(df_vx_train_seasonal) > 0:
            table_id = f"dev-cainz-demandforecast.short_term_cloudrunjobs.weekly-train-seasonal-15-"
            try:
                # Data upload to BigQuery
                client = BigqueryClient()
                job = client.load_table_from_dataframe(df_vx_train_seasonal, table_id)
                job.result()
                logger.info("Data uploaded to BigQuery for seasonal items.")

            except Exception as e:
                logger.error(f"Error uploading seasonal data to BigQuery: {type(e)} - ")

        # Update df_vx_train to exclude seasonal items
        df_vx_train = df_vx_train_bk[~df_vx_train_bk['PrdCd'].isin(seasonal_prdcd_list)].reset_index(drop=True)
        logger.info("Seasonal items processed and removed from the main dataframe.")

    if THEME_MD_MODE:
        logger.info("Processing theme merchandise items...")

        # The 'if 1' block is kept for logic preservation, but consider refactoring if possible
        if True:  # Changed from 'if 1' for clarity
            df_vx_train['theme_md_div'] = 0
            df_vx_train.loc[df_vx_train['PrdCd'].isin(this_tenpo_theme_md_prdcd_list), 'theme_md_div'] = 1
        else:
            df_vx_train = pd.merge(df_vx_train, theme_md_df[['店番', 'JANコード', 'テーマMD種別']].rename(columns={'テーマMD種別': 'theme_md_div'}),
                                   left_on=['tenpo_cd', 'PrdCd'], right_on=['店番', 'JANコード'], how='left').reset_index(drop=True)

            df_vx_train['is_theme_md'] = 0
            df_vx_train.loc[~df_vx_train['theme_md_div'].isna(), 'is_theme_md'] = 1

            df_vx_train = df_vx_train.drop(['店番', 'JANコード'], axis=1)
        logger.info("Theme merchandise items processed.")

    return df_vx_train

###########################
# # OLD CODE
# '''
# if divide_by_salesamount_v2:
    
#     def set_div_points(x, div_points):
#         if x is None:
#             return 0
#         prev_dp = 0
#         for dp in div_points:
#             if x > dp:
#                 prev_dp = dp
#             else:
#                 return prev_dp
#         return None
    
#     div_points = [0, 30, 999999]
#     if divide_by_salesamount_v3:
#         div_points = [0, 10, 30, 999999]
    
#     lastweek_date = df_vx_train['weekstartdatestamp'].max()
#     last13week_date = lastweek_date - datetime.timedelta(days=int(4.3*13))
#     df_vx_train_last13week = df_vx_train[df_vx_train['weekstartdatestamp'] >= last13week_date]
#     df_vx_train_last13week['URISU_AVE'] = df_vx_train_last13week.groupby("PrdCd",as_index=False)['SalesAmount'].transform(lambda x:x.mean())
#     df_vx_train_last13week['DivPoint'] = df_vx_train_last13week['URISU_AVE'].apply(lambda x:set_div_points(x, div_points))
    
#         # 20240814 add
#     df_vx_train_last13week['DivPoint'] = df_vx_train_last13week['DivPoint'].astype(int)
    
#     df_vx_train_last13week = df_vx_train_last13week[['PrdCd', 'URISU_AVE', 'DivPoint']].drop_duplicates().reset_index(drop=True)
#     df_vx_train = pd.merge(df_vx_train, df_vx_train_last13week, on='PrdCd', how='left').reset_index(drop=True)
    
#     for dp in div_points:
#         if dp != div_points[-1]:# 分割店リストの最後の要素は反映されていないのでスキップする        
            
#             #df_vx_train_tmp = df_vx_train[df_vx_train['DivPoint']==dp]
#             # 20240814 fix
#             df_vx_train_tmp = df_vx_train[df_vx_train['DivPoint']==int(dp)].reset_index(drop=True)
            
#             if len(df_vx_train_tmp) > 0:
#                 table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-train-" + str(today) + str(OUTPUT_TABLE_SUFFIX) + str(dp) + 'div'


#                 upload_complete = False
#                 while upload_complete == False:        
#                     try:
#                         # データアップロード時にコメントアウトを外す
#                         client = BigqueryClient()
#                         job = client.load_table_from_dataframe(df_vx_train_tmp, table_id)
#                         job.result()
#                         print("==data-uploaded-bq===")    
#                         upload_complete = True
#                     except Exception as e:
#                         print('errtype:', str(type(e)))
#                         print('err:', str(e))
#                         print('data upload retry ', table_id)
#                         time.sleep(20)
            
#             else:
#                 print("==data-uploaded-length0===")  
                
                
        
    
# else:
#     if len(df_vx_train) > 0:
        
        
#         if add_reference_store_unitedmodel:
#             table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-train-" + str(today) + str(OUTPUT_TABLE_SUFFIX)

#             upload_complete = False
#             while upload_complete == False:            
#                 try:
#                     # データアップロード時にコメントアウトを外す
#                     client = BigqueryClient()
#                     job = client.load_table_from_dataframe(df_vx_train, table_id)
#                     job.result()
#                     print("==data-uploaded-bq===")
#                     upload_complete = True
#                 except Exception as e:
#                     print('errtype:', str(type(e)))
#                     print('err:', str(e))
#                     print('data upload retry ', table_id)
#                     time.sleep(20)
                    
                    
                    
            
#         else:
#             # 新店は出力テーブルを分ける
#             if tenpo_cd_ref is None:        
#                 table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-train-" + str(today) + str(OUTPUT_TABLE_SUFFIX)

#                 upload_complete = False
#                 while upload_complete == False:            
#                     try:
#                         # データアップロード時にコメントアウトを外す
#                         client = BigqueryClient()
#                         job = client.load_table_from_dataframe(df_vx_train, table_id)
#                         job.result()
#                         print("==data-uploaded-bq===")
#                         upload_complete = True
#                     except Exception as e:
#                         print('errtype:', str(type(e)))
#                         print('err:', str(e))
#                         print('data upload retry')
#                         time.sleep(20)

#             else:
#                 table_id = "dev-cainz-demandforecast.short_term_cloudrunjobs." + "weekly-train-new-store-" + str(today) + str(OUTPUT_TABLE_SUFFIX)

#                 upload_complete = False
#                 while upload_complete == False:    
#                     try:
#                         # データアップロード時にコメントアウトを外す
#                         client = BigqueryClient()
#                         job = client.load_table_from_dataframe(df_vx_train, table_id)
#                         job.result()
#                         print("==data-uploaded-bq===")    
#                         upload_complete = True
#                     except Exception as e:
#                         print('errtype:', str(type(e)))
#                         print('err:', str(e))
#                         print('data upload retry ', table_id)
#                         time.sleep(20)
                
#     else:
#         print('train data len=0 fin')
# '''

#######################
# REFACTORED CODE
def process_and_upload_data(df_vx_train, today, OUTPUT_TABLE_SUFFIX, divide_by_salesamount_v2, divide_by_salesamount_v3, add_reference_store_unitedmodel, tenpo_cd_ref):

    def set_div_points(x, div_points):
        if x is None:
            return 0
        
        prev_dp = 0
        for dp in div_points:
            if x > dp:
                prev_dp = dp
            else:
                return prev_dp
        return prev_dp  # Return the last prev_dp if x is greater than all div_points

    if divide_by_salesamount_v2:
        div_points_v2 = [0, 30, 999999]
        div_points_v3 = [0, 10, 30, 999999]

        div_points = div_points_v3 if divide_by_salesamount_v3 else div_points_v2

        lastweek_date = df_vx_train['weekstartdatestamp'].max()
        last13week_date = lastweek_date - datetime.timedelta(days=int(4.3 * 13))
        df_vx_train_last13week = df_vx_train[df_vx_train['weekstartdatestamp'] >= last13week_date].copy()  # Avoid SettingWithCopyWarning
        df_vx_train_last13week['URISU_AVE'] = df_vx_train_last13week.groupby("PrdCd", as_index=False)['SalesAmount'].transform('mean')
        df_vx_train_last13week['DivPoint'] = df_vx_train_last13week['URISU_AVE'].apply(lambda x: set_div_points(x, div_points))

        df_vx_train_last13week['DivPoint'] = df_vx_train_last13week['DivPoint'].astype(int)

        df_vx_train_last13week = df_vx_train_last13week[['PrdCd', 'URISU_AVE', 'DivPoint']].drop_duplicates().reset_index(drop=True)
        df_vx_train = pd.merge(df_vx_train, df_vx_train_last13week, on='PrdCd', how='left').reset_index(drop=True)

        for dp in div_points:
            if dp != div_points[-1]:  # Skip the last element
                df_vx_train_tmp = df_vx_train[df_vx_train['DivPoint'] == int(dp)].reset_index(drop=True)

            if not df_vx_train_tmp.empty:
                table_id = f"dev-cainz-demandforecast.short_term_cloudrunjobs.weekly-train-{today}{OUTPUT_TABLE_SUFFIX}{dp}div"
                upload_complete = False
                while not upload_complete:
                    try:
                        client = BigqueryClient()
                        job = client.load_table_from_dataframe(df_vx_train_tmp, table_id)
                        job.result()
                        logger.info(f"==data-uploaded-bq=== to {table_id}")
                        upload_complete = True
                    except Exception as e:
                        logger.info(f"errtype: {type(e)}")
                        logger.info(f"err: {e}")
                        logger.info(f"data upload retry {table_id}")
                        time.sleep(20)
            else:
                logger.info("==data-uploaded-length0===")
    else:
        if not df_vx_train.empty:

            if add_reference_store_unitedmodel:
                table_id = f"dev-cainz-demandforecast.short_term_cloudrunjobs.weekly-train-{today}{OUTPUT_TABLE_SUFFIX}"
            elif tenpo_cd_ref is None: # 新店は出力テーブルを分ける
                table_id = f"dev-cainz-demandforecast.short_term_cloudrunjobs.weekly-train-{today}{OUTPUT_TABLE_SUFFIX}"
            else:
                table_id = f"dev-cainz-demandforecast.short_term_cloudrunjobs.weekly-train-new-store-{today}{OUTPUT_TABLE_SUFFIX}"

            upload_complete = False
            while not upload_complete:
                try:
                    client = BigqueryClient()
                    job = client.load_table_from_dataframe(df_vx_train, table_id)
                    job.result()
                    logger.info(f"==data-uploaded-bq=== to {table_id}")
                    upload_complete = True
                except Exception as e:
                    logger.info(f"errtype: {type(e)}")
                    logger.info(f"err: {e}")
                    logger.info(f"data upload retry {table_id}")
                    time.sleep(20)
        else:
            logger.info('train data len=0 fin')

        
def main():
    tenpo_cd_ref = None
    path_tran_ref = None
    THEME_MD_MODE = int(os.environ.get("THEME_MD_MODE", 0))
    TURN_BACK_YYYYMMDD = os.environ.get("TURN_BACK_YYYYMMDD", "")
    OUTPUT_TABLE_SUFFIX = os.environ.get("OUTPUT_TABLE_SUFFIX", "")
    turn_back_yyyymmdd = int(TURN_BACK_YYYYMMDD) if TURN_BACK_YYYYMMDD else TURN_BACK_YYYYMMDD

    df_calendar = extract_as_df_with_encoding("Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv","dev-cainz-demandforecast","utf-8")
    df_zen_calendar = df_calendar[["nenshudo","week_from_ymd", "znen_week_from_ymd"]]
    df_zen_calendar = df_zen_calendar.rename(columns={'week_from_ymd': '週開始日付', 'znen_week_from_ymd':'前年週開始日付'})

    dfc_tmp = df_calendar[["nenshudo", "week_from_ymd", "week_to_ymd"]]
    dfc_tmp["week_from_ymd"] = dfc_tmp["week_from_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    dfc_tmp["week_to_ymd"] = dfc_tmp["week_to_ymd"].apply(lambda x : pd.to_datetime(str(x)))
    
    df_today_nenshudo  = dfc_tmp["nenshudo"][(dfc_tmp["week_from_ymd"] <= today_date_str)&(dfc_tmp["week_to_ymd"] >=     today_date_str)]
    today_nenshudo = df_today_nenshudo.values[0]
    logger.info(f"today_nenshudo: {today_nenshudo}")


#     if output_6wk_2sales:
#         path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-62/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
#     else:
#         path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
        
    if output_6wk_2sales:
        path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-62/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
    else:
        path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd)+"/{}_{}_time_series.csv"

    if add_reference_store:
        #参照店舗情報を読み込む
        path_reference_store = "Basic_Analysis_unzip_result/01_Data/37_reference_store/reference_store.csv"
        reference_store_df = extract_as_df(path_reference_store, bucket_name)
        reference_store_df["OPEN_DATE"] = reference_store_df["OPEN_DATE"].apply(lambda x : pd.to_datetime(str(x)))
        reference_store_df["OPEN_DATE_REF"] = reference_store_df["OPEN_DATE_REF"].apply(lambda x : pd.to_datetime(str(x)))

        newstore_refstore_dict = dict(zip(reference_store_df['STORE'], reference_store_df['STORE_REF']))
        newstore_opendate_dict = dict(zip(reference_store_df['STORE'], reference_store_df['OPEN_DATE']))
        # 参照店舗があれば、新店として扱う
        if tenpo_cd in newstore_refstore_dict:
            tenpo_cd_ref = newstore_refstore_dict[tenpo_cd]
            logger.info(f"参照店舗: {tenpo_cd_ref}")       


            if tenpo_cd_ref:
                term = '62' if output_6wk_2sales else '6'  # Determine  term string
                path_tran_ref = f"{stage1_result_path}{today}-{term}/{tenpo_cd_ref}/{{}}_{{}}_time_series.csv" # use f-strings + define path
     
    this_tenpo_theme_md_prdcd_list = []            
    if THEME_MD_MODE:    
        theme_md_prdcd_list = []
        another_tenpo_theme_md_prdcd_list = []
        path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-allprd/'+str(tenpo_cd)+"/{}_{}_time_series.csv"
        # 新店であれば、参照店のパスを設定しなおす
        if tenpo_cd_ref is not None:
            path_tran = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-allprd/'+str(tenpo_cd_ref)+"/{}_{}_time_series.csv"

    logger.info(f"path_tran: {path_tran}")
    logger.info(f"path_tran_ref: {path_tran_ref}")
    
    ####################func created 1
    sales_df = process_sales_data(dpt_list, tenpo_cd, tenpo_cd_ref, path_tran, bucket_name, THEME_MD_MODE, add_ec_salesamount, restrict_minmax, restrinct_tenpo_hacchu_end, my_date)
    
    ###################func call
    sales_df = interpolate_df(sales_df, df_calendar, add_ec_salesamount)
    
    ####################func created 2
    sales_df = interpolate_one_year_sales(sales_df, df_calendar, interpolate_1yearsales, exclude_salesless1year)
    
    df_odas_calender = pd.DataFrame()

    if tenpo_cd_ref is None:
        df_odas_calender = odas_correct(df_calendar, tenpo_cd, use_jan_connect=use_jan_connect)
    else:
        df_odas_calender = odas_correct(df_calendar, tenpo_cd_ref, use_jan_connect=use_jan_connect)
        df_odas_calender['TENPO_CD'] = tenpo_cd
    
    ####################func created 3
    sales_df = correct_odas_impact(sales_df, df_odas_calender, odas_imprvmnt, odas_hosei_under_threshold)

    
    sales_df = sales_df.drop('odas_amount', axis=1)


    # ここでodas補正後のdrop_duplicatesで帳尻合わせしているが・・・(上のgroupbyで対処)
    sales_df = sales_df.drop_duplicates().reset_index(drop=True)
    
    if class_wave_add:
        sales_df['URI_SU_CLASS'] = sales_df.groupby(["cls_cd", "nenshudo"], as_index=False)['URI_SU'].transform(lambda x: x.sum())
    if class_wave_mean_add:
        sales_df['URI_SU_CLASS8ema'] = sales_df.groupby("PRD_CD", as_index=False)['URI_SU_CLASS'].transform(lambda x:       x.ewm(span=8).mean())
        
    ####################func created 4 
    sales_df = process_sales_data1(sales_df, interporate_by_class_wave, dfc_tmp)

    
    if logarithmize_target_variable:
        logger.info("start logarithmize_target_variable")
        sales_df['URI_SU'] = np.log1p(sales_df['URI_SU'])
        logger.info("end logarithmize_target_variable")
    
    ######################func created 5
    train_tmp = process_sales_data_internal(sales_df, df_calendar, add_ec_salesamount, class_wave_add,
                                        class_wave_mean_add, THEME_MD_MODE, bakugai_hosei, df_zen_calendar)
    
    tmp_df_calendar = copy.deepcopy(df_calendar)
    tmp_df_calendar['week_from_ymd'] = pd.to_datetime(tmp_df_calendar['week_from_ymd'], format='%Y%m%d')
    tmp_df_calendar['week_to_ymd'] = pd.to_datetime(tmp_df_calendar['week_to_ymd'], format='%Y%m%d')
    tenkyu_master = load_tenkyu_master(tenpo_cd, tmp_df_calendar)

    tenkyu_master = pd.merge(tenkyu_master, df_calendar[['nenshudo', 'week_from_ymd']].rename(columns={'week_from_ymd':'週開始日付'}), how='left')


    #店舗改装期間の補正
    train_tmp = pd.merge(train_tmp, tenkyu_master[['nenshudo', '週開始日付', '店休日数']], on=['週開始日付'], how='left')


    nenshudo_list = train_tmp.loc[train_tmp['店休日数'] > 1]['nenshudo'].unique().tolist()
    remodeling_nenshudo_list = []
    for nenshudo in nenshudo_list:
        remodeling_nenshudo_list.append(nenshudo)
    for i in range(1, 5):
        remodeling_nenshudo_list.append(nenshudo + i)
        remodeling_nenshudo_list.append(nenshudo - i)
    train_tmp.loc[train_tmp['nenshudo'].isin(remodeling_nenshudo_list), 
       "補正対象Flag"] = 1

    window_size = 13
    train_tmp["売上実績数量（3か月平均）"] = train_tmp.groupby('商品コード')["売上実績数量"].transform(lambda x:x.shift(1).rolling(window_size, min_periods=1).mean())

    # 対象範囲を補正
    train_tmp.loc[(train_tmp["補正対象Flag"]==1) & (~train_tmp["売上実績数量（3か月平均）"].isnull()), "売上実績数量"] = train_tmp["売上実績数量（3か月平均）"]
        
    train_tmp = train_tmp.drop(['補正対象Flag', '売上実績数量（3か月平均）'], axis=1)
    train_tmp.loc[train_tmp['店休日数'] > 1, '店休日数'] = 0

        
    train_tmp['8週平均ema'] = train_tmp.groupby("商品コード",as_index=False)['売上実績数量'].transform(lambda x: x.ewm(span=8).mean())
    train_tmp = train_tmp.drop('週開始日付tmp', axis=1)

    df_merged = train_tmp
    
    if '前年週開始日付' not in df_merged.columns.tolist(): # 爆買い補正をoffにすると前年週開始日付が残った状態になっているのでifだけを追加
        df_merged = pd.merge(df_merged,df_zen_calendar,on='週開始日付')
    
    ###################func created 6
    df_vx_train, train_start_nenshudo, train_start_week_from_ymd = process_data(df_merged, add_ec_salesamount, class_wave_add, class_wave_mean_add, THEME_MD_MODE, get_last_year_sales_amount, get_last_year_sales_amount_left, turn_back_time, turn_back_yyyymmdd, dfc_tmp, df_calendar,today_nenshudo, tenpo_cd)
    
    df_vx_train['training_weight'] = 7000
    
    ###################func created 7 
    df_vx_train = process_sales_data2(salesup_flag, tenpo_cd, df_vx_train, df_calendar)
    
    ###################func created 8
    df_vx_train = process_data1(df_merged, add_ec_salesamount, class_wave_add, class_wave_mean_add, THEME_MD_MODE,
                   get_last_year_sales_amount, get_last_year_sales_amount_left, turn_back_time,
                   turn_back_yyyymmdd, dfc_tmp, df_calendar, today_nenshudo, tenpo_cd,
                   no_sales_term_weight_zero, last_month_weight_larger, add_split_column, df_vx_train, train_start_nenshudo)
    
    ##################func created 9
    df_vx_train = process_training_data(df_vx_train, devide_season_items, THEME_MD_MODE, this_tenpo_theme_md_prdcd_list, today, OUTPUT_TABLE_SUFFIX, BigqueryClient)

    logger.info("Data processing complete.")
    
    ###############func created 10
    process_and_upload_data(df_vx_train, today, OUTPUT_TABLE_SUFFIX, divide_by_salesamount_v2, divide_by_salesamount_v3, add_reference_store_unitedmodel, tenpo_cd_ref)

    
    
if __name__ == "__main__":
    main()  

    
    
