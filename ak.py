
import pytest
from io import StringIO
import sys
import os
import pytest
from unittest.mock import patch, mock_open, MagicMock
from datetime import datetime, timedelta
import pandas as pd
import yaml

patcher = patch("google.cloud.storage.Client", return_value=MagicMock())
patcher.start()
# Ensure relative imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Import application code AFTER patching storage.Client
from main import (
   get_chanceloss_data, create_case_pack_bara_groups, create_prdcd_hattyujan_df, calculate_prdcd_hcjan_coefficients, main
)
from repos.cainz_demand_forecast.cainz.common import common
from repos.cainz_demand_forecast.cainz.short_term import short_term_preprocess_common
@pytest.fixture
def sample_bigquery_result_df():
   return pd.DataFrame({
       'BUMON_CD': ['064', '064', '032'],
       'TENPO_CD': ['0760', '0760', '0760'],
       'PRD_NO': ['12000010007863', '15999964000042', '09000010246780'],
       'NENSHUDO': [202530, 202530, 202530],
       'PRD_CD': [4901777232310, 4901777235410, 4961010486054],
       'KEPPIN_CNT': [1, 3, 4],
       'CHANCE_LOSS_PRD_SU': [1.0, 1.0, 0.0],
       'CHANCE_LOSS_KN': [132.0, 356.0, 0.0],
   })
@patch('main.bigquery.Client')
@patch('main.bigquery_storage_v1.BigQueryReadClient')
def test_get_chanceloss_data(mock_bq_storage_client, mock_bq_client, sample_bigquery_result_df):
   mock_query = MagicMock()
   mock_query.result.return_value.to_dataframe.return_value = sample_bigquery_result_df
   mock_bq_client_instance = mock_bq_client.return_value
   mock_bq_client_instance.query.return_value = mock_query
   df_result = get_chanceloss_data('dummy_project', 'dev_cainz_nssol', 'T_090_PRD_CHANCE_LOSS_NB_DPT', 760)
   expected_query = "  SELECT * FROM dev_cainz_nssol.T_090_PRD_CHANCE_LOSS_NB_DPT where TENPO_CD = '0760'"
   mock_bq_client_instance.query.assert_called_with(expected_query)
   assert not df_result.empty
   assert set(['PRD_CD', 'NENSHUDO', 'TENPO_CD', 'CHANCE_LOSS_PRD_SU', 'CHANCE_LOSS_KN']).issubset(df_result.columns)
   assert df_result['CHANCE_LOSS_PRD_SU'].dtype == float
   assert df_result['CHANCE_LOSS_KN'].dtype == float
   zero_sl_rows = df_result['CHANCE_LOSS_PRD_SU'].astype(int) == 0
   assert all(df_result.loc[zero_sl_rows, 'CHANCE_LOSS_KN'] == 0.0)

def test_create_case_pack_bara_groups_large_sample():
   prd_asc = pd.DataFrame({
      'prd_cd': [47478640, 41570112366, 41570112380, 71990095116, 71990095123],
      'dpt': [77, 64, 64, 77, 77],
      'prd_nm_kj': [
          "＜長野＞Ｃよなよなエール　ビール　３５０缶×２４",
          "Ｃアーモンドブリーズオリジナル１Ｌ×６本",
          "Ｃアーモンドブリーズ砂糖不使用１Ｌ×６本",
          "ブルームーン　３５５ｍｌ×６",
          "Ｃブルームーン　３５５ｍｌ×２４"
      ],
      'baika_toitsu': [6580, 1750, 1750, 1968, 7850],
      'hacchu_tani_toitsu_kosu': [1, 1, 1, 4, 1],
      'daihyo_torihikisaki_cd': [926221, 762148, 762148, 987956, 926221],
      'daihyo_torihikisaki_nm': [
          '日本酒類販売（株）流通第三本部営業二部',
          'マルサンアイ（株）　関信越支店',
          'マルサンアイ（株）　関信越支店',
          'ＰＯＳ　ＰＬＵ登録',
          '日本酒類販売（株）流通第三本部営業二部'
      ],
      'asc_riyu_cd': [3, 3, 3, 3, 3],
      'iri_su': [24, 6, 6, 6, 4],
      'asc_prd_cd': [47478619, 41570112359, 41570112373, 4902335060017, 71990095116],
      'asc_dpt': [77, 64, 64, 77, 77],
      'asc_prd_nm_kj': [
          'よなよなエール　３５０ｍｌ',
          'アーモンドブリーズオリジナル１Ｌ',
          'アーモンドブリーズ砂糖不使用１Ｌ',
          'ブルームーン　３５５ｍｌ',
          'ブルームーン　３５５ｍｌ×６'
      ],
      'asc_baika_toitsu': [278, 298, 298, 328, 1968],
      'asc_hacchu_tani_toitsu_kosu': [24, 6, 6, 24, 4],
      'asc_daihyo_torihikisaki_cd': [987956, 987956, 987956, 987956, 987956],
      'asc_daihyo_torihikisaki_nm': [
          'ＰＯＳ　ＰＬＵ登録', 'ＰＯＳ　ＰＬＵ登録', 'ＰＯＳ　ＰＬＵ登録', 'ＰＯＳ　ＰＬＵ登録', 'ＰＯＳ　ＰＬＵ登録'
      ]
   })
   prd_asc_tmp = pd.DataFrame({
      'PRD_CD': [47478640, 41570112366, 41570112380, 71990095116, 71990095123],
      'ASC_PRD_CD': [47478619, 41570112359, 41570112373, 4902335060017, 71990095116]
   })
   groups = create_case_pack_bara_groups(prd_asc, prd_asc_tmp)
   group_sets = [set(g) for g in groups]
   assert any({47478640, 47478619}.issubset(g) for g in group_sets)
   assert any({41570112366, 41570112359}.issubset(g) for g in group_sets)
   assert any({41570112380, 41570112373}.issubset(g) for g in group_sets)
   assert any({71990095116, 4902335060017, 71990095123}.issubset(g) for g in group_sets)
    
def test_create_prdcd_hattyujan_df_basic():
   prd_asc = pd.DataFrame({
      'prd_cd': [47478640, 41570112366, 41570112380, 71990095116, 71990095123],
      'asc_riyu_cd': [3, 3, 3, 3, 3],
      'asc_prd_cd': [47478619, 41570112359, 41570112373, 4902335060017, 71990095116],
      'daihyo_torihikisaki_nm': [
          '日本酒類販売（株）流通第三本部営業二部',
          'マルサンアイ（株）　関信越支店',
          'マルサンアイ（株）　関信越支店',
          'ＰＯＳ　ＰＬＵ登録',
          '日本酒類販売（株）流通第三本部営業二部'
      ],
      'asc_daihyo_torihikisaki_nm': [
          '日本酒類販売（株）流通第三本部営業二部',
          'ＰＯＳ　ＰＬＵ登録',
          'マルサンアイ（株）　関信越支店',
          'ＰＯＳ　ＰＬＵ登録',
          '日本酒類販売（株）流通第三本部営業二部'
      ]
   })
   groups = [
      {47478640, 47478619},
      {41570112366, 41570112359},
      {41570112380, 41570112373},
      {4902335060017, 71990095123, 71990095116}
   ]
   df_result = create_prdcd_hattyujan_df(prd_asc, groups)
   assert set(['PRD_CD', 'HACCHU_JAN']).issubset(df_result.columns)
   assert len(df_result) > 0
   assert all(df_result['PRD_CD'] != df_result['HACCHU_JAN'])
    
def test_calculate_prdcd_hcjan_coefficients(monkeypatch):
   prd_asc = pd.DataFrame({
      'prd_cd': [47478619, 41570112359, 41570112373, 4902335060017, 71990095116],
      'asc_riyu_cd': [3, 3, 3, 3, 3],
      'asc_prd_cd': [47478640, 41570112366, 41570112380, 71990095123, 71990095123],
      'iri_su': [24, 6, 6, 6, 4],
      'hacchu_tani_toitsu_kosu': [24, 6, 6, 24, 4]
   })
   prdcd_hattyujan_df = pd.DataFrame({
      'PRD_CD': [47478619, 41570112359, 41570112373, 4902335060017, 71990095116],
      'HACCHU_JAN': [47478640, 41570112366, 41570112380, 71990095123, 71990095123]
   })
   def fake_exit():
       raise Exception("Exit called")
   monkeypatch.setattr(sys, "exit", fake_exit)
   coeff_dict, coeff_log = calculate_prdcd_hcjan_coefficients(prd_asc, prdcd_hattyujan_df)
   assert isinstance(coeff_dict, dict)
   assert isinstance(coeff_log, list)
   for key in prdcd_hattyujan_df['PRD_CD']:
       assert key in coeff_dict or key not in prd_asc['prd_cd'].values
   for log_entry in coeff_log:
       assert isinstance(log_entry, list)
       assert len(log_entry) >= 7
      
@patch('main.storage.Client')
def test_extract_as_df(mock_storage_client):
   expected_df = pd.DataFrame({
      'BUMON_CD': [84, 84, 84, 84, 84],
      'PRD_CD': [4549509192435, 4511413407363, 4511413404164, 4549509190035, 4511413404386],
      'TENPO_CD': [760, 760, 760, 760, 760],
      'NENSHUDO': [202530, 202530, 202530, 202530, 202530],
      'BAIKA': [98, 358, 398, 108, 438],
      'URI_SU': [1, 1, 1, 4, 1],
      'URI_KIN': [90, 332, 369, 400, 406]
   })
   csv_io = StringIO(expected_df.to_csv(index=False))
   mock_bucket = MagicMock()
   mock_blob = MagicMock()
   mock_blob.open.return_value.__enter__.return_value = csv_io
   mock_bucket.blob.return_value = mock_blob
   mock_storage_client.return_value.bucket.return_value = mock_bucket
   df = common.extract_as_df('dummy_path.csv', 'dev-cainz-demandforecast', encoding='utf-8', usecols=None)
   pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_df.reset_index(drop=True))


@patch('main.storage.Client')
def test_upload_timeseries_df(mock_storage_client):
   product_info_df = pd.DataFrame({
      'PRD_CD': [1, 2, 3],
      'missing_ratio': [0.1, 0.3, 0.05],
      'term': [200, 100, 220],
      'missing_ratio_13': [0.4, 0.6, 0.2],
      'URISU_AVE13WK': [3, 1, 5]
   })
   unique_tenpo_df = pd.DataFrame({
      'PRD_CD': [1, 2, 3, 4],
      'TENPO_CD': [760, 760, 760, 760],
      'cls_cd': [10, 20, 10, 20],
      'line_cd': [100, 200, 100, 200],
      'hnmk_cd': [1000, 2000, 1000, 2000],
      'baika_toitsu': [1, 2, 1, 2],
      'low_price_kbn': [0, 1, 0, 1],
      'nenshudo': [202501, 202502, 202503, 202504],
      'URI_SU': [10, 20, 30, 40],
      'URI_KIN': [100, 200, 300, 400],
      'BAIKA': [1000, 2000, 3000, 4000],
      'prd_nm_kj': ['prod1', 'prod2', 'prod3', 'prod4'],
      'sell_start_ymd': [20200101, 20200201, 20200301, 20200401]
   })
   tenpo_df = pd.DataFrame({
      'TENPO_CD': [760],
      'CHUSHA_KANO_DAISU': [5],
      'OKUNAI_URIBA_MENSEKI': [50],
      'OKUGAI_URIBA_MENSEKI': [25],
      'SHIKICHI_MENSEKI': [20]
   })
   dpt = 69
   tenpo = 760
   threshold_missing_ratio = 0.2
   threshold_timeseries_length = 180
   path_upload_tmp_local = "/tmp/test_timeseries.csv"
   path_upload_time_series_blob = "path/to/blob/{}_{}.csv"
   path_upload_time_series_blob2 = "path/to/blob2/{}_{}.csv"
   bucket_name = "dummy_bucket"
   output_6wk_2sales = True
   mock_bucket = MagicMock()
   mock_blob = MagicMock()
   mock_bucket.blob.return_value = mock_blob
   mock_storage_client.return_value.bucket.return_value = mock_bucket
   time_series_prod_cd, time_series_df = short_term_preprocess_common.upload_timeseries_df(
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
   assert all(product_info_df.loc[product_info_df['PRD_CD'].isin(time_series_prod_cd), 'missing_ratio'] <= threshold_missing_ratio)
   assert all(product_info_df.loc[product_info_df['PRD_CD'].isin(time_series_prod_cd), 'term'] >= threshold_timeseries_length)
   expected_columns = [
      'PRD_CD', 'TENPO_CD', 'cls_cd', 'line_cd', 'hnmk_cd', 'baika_toitsu',
      'low_price_kbn', 'nenshudo', 'URI_SU','URI_KIN','BAIKA',
      'prd_nm_kj', 'sell_start_ymd',
      'CHUSHA_KANO_DAISU', 'OKUNAI_URIBA_MENSEKI', 'OKUGAI_URIBA_MENSEKI', 'SHIKICHI_MENSEKI'
   ]
   for col in expected_columns:
       assert col in time_series_df.columns
   assert mock_blob.upload_from_filename.call_count == 2

import main

@pytest.fixture
def mock_env_vars(monkeypatch):
   monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "0")
   monkeypatch.setenv("CLOUD_RUN_TASK_COUNT", "1")
   monkeypatch.setenv("TODAY_OFFSET", "0")
   monkeypatch.setenv("EXECUTE_STAGE2", "0")
   monkeypatch.setenv("OUTPUT_HACCHUJAN_INFO", "0")
   monkeypatch.setenv("OUTPUT_HACCHUJAN_TABLE_TEST", "0")
   monkeypatch.setenv("OUTPUT_HACCHUJAN_INFO_GCS", "1")
   monkeypatch.setenv("THEME_MD_MODE", "0")
@pytest.fixture
def mock_config():
   return {
       'path_week_master': 'dummy_path_week_master.csv',
       'start_holdout_nenshudo': 202201,
       'start_nenshudo': 202201
   }
@pytest.fixture
def mock_product_master_df():
   #today = datetime(2025, 10, 3)
   today = datetime.now()
   week_from_ymd = (today - timedelta(days=2)).strftime('%Y-%m-%d')
   week_to_ymd = (today + timedelta(days=2)).strftime('%Y-%m-%d')
   return pd.DataFrame({
       'prd_cd': [47478640],
       'dpt': [77],
       'prdnmkj': ['Prod A'],
       'baikatoitsu': [6580],
       'hacchutanitoitsukosu': [1],
       'daihyo_torihikisaki_cd': [926221],
       'daihyo_torihikisaki_nm': ['Comp A'],
       'asc_riyu_cd': [3],
       'asc_prd_cd': [47478619],
       'asc_daihyo_torihikisaki_cd': [987956],
       'asc_daihyo_torihikisaki_nm': ['POS PLU'],
       'iri_su': [24],
       'hacchu_tani_toitsu_kosu': [24],
       'baika': [98],
       'uri_su': [1],
       'uri_kin': [90],
       'tenpo_cd': [760],
       'nenshudo': [202530],
       'week_from_ymd': [week_from_ymd],
       'week_to_ymd': [week_to_ymd],
   })
@pytest.fixture
def mock_chance_loss_df():
   return pd.DataFrame({
       'PRD_CD': [47478640],
       'NENSHUDO': [202530],
       'TENPO_CD': [760],
       'CHANCE_LOSS_PRD_SU': [1.0],
       'CHANCE_LOSS_KN': [132.0],
   })
@pytest.fixture
def mock_jan_master_df():
   return pd.DataFrame({
       'PRD_CD': [47478640],
       'JAN_CODE': ['1234567890123'],
   })
@pytest.fixture
def mock_store_prd_hacchu_ymd_df():
   return pd.DataFrame({
       'TENPO_CD': [760],
       'PRD_CD': [47478640],
       'HACCHU_YMD': [20250930],
   })
@pytest.fixture
def mock_dfm_base_df():
   return pd.DataFrame({
       'prd_cd': [47478640],
       'TENPO_CD': [760],
       'NENSHUDO': [202530],
   })
@pytest.fixture
def mock_process_data_for_tenpo_df():
   return pd.DataFrame({'dummy_col': [1]})
def test_main_full_flow(
   mock_env_vars, mock_config, mock_product_master_df, mock_chance_loss_df,
   mock_jan_master_df, mock_store_prd_hacchu_ymd_df, mock_dfm_base_df,
   mock_process_data_for_tenpo_df,
):
   with patch('builtins.open', mock_open(read_data="dummy")), \
        patch('yaml.safe_load', return_value=mock_config), \
        patch('main.storage.Client') as mock_storage_client, \
        patch('main.bigquery.Client') as mock_bq_client, \
        patch('main.common.extract_as_df') as mock_extract_as_df, \
        patch('main.process_data') as mock_process_data, \
        patch('main.process_data_for_tenpo') as mock_process_data_for_tenpo, \
        patch('main.output_hacchujan_info') as mock_output_hacchujan_info, \
        patch('main.create_case_pack_bara_groups') as mock_create_case_pack_bara_groups, \
        patch('main.create_prdcd_hattyujan_df') as mock_create_prdcd_hattyujan_df, \
        patch('main.calculate_prdcd_hcjan_coefficients') as mock_calculate_prdcd_hcjan_coefficients, \
        patch('main.check_and_trigger') as mock_check_and_trigger, \
        patch('main.load_product_master_data') as mock_load_product_master_data:
       mock_extract_as_df.return_value = mock_product_master_df
       mock_load_product_master_data.return_value = mock_product_master_df
       mock_create_case_pack_bara_groups.return_value = [{47478640, 47478619}]
       mock_create_prdcd_hattyujan_df.return_value = mock_product_master_df
       mock_calculate_prdcd_hcjan_coefficients.return_value = ({47478640: 1.0}, [['log']])
       mock_output_hacchujan_info.return_value = mock_product_master_df
       mock_process_data.return_value = (
           mock_chance_loss_df,
           mock_jan_master_df,
           mock_store_prd_hacchu_ymd_df,
           mock_dfm_base_df,
       )
       mock_process_data_for_tenpo.return_value = mock_process_data_for_tenpo_df
       mock_bq_client_instance = mock_bq_client.return_value
       mock_query = MagicMock()
       mock_query.result.return_value.to_dataframe.return_value = mock_product_master_df
       mock_bq_client_instance.query.return_value = mock_query
       mock_bucket = MagicMock()
       mock_storage_client.return_value.bucket.return_value = mock_bucket
       mock_blob = MagicMock()
       mock_bucket.list_blobs.return_value = [mock_blob]
       mock_blob.delete.return_value = None
       mock_check_and_trigger.return_value = None
       main.main()
       mock_extract_as_df.assert_called()
       mock_load_product_master_data.assert_called()
       mock_create_case_pack_bara_groups.assert_called()
       mock_create_prdcd_hattyujan_df.assert_called()
       mock_calculate_prdcd_hcjan_coefficients.assert_called()
       mock_output_hacchujan_info.assert_called()
       mock_process_data_for_tenpo.assert_called()
       mock_check_and_trigger.assert_called()
       mock_process_data.assert_called()

def teardown_module(module):
    patcher.stop()
   
