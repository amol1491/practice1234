def main():
    tenpo_cd_ref = None
    path_tran_ref = None 
    # OUTPUT_TABLE_SUFFIX = os.environ.get("OUTPUT_TABLE_SUFFIX", "")
    # tenpo_cd = tenpo_cd_list[TASK_INDEX]
    logger.info(f'TASK_INDEX: {TASK_INDEX}, tenpo_cd: {tenpo_cd}')                
    dfc = common.extract_as_df(path_week_master, bucket_name)
    #dfc = extract_as_df(path_week_master)
    print("############dfc##########", dfc)
    df_calendar = extract_as_df_with_encoding("Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv", "utf-8")
    print("#########df_calendar######", df_calendar)
    #tenpo_cd = sys.argv[1]
    # add 20230614********************************************
    dfc_tmp = df_calendar[["nenshudo", "week_from_ymd", "week_to_ymd"]]
    print("############dfc_tmp#######", dfc_tmp)
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




    path_reference_store = "Basic_Analysis_unzip_result/01_Data/37_reference_store/reference_store.csv"
    stage1_result_path = "01_short_term/01_stage1_result/01_weekly/"
    if add_reference_store:
        # 参照店舗の紐づけ情報を読み込む
        path_reference_store = "Basic_Analysis_unzip_result/01_Data/37_reference_store/reference_store.csv"
        reference_store_df = extract_as_df(path_reference_store)
        print("###################reference_store_df#########", reference_store_df)
        reference_store_df["OPEN_DATE"] = reference_store_df["OPEN_DATE"].apply(lambda x : pd.to_datetime(str(x)))
        reference_store_df["OPEN_DATE_REF"] = reference_store_df["OPEN_DATE_REF"].apply(lambda x : pd.to_datetime(str(x)))

        newstore_refstore_dict = dict(zip(reference_store_df['STORE'], reference_store_df['STORE_REF']))
        newstore_opendate_dict = dict(zip(reference_store_df['STORE'], reference_store_df['OPEN_DATE']))
        print("##############newstore_refstore_dict################", newstore_refstore_dict)
        # 参照店舗の有無をチェックして、あればpath_tran_refを設定する
        if tenpo_cd in newstore_refstore_dict:
            tenpo_cd_ref = newstore_refstore_dict[tenpo_cd]
           
            if output_6wk_2sales:
                path_tran_ref = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-62/'+str(tenpo_cd_ref)+"/{}_{}_time_series.csv"
            else:
                path_tran_ref = "01_short_term/01_stage1_result/01_weekly/"+str(today)+'-6/'+str(tenpo_cd_ref)+"/{}_{}_time_series.csv"



        
        
        
    ##################function 1
    sales_df, this_tenpo_theme_md_prdcd_list = process_sales_data(tenpo_cd, tenpo_cd_ref, path_tran)
    
    print("############sales_df############", sales_df)          
    if(len(sales_df) <= 0):
        logger.info("=====There is no sales data, please check: stage1 has been executed======")
        sys.exit(1)

    sales_df = sales_df[['PRD_CD', 'nenshudo', 'URI_SU','TENPO_CD', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    logger.info(f"sales_df SKU: {len(sales_df['PRD_CD'].unique())}")
    
                
                
    ############################Function 2  
    sales_df = extract_ec_sales_data(sales_df, tenpo_cd)                
        
                
    ############################Function 3                
    sales_df, nominmax_prdcd_df, minmax0_prdcd_df, hacchu_teishi_prdcd_df2 = apply_minmax_restrictions(
        sales_df, tenpo_cd, restrict_minmax, restrinct_tenpo_hacchu_end)
                
    print("###########sales_df#########", sales_df)
    train_df = sales_df
    df_calendar_tmp_sales = df_calendar[["nenshudo","week_from_ymd"]]
        
                
    ############################Function 4     
    train_end_nenshudo, target_nenshudo, start_nenshudo, end_nenshudo = calculate_nenshudo_values(today_nenshudo, max_syudo_dic)
                
    ############################Function 5
    train_df, df_calendar_expand = process_train_data(train_df, df_calendar, df_calendar_tmp_sales, today_nenshudo, train_end_nenshudo, target_nenshudo, end_nenshudo, max_syudo_dic)

                
    ############################Function 6
    metrics_result = calculate_sales_metrics(today_nenshudo, dfc_tmp, train_df)
    print("################train_df########", train_df)  
    print("################df_calendar_expand########", df_calendar_expand)
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
        train_df = train_df[['商品コード','週開始日付','店舗コード','売上実績数量', '売上実績数量EC', '年週度', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]
    else:
        train_df = train_df[['商品コード','週開始日付','店舗コード','売上実績数量','年週度', 'baika_toitsu', 'BAIKA', 'DPT', 'line_cd', 'cls_cd', 'hnmk_cd']]

    target_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == target_nenshudo].values[0]
    start_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == start_nenshudo].values[0]
    context_end_nenshudo = target_nenshudo
    context_end_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == context_end_nenshudo].values[0]
    end_week_from_ymd = df_calendar["week_from_ymd"][df_calendar["nenshudo"] == end_nenshudo].values[0]


    if output_collected_sales_value == False:
        train_df = train_df[train_df['週開始日付'] >= start_week_from_ymd].reset_index(drop=True)

    train_df = train_df.sort_values('年週度').reset_index(drop=True)

    dftarget = train_df
    dftarget['週開始日付_予測対象'] = dftarget['週開始日付']
    dftarget2 = dftarget[['商品コード','週開始日付_予測対象', '店舗コード', 'baika_toitsu', 'BAIKA', 'DPT',  'line_cd', 'cls_cd', 'hnmk_cd']]



    if output_collected_sales_value == False:
        dftarget2 = dftarget2[dftarget2['週開始日付_予測対象'] >= start_week_from_ymd].reset_index(drop=True)

    df_calendar = extract_as_df_with_encoding("Basic_Analysis_utf8/01_Data/10_週番マスタ/10_週番マスタ.csv" ,"utf-8")

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

    logger.info("==============週開始日付_予測対象================")

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
    
    ############################Function 7
    sales_df = optimize_odas_improvement(sales_df)    
    print("#########sales_df = optimize_odas_improvement(sales_df)###########", sales_df)
    sales_df = sales_df.drop('odas_amount', axis=1)
    sales_df = sales_df.drop_duplicates().reset_index(drop=True)
    if class_wave_add:
        sales_df['URI_SU_CLASS'] = sales_df.groupby(["cls_cd", "nenshudo"], as_index=False)['URI_SU'].transform(lambda x: x.sum())
        if class_wave_mean_add:
            sales_df['URI_SU_CLASS8ema'] = sales_df.groupby("PRD_CD", as_index=False)['URI_SU_CLASS'].transform(lambda x: x.ewm(span=8).mean())
                
                
    ############################Function 8
    metrics_result = calculate_and_update_metrics(sales_df, today_nenshudo, dfc_tmp, tenpo_cd, dfc, metrics_result)
    
    if logarithmize_target_variable:
        # print('start logarithmize_target_variable')
        logger.info('start logarithmize_target_variable')
        sales_df['URI_SU'] = np.log1p(sales_df['URI_SU'])
        # print('end logarithmize_target_variable')
        logger.info('end logarithmize_target_variable')

    # 20230823 上からこちらに移動（補正後の値をとっておくため）
    # ここで、URI_SUをセーブして、あとで使ってる　******************************************************
    sales_df_saved = sales_df
    # ***************************************************************************************************
    # 売り数も直近まで入っているが、売り数０のレコードは無い


                
    ############################Function 9
    sales_df_saved, dftarget3 = process_sales_data2(sales_df, df_calendar, dftarget2, tenpo_cd, sales_df_saved)
                
                
    ############################Function 10
    df_vx_test = prepare_vx_test_data(sales_df_saved, tenpo_cd)
                
    ############################Function 11
    df_vx_test, kikaku_master, list_price_yoyaku, longs_df = process_kakaku_jizen_kichi_data(df_vx_test, tenpo_cd, target_week_from_ymd)
                
    ############################Function 12
    upload_collected_sales_data(df_vx_test, today_nenshudo, df_calendar, max_syudo_dic)
                
    print("################df_vx_test############", df_vx_test)          
    ############################
    if salesup_flag:    
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


        flag_col_name_list = ['BusyPeriodFlagNenmatsu','BusyPeriodFlagGw','BusyPeriodFlagNewLife','BusyPeriodFlagEndRainySsn',
                          'BusyPeriodFlagObon']

        for my_flag_col_name in flag_col_name_list:
            df_vx_test[my_flag_col_name] = 0
            for my_salesup_table in salesup_train_table_name_list:
                my_salesup_prdcd_list = get_salesup_prdcd_list(my_salesup_table, my_flag_col_name, tenpo_cd)
                df_vx_test[my_flag_col_name][df_vx_test['PrdCd'].isin(my_salesup_prdcd_list)] = 1
                

    ############################Function 13
    df_vx_test = process_and_upload_seasonal_data(df_vx_test, tenpo_cd, start_week_from_ymd, end_week_from_ymd, target_week_from_ymd, OUTPUT_TABLE_SUFFIX)
                    
    ############################Function 14
    df_vx_test, prediction_table_name_small, prediction_table_name_large = process_sales_data_division(df_vx_test, start_week_from_ymd, end_week_from_ymd, target_week_from_ymd, OUTPUT_TABLE_SUFFIX, this_tenpo_theme_md_prdcd_list, tenpo_cd)
    
    ############################Function 15
    manage_cloud_function_trigger(tenpo_cd, TASK_COUNT, prediction_table_name_large, prediction_table_name_small)






















2025-10-24 13:57:30,780 - INFO - Deleting files under stage2 completion check folder...
2025-10-24 13:57:30,841 - INFO - ******************* today 2025-10-24
2025-10-24 13:57:30,842 - INFO - ******************* today_date_str 2025-10-24
2025-10-24 13:57:30,843 - INFO - ******************* my_date 20251024
2025-10-24 13:57:30,846 - INFO - TASK INDEX: 0
2025-10-24 13:57:30,846 - INFO - TASK COUNT: 1
2025-10-24 13:57:30,847 - INFO - TODAY OFFSET: 0
2025-10-24 13:57:30,847 - INFO - OUTPUT_TABLE_SUFFIX: 
2025-10-24 13:57:30,848 - INFO - CALL NEXT_PIPELINE: 1
2025-10-24 13:57:30,848 - INFO - OUTPUT_METRICS_VALUE: 0
2025-10-24 13:57:30,849 - INFO - SEASONAL_TRAINDATA_TABLE: 
2025-10-24 13:57:30,849 - INFO - TURN_BACK_YYYYMMDD: 
2025-10-24 13:57:30,849 - INFO - THEME_MD_MODE: 0
2025-10-24 13:57:30,850 - INFO - TASK_INDEX: 0, tenpo_cd: 760
INFO: Pandarallel will run on 16 workers.
INFO: Pandarallel will use Memory file system to transfer data between the main process and workers.
############################ ['/home/jupyter/Refactored_Files/cloudrun/stage23_weekly_test_20250416_218str_salesupflag_narrow_criteria', '/opt/conda/lib/python310.zip', '/opt/conda/lib/python3.10', '/opt/conda/lib/python3.10/lib-dynload', '', '/opt/conda/lib/python3.10/site-packages', '/opt/conda/lib/python3.10/site-packages/setuptools/_vendor', '/home/jupyter/Refactored_Files/cloudrun/stage23_weekly_test_20250416_218str_salesupflag_narrow_criteria/repos/cainz_demand_forecast/cainz', '/home/jovyan/work/playground/nagano/lib', 'repos/cainz_demand_forecast/cainz/']
############dfc##########       nenshudo  nendo  shudo  minashi_tsuki  week_from_ymd  week_to_ymd  \
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
#########df_calendar######       nenshudo  nendo  shudo  minashi_tsuki  week_from_ymd  week_to_ymd  \
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
############dfc_tmp#######       nenshudo  week_from_ymd  week_to_ymd
0       200301       20030224     20030302
1       200302       20030303     20030309
2       200303       20030310     20030316
3       200304       20030317     20030323
4       200305       20030324     20030330
...        ...            ...          ...
1456    203048       20310120     20310126
1457    203049       20310127     20310202
1458    203050       20310203     20310209
1459    203051       20310210     20310216
1460    203052       20310217     20310223

[1461 rows x 3 columns]
2025-10-24 13:57:31,322 - INFO - path_tran: 01_short_term/01_stage1_result/01_weekly/2025-10-24-62/760/{}_{}_time_series.csv
###################reference_store_df#########     STORE        STORE_NAME  OPEN_DATE  STORE_REF STORE_NAME_REF  \
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
##############newstore_refstore_dict################ {872: 849, 873: 790, 874: 775, 875: 832, 876: 168, 877: 815, 878: 832, 879: 840, 900: 856, 907: 742, 902: 855, 904: 844, 910: 833, 868: 832, 903: 832, 908: 832, 909: 832, 911: 741, 932: 832, 934: 832, 954: 856}
2025-10-24 13:57:33,683 - INFO - sales_df.shape: (3721, 18)
2025-10-24 13:57:33,694 - INFO - sales_df SKU: 30
2025-10-24 13:57:33,695 - INFO - 
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
                FROM `dev_cainz_nssol.shipment_with_store_inventory` 
                WHERE ship_place_code = '0760' 
                ORDER BY PRD_CD, NENDO, SHUDO
            ) 
            GROUP BY PRD_CD, NENDO, SHUDO 
            ORDER BY PRD_CD, NENDO, SHUDO
        
############sales_df############              PRD_CD  TENPO_CD  cls_cd  line_cd      hnmk_cd  baika_toitsu  \
0     4550596138817       760    6922      692  24008460001          1980   
1     4550596138817       760    6922      692  24008460001          1980   
2     4550596138817       760    6922      692  24008460001          1980   
3     4550596138817       760    6922      692  24008460001          1980   
4     4550596138817       760    6922      692  24008460001          1980   
...             ...       ...     ...      ...          ...           ...   
3716  4550596144238       760    9727      972  24011030000          4980   
3717  4550596144238       760    9727      972  24011030000          4980   
3718  4550596144238       760    9727      972  24011030000          4980   
3719  4550596144238       760    9727      972  24011030000          4980   
3720  4550596144238       760    9727      972  24011030000          4980   

      low_price_kbn  nenshudo  URI_SU  URI_KIN   BAIKA  \
0                 0    202513     1.0   1800.0  1980.0   
1                 0    202514     3.0   5400.0  1980.0   
2                 0    202515     6.0  11057.0  1980.0   
3                 0    202516     3.0   5400.0  1800.0   
4                 0    202517     9.0  15429.0  1980.0   
...             ...       ...     ...      ...     ...   
3716              0    202526     7.0  31695.0  4980.0   
3717              0    202527     3.0  13583.0  4980.0   
3718              0    202528     4.0  18112.0  4980.0   
3719              0    202529     3.0  12146.0  4980.0   
3720              0    202530     1.0   4528.0  4980.0   

                      prd_nm_kj  sell_start_ymd  CHUSHA_KANO_DAISU  \
0     ◆すき間を埋めるキッチンマットＬ－Ｆｉｔ透明　ＸＬ        20250524                0.0   
1     ◆すき間を埋めるキッチンマットＬ－Ｆｉｔ透明　ＸＬ        20250524                0.0   
2     ◆すき間を埋めるキッチンマットＬ－Ｆｉｔ透明　ＸＬ        20250524                0.0   
3     ◆すき間を埋めるキッチンマットＬ－Ｆｉｔ透明　ＸＬ        20250524                0.0   
4     ◆すき間を埋めるキッチンマットＬ－Ｆｉｔ透明　ＸＬ        20250524                0.0   
...                         ...             ...                ...   
3716         ◆自動首振ＡＣサーキュレーター　ＧＹ        20250510                0.0   
3717         ◆自動首振ＡＣサーキュレーター　ＧＹ        20250510                0.0   
3718         ◆自動首振ＡＣサーキュレーター　ＧＹ        20250510                0.0   
3719         ◆自動首振ＡＣサーキュレーター　ＧＹ        20250510                0.0   
3720         ◆自動首振ＡＣサーキュレーター　ＧＹ        20250510                0.0   

      OKUNAI_URIBA_MENSEKI  OKUGAI_URIBA_MENSEKI  SHIKICHI_MENSEKI  DPT  
0                  14969.0                   0.0               0.0   69  
1                  14969.0                   0.0               0.0   69  
2                  14969.0                   0.0               0.0   69  
3                  14969.0                   0.0               0.0   69  
4                  14969.0                   0.0               0.0   69  
...                    ...                   ...               ...  ...  
3716               14969.0                   0.0               0.0   97  
3717               14969.0                   0.0               0.0   97  
3718               14969.0                   0.0               0.0   97  
3719               14969.0                   0.0               0.0   97  
3720               14969.0                   0.0               0.0   97  

[3721 rows x 18 columns]
2025-10-24 13:57:57,434 - INFO - minmax_df.shape: (2016994, 15)
2025-10-24 13:58:18,202 - INFO - restrict minmax sales_df SKU: 15
2025-10-24 13:59:06,163 - INFO - exclude store hacchuend sales_df SKU: 15
2025-10-24 13:59:06,454 - INFO - train_end_nenshudo: 202529
2025-10-24 13:59:06,456 - INFO - target_nenshudo: 202530
2025-10-24 13:59:06,456 - INFO - start_nenshudo: 202510
2025-10-24 13:59:06,457 - INFO - end_nenshudo: 202540
###########sales_df#########              PRD_CD  nenshudo  URI_SU  TENPO_CD  baika_toitsu   BAIKA  DPT  \
0     4550596138817    202513     1.0       760          1980  1980.0   69   
1     4550596138817    202514     3.0       760          1980  1980.0   69   
2     4550596138817    202515     6.0       760          1980  1980.0   69   
3     4550596138817    202516     3.0       760          1980  1800.0   69   
4     4550596138817    202517     9.0       760          1980  1980.0   69   
...             ...       ...     ...       ...           ...     ...  ...   
1979  4549509308706    202528     1.0       760           298   246.0   69   
1980  4549509308706    202529     1.0       760           298   258.0   69   
1981  4549509308706    202530     1.0       760           298   271.0   69   
1982  4549509308706    202531     1.0       760           298   143.0   69   
1983  4549509308706    202533     2.0       760           298   298.0   69   

      line_cd  cls_cd      hnmk_cd  URI_SU_EC  
0         692    6922  24008460001        0.0  
1         692    6922  24008460001        0.0  
2         692    6922  24008460001        0.0  
3         692    6922  24008460001        0.0  
4         692    6922  24008460001        0.0  
...       ...     ...          ...        ...  
1979      692    6940  16044580000        0.0  
1980      692    6940  16044580000        0.0  
1981      692    6940  16044580000        0.0  
1982      692    6940  16044580000        0.0  
1983      692    6940  16044580000        0.0  

[1984 rows x 11 columns]
################train_df########          年週度          商品コード  売上実績数量  TENPO_CD  baika_toitsu   BAIKA  DPT  \
0     201701  4549509833536     0.0       0.0        2280.0  1980.0   69   
1     201702  4549509833536     0.0       0.0        2280.0  1980.0   69   
2     201703  4549509833536     0.0       0.0        2280.0  1980.0   69   
3     201704  4549509833536     0.0       0.0        2280.0  1980.0   69   
4     201705  4549509833536     0.0       0.0        2280.0  1980.0   69   
...      ...            ...     ...       ...           ...     ...  ...   
6760  202530  4549509780502     0.0       0.0        1180.0  1180.0   69   
6761  202531  4549509780502     1.0     760.0        1180.0  1180.0   69   
6762  202532  4549509780502     0.0       0.0        1180.0  1180.0   69   
6763  202533  4549509780502     2.0     760.0        1180.0  1180.0   69   
6764  202534  4549509780502     5.0     760.0        1180.0  1180.0   69   

      line_cd  cls_cd      hnmk_cd  売上実績数量EC     週開始日付  
0         692    6941  21005430000       0.0  20170227  
1         692    6941  21005430000       0.0  20170306  
2         692    6941  21005430000       0.0  20170313  
3         692    6941  21005430000       0.0  20170320  
4         692    6941  21005430000       0.0  20170327  
...       ...     ...          ...       ...       ...  
6760      692    6924  20024250000       0.0  20250915  
6761      692    6924  20024250000       0.0  20250922  
6762      692    6924  20024250000       0.0  20250929  
6763      692    6924  20024250000       0.0  20251006  
6764      692    6924  20024250000       0.0  20251013  

[6765 rows x 12 columns]
################df_calendar_expand########    nenshudo  week_from_ymd
0    202535       20251020
1    202536       20251027
2    202537       20251103
3    202538       20251110
4    202539       20251117
5    202540       20251124
2025-10-24 13:59:06,695 - INFO - ==============週開始日付_予測対象================
2025-10-24 13:59:13,469 - INFO - ===df_odas_calender_new===
      TENPO_CD         PRD_CD  odas_amount  sales_ymd  nenshudo
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
2025-10-24 13:59:13,496 - INFO - start odas correction improvements *******************************
2025-10-24 13:59:13,686 - INFO - processing odas correction improvements *******************************
2025-10-24 13:59:13,687 - INFO - odas correction elapsed time: 0.192 seconds
2025-10-24 13:59:13,951 - INFO - end odas Correction improvements *******************************
2025-10-24 13:59:13,952 - INFO - odas correction elapsed time: 0.456 seconds
#########sales_df = optimize_odas_improvement(sales_df)###########              PRD_CD  nenshudo  URI_SU  URI_SU_EC  TENPO_CD  baika_toitsu  \
0     4550596138817    202513     1.0        0.0       760          1980   
1     4550596138817    202514     3.0        0.0       760          1980   
2     4550596138817    202515     6.0        0.0       760          1980   
3     4550596138817    202516     3.0        0.0       760          1980   
4     4550596138817    202517     9.0        0.0       760          1980   
...             ...       ...     ...        ...       ...           ...   
1979  4549509308706    202528     1.0        0.0       760           298   
1980  4549509308706    202529     1.0        0.0       760           298   
1981  4549509308706    202530     1.0        0.0       760           298   
1982  4549509308706    202531     1.0        0.0       760           298   
1983  4549509308706    202533     2.0        0.0       760           298   

       BAIKA  DPT  line_cd  cls_cd      hnmk_cd  odas_amount  URI_SU_NEW  \
0     1980.0   69      692    6922  24008460001          0.0         1.0   
1     1980.0   69      692    6922  24008460001          0.0         3.0   
2     1980.0   69      692    6922  24008460001          0.0         6.0   
3     1800.0   69      692    6922  24008460001          0.0         3.0   
4     1980.0   69      692    6922  24008460001          0.0         9.0   
...      ...  ...      ...     ...          ...          ...         ...   
1979   246.0   69      692    6940  16044580000          0.0         1.0   
1980   258.0   69      692    6940  16044580000          0.0         1.0   
1981   271.0   69      692    6940  16044580000          0.0         1.0   
1982   143.0   69      692    6940  16044580000          0.0         1.0   
1983   298.0   69      692    6940  16044580000          0.0         2.0   

      URI_SU_NEW_org URI_SU_NEW_OVER0_IMPLVMNT_TYPE  \
0                1.0                            NaN   
1                3.0                            NaN   
2                6.0                            NaN   
3                3.0                            NaN   
4                9.0                            NaN   
...              ...                            ...   
1979             1.0                            NaN   
1980             1.0                            NaN   
1981             1.0                            NaN   
1982             1.0                            NaN   
1983             2.0                            NaN   

      URI_SU_NEW_OVER0_IMPLVMNT_bk  
0                              1.0  
1                              3.0  
2                              6.0  
3                              3.0  
4                              9.0  
...                            ...  
1979                           1.0  
1980                           1.0  
1981                           1.0  
1982                           1.0  
1983                           2.0  

[1984 rows x 16 columns]
2025-10-24 14:00:13,407 - INFO -   SELECT DISTINCT PrdCd FROM `short_term_cloudrunjobs.weekly-train-2025-06-10_obon_218str_wk6sls20div` WHERE tenpo_cd = 760 AND BusyPeriodFlagNenmatsu > 0
################df_vx_test############               PrdCd  WeekStartDate  PreviousYearSalesActualQuantity  \
0     4549509833536       20170227                              0.0   
1     4550596138763       20170227                              0.0   
2     4936695609296       20170227                              0.0   
3     4550596138794       20170227                              0.0   
4     4550596138824       20170227                              0.0   
...             ...            ...                              ...   
6850  4550596138817       20251124                              0.0   
6851  4549509833536       20251124                              1.0   
6852  4549509303220       20251124                              3.0   
6853  4550596138824       20251124                              0.0   
6854  4549509780502       20251124                              7.0   

      PreviousYearEcSalesActualQuantity  PreviousYearClassSalesActualQuantity  \
0                                   0.0                                   0.0   
1                                   0.0                                   0.0   
2                                   0.0                                   0.0   
3                                   0.0                                   0.0   
4                                   0.0                                   0.0   
...                                 ...                                   ...   
6850                                0.0                                   0.0   
6851                                0.0                                   1.0   
6852                                0.0                                   7.0   
6853                                0.0                                   0.0   
6854                                0.0                                   7.0   

      PreviousYearClassSalesActualQuantity8ema  time_leap8  SalesAmount  \
0                                     0.000000    0.000000          0.0   
1                                     0.000000    0.000000          0.0   
2                                     0.000000    0.000000          2.0   
3                                     0.000000    0.000000          0.0   
4                                     0.000000    0.000000          0.0   
...                                        ...         ...          ...   
6850                                  0.000000    0.000000          0.0   
6851                                  2.526874    1.284836          0.0   
6852                                 28.849209    5.606430          0.0   
6853                                  0.000000    0.000000          0.0   
6854                                  5.572462    3.977003          0.0   

      SalesAmountEC  SalesAmountCLASS  SalesAmountCLASS8ema  baika_toitsu  \
0               NaN               NaN                   NaN        2280.0   
1               NaN               NaN                   NaN         698.0   
2               0.0               2.0                   2.0        1480.0   
3               NaN               NaN                   NaN        1480.0   
4               NaN               NaN                   NaN        2480.0   
...             ...               ...                   ...           ...   
6850            NaN               NaN                   NaN        1980.0   
6851            NaN               NaN                   NaN        2280.0   
6852            NaN               NaN                   NaN         398.0   
6853            NaN               NaN                   NaN        2480.0   
6854            NaN               NaN                   NaN        1180.0   

       BAIKA  DPT  line_cd  cls_cd      hnmk_cd weekstartdatestamp  tenpo_cd  \
0     1980.0   69      692    6941  21005430000         2017-02-27       760   
1      698.0   69      692    6922  24008460001         2017-02-27       760   
2      980.0   69      690    6902  11016720000         2017-02-27       760   
3     1480.0   69      692    6922  24008460001         2017-02-27       760   
4     2480.0   69      692    6922  24008460001         2017-02-27       760   
...      ...  ...      ...     ...          ...                ...       ...   
6850  1980.0   69      692    6922  24008460001         2025-11-24       760   
6851  2280.0   69      692    6941  21005430000         2025-11-24       760   
6852   398.0   69      693    6931  16026220500         2025-11-24       760   
6853  2480.0   69      692    6922  24008460001         2025-11-24       760   
6854  1180.0   69      692    6924  20024250000         2025-11-24       760   

           TenpoCdPrdCd  
0     760_4549509833536  
1     760_4550596138763  
2     760_4936695609296  
3     760_4550596138794  
4     760_4550596138824  
...                 ...  
6850  760_4550596138817  
6851  760_4549509833536  
6852  760_4549509303220  
6853  760_4550596138824  
6854  760_4549509780502  
    
