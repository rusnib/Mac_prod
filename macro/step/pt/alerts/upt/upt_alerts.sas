/********************************************************************************************************/
/* Инициализация */

%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
cas casauto;
caslib _all_ assign;

/* Подключение макросов */

%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
%include "&common_path./data_prep_product.sas"; 
%include "&common_path./data_prep_pbo.sas"; 
%include "&common_path./upt/upt_alert_year_trend.sas"; 
%include "&common_path./upt/upt_alert_strange_seasonality.sas"; 


/********************************************************************************************************/
/* Подготовка справочников */

/* Справочник SKU */
%data_prep_product(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PRODUCT_DICTIONARY
);

/* Справочник ПБО */
%data_prep_pbo(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PBO_DICTIONARY
);


/********************************************************************************************************/
/* Подготовка прогнозов */

proc fedsql sessref=casauto;
	create table CASUSER.UPT_FCST_MONTH {options replace=true} as
	select distinct
		  cast(pmix.prod as integer) as PROD /*– ИД продукта*/
		, cast(pmix.location as integer) as LOCATION /*– ИД ресторана*/
		, pmix.data as DATA /*– Дата прогноза или факта (месяц)*/
		
		, pmix.TOTAL_FCST_QNT_MON
		, pmix.TOTAL_FCST_RUR_MON
		, gc.BASE_FORECAST_GC_M
		
		, case 
			when abs(gc.BASE_FORECAST_GC_M) > 1e-5 
				and abs(pmix.TOTAL_FCST_QNT_MON) > 1e-5 
			then 1000 * pmix.TOTAL_FCST_QNT_MON / gc.BASE_FORECAST_GC_M 
			else 0
			end
		  as upt 
		
	from MN_SHORT.PLAN_PMIX_MONTH as pmix 
	left join MN_SHORT.PLAN_GC_MONTH as gc
		on  pmix.location = gc.location 
			and pmix.data = gc.data
	;
quit;


/********************************************************************************************************/
/* Подготовка фактов */

%let lmvReportDttm = &ETL_CURRENT_DTTM.;

data CASUSER.PMIX_SALES (replace=yes drop=valid_from_dttm valid_to_dttm);
    set ETL_IA.PMIX_SALES (where=(valid_to_dttm>=&lmvReportDttm.));
run;

data CASUSER.PBO_SALES (replace=yes drop=valid_from_dttm valid_to_dttm);
   set ETL_IA.PBO_SALES (where=(valid_to_dttm>=&lmvReportDttm.));
run;

/* В том числе Убираем канал и суммируем до месяцев ! */
proc fedsql sessref=casauto;
	create table CASUSER.UPT_FACT_MONTH {options replace=true} as
	select distinct
/* 		  pmix.channel_cd */
		  pmix.pbo_location_id
		, pmix.product_id
		, cast(intnx('month', pmix.sales_dt, 0, 'B') as date) as sales_dt
		, sum(pmix.sales_qty) as sales_qty
		, sum(gc.receipt_qty) as receipt_qty
/* 		, case  */
/* 			when abs(gc.receipt_qty)> 1e-5 */
/* 				and abs(pmix.sales_qty)> 1e-5 */
/* 			then pmix.sales_qty / gc.receipt_qty * 1000  */
/* 			else 0 */
/* 			end */
/* 		  as upt */

	from CASUSER.PMIX_SALES as pmix 

	left join CASUSER.PBO_SALES as gc
		on 		pmix.channel_cd 	 = gc.channel_cd 
			and pmix.pbo_location_id = gc.pbo_location_id
			and pmix.sales_dt 		 = gc.sales_dt
	
	group by 1,2,3
	;
quit;



/********************************************************************************************************/
/* Запуск алертов */


/* Допустимые параметры:
	Агрегация ПБО:
		A_AGREEMENT_TYPE
		LVL1_NM
		LVL2_NM
		LVL3_NM
		pbo_location_nm
	Граничные даты:
		22333 - 22feb2021
		22646 - 01jan2022
	Сезоны:
		month
		qtr
*/


/* Изменения годовых трендов */
/* На уровне Категория - Регион, Граничная дата - текущая */
%upt_alert_year_trend(
      mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = UPT_YEAR_TREND_CAT_REG_CURRDT
);

/* Изменения годовых трендов */
/* На уровне Категория - Agreement type, Граничная дата - текущая */
%upt_alert_year_trend(
      mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = A_AGREEMENT_TYPE
	, mpBorderDt = today()
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = UPT_YEAR_TREND_CAT_AGR_CURRDT
);

/* Изменения годовых трендов */
/* На уровне Категория - ПБО, Граничная дата - текущая */
%upt_alert_year_trend(
      mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = pbo_location_nm
	, mpBorderDt = today()
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = UPT_YEAR_TREND_CAT_PBO_CURRDT
);

/* Изменения годовых трендов */
/* На уровне Категория - Регион, Граничная дата - следующий календарный год */
%upt_alert_year_trend(
      mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = intnx('year', today(), 1, 'B')
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = UPT_YEAR_TREND_CAT_REG_CALYEAR
);

/* Изменения годовых трендов */
/* На уровне Категория - Agreement type, Граничная дата - следующий календарный год */
%upt_alert_year_trend(
      mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = A_AGREEMENT_TYPE
	, mpBorderDt = intnx('year', today(), 1, 'B')
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = UPT_YEAR_TREND_CAT_AGR_CALYEAR
);

/* Изменения годовых трендов */
/* На уровне Категория - ПБО, Граничная дата - следующий календарный год */
%upt_alert_year_trend(
      mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = pbo_location_nm
	, mpBorderDt = intnx('year', today(), 1, 'B')
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = UPT_YEAR_TREND_CAT_PBO_CALYEAR
);

/*************************************************************************************/
/* Изменения сезонных трендов */
/* На уровне Категория - Регион - Полугодие */
%upt_alert_strange_seasonality(
	  mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpSeason = semiyear
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = UPT_SMY_CHANGE_CAT_REG_CURRDT
);

/* Изменения сезонных трендов */
/* На уровне Категория - Регион - Квартал */
%upt_alert_strange_seasonality(
	  mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpSeason = qtr
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = UPT_QTR_CHANGE_CAT_REG_CURRDT
);

/* Изменения сезонных трендов */
/* На уровне Категория - Вся сеть - Месяц */
%upt_alert_strange_seasonality(
	  mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL1_NM
	, mpBorderDt = today()
	, mpSeason = month
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = UPT_MTH_CHANGE_CAT_ALL_CURRDT
);

/*************************************************************************************/
/* Сохранение алертов в excel-файл.
	Необходимо следить за:
		- указанный путь 
		- наименование excel-страницы 
		- соответствие названию таблицы
*/
ods excel file="&common_path./UPT_ALERTS.xlsx"  style=statistical;

ods excel options(sheet_interval = 'none' sheet_name = "Год-год, Кат.Рег., Тек.дата"	);
proc print data = DM_ALERT.UPT_YEAR_TREND_CAT_REG_CURRDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Кат.Рег., След.кал.год"	);
proc print data = DM_ALERT.UPT_YEAR_TREND_CAT_REG_CALYEAR	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Кат.AGR., Тек.дата"	);
proc print data = DM_ALERT.UPT_YEAR_TREND_CAT_AGR_CURRDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Кат.AGR., След.кал.год"	);
proc print data = DM_ALERT.UPT_YEAR_TREND_CAT_AGR_CALYEAR	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Кат.ПБО., Тек.дата"	);
proc print data = DM_ALERT.UPT_YEAR_TREND_CAT_PBO_CURRDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Кат.ПБО., След.кал.год"	);
proc print data = DM_ALERT.UPT_YEAR_TREND_CAT_PBO_CALYEAR	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Полугодие-Категория-Регион"	);
proc print data = DM_ALERT.UPT_SMY_CHANGE_CAT_REG_CURRDT	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Квартал-Категория-Регион"	);
proc print data = DM_ALERT.UPT_QTR_CHANGE_CAT_REG_CURRDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Месяц-Категория-Вся сеть"	);
proc print data = DM_ALERT.UPT_MTH_CHANGE_CAT_ALL_CURRDT 	label; run;

ods excel close;
