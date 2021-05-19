/********************************************************************************************************/
/* Инициализация */

%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
cas casauto;
caslib _all_ assign;


/* Подключение макросов */

%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
%include "&common_path./data_prep_product.sas"; 
%include "&common_path./data_prep_pbo.sas"; 
%include "&common_path./gc/gc_alert_year_trend.sas"; 
%include "&common_path./gc/gc_alert_strange_seasonality.sas"; 
%include "&common_path./gc/gc_alert_unprecedented_value.sas"; 


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

/* Получение списка VF-проектов */
%vf_get_project_list(mpOut=work.vf_project_list);

/* Извлечение ID для VF-проекта PBO по его имени */
%let mpVfPboProjName = nm_abt_pbo;
%let lmvVfPboName = &mpVfPboProjName.;
%let lmvVfPboId = %vf_get_project_id_by_name(mpName=&lmvVfPboName., mpProjList=work.vf_project_list);

/*Вытащить данные из проекта*/
proc fedsql sessref=casauto noprint;
	create table CASUSER.PBO_FCST{options replace=true} as
	select t1.*
			, month(cast(t1.SALES_DT as date)) as MON_START
			, month(cast(intnx('day', cast(t1.SALES_DT as date),6) as date)) as MON_END
	from "Analytics_Project_&lmvVfPboId".horizon t1
	;
quit;

/********************************************************************************************************/
/* Запуск алертов. 
	Допустимые значения параметров:
		Агрегация ПБО:
			A_AGREEMENT_TYPE
			LVL1_NM
			LVL2_NM
			LVL3_NM
			pbo_location_nm
		Агрегация SKU:
			PROD_LVL1_NM
			PROD_LVL2_NM
			PROD_LVL3_NM
			product_nm
		Граничные даты:
			22333 - 22feb2021
			22646 - 01jan2022
			today()
			&GL_ETL_TODAY.
			intnx('year', today(), 1, 'B')
		Сезоны:
			month
			qtr
*/

/* Изменения годовых трендов */
/* На уровне Регионов, Граничная дата - текущая */
%gc_alert_year_trend(
	  mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = GC_YEAR_TREND_REGION_CURRDT
);

/* Изменения годовых трендов */
/* На уровне Регионов, Граничная дата - следующий календарный год */
%gc_alert_year_trend(
	  mpLocLvl = LVL2_NM
	, mpBorderDt = intnx('year', today(), 1, 'B')
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = GC_YEAR_TREND_REGION_CALYEAR
);

/* Изменения годовых трендов */
/* На уровне Городов, Граничная дата - текущая */
%gc_alert_year_trend(
	  mpLocLvl = LVL3_NM
	, mpBorderDt = today()
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = GC_YEAR_TREND_CITY_CURRDT
);

/* Изменения годовых трендов */
/* На уровне Городов, Граничная дата - следующий календарный год */
%gc_alert_year_trend(
	  mpLocLvl = LVL3_NM
	, mpBorderDt = intnx('year', today(), 1, 'B')
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = GC_YEAR_TREND_CITY_CALYEAR
);

/*************************************************************************************/
/* Изменения сезонных трендов */
/* На уровне Регионов */
%gc_alert_strange_seasonality(
	  mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpSeason = month
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = GC_MTH_CHANGE_REGION_CURRDT
);

/* Изменения сезонных трендов */
/* На уровне Городов */
%gc_alert_strange_seasonality(
	  mpLocLvl = LVL3_NM
	, mpBorderDt = today()
	, mpSeason = month
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = GC_MTH_CHANGE_CITY_CURRDT
);

%gc_alert_strange_seasonality(
	  mpLocLvl = LVL1_NM
	, mpBorderDt = today()
	, mpSeason = semiyear
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = GC_SMY_CHANGE_CITY_CURRDT
);

/*************************************************************************************/
/* Неестественные значения */
/* На уровне ПБО - Месяц */
%gc_alert_unprecedented_value(
	  mpLocLvl = pbo_location_nm
	, mpTimeLvl = month
	, mpBorderDt = today()
	, mpOutTableNm = GC_ODDLY_VALUE_PBO_MTH
);

/*************************************************************************************/
/* Сохранение алертов в excel-файл.
	Необходимо следить за:
		- указанный путь 
		- наименование excel-страницы 
		- соответствие названию таблицы
*/
ods excel file="&common_path./GC_ALERTS.xlsx"  style=statistical;

ods excel options(sheet_interval = 'none' sheet_name = "Год-год, Регион, Тек.дата"	);
proc print data = DM_ALERT.GC_YEAR_TREND_REGION_CURRDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Регион, След.кал.год"	);
proc print data = DM_ALERT.GC_YEAR_TREND_REGION_CALYEAR	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Город, Тек.дата"	);
proc print data = DM_ALERT.GC_YEAR_TREND_CITY_CURRDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Город, След.кал.год"	);
proc print data = DM_ALERT.GC_YEAR_TREND_CITY_CALYEAR 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Месяц-Месяц, Регион"	);
proc print data = DM_ALERT.GC_MTH_CHANGE_REGION_CURRDT	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Месяц-Месяц, Город"	);
proc print data = DM_ALERT.GC_MTH_CHANGE_CITY_CURRDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Неестеств.знач., ПБО-Месяц"	);
proc print data = DM_ALERT.GC_ODDLY_VALUE_PBO_MTH 	label; run;

ods excel close;