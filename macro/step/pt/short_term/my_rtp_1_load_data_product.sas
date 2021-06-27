/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза (продукты)
*	  Краткосроный прогноз UNITS на уровне PBO-SKU-Day
*
*  ПАРАМЕТРЫ:
*     mpMode 		- Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг)
*     mpRetroLaunch	- Режим запуска ретроспективный (Y) или регламентный (N) - влияет на способ сборки скоринговой выборки 
*							(в случае, если структура ABT будет собираться на основе product_chain, то данный параметр скорее всего не нужен)
*	  mpOutTrain	- выходная таблица набора для обучения
*	  mpOutScore	- выходная таблица набора для скоринга
*	
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*    %rtp_1_load_data_product(mpMode=S, mpRetroLaunch=N, mpOutScore=casuser.all_ml_scoring);
*	 %rtp_1_load_data_product(mpMode=T, mpRetroLaunch=N, mpOutTrain=casuser.all_ml_train);
*	 %rtp_1_load_data_product(mpMode=A, mpRetroLaunch=N, mpOutTrain=casuser.all_ml_train, mpOutScore=casuser.all_ml_scoring);
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
*  27-08-2020  Борзунов		Заменен источник данных на ETL_IA. Добавлена выгрузка на диск целевых таблиц
*  24-09-2020  Борзунов		Добавлена промо-разметка из ПТ
*  21-06-2021  Повод		Изменена логика формирования структуры ABT (шаг 2)
****************************************************************************/
%macro rtp_1_load_data_product(
			  mpMode		= A
			, mpRetroLaunch = N
			, mpOutTrain	= mn_short.all_ml_train
			, mpOutScore	= mn_short.all_ml_scoring
			, mpWorkCaslib	= mn_short
	);

	options symbolgen mprint;
	
	%local lmvMode 							/* Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг) */
			lmvRetroLaunch					/* Режим запуска ретроспективный (Y) или регламентный (N) - влияет на способ сборки скоринговой выборки */
			lmvInLib						/* Входная библиотека */
			lmvReportDttm 					/* Дата-время для ETL-процессов */
			lmvTrainStartDate 				/* Дата начала обучающей выборки */		
			lmvTrainEndDate 	    		/* Дата окончания обучающей выборки */
			lmvScoreStartDate       		/* Дата начала обучающей выборки */
			lmvScoreEndDate         		/* Дата окончания обучающей выборки */
			lmvLibrefOutTrain				/* Локальный параметр "Результирующая CAS-библиотека для train-sample" на базе значения входящего макро-параметра mpOutTrain */				
			lmvTabNmOutTrain				/* Локальный параметр "Результирующая CAS-таблица для train-sample" на базе значения входящего макро-параметра mpOutTrain */	
			lmvLibrefOutScore				/* Локальный параметр "Результирующая CAS-библиотека для score-sample" на базе значения входящего макро-параметра mpOutScore */				
			lmvTabNmOutScore				/* Локальный параметр "Результирующая CAS-таблица для score-sample" на базе значения входящего макро-параметра mpOutScore */				
			lmvWorkCaslib					/* CAS-библиотека, содержащая вспомогательные таблицы (продажи, цены, и т.д.) на базе значения входящего макро-параметра mpWorkCaslib */				
			;
			
	%let lmvMode = &mpMode.;
	%let lmvRetroLaunch = &mpRetroLaunch.;
	%let lmvInLib=ETL_IA;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;				/* Текущее дата-время для ETL-процессов */
	%let lmvWorkCaslib = &mpWorkCaslib.;				/* См. выше */
	
	/* Почему берем историю только за 2 года??? может имеет смысл удлиннить и вырезать кусок с марта по сентябрь 2020??? */
	%let lmvTrainStartDate 	= %sysfunc(intnx(year,&etl_current_dt.,-3,s));		/* Дата начала обучающей выборки */
	%let lmvTrainEndDate 	= &VF_HIST_END_DT_SAS.;								/* Дата окончания обучающей выборки */
	%let lmvScoreStartDate 	= %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,1,s));	/* Дата начала обучающей выборки */
	%let lmvScoreEndDate 	= %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));	/* Дата окончания обучающей выборки */

	/* Дата для фильтрации фактических продаж при ретроспективном режиме прогнозирования.
		Необходимо для определения списка прогнозируемых ПБО-SKU при формировании скоринговой выборки	 */
	%let lmvRetroCheckEndDate 	= %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,35,s));	

	/* "COVID-период" - временной интервал, исключаемый из обучающей выборки и прогнозирования */
	%let lmvCovidStartDate 	= %sysfunc(intnx(day,'01mar2020'd, 0, s));
	%let lmvCovidEndDate 	= %sysfunc(intnx(day,'31aug2020'd, 0, s));
	
	%member_names (mpTable=&mpOutTrain, mpLibrefNameKey=lmvLibrefOutTrain, mpMemberNameKey=lmvTabNmOutTrain);
	%member_names (mpTable=&mpOutScore, mpLibrefNameKey=lmvLibrefOutScore, mpMemberNameKey=lmvTabNmOutScore);
	
/* ------------ Start. Создаем CAS сессию, если ее нет. Удаляем старые таблицы ---- */
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	proc casutil;
		droptable casdata="&lmvTabNmOutTrain." incaslib="&lmvLibrefOutTrain." quiet;
		droptable casdata="&lmvTabNmOutScore." incaslib="&lmvLibrefOutScore." quiet;
		droptable casdata="abt1_ml" incaslib="casuser" quiet;
	run;
/* ------------ End. Создаем CAS сессию, если ее нет. Удаляем старые таблицы ------ */

	
/************************************************************************************
 *	В данном макросе идет сборка витрины для ML прогноза PBO-SKU-Day				*
 ************************************************************************************/
/*			UNITS в краткосрочном прогнозе прогнозируются тяжелыми ML моделями:
 *			- модель прогнозирования "старых" товаров (витрина собирается в этом макросе)
 *			- модель прогнозирования "новых" товаров (витрина собирается отдельно). 
 *		
 *			Ниже происходит полная сборка витрин, необходимых для обучения и скоринга
 *		моделей краткосрочного прогноза "старых" товаров на уровне PBO-SKU-Day.
 *		Обучение и скоринг моделей "новых" товаров происходит отдельно.
 */
	

/************************************************************************************
 *	1.	Сбор "каркаса" из pmix														*
 ************************************************************************************/
/*			Текущая проблема - не знаем нулевых продаж, поэтому каркас строится на 
 *		основании продаж! Необходимо решить проблему, т.к. скорее всего этом
 *		значительно снижает качество прогноза
 * 		
 * 		 
 */


/* ------------ Start. Считаем целевую переменную и подтягиваем справочник -------- */
/*			Целевой переменной является величина SALES_QTY + SALES_QTY_PROMO
 */
	proc fedsql sessref=casauto; 
			create table casuser.abt1_ml{options replace=true} as
			select 
				t1.PBO_LOCATION_ID,
					t1.PRODUCT_ID,
					t1.CHANNEL_CD,
					t1.SALES_Dt,
					(t1.SALES_QTY + t1.SALES_QTY_PROMO) as sum_qty
			from 
				&lmvWorkCaslib..pmix_sales t1
				
			inner join
				 &lmvWorkCaslib..product_dictionary_ml as t2 
			on
				t1.product_id = t2.product_id

			inner join
				 &lmvWorkCaslib..pbo_dictionary_ml as t3
			on
				t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
				
			where   t1.SALES_DT >= %str(date%')%sysfunc(putn(&lmvTrainStartDate.,yymmdd10.))%str(%') 
				and t1.SALES_DT <= %str(date%')%sysfunc(putn(&lmvScoreEndDate.,yymmdd10.))%str(%')
				
				/* !!! Filter begin !!! */
				and t1.CHANNEL_CD = 'ALL'
				and t2.prod_lvl2_id = 90
				and t3.lvl2_id 		= 74
				/* !!! Filter end !!! */
				
		;
	quit;
	quit;
/* ------------ End. Считаем целевую переменную и подтягиваем справочник ---------- */


/* ------------ Start. Добавляем цены из справочника цен -------------------------- */
	proc casutil;
	  droptable casdata="abt2_ml" incaslib="casuser" quiet;
	run;

	/* Макс П.: Надо заменить на новые цены, но там другая структура таблиц и их несколько */
	proc fedsql sessref=casauto; 
		create table casuser.abt2_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,																/* ??? От Димы З.: */
				max(t2.GROSS_PRICE_AMT) as GROSS_PRICE_AMT								/* Агрегация промо-цен до нужного разреза делается при помощи агрегата «минимум» */
			from																		
				casuser.abt1_ml as t1 left join											
				&lmvWorkCaslib..price_ml as t2											/* Это актуальные цены??? Дима З. писал про mn_dict.price_feat. Надо уточнить */
			on																			/* Может вынести в макро*/ 
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT <= t2.end_dt and   
				t1.SALES_DT >= t2.start_dt
			group by 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty
			;
	quit;
/* ------------ End. Добавляем цены из справочника цен ---------------------------- */


	proc casutil;
	  droptable casdata="abt1_ml" incaslib="casuser" quiet;
	  droptable casdata="abt3_ml" incaslib="casuser" quiet;
	run;


/* ------------ Start. Протяжка временных рядов до окончания периода прогноза ----- */
/* ??? как работает протяжка на таком ряде:
		Date 	sum_qty 	GROSS_PRICE_AMT
	01.01.2019		.			10
	05.01.2019		4			.
	07.01.2019		6			9
	-- конец истории
	09.01.2019 -- конец периода прогноза

	Вот так? ВОТ ЭТО БОЛЬШЕ ПОХОЖЕ НА ПРАВДУ
		Date 	sum_qty 	GROSS_PRICE_AMT
	01.01.2019		.			10
	02.01.2019		.			10
	03.01.2019		.			10
	04.01.2019		.			10
	05.01.2019		4			10
	06.01.2019		.			10
	07.01.2019		6			9
	08.01.2019		.			9
	09.01.2019 		.			9
	
	Или вот так?
		Date 	sum_qty 	GROSS_PRICE_AMT
	01.01.2019		.			10
	05.01.2019		4			10
	07.01.2019		6			9
	08.01.2019		.			9
	09.01.2019 		.			9
 */
/* ??? 		Дима З. + Данил М. вроде бы сделали цены на будущее, наверное стоит их подтянуть вместо 
		простой протяжки с истории
	
	Думаю, что если будем добавлять нули в обучающую выборку, то все равно надо протягивать вперед и назад вместе с ценами.
 */	
	%let fc_end=%sysfunc(putn(&lmvScoreEndDate,yymmdd10.));

	proc cas;
	timeData.timeSeries result =r /
		series={
			{name="sum_qty", setmiss="MISSING"},
			{name="GROSS_PRICE_AMT", setmiss="PREV"}
		}
		tEnd= "&fc_end"
		table={
			caslib="casuser",
			name="abt2_ml",
			groupby={"PBO_LOCATION_ID","PRODUCT_ID", "CHANNEL_CD"}
		}
		timeId="SALES_DT"
		trimId="LEFT"
		interval="day"
		casOut={caslib="casuser", name="abt3_ml", replace=True}
		;
	run;
	quit;
/* ------------ End. Протяжка временных рядов до окончания периода прогноза ------- */


	proc casutil;
	  droptable casdata="abt2_ml" incaslib="casuser" quiet;
	  droptable casdata="abt4_ml" incaslib="casuser" quiet;
	run;



/************************************************************************************
 *	2.	Фильтрация данных согласно справочникам и PLM таблицам						*
 ************************************************************************************/
/*			Собранный каркас необходимо отфильтровать согласно данным по периодам
 *		работы ресторанов и периодам ввода и вывода товаров:
 *			- временные закрытия - IA_PBO_CLOSED_PERIODS
 *			- ввод-вывод товаров - PRODUCT_CHAIN
 *			- информации из справочника ресторанов по закрытиям
 *			- ассортиментной матрицей IA_ASSORT_MATRIX 		
 */


/* ------------ Start. Разделяем таблицу на 2 части: TRAIN & SCORE ---------------- */	
/* 	data  */
/* 		casuser.abt3_ml_train  */
/* 		casuser.abt3_ml_score */
/* 		; */
/* 		set casuser.abt3_ml; */
/* 		if sales_dt >= %str(date%')%sysfunc(putn(&lmvTrainStartDate.,yymmdd10.))%str(%') */
/* 			and sales_dt <= %str(date%')%sysfunc(putn(&lmvTrainEndDate.,yymmdd10.))%str(%') */
/* 				then output casuser.abt3_ml_train; */
/* 		else if sales_dt >= %str(date%')%sysfunc(putn(&lmvScoreStartDate.,yymmdd10.))%str(%') */
/* 			and sales_dt <= %str(date%')%sysfunc(putn(&lmvScoreEndDate.,yymmdd10.))%str(%') */
/* 				then output casuser.abt3_ml_score; */
/* 	run; */

	data 
		casuser.abt3_ml_train 
		casuser.abt3_ml_score
		;
		set casuser.abt3_ml;
		if 		sales_dt >= &lmvTrainStartDate.
			and sales_dt <= &lmvTrainEndDate.
				then output casuser.abt3_ml_train;
		else if sales_dt >= &lmvScoreStartDate.
			and sales_dt <= &lmvScoreEndDate.
				then output casuser.abt3_ml_score;
	run;
/* ------------ End. Разделяем таблицу на 2 части: TRAIN & SCORE ------------------- */


/* ------------ Start. Убираем временные закрытия PBO из TRAIN --------------------- */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml_train_1{options replace=true} as
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt3_ml_train as t1
			left join
				&lmvWorkCaslib..pbo_closed_ml as t2
			on
				t1.sales_dt >= t2.start_dt and
				t1.sales_dt <= t2.end_dt and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.channel_cd = t2.channel_cd
			where
				t2.pbo_location_id is missing
		;
	quit;
/* ------------ End. Убираем временные закрытия PBO ------------------------------- */


/* ------------ Start. Убираем закрытые насовсем PBO ------------------------------ */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml_train_2{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt4_ml_train_1 as t1
			left join
				&lmvWorkCaslib..closed_pbo as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt >= t2.OPEN_DATE and
				t1.sales_dt <= t2.CLOSE_DATE
			where
				t2.pbo_location_id is not missing
		;
	quit;
/* ------------ End. Убираем закрытые насовсем PBO -------------------------------- */


/* ------------ Start. Оставляем нулевые продажи согласно product chain ----------- */

	/* Разбиваем PRODUCT_CHAIN_ENH на 3 таблицы */
	data 
		casuser.chain_fullhouse 
		casuser.chain_miss_sku
		casuser.chain_miss_loc
		;
		set casuser.product_chain_enh;
		if successor_dim2_id = . then 
			output casuser.chain_miss_loc;
		else if successor_product_id  = . then 
			output casuser.chain_miss_sku;
		else 
			output casuser.chain_fullhouse;
		
	run;

	/* Присоединяем product_chain_enh с учетом особенностей по наличию пропущенных ID */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml_train_3{options replace=true} as 
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				case 
					when fh.predecessor_product_id is null 
					 and ms.predecessor_product_id is null 
					 and ml.predecessor_product_id is null 
						then .
					else coalesce(t1.sum_qty, 0)
				 end as sum_qty,
				t1.GROSS_PRICE_AMT
			from 
				casuser.abt4_ml_train_2 as t1

			left join 
				casuser.chain_fullhouse as fh
			on      fh.lifecycle_cd 	= 'N'
				and t1.sales_dt	 		between datepart(fh.successor_start_dt) and datepart(fh.predecessor_end_dt)
				and	t1.product_id 		= fh.successor_product_id							
				and t1.pbo_location_id 	= fh.successor_dim2_id 				

			left join 
				casuser.chain_miss_sku as ms
			on      ms.lifecycle_cd 	= 'N'
				and t1.sales_dt	 		between datepart(ms.successor_start_dt) and datepart(ms.predecessor_end_dt)				
				and t1.pbo_location_id 	= ms.successor_dim2_id 	

			left join 
				casuser.chain_miss_loc as ml
			on      ml.lifecycle_cd 	= 'N'
				and t1.sales_dt	 		between datepart(ml.successor_start_dt) and datepart(ml.predecessor_end_dt)
				and	t1.product_id 		= ml.successor_product_id		
		;
	quit;
/* ------------ End. Оставляем нулевые продажи согласно product chain ------------- */
	

/* ------------ Start. Убираем из истории COVID-период  --------------------------- */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml_train_4{options replace=true} as 
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from 
				casuser.abt4_ml_train_3 as t1
			where  t1.SALES_DT < %str(date%')%sysfunc(putn(&lmvCovidStartDate.,yymmdd10.))%str(%')
				or t1.SALES_DT > %str(date%')%sysfunc(putn(&lmvCovidEndDate.,yymmdd10.))%str(%')
		;
	quit;
/* ------------ End. Убираем из истории COVID-период  ----------------------------- */


/* ------------ Start. Убираем из истории пропуски в продажах --------------------- */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml_train_5{options replace=true} as 
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from 
				casuser.abt4_ml_train_4 as t1
			where 		
				t1.sum_qty is not null
/* 			(t1.sum_qty is not missing and t1.SALES_DT <= %str(date%')%sysfunc(putn(&lmvTrainEndDate.,yymmdd10.))%str(%')) or */
/* 			(t1.SALES_DT > %str(date%')%sysfunc(putn(&lmvTrainEndDate.,yymmdd10.))%str(%')) */
		;
	quit;
/* ------------ End. Убираем из истории пропуски в продажах ----------------------- */


/* ------------ Start. Убираем возможные дубликаты от соединения с product_chain --- */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml_train{options replace=true} as 
			select 
				  PBO_LOCATION_ID
				, PRODUCT_ID
				, CHANNEL_CD
				, SALES_DT
				, avg(sum_qty) as sum_qty
				, avg(GROSS_PRICE_AMT) as GROSS_PRICE_AMT
			from 
				casuser.abt4_ml_train_5
			group by
				  PBO_LOCATION_ID
				, PRODUCT_ID
				, CHANNEL_CD
				, SALES_DT
		;
	quit;
/* ------------ End. Убираем возможные дубликаты от соединения с product_chain ----------------------- */



/* ------------ Start. Формиурем список прогнозируемых ПБО-SKU ------------------------- */
	%if &lmvRetroLaunch. = N %then %do;		/* В случае регламентного режима прогнозирования */
		/* Список на основе ассортиментной матрицы */
		proc fedsql sessref=casauto;
			create table casuser.score_list {options replace = true} as	
				select distinct PBO_LOCATION_ID, PRODUCT_ID				
				from &lmvWorkCaslib..assort_matrix
				where %str(date%')%sysfunc(putn(&lmvScoreStartDate.,yymmdd10.))%str(%')
					between start_dt and end_dt 				
			;
		quit;
	%end;
	%if &lmvRetroLaunch. = Y %then %do;		/* В случае ретроспективного режима прогнозирования */
		/* Список на основе фактических продаж */
		proc fedsql sessref=casauto;
			create table casuser.score_list {options replace = true} as	
				select distinct PBO_LOCATION_ID, PRODUCT_ID	
				from &lmvWorkCaslib..pmix_sales
				where SALES_DT between %str(date%')%sysfunc(putn(&lmvScoreStartDate.,yymmdd10.))%str(%')
					and %str(date%')%sysfunc(putn(&lmvRetroCheckEndDate.,yymmdd10.))%str(%')
			;
		quit;
	%end;
/* ------------ End. Формиурем список прогнозируемых ПБО-SKU ------------------------------ */


/* ------------ Start. Пересекаем скоринговую витрину со списком прогнозируемых ПБО-SKU --- */	
		proc fedsql sessref=casauto;
			create table casuser.abt4_ml_score {options replace = true} as	
				select distinct
					  t1.PBO_LOCATION_ID
					, t1.PRODUCT_ID
					, t1.CHANNEL_CD
					, t1.SALES_DT
					, . as sum_qty				/* Значение поля продаж заполняем missing */
					, t1.GROSS_PRICE_AMT					

				from
					casuser.abt3_ml_score as t1
				inner join
					casuser.score_list as t2
				on
					t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
					t1.PRODUCT_ID = t2.PRODUCT_ID 
			;
		quit;

/* ------------ End. Пересекаем скоринговую витрину со списком прогнозируемых ПБО-SKU ----- */	


/* ------------ Start. Соединяем обработанные TRAIN & SCORE в одну таблицу -------- */	
	data casuser.abt4_ml;
		set 
			casuser.abt4_ml_train
			casuser.abt4_ml_score
		;
	run;
/* ------------ End. Соединяем обработанные TRAIN & SCORE в одну таблицу ----------- */


/* ------------ Start. Очистка CAS-cash от промежуточных таблиц -------------------- */	
	
	proc casutil;
		droptable casdata="abt3_ml" incaslib="casuser" quiet;
		droptable casdata="abt4_ml_train_1" incaslib="casuser" quiet;
		droptable casdata="abt4_ml_train_2" incaslib="casuser" quiet;
		droptable casdata="abt4_ml_train_3" incaslib="casuser" quiet;
		droptable casdata="abt4_ml_train_4" incaslib="casuser" quiet;
		droptable casdata="abt4_ml_train_5" incaslib="casuser" quiet;
	run;

/* ------------ End. Очистка CAS-cash от промежуточных таблиц ----------------------- */	



/************************************************************************************
 *	3.	Расчет лагов в качестве фичей												*
 ************************************************************************************/
/*			Для прогнозирования временных рядов с помощью методов ML одной из
 *		best practice является добавление лагов продаж, т.е. характеристик продаж
 *		на истории, как "фичи" в модель ML. Примеры:
 *			- продажи 91 день назад (желательно кратно 7 дням из-за сильной
 *					недельной сезонности)
 *			- средние продажи за квартал за 91 день до даты прогнозы
 *			- медиана, стандартные отклонения, квантили и пр. 		
 */

	proc casutil;
	  droptable casdata='lag_abt1' incaslib='casuser' quiet;
	  droptable casdata='lag_abt2' incaslib='casuser' quiet;
	  droptable casdata='lag_abt3' incaslib='casuser' quiet;
	  droptable casdata='abt5_ml' incaslib='casuser' quiet;
	run;


/* ------------ Start. Считаем медиану и среднее арифметическое ------------------- */
	options nosymbolgen nomprint nomlogic;
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='abt4_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = 'PRODUCT_ID'},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT", 
		code=
			%unquote(%str(%"))			
			%let names=; 																				/* будущий список выходных переменных для proc cas */
			%let minlag=35; 																			/* параметр минимальный лаг. ??? может поставим 35 (т.к. KPI на N+1)??? модель будет работать на горизонте больше 35 дней вперед??? вынести в макро???*/
			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;															/* ??? может вынести в макро */
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list,&ic); 													/* текущее окно */
				%let intnm=%rtp_namet(&window);        													/* название интервала окна; 7 -> week итд */
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												/* from = (lag) + (window) */
					lag_&intnm._avg[t]=mean(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
					lag_&intnm._med[t]=median(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
				end;
				%let names={name=%tslit(lag_&intnm._avg)}, &names;
				%let names={name=%tslit(lag_&intnm._med)}, &names; 
			%end; 																						/* ic over window_list */
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))  																		
		,
		arrayOut={
			table={name='lag_abt1', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;
/* ------------ End. Считаем медиану и среднее арифметическое --------------------- */


/* ------------ Start. Считаем стандартное отклонение ----------------------------- */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='abt4_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = 'PRODUCT_ID'},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT",
		code=
			%unquote(%str(%"))
			%let names=; 																				/* будущий список выходных переменных для proc cas */
			%let minlag=35; 																			/* параметр минимальный лаг. ??? может поставим 35 (т.к. KPI на N+1)??? модель будет работать на горизонте больше 35 дней вперед??? вынести в макро???*/
			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;															/* ??? может вынести в макро */
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list,&ic); 													/* текущее окно */
				%let intnm=%rtp_namet(&window);        													/* название интервала окна; 7 -> week итд */
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												/*from = (lag) + (window)*/
					lag_&intnm._std[t]=std(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
				end;
				%let names={name=%tslit(lag_&intnm._std)}, &names;
			%end; 																						/* ic over window_list*/
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))
		,
		arrayOut={
			table={name='lag_abt2', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;
/* ------------ End. Считаем стандартное отклонение ------------------------------- */


/* ------------ Start. Считаем процентили ----------------------------------------- */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='abt4_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = 'PRODUCT_ID'},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT",
		code=
			%unquote(%str(%"))
			%let names=; 																				/* будущий список выходных переменных для proc cas */
			%let minlag=35; 																			/* параметр минимальный лаг. ??? может поставим 35 (т.к. KPI на N+1)??? модель будет работать на горизонте больше 35 дней вперед??? вынести в макро???*/
			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;															/* ??? может вынести в макро */
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list,&ic); 													/* текущее окно */
				%let intnm=%rtp_namet(&window);        													/* название интервала окна; 7 -> week итд */
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												/* from=(lag)+(window) */
					lag_&intnm._pct10[t]=pctl(10,%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
					lag_&intnm._pct90[t]=pctl(90,%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
				end;
				%let names={name=%tslit(lag_&intnm._pct10)}, &names;
				%let names={name=%tslit(lag_&intnm._pct90)}, &names;
			%end; 																						/* ic over window_list */
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))
		,
		arrayOut={
			table={name='lag_abt3', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;
/* ------------ End. Считаем процентили ------------------------------------------- */
	
	
/*		соеденим среднее, медиану, стд, процентили вместе, убирая пропуску вр ВР
 *		??? не могу найти место, где убираются пропуски во временных рядах
 */

/* ------------ Start. Соеденим среднее, медиану ---------------------------------- */
	options symbolgen mprint mlogic;
	proc fedsql sessref=casauto;
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t2.lag_halfyear_avg,
				t2.lag_halfyear_med,
				t2.lag_month_avg,
				t2.lag_month_med,
				t2.lag_qtr_avg,
				t2.lag_qtr_med,
				t2.lag_week_avg,
				t2.lag_week_med,
				t2.lag_year_avg,
				t2.lag_year_med
			from
				casuser.abt4_ml as t1,
				casuser.lag_abt1 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;
/* ------------ End. Соеденим среднее, медиану, стд, процентили вместе ------------ */


	proc casutil;
	  droptable casdata="abt4_ml" incaslib="casuser" quiet;
	  droptable casdata="lag_abt1" incaslib="casuser" quiet;
	run;


/* ------------ Start. Соеденим стандартное отклонение ---------------------------- */
	proc fedsql sessref=casauto;
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t2.lag_halfyear_std,
				t2.lag_month_std,
				t2.lag_qtr_std,
				t2.lag_week_std,
				t2.lag_year_std
			from
				casuser.abt5_ml as t1,
				casuser.lag_abt2 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;
/* ------------ End. Соеденим стандартное отклонение ------------------------------ */


	proc casutil;
	  droptable casdata="lag_abt2" incaslib="casuser" quiet;
	run;


/* ------------ Start. Соеденим процентили ---------------------------------------- */
	proc fedsql sessref=casauto;
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t2.lag_halfyear_pct10,		 
				t2.lag_halfyear_pct90,		 
				t2.lag_month_pct10	,
				t2.lag_month_pct90	,
				t2.lag_qtr_pct10,	
				t2.lag_qtr_pct90,	
				t2.lag_week_pct10,	
				t2.lag_week_pct90,	
				t2.lag_year_pct10,	
				t2.lag_year_pct90
			from
				casuser.abt5_ml as t1,
				casuser.lag_abt3 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;
/* ------------ End. Соеденим процентили ------------------------------------------ */


	proc casutil;
	  droptable casdata='lag_abt3' incaslib='casuser' quiet;
	  droptable casdata="abt6_ml" incaslib="casuser" quiet;
	run;


/* ------------ Start. ??? Что здесь делается??? ----------------------------------- */
/* 			Что-то происходит с мэтчингом старых и новых обозначений механик промо,
 *		но не могу понять, что???
 */

	/* Генерим макропеременные для вставки в код */
	data _null_;
		set &lmvWorkCaslib..promo_mech_transformation end=end;
		length sql_list sql_max_list $1000;
		retain sql_list sql_max_list;
		by new_mechanic;

		if _n_ = 1 then do;
			sql_list = cats('t1.', new_mechanic);
			sql_max_list = cat('max(coalesce(t2.', strip(new_mechanic), ', 0)) as ', strip(new_mechanic));
		end;
		else if first.new_mechanic then do;
			sql_list = cats(sql_list, ', t1.', new_mechanic);
			sql_max_list = cat(strip(sql_max_list), ', max(coalesce(t2.', strip(new_mechanic), ', 0)) as ', strip(new_mechanic));
		end;

		if end then do;
			call symputx('promo_list_sql', sql_list, 'G');
			call symputx('promo_list_sql_max', sql_max_list, 'G');
		end;
	run;

	%let promo_list_sql_t2 = %sysfunc(tranwrd(%quote(&promo_list_sql.),%str(t1),%str(t2)));

	%put &promo_list_sql.;
	%put &promo_list_sql_max.;
	%put &promo_list_sql_t2.;
/* ------------ End. ??? Что здесь делается??? ------------------------------------- */


/* ------------ Start. Соединяем информацию о промо в одну табличку ---------------- */
	proc fedsql sessref = casauto;
		create table casuser.abt_promo{options replace = true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				/* max(coalesce(t2.other_promo, 0)) as other_promo,  
				max(coalesce(t2.support, 0)) as support,
				max(coalesce(t2.bogo, 0)) as bogo,
				max(coalesce(t2.discount, 0)) as discount,
				max(coalesce(t2.evm_set, 0)) as evm_set,
				max(coalesce(t2.non_product_gift, 0)) as non_product_gift,
				max(coalesce(t2.pairs, 0)) as pairs,
				max(coalesce(t2.product_gift, 0)) as product_gift,
				*/
				max(coalesce(t3.side_promo_flag, 0)) as side_promo_flag,
				&promo_list_sql_max.
			from
				casuser.abt5_ml as t1
			left join
				&lmvWorkCaslib..promo_transposed as t2									/* Что это за табличка??? */
			on
				t1.product_id = t2.product_LEAF_ID and
				t1.pbo_location_id = t2.PBO_LEAF_ID and
				t1.SALES_DT <= t2.END_DT and
				t1.SALES_DT >= t2.START_DT
			left join
				&lmvWorkCaslib..promo_ml_main_code as t3								/* Что это за табличка??? Что такое MAIN_CODE??? */
			on
				t1.product_id = t3.product_MAIN_CODE and
				t1.pbo_location_id = t3.PBO_LEAF_ID and
				t1.SALES_DT <= t3.END_DT and
				t1.SALES_DT >= t3.START_DT
			group by
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT
		;
/* ------------ End. Соединяем информацию о промо в одну табличку ------------------ */


/* ------------ Start. Добавляем промо в основную витрину -------------------------- */
/* 			Какие параметры промо здесь добавляются??? Только индикаторы механик без
 *		промо цен, скидок и пр.???
 *
 */
		create table casuser.abt6_ml{options replace = true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10	,
				t1.lag_month_pct90	,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t2.other_promo,  
				t2.support,
				t2.bogo,
				t2.discount,
				t2.evm_set,
				t2.non_product_gift,
				t2.pairs,
				t2.product_gift,
				*/
				&promo_list_sql_t2.,
				t2.side_promo_flag 
			from
				casuser.abt5_ml as t1
			left join
				casuser.abt_promo as t2
			on
				t1.product_id = t2.product_id and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.SALES_DT = t2.SALES_DT and
				t1.CHANNEL_CD = t2.CHANNEL_CD
		;
	quit;
/* ------------ End. Добавляем промо в основную витрину ---------------------------- */


	proc casutil;
		droptable casdata="abt_promo" incaslib="casuser" quiet;
		droptable casdata="abt5_ml" incaslib="casuser" quiet;
	run;


/************************************************************************************
 *	4.	Добавление внешних факторов													*
 ************************************************************************************/
/*			Стандартными фичами в фастфуда (можно посмотреть научные статьи ваитрине
 *		интернете) являются параметры макроэкономики, погода и информация о 
 *		конкурентах. В рамках проекта проведенные исследования показывают, что 
 *		на краткосрочный прогноз данные факторы не влияют. Тем более не известны
 *		значения этих факторов на будущее.
 *			Тем не менее мы добавим эти факторы, в надежде, что погода и осадки лучше 
 *		объяснят историю и в будущем при квантовом прогнозировании прогноза погоды
 *		он позволит улучшить качество прогноза спроса. Также добавим факторы макроэкономики
 *		на случай если в будущей получится снизить их гранулярность с уровня квартала 
 *		до уровня дня
 *
 *			В ходе проекта получено преобразование температуры воздуха, которое 
 *		потенциально лучше объясняет историю, чем просто средняя температура за день.
 *		
 *		??? надо добавить это преобразование как фичу. Пример преобразования приведен
 *		вот здесь: https://team-1619423095896.atlassian.net/browse/MSTF-30
 *		??? Историю погоды надо брать с 2014 года. Сереже и Никите написал
 *		
 */


	proc casutil;
		droptable casdata="abt7_ml" incaslib="casuser" quiet;
	run;


/* ------------ Start. Добавляем параметры макроэкономики (CPI, GDP, RDI) --------- */
	proc fedsql sessref = casauto;
		create table casuser.abt7_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t2.A_CPI,
				t2.A_GPD,
				t2.A_RDI
			from
				casuser.abt6_ml as t1 left join 
				&lmvWorkCaslib..macro_transposed_ml as t2
			on
				t1.sales_dt = t2.period_dt
		;
	quit;
/* ------------ End. Добавляем параметры макроэкономики --------------------------- */


	proc casutil;
	  droptable casdata="abt6_ml" incaslib="casuser" quiet;
	   droptable casdata = "abt8_ml" incaslib = "casuser" quiet;
	run;


/* ------------ Start. Добавляем температуру и осадки ----------------------------- */
/*			??? Необходимо добавить фичу отклонение температуры от среднеклиматической
 *		нормы. Описание см. в начале раздела про внешние факторы
 */
	proc fedsql sessref =casauto;
		create table casuser.abt8_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t2.TEMPERATURE,
				t2.PRECIPITATION
			from
				casuser.abt7_ml as t1
			left join
				&lmvWorkCaslib..weather as t2
			on 
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = datepart(t2.REPORT_DT)
		;
	quit;
/* ------------ End. Добавляем температуру и осадки ------------------------------- */


	proc casutil;
	  droptable casdata="abt7_ml" incaslib="casuser" quiet;
	run;


/* ------------ Start. Добавляем TRP конкурентов ---------------------------------- */
/* 			??? Информация по TRP конкурентов предоставляется по неделям. Сейчас мы
 *		просто протягиваем TRP за всю неделю на каждый день. Тонкий момент, возможно
 *		это не совсем правильно.
 */
	proc casutil;
		droptable casdata="abt9_ml" incaslib="casuser" quiet;
	run;


	proc fedsql sessref = casauto;
		create table casuser.abt9_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t2.comp_trp_BK,
				t2.comp_trp_KFC
			from
				casuser.abt8_ml as t1
			left join
				&lmvWorkCaslib..comp_transposed_ml_expand as t2
			on
				t1.sales_dt = t2.REPORT_DT
		;
	quit;
/* ------------ End. Добавляем TRP конкурентов ------------------------------------ */


	proc casutil;
	    droptable casdata="abt8_ml" incaslib="casuser" quiet;
	run;


/* ------------ Start. Добавляем медиаподдержку (McDonald's TRP) ------------------ */
/* 			??? Информация по TRP на промо. Леша Щекалев нашел, что мы не до конца
 *		правильно протягиваем промо по дням (где промо заканчивается в середине недели). 
 *		Надо синхронизироваться с Лешей по тому, как правильно считать TRP.
 */
	proc casutil;
	  droptable casdata="abt10_ml" incaslib="casuser" quiet;
	run;


	proc fedsql sessref=casauto;
		create table casuser.abt10_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t2.sum_trp
			from
				casuser.abt9_ml as t1
			left join
				&lmvWorkCaslib..sum_trp as t2
			on 
				t1.product_id = t2.PRODUCT_LEAF_ID and
				t1.pbo_location_id = t2.PBO_LEAF_ID and
				t1.sales_dt = t2.report_dt
		;
	quit;
/* ------------ End. Добавляем медиаподдержку (McDonald's TRP) -------------------- */


	proc casutil;
		droptable casdata="abt9_ml" incaslib="casuser" quiet;
		 droptable casdata="abt11_ml" incaslib="casuser" quiet;
	run;


/* ------------ Start. Добавляем характеристики и иерархию продуктов -------------- */
	proc fedsql sessref=casauto;
		create table casuser.abt11_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t2.prod_lvl4_id, 
				t2.prod_lvl3_id,
				t2.prod_lvl2_id,
				t2.a_hero_id as hero,
				t2.a_item_size_id as item_size,
				t2.a_offer_type_id as offer_type,
				t2.a_price_tier_id as price_tier
		from
			casuser.abt10_ml as t1
		left join
			&lmvWorkCaslib..product_dictionary_ml as t2
		on
			t1.product_id = t2.product_id
		;
	quit;
/* ------------ End. Добавляем характеристики и иерархию продуктов ---------------- */

	 
	proc casutil;
	  droptable casdata="abt10_ml" incaslib="casuser" quiet;
	   droptable casdata="abt12_ml" incaslib="casuser" quiet;
	run;
	

/* ------------ Start. Добавляем характеристики и иерархию ресторанов ------------- */
	proc fedsql sessref=casauto;
		create table casuser.abt12_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t2.lvl3_id,
				t2.lvl2_id,
				t2.A_AGREEMENT_TYPE_id as agreement_type,
				t2.A_BREAKFAST_id as breakfast,
				t2.A_BUILDING_TYPE_id as building_type,
				t2.A_COMPANY_id as company,
				t2.A_DELIVERY_id as delivery,
				t2.A_DRIVE_THRU_id as drive_thru,
				t2.A_MCCAFE_TYPE_id as mccafe_type,
				t2.A_PRICE_LEVEL_id as price_level,
				t2.A_WINDOW_TYPE_id as window_type
			from
				casuser.abt11_ml as t1
			left join
				&lmvWorkCaslib..pbo_dictionary_ml as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
/* ------------ End. Добавляем характеристики и иерархию ресторанов --------------- */


	proc casutil;
		droptable casdata="abt11_ml" incaslib="casuser" quiet;
		droptable casdata="abt13_ml" incaslib="casuser" quiet;
	run;


/* ------------ Start. Добавляем характеристики даты (WOY, DOW, MNTH) ------------- */
	proc fedsql sessref = casauto;
		create table casuser.abt13_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t2.week,
				t2.weekday,
				t2.month,
				t2.weekend_flag
			from
				casuser.abt12_ml as t1
			left join
				&lmvWorkCaslib..cldr_prep_features as t2
			on
				t1.sales_dt = t2.date
		;
	quit;
/* ------------ End. Добавляем характеристики даты (WOY, DOW, MNTH) --------------- */


/* ------------ Start. Добавляем события/праздники -------------------------------- */
	proc casutil;
		droptable casdata="abt14_ml" incaslib="casuser" quiet;
		droptable casdata="abt12_ml" incaslib="casuser" quiet;
	run;


	proc fedsql sessref=casauto;
		create table casuser.abt14_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t1.week,
				t1.weekday,
				t1.month,
				t1.weekend_flag,
				coalesce(t2.defender_day, 0) as defender_day,
				coalesce(t2.female_day, 0) as female_day,
				coalesce(t2.may_holiday, 0) as may_holiday,
				coalesce(t2.new_year , 0) as new_year,
				coalesce(t2.russia_day, 0) as russia_day,
				coalesce(t2.school_start, 0) as school_start,
				coalesce(t2.student_day, 0) as student_day,
				coalesce(t2.summer_start, 0) as summer_start,
				coalesce(t2.valentine_day, 0) as valentine_day
			from
				casuser.abt13_ml as t1
			left join
				&lmvWorkCaslib..russia_event_t as t2
			on
				t1.sales_dt = t2.date
		;	
	quit;
/* ------------ End. Добавляем события/праздники ---------------------------------- */


	proc casutil;
		droptable casdata="abt13_ml" incaslib="casuser" quiet;
	run;


/************************************************************************************
 *	5.	Добавление ценовых рангов													*
 ************************************************************************************/
/*			Ценовые характеристики позволяют частино учесть эффект каннибализации 
 *		внутри категорий товаров. Делятся на два типа:
 *			- ценовой индекс: отношение цены на товар к средней цене категории без 
 *		учета этого товара. Уровень PBO-SKU-DATE
 *			- ценовой ранг: ранг цены товара в категории. 0 - самый дешевый товар
 *		в категории, максимальный ранг - самый дорогой товар категории. Уровень
 *		PBO-SKU-DATE.
 *			??? почему мы во всем блоке не используем канал CHANNEL_CD
 */
 
 
	proc casutil;
		droptable casdata="abt15_ml" incaslib="casuser" quiet;
		droptable casdata="unique_day_price" incaslib="casuser" quiet;
		droptable casdata="sum_count_price" incaslib="casuser" quiet;
		droptable casdata="price_rank" incaslib="casuser" quiet;
		droptable casdata="price_rank2" incaslib="casuser" quiet;
		droptable casdata="price_rank3" incaslib="casuser" quiet;
		droptable casdata="price_feature" incaslib="casuser" quiet;
	run;


/* ------------ Start. Подготавливаем базовую табличку для расчета ---------------- */
/* уникальные ПБО/день/категория товаров/товар/цена */


	proc fedsql sessref = casauto;
		create table casuser.unique_day_price as 
			select distinct
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.product_id,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt14_ml as t1
		;
	quit;
/* ------------ End. Подготавливаем базовую табличку для расчета ------------------ */


/* ------------ Start. Считаем суммарную цену в групе и количество товаров -------- */
	proc fedsql sessref = casauto;
		create table casuser.sum_count_price{options replace = true} as
			select
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				count(t1.product_id) as count_product,
				sum(t1.GROSS_PRICE_AMT) as sum_gross_price_amt
			from casuser.unique_day_price as t1
			group by
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt
		;
	quit;
/* ------------ End. Считаем суммарную цену в групе и количество товаров ---------- */


/* ------------ Start. Считаем позицию товара в отсортированном списке цен -------- */
	data casuser.price_rank / sessref = casauto;
		set casuser.unique_day_price;
		by pbo_location_id sales_dt PROD_LVL3_ID GROSS_PRICE_AMT ;
		if first.PROD_LVL3_ID then i = 0;
		if GROSS_PRICE_AMT ^= lag(GROSS_PRICE_AMT) then i+1;
	run;
/* ------------ End. Считаем позицию товара в отсортированном списке цен ---------- */


/* ------------ Start. Считаем максимальный ранг в рамках категории --------------- */
	proc fedsql sessref = casauto;
		create table casuser.price_rank2{options replace=true} as
			select
				t1.pbo_location_id,
				t1.sales_dt,
				t1.PROD_LVL3_ID,
				max(t1.i) as max_i
			from
				casuser.price_rank as t1
			group by
				t1.pbo_location_id,
				t1.sales_dt,
				t1.PROD_LVL3_ID
		; 
	quit;
/* ------------ End. Считаем максимальный ранг в рамках ктегории ------------------ */


/* ------------ Start. Соединяем таблицы price_rank, price_rank2 ------------------ */
	proc fedsql sessref=casauto;
		create table casuser.price_rank3{options replace=true} as
			select
				t1.product_id,
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.GROSS_PRICE_AMT,
				t1.i,
				t2.max_i
			from
				casuser.price_rank as t1
			left join
				casuser.price_rank2 as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
				t1.sales_dt = t2.sales_dt
		;
	quit;
/* ------------ End. Соединяем таблицы price_rank, price_rank2 -------------------- */


/* ------------ Start. Соединяем PRICE_RANK и считаем PRICE_INDEX ----------------- */
	proc fedsql sessref=casauto;
		create table casuser.price_feature{options replace=true} as
			select
				t1.product_id,
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.GROSS_PRICE_AMT,
				t1.i,
				t1.max_i,
				t2.count_product,
				t2.sum_gross_price_amt,
				divide(t1.i,t1.max_i) as price_rank,
				(
					case
						when t2.sum_gross_price_amt = t1.GROSS_PRICE_AMT then 1
						else divide(t1.GROSS_PRICE_AMT,
									divide((t2.sum_gross_price_amt - t1.GROSS_PRICE_AMT),
											(t2.count_product - 1)
											)
									)
					end
				) as price_index
			from
				casuser.price_rank3 as t1
			left join
				casuser.sum_count_price as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
				t1.sales_dt = t2.sales_dt
			where GROSS_PRICE_AMT is not null
		;
	quit;
/* ------------ End. Соединяем PRICE_RANK и считаем PRICE_INDEX ------------------- */


/* ------------ Start. Добавляем PRICE_RANK и PRICE_INDEX в витрину --------------- */
	proc fedsql sessref = casauto;
		create table casuser.abt15_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t1.week,
				t1.weekday,
				t1.month,
				t1.weekend_flag,
				t1.defender_day,
				t1.female_day,
				t1.may_holiday,
				t1.new_year,
				t1.russia_day,
				t1.school_start,
				t1.student_day,
				t1.summer_start,
				t1.valentine_day, 
				t2.price_rank,
				t2.price_index
			from
				casuser.abt14_ml as t1
			left join
				casuser.price_feature as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id and
				t1.sales_dt = t2.sales_dt
		;
	quit;
/* ------------ End. Добавляем PRICE_RANK и PRICE_INDEX в витрину ----------------- */


	proc casutil;
		droptable casdata="unique_day_price" incaslib="casuser" quiet;
		droptable casdata="sum_count_price" incaslib="casuser" quiet;
		droptable casdata="price_rank" incaslib="casuser" quiet;
		droptable casdata="price_rank2" incaslib="casuser" quiet;
		droptable casdata="price_rank3" incaslib="casuser" quiet;
		droptable casdata="price_feature" incaslib="casuser" quiet;
		droptable casdata="abt14_ml" incaslib="casuser" quiet;
	run;



	proc casutil;
		droptable casdata="abt16_ml" incaslib="casuser" quiet;
	run;

/* ------------ Start. Перекодируем channel_cd ------------------------------------ */
/* ???Что делает этот макрос */
	%text_encoding(mpTable=casuser.abt15_ml, mpVariable=channel_cd);
/* ------------ End. Перекодируем channel_cd -------------------------------------- */


/* ------------ Start. Заменяем текстовое поле на числовое ------------------------ */
/* ???Где происходит замена */
/* ???Мы убираем CHANNEL_CD. Мы уверены, что не будет коллизии в CHANNEL_CD_id*/
	proc fedsql sessref = casauto;
		create table casuser.abt16_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD_id as channel_cd,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				/*
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				*/
				&promo_list_sql.,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t1.week,
				t1.weekday,
				t1.month,
				t1.weekend_flag,
				t1.defender_day,
				t1.female_day,
				t1.may_holiday,
				t1.new_year,
				t1.russia_day,
				t1.school_start,
				t1.student_day,
				t1.summer_start,
				t1.valentine_day, 
				t1.price_rank,
				t1.price_index
			from
				casuser.abt15_ml as t1
		;
	quit;
/* ------------ End. Заменяем текстовое поле на числовое -------------------------- */


	proc casutil;
		droptable casdata="abt15_ml" incaslib="casuser" quiet;
	quit;

/* ------------ Start. Формируем выходные таблички -------------------------------- */
	proc fedsql sessref=casauto;
	%if &lmvMode. = A or &lmvMode = T %then %do;
		create table casuser.&lmvTabNmOutTrain.{options replace = true} as 
			select *
			from casuser.abt16_ml 
			
			where sales_dt >= date %str(%')%sysfunc(putn(&lmvTrainStartDate., yymmdd10.))%str(%')
			  and sales_dt <= date %str(%')%sysfunc(putn(&lmvTrainEndDate.  , yymmdd10.))%str(%')
			;
	%end;
	%if &lmvMode. = A or &lmvMode = S %then %do;
		create table casuser.&lmvTabNmOutScore.{options replace = true} as 
			select * 
			from casuser.abt16_ml 
			
			where sales_dt >= date %str(%')%sysfunc(putn(&lmvScoreStartDate., yymmdd10.))%str(%')
			  and sales_dt <= date %str(%')%sysfunc(putn(&lmvScoreEndDate.  , yymmdd10.))%str(%')
		;
	%end;
	quit;
/* ------------ End. Формируем выходные таблички ---------------------------------- */


/* ------------ Start. Сохраняем целевые таблицы и удаляем промежуточные ---------- */
	proc casutil;
		droptable casdata="abt16_ml" incaslib="casuser" quiet;
		promote casdata="&lmvTabNmOutTrain." incaslib="casuser" outcaslib="&lmvLibrefOutTrain.";
		promote casdata="&lmvTabNmOutScore." incaslib="casuser" outcaslib="&lmvLibrefOutScore.";
		save incaslib="&lmvLibrefOutScore." outcaslib="&lmvLibrefOutScore." casdata="&lmvTabNmOutScore." casout="&lmvTabNmOutScore..sashdat" replace; 
		save incaslib="&lmvLibrefOutTrain." outcaslib="&lmvLibrefOutTrain." casdata="&lmvTabNmOutTrain." casout="&lmvTabNmOutTrain..sashdat" replace;
/* 		droptable casdata="&lmvTabNmOutScore." incaslib="casuser" quiet; */
/* 		droptable casdata="&lmvTabNmOutTrain." incaslib="casuser" quiet; */
	quit;
/* ------------ End. Сохраняем целевые таблицы и удаляем промежуточные ------------ */


%mend rtp_1_load_data_product;