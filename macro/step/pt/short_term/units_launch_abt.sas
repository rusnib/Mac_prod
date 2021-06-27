cas casauto;
caslib _all_ assign;


%add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt,
							PromoCalculationRk=);

/* %rtp_load_data_to_caslib(mpWorkCaslib=mn_short); */


/* Список всех ПБО из справочника и дат их открытия-закрытия*/
/*
%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
%include "&common_path./data_prep_pbo.sas"; 
%data_prep_pbo(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PBO_DICTIONARY_ML
);
%include "&common_path./data_prep_product.sas"; 
%data_prep_product(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PRODUCT_DICTIONARY_ML
);

proc casutil;
	  load data=IA.IA_PMIX_SALES casout='PMIX_SALES' outcaslib='casuser' replace;
run;
*/

/* %my_rtp_1_load_data_product( */
/* 			  mpMode		= A */
/* 			, mpRetroLaunch = N */
/* 			, mpOutTrain	= casuser.all_ml_train */
/* 			, mpOutScore	= casuser.all_ml_scoring */
/* 			, mpWorkCaslib	= mn_short */
/* 	); */



/* Дата начала прогнозирования и текущая дата и дата начала скоринговой выборки:
'01dec2020'd
'01jan2021'd
'27feb2021'd
*/

%let ETL_CURRENT_DT      =  '27feb2021'd;

%let VF_FC_HORIZ					=  104;
%let VF_FC_START_DT 				= date%str(%')%sysfunc(putn(%sysfunc(intnx(day/*week.2*/,&ETL_CURRENT_DT.,0,b)),yymmdd10.))%str(%'); 
%let VF_FC_START_DT_SAS				= %sysfunc(inputn(%scan(%bquote(&VF_FC_START_DT.),2,%str(%')),yymmdd10.));
%let VF_FC_START_MONTH_SAS 			= %sysfunc(intnx(month,&VF_FC_START_DT_SAS,0,b));
%let VF_HIST_END_DT 				= %sysfunc(intnx(day,&VF_FC_START_DT_SAS,-1),yymmddd10.);	
%let VF_HIST_END_DT_SAS				= %sysfunc(inputn(&VF_HIST_END_DT.,yymmdd10.));	
%let VF_FC_END_DT 					= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*(&VF_FC_HORIZ-1)),yymmddd10.);		
%let VF_FC_AGG_END_DT 				= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*&VF_FC_HORIZ-1),yymmddd10.);
%let VF_FC_AGG_END_DT_SAS 			= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*&VF_FC_HORIZ-1));
%let VF_HIST_START_DT 				= date'2017-01-02';
%let VF_HIST_START_DT_SAS			= %sysfunc(inputn(%scan(%bquote(&VF_HIST_START_DT),2,%str(%')),yymmdd10.));
%let VF_FC_END_SHORT_DT_SAS			= %sysfunc(intnx(day, &VF_FC_START_DT_SAS., 90));
%let VF_FC_END_SHORT_DT 			= date%str(%')%sysfunc(putn(&VF_FC_END_SHORT_DT_SAS.,yymmdd10.))%str(%');

%let lmvTrainStartDate 	= %sysfunc(intnx(year,&etl_current_dt.,-2,s));		/* Дата начала обучающей выборки */
%let lmvTrainEndDate 	= &VF_HIST_END_DT_SAS.;								/* Дата окончания обучающей выборки */
%let lmvScoreStartDate 	= %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,1,s));	/* Дата начала обучающей выборки */
%let lmvScoreEndDate 	= %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));	/* Дата окончания обучающей выборки */

%put &=lmvTrainStartDate;
%put &=lmvTrainEndDate;
%put &=lmvScoreStartDate;
%put &=lmvScoreEndDate;

data _null_;
format train_start_dt 	date9.;
format train_end_dt 	date9.;
format score_start_dt 	date9.;
format score_end_dt 	date9.;
train_start_dt 	= &lmvTrainStartDate.;
train_end_dt 	= &lmvTrainEndDate.;
score_start_dt 	= &lmvScoreStartDate.;
score_end_dt 	= &lmvScoreEndDate.;
put train_start_dt	=;
put train_end_dt	=;
put score_start_dt	=;
put score_end_dt	=;
run;

%rtp_1_load_data_product(
			  mpMode		= A
			, mpRetroLaunch = Y
			, mpOutTrain	= max_casl.all_ml_train_mar
			, mpOutScore	= max_casl.all_ml_scoring_mar
			, mpWorkCaslib	= mn_short
	);


/* ************************************************************************************ */


proc casutil;
	droptable 
		casdata		= "all_ml_mar" 
		incaslib	= "max_casl" 
		quiet         
	;                 
run; 

data max_casl.all_ml_mar;
	set max_casl.all_ml_train_mar
		max_casl.all_ml_scoring_mar
	;
	keep channel_cd product_id pbo_location_id sales_dt sum_qty lag_week_med DISCOUNT EVM_SET lag_month_med COMP_TRP_BK COMP_TRP_KFC BOGO ;
run;

proc casutil;         
	promote           
		casdata		= "all_ml_mar" 
		incaslib	= "max_casl" 
		casout		= "all_ml_mar"  
		outcaslib	= "max_casl"
	;                 
run; 


/* ************************************************************************************ */


proc fedsql sessref=casauto; 
			create table max_casl.train_prod_part{options replace=true} as
			select t1.*
			from 
				MN_SHORT.ALL_ML_TRAIN as t1
				
			inner join
				 MN_SHORT.product_dictionary_ml as t2 
			on
				t1.product_id = t2.product_id

			inner join
				 MN_SHORT.pbo_dictionary_ml as t3
			on
				t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
				
			where   
				/* !!! Filter begin !!! */
				t1.CHANNEL_CD = 1
				and t2.prod_lvl2_id = 90
				and t3.lvl2_id 		= 74
				/* !!! Filter end !!! */
				
		;
	quit;




/* Сравнить abt4_ml_2 vs. abt4_ml_3 */



proc fedsql sessref=casauto;
	create table casuser.stat_2{options replace=true} as
	select sales_dt
		, count(sales_dt) as count_obs
		, count(distinct CHANNEL_CD) as count_ch
		, count(distinct product_id) as count_sku
		, count(distinct pbo_location_id) as count_loc
		, sum(sum_qty) as sum_qty
	from CASUSER.ABT4_ML_2
	group by sales_dt
	;
quit;


proc fedsql sessref=casauto;
	create table casuser.stat_3{options replace=true} as
	select sales_dt
		, count(sales_dt) as count_obs
		, count(distinct CHANNEL_CD) as count_ch
		, count(distinct product_id) as count_sku
		, count(distinct pbo_location_id) as count_loc
		, sum(sum_qty) as sum_qty
	from CASUSER.ABT4_ML_3
	group by sales_dt
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.full_join{options replace=true} as
	select 
		  coalesce(t1.sales_dt, t2.sales_dt) as sales_dt
		, t1.count_obs as count_obs_2
		, t2.count_obs as count_obs_3
		, t1.count_ch as count_ch_2
		, t2.count_ch as count_ch_3
		, t1.count_sku as count_sku_2
		, t2.count_sku as count_sku_3
		, t1.count_loc as count_loc_2
		, t2.count_loc as count_loc_3
		, t1.sum_qty as sum_qty_2
		, t2.sum_qty as sum_qty_3		


	from casuser.stat_2 as t1
	full join casuser.stat_3 as t2
	on  t1.sales_dt 		= t2.sales_dt
	;
quit;


ods excel 
	file='/opt/sas/mcd_config/macro/step/pt/short_term/chain_applying.xlsx' 
	style=statistical
	;

	proc print data=casuser.full_join; 
	run;

ods excel 
	close;