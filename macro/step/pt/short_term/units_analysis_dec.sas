cas casauto;
caslib _all_ assign;

/*
proc fedsql sessref=casauto;
	create table CASUSER.TEST_MAIN_SKU {options replace=true} as
	select distinct main.successor_product_id
	from 	
		 MAX_CASL.PRODUCT_CHAIN_ENH as main
	inner join 
		MAX_CASL.DISTINCT_FJ_DEC_EXAMPLE as fj
	on 	main.successor_dim2_id = fj.pbo_location_id
	and main.successor_product_id =  fj.act_product_id
	and fj.fcst_product_id is null
	and main.predecessor_end_dt >= date '2020-12-01' 
	and main.successor_start_dt <= date '2020-12-31' 
	;
quit;
*/

%macro mCheckModelsResults(
		  lmvTrainTable		= MAX_CASL.ALL_ML_TRAIN_DEC  		/* MN_SHORT.ALL_ML_TRAIN or MAX_CASL.ALL_ML_TRAIN_DEC */
		, lmvScorTable		= MAX_CASL.ALL_ML_SCORING_DEC  		/* MN_SHORT.ALL_ML_SCORING or MAX_CASL.ALL_ML_SCORING_DEC */
		, lmvResTable		= MAX_CASL.PMIX_DAYS_RESULT_DEC  	/* MN_SHORT.PMIX_DAYS_RESULT or MAX_CASL.PMIX_DAYS_RESULT_DEC */
		, lmvModelListName  = PMIX_MODEL_TABLE					/* PMIX_MODEL_TABLE */
		, lmvModelsCasLib 	= CASUSER							/* MODELS or CASUSER */
	);

proc fedsql sessref=casauto;
	create table CASUSER.COUNT_ROWS_TRAIN {options replace=true} as
	select distinct main.pbo_location_id, main.product_id
	from &lmvTrainTable. as main
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.COUNT_ROWS_SCOR {options replace=true} as
	select distinct main.pbo_location_id, main.product_id
	from &lmvScorTable. as main
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.COUNT_ROWS_RES {options replace=true} as
	select distinct main.pbo_location_id, main.product_id
	from &lmvResTable. as main
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.COUNT_ROWS_FJ {options replace=true} as
	select 
          scor.pbo_location_id 	as scor_pbo_location_id
		, res.pbo_location_id 	as res_pbo_location_id
		, train.pbo_location_id as train_pbo_location_id

		, scor.product_id 	as scor_product_id
		, res.product_id 	as res_product_id
		, train.product_id 	as train_product_id
	from 	
		CASUSER.COUNT_ROWS_SCOR as scor

	full join 
		CASUSER.COUNT_ROWS_RES as res
	on scor.pbo_location_id = res.pbo_location_id
	and scor.product_id = res.product_id

	full join 
		CASUSER.COUNT_ROWS_TRAIN as train
	on scor.pbo_location_id = train.pbo_location_id
	and scor.product_id = train.product_id
	;
quit;

	
proc fedsql sessref=casauto;
	create table CASUSER.COUNT_ROWS_FJ_DICT {options replace=true} as
	select main.scor_pbo_location_id
		, main.res_pbo_location_id
		, main.scor_product_id
		, main.res_product_id
		, case 
			when main.res_pbo_location_id is null
			 and main.res_product_id is null
				then 0
			else 1
		  end as flag_res
		, case 
			when main.train_pbo_location_id is null
			 and main.train_product_id is null
				then 0
			else 1
		  end as flag_train
		, case 
			when main.scor_pbo_location_id is null
			 and main.scor_product_id is null
				then 0
			else 1
		  end as flag_scor
		, sku.PROD_LVL2_ID
		, sku.PROD_LVL2_NM
		, loc.LVL2_ID
		, loc.LVL2_NM
	from 	
		CASUSER.COUNT_ROWS_FJ as main
	inner join 
		MAX_CASL.PBO_DICTIONARY as loc
	on main.scor_pbo_location_id = loc.pbo_location_id
	inner join 
		MAX_CASL.PRODUCT_DICTIONARY as sku
	on main.scor_product_id = sku.product_id
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.COUNT_ROWS_FJ_COUNT {options replace=true} as
	select PROD_LVL2_ID
		, PROD_LVL2_NM
		, LVL2_ID
		, LVL2_NM
		, count(scor_pbo_location_id) as count_rows
		, sum(flag_res) as count_res
		, sum(flag_train) as count_train
		, sum(flag_scor) as count_scor
	from CASUSER.COUNT_ROWS_FJ_DICT 
	group by 1,2,3,4
	;
quit;

/* MODELS INFORMATION */
data CASUSER.MODEL_LIST;
	set &lmvModelsCasLib..&lmvModelListName.;
	PROD_LVL2_ID = input(scan('filter'n, 7), best32.);
	LVL2_ID = input(scan('filter'n, 3), best32.);
run;

proc contents 
	data = &lmvModelsCasLib.._ALL_
	out  = CASUSER.INFO
	noprint
	;
run;

proc fedsql sessref=casauto;
	create table CASUSER.MODEL_INFO {options replace=true} as
	select distinct MEMNAME
		, scan(MEMNAME, 2, '_') as num
	from CASUSER.INFO
	where MEMNAME like '%'||'FOREST'||'%' 
	;
quit;

data CASUSER.MODEL_INFO;
	set CASUSER.MODEL_INFO;
	n = input(num, best32.);
	drop num;
run;

proc fedsql sessref=casauto;
	create table CASUSER.MODELS {options replace=true} as
	select
		  main.PROD_LVL2_ID
		, main.LVL2_ID
		, main.n
		, main.score
		, main.train
		, mdl.MEMNAME as trained_model
	from 	
		CASUSER.MODEL_LIST as main
	left join 
		CASUSER.MODEL_INFO as mdl
			on main.n = mdl.n
	;
quit;

/* JOIN MODELS INFO */
proc fedsql sessref=casauto;
	create table CASUSER._MODELS_PROCESSING_STAT {options replace=true} as
	select 
		  mdl.n
		, mdl.trained_model
		, mdl.score
		, mdl.train
		, main.*
	from
		CASUSER.MODELS as mdl 	
	left join 
		CASUSER.COUNT_ROWS_FJ_COUNT as main
	on mdl.PROD_LVL2_ID = main.PROD_LVL2_ID
	and mdl.LVL2_ID = main.LVL2_ID
	;
quit;

%mend mCheckModelsResults;


/* %mCheckModelsResults( */
/* 		  lmvTrainTable		= MAX_CASL.ALL_ML_TRAIN_DEC  		 */
/* 		, lmvScorTable		= MAX_CASL.ALL_ML_SCORING_DEC  		 */
/* 		, lmvResTable		= MAX_CASL.PMIX_DAYS_RESULT_DEC  	 */
/* 		, lmvModelListName  = PMIX_MODEL_TABLE					 */
/* 		, lmvModelsCasLib 	= CASUSER							 */
/* 	); */



%mCheckModelsResults(
		  lmvTrainTable		= MN_SHORT.ALL_ML_TRAIN  		
		, lmvScorTable		= MN_SHORT.ALL_ML_SCORING  		
		, lmvResTable		= MN_SHORT.PMIX_DAYS_RESULT  	
		, lmvModelListName  = PMIX_MODEL_TABLE				
		, lmvModelsCasLib 	= MODELS						
	);