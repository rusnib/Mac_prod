
cas casauto;
caslib _all_ assign;
/*
proc fedsql sessref=casauto;
	create table casuser.test_fcst{options replace=true} as
		select CHANNEL_CD
				,count(SALES_DT) as count
				,sum(P_SUM_QTY) as P_SUM_QTY
		
		from MN_SHORT.PMIX_DAYS_RESULT
		
		group by CHANNEL_CD
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_train{options replace=true} as
		select CHANNEL_CD
				,count(SALES_DT) as count
				,sum(SUM_QTY) as SUM_QTY
		
		from MN_SHORT.ALL_ML_TRAIN
		
		group by CHANNEL_CD
	;
quit;
*/

data casuser.PMIX_DAYS_RESULT;
	set MN_SHORT.PMIX_DAYS_RESULT
	;
where CHANNEL_CD = 2;
run;

data casuser.FACT_N_FCST;
	set 
		MN_SHORT.ALL_ML_TRAIN 
		casuser.PMIX_DAYS_RESULT
	;
	keep 
		SUM_QTY P_SUM_QTY 
		product_id /*PROD_LVL2_ID*/
		pbo_location_id /*LVL2_ID*/
		SALES_DT
	;
run;

proc casutil;
	droptable 
		casdata="FACT_N_FCST_AGGR" 
		incaslib="casuser" 
		quiet
	;
run;

proc fedsql sessref=casauto;
	create table casuser.FACT_N_FCST_AGGR{options replace=true} as
		select
/* 			t1.pbo_location_id,  */
/* 			t1.product_id, */
			t2.LVL2_ID as PBO_LVL2_ID, 
			t3.LVL2_ID as PROD_LVL2_ID,
			t1.SALES_DT,
			sum(t1.SUM_QTY) as SUM_QTY,
			sum(t1.P_SUM_QTY) as P_SUM_QTY
		from casuser.FACT_N_FCST as t1
		inner join casuser.pbo_hier_flat as t2
			on t1.pbo_location_id=t2.PBO_LOCATION_ID
		inner join casuser.product_hier_flat as t3
			on t1.product_id=t3.product_id
		group by t2.LVL2_ID , t3.LVL2_ID, t1.SALES_DT
	;
quit;

data casuser.test_full;
set casuser.FACT_N_FCST_AGGR;
where PROD_LVL2_ID = 244 and PBO_LVL2_ID = 441;
run;

proc casutil;
	promote 
		casdata="FACT_N_FCST_AGGR" 
		casout="FACT_N_FCST_AGGR"  
		incaslib="casuser" 
		outcaslib="casuser"
	;
run;


data casuser.test_full_my;
set MAX_CASL.UNITS_ABT_MAY_TEST;
where PROD_LVL2_ID = 244 and LVL2_ID = 441;
keep LVL2_ID PROD_LVL2_ID SALES_DT sum_qty;
run;
