%fcst_create_abt_pbo_gc(mpMode=gc
	 ,mpOutTableDmVf = PUBLIC.NIK_T1
	 ,mpOutTableDmABT = PUBLIC.NIK_T2
	 );

data MN_DICT.DM_TRAIN_TRP_GC;
set PUBLIC.NIK_T1;
run;

proc casutil incaslib='MN_DICT' outcaslib='MN_DICT';
	promote casdata='DM_TRAIN_TRP_GC';
run;

proc fedsql sessref=casauto;
	create table casuser.test {options replace=true} as
		select 
			min(sales_dt	) as min_dt
			,max(sales_dt	) as max_dt
		from
			MN_DICT.DM_TRAIN_TRP_GC as main
		
	;
quit;

/*
Сджойнить витрины GC full join
Посмотреть как изменилось кол-во строк

просуммировать все поля
запромоутить 
построить отчет в VA

вычислить минимальную и максимальные даты из каждой таблицы
1691932


PUBLIC.FULL_JOIN_GC

PUBLIC.GC_SM_TRAIN_TRP
1688929


MN_DICT.DM_TRAIN_TRP_GC
997177


*/

proc fedsql sessref=casauto;
	create table casuser.test_my1 {options replace=true} as
		select 
			main.CHANNEL_CD, 		
			main.PBO_LOCATION_ID,
			sales_dt	
		from
			PUBLIC.GC_TRAIN_ABT_TRP as main
		where SALES_DT >= date '2017-01-01'  
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_my2 {options replace=true} as
		select distinct
			main.CHANNEL_CD, 		
			main.PBO_LOCATION_ID
		from
			casuser.test_my1 as main
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_nik1 {options replace=true} as
		select 
			main.CHANNEL_CD, 		
			main.PBO_LOCATION_ID,
			sales_dt	
		from
			PUBLIC.NIK_T2 as main
		where SALES_DT >= date '2017-01-01'  
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_nik2 {options replace=true} as
		select distinct
			main.CHANNEL_CD, 		
			main.PBO_LOCATION_ID
		from
			casuser.test_nik1 as main
	;
quit;

proc fedsql sessref=casauto;
	create table PUBLIC.NIK_T1_UPD {options replace=true} as
		select main.*
			
		from
			PUBLIC.NIK_T1 as main
/* 			casuser.test_nik as nik */
/* 		where main.CHANNEL_CD 		= nik.CHANNEL_CD 		and */
/* 			main.PBO_LOCATION_ID 	= nik.PBO_LOCATION_ID 	and */
/* 			main.SALES_DT 			= nik.SALES_DT */
	;
quit;

proc casutil incaslib='casuser' outcaslib='casuser';
	droptable casdata='FULL_JOIN_GC' quiet;
run;
proc fedsql sessref=casauto;
	create table casuser.FULL_JOIN_GC {options replace=true} as
		select
			main.*
			, nik.COVID_PATTERN 	as nik_COVID_PATTERN 	
			, nik.COVID_LOCKDOWN 	as nik_COVID_LOCKDOWN 	
			, nik.COVID_LEVEL		as nik_COVID_LEVEL		
			, nik.SUM_TRP_LOG		as nik_SUM_TRP_LOG		
			, nik.TARGET			as nik_TARGET	
		from
			PUBLIC.GC_SM_TRAIN_TRP as main
		left join
			PUBLIC.NIK_T1 /*MN_DICT.DM_TRAIN_TRP_GC*/ as nik
		on
			main.CHANNEL_CD 		= nik.CHANNEL_CD 		and
			main.PBO_LOCATION_ID 	= nik.PBO_LOCATION_ID 	and
			main.SALES_DT 			= nik.SALES_DT
	;
quit;

proc casutil incaslib='casuser' outcaslib='casuser';
	promote casdata='FULL_JOIN_GC';
run;


proc fedsql sessref=casauto;
	create table casuser.FULL_JOIN_GC_2 {options replace=true} as
		select
			  coalesce(main.PBO_LOCATION_ID , nik.PBO_LOCATION_ID ) as PBO_LOCATION_ID 
			, coalesce(main.SALES_DT , nik.SALES_DT ) as SALES_DT  		
			, main.Deseason_sm_multi 
			, nik.Deseason_sm_multi as Deseason_sm_multi_nik
		from
			PUBLIC.GC_TRAIN_ABT_TRP as main
		full join
			PUBLIC.NIK_T2 /*MN_DICT.DM_TRAIN_TRP_GC*/ as nik
		on
			main.CHANNEL_CD 		= nik.CHANNEL_CD 		and
			main.PBO_LOCATION_ID 	= nik.PBO_LOCATION_ID 	and
			main.SALES_DT 			= nik.SALES_DT
		where main.CHANNEL_CD = 'ALL'
	;
quit;

proc casutil incaslib='casuser' outcaslib='casuser';
	promote casdata='FULL_JOIN_GC_2';
run;


proc fedsql sessref=casauto;
	create table casuser.FULL_JOIN_GC {options replace=true} as
		select
			main.*
			, nik.COVID_PATTERN 	as nik_COVID_PATTERN 	
			, nik.COVID_LOCKDOWN 	as nik_COVID_LOCKDOWN 	
			, nik.COVID_LEVEL		as nik_COVID_LEVEL		
			, nik.SUM_TRP_LOG		as nik_SUM_TRP_LOG		
			, nik.TARGET			as nik_TARGET	
		from
			PUBLIC.GC_SM_TRAIN_TRP as main
		full join
			MN_DICT.DM_TRAIN_TRP_GC as nik
		on
			main.CHANNEL_CD 		= nik.CHANNEL_CD 		and
			main.PBO_LOCATION_ID 	= nik.PBO_LOCATION_ID 	and
			main.SALES_DT 			= nik.SALES_DT
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.test1 {options replace=true} as
		select *
		from casuser.FULL_JOIN_GC
		where sales_dt = '10jul2019'
	;
quit;

data WORK.test1;
set casuser.FULL_JOIN_GC;
run;

data WORK.test2;
set WORK.test1;
where sales_dt = '10jul2019'd ;
run;

proc sql;
create table work.test3 as
select * 
from work.test2
where int(target) <> int(nik_target)
;
quit;


/******************************************************************/

proc fedsql sessref=casauto;
	create table casuser.test_gc_fcst {options replace=true} as
		select 
			min(sales_dt	) as min_dt
			,max(sales_dt	) as max_dt
			,count(distinct pbo_location_id) as count_pbo
			,sum(gc_fcst) as gc_fcst
		from
			MN_DICT.GC_FORECAST_RESTORED as main
		group by sales_dt
		
	;
quit;
