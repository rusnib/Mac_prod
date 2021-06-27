cas casauto;
caslib _all_ assign;


proc casutil;
	  load data=IA.ia_pbo_close_period casout='ia_pbo_close_period' outcaslib='casuser' replace;
run;

/* заполняем пропуски в end_dt */
proc fedsql sessref=casauto;
	create table casuser.pbo_closed_ml {options replace=true} as
		select 
			CHANNEL_CD,
			PBO_LOCATION_ID,
			datepart(start_dt) as start_dt,
			coalesce(datepart(end_dt), date '2100-01-01') as end_dt,
			CLOSE_PERIOD_DESC
		from
			casuser.ia_pbo_close_period
	;
quit;

/* Удаляем даты закрытия pbo из abt */
proc fedsql sessref=casauto;
	create table casuser.gc_days{options replace=true} as
		select 
			t1.*
		from
			MN_DICT.GC_FORECAST_RESTORED as t1
		left join
			casuser.pbo_closed_ml as t2
		on
			t1.sales_dt >= t2.start_dt and
			t1.sales_dt <= t2.end_dt and
			t1.pbo_location_id = t2.pbo_location_id and
			t1.channel_cd = t2.channel_cd
		where
			t2.pbo_location_id is missing
	;
quit;


data casuser.d_before;
	set MN_DICT.GC_FORECAST_RESTORED;
	where channel_cd = 'ALL';
run;

data casuser.d_after;
	set casuser.gc_days;
	where channel_cd = 'ALL';
run;


proc fedsql sessref=casauto;
	create table casuser.distinct_after {options replace=true} as
		select distinct pbo_location_id
		from casuser.gc_days;
/* 		where channel_cd = "ALL" */
	;
	create table casuser.distinct_before {options replace=true} as
		select distinct pbo_location_id
		from MN_DICT.GC_FORECAST_RESTORED;
/* 			where channel_cd = "ALL" */
	;
quit;
