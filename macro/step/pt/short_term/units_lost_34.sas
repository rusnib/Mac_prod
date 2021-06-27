
cas casauto;
caslib _all_ assign;


%add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt,
							PromoCalculationRk=);
/* Почему потеряли 34 ресторана ??? */

/* Список комповых ресторанов: */

/* Календарь по месяцам */
data casuser.calendar(keep=mon_dt);
d1 = '1may2021'd;
d2 = '1aug2021'd;
format mon_dt date9.;
do i = 0 to intck('month', d1, d2);
	mon_dt = intnx('month', d1, i, 'B');
	output;
end;
run;

/* Список всех ПБО из справочника и дат их открытия-закрытия*/
%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
%include "&common_path./data_prep_pbo.sas"; 
%data_prep_pbo(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PBO_DICTIONARY
);


/* Расчет комповых ресторанов-месяцев */
proc fedsql sessref=casauto;
	create table casuser.comp_list{options replace=true} as
	select
		  pbo.pbo_location_id
		, pbo.LVL2_ID
		, pbo.A_OPEN_DATE
		, pbo.A_CLOSE_DATE
		, cal.mon_dt
	from 
		CASUSER.PBO_DICTIONARY as pbo
	cross join
		CASUSER.CALENDAR as cal
	where 
		intnx('month', cal.mon_dt, -12, 'b') >= 
      		case 
	   			when day(pbo.A_OPEN_DATE)=1 
					then cast(pbo.A_OPEN_DATE as date)
	   			else 
					cast(intnx('month',pbo.A_OPEN_DATE,1,'b') as date)
      		end
	    and cal.mon_dt <=
			case
				when pbo.A_CLOSE_DATE is null 
					then cast(intnx('month', date '2021-09-01', 12) as date)
				when pbo.A_CLOSE_DATE=intnx('month', pbo.A_CLOSE_DATE, 0, 'e') 
					then cast(pbo.A_CLOSE_DATE as date)
		   		else 
					cast(intnx('month', pbo.A_CLOSE_DATE, -1, 'e') as date)
			end
	;
quit;

data casuser.comp_list_jun;
	set casuser.comp_list;
	where mon_dt = '01jun2021'd;
run;


/* Те самые 34 ресторана */
proc fedsql sessref=casauto noprint;
create table casuser.lost_34{options replace=true} 
as
select
b.pbo_location_id
,b.LVL2_ID
from casuser.comp_list_jun b  
  left join (select distinct pbo_location_id from max_casl.SHARE_FCST_UNITS_N_SALE a
        where a.sales_dt between date'2021-06-01' and date'2021-06-30' ) t
on b.pbo_location_id = t.pbo_location_id
where t.pbo_location_id is null
;
quit;

/* Ищем их в скоринге */
/* 1 */
proc fedsql sessref = casauto;
	create table casuser.score_distinct{options replace=true} as 
		select distinct	
			  t1.PBO_LOCATION_ID			
		from 
			MAX_CASL.FINAL_FCST_1 as t1
/* 			MN_SHORT.ALL_ML_SCORING as t1 */
/* 			casuser.nodups as t1 */
/* 			MN_DICT.PBO_FORECAST_RESTORED as t1 */
/* 			casuser.ASSORT_MATRIX as t1 */
/* 			casuser.fact_predict_cmp_net as t1 */
/* 			MN_SHORT.PMIX_DAYS_RESULT as t1 */
/* 		inner join ( */
/* 				select distinct	channel_cd_id */
/* 				from MN_DICT.ENCODING_CHANNEL_CD 		 */
/* 				where channel_cd = 'ALL' */
/* 		 	) as t3 */
/* 		on t1.channel_cd = t3.channel_cd_id  */
		where
			 intnx('month', datepart(t1.SALES_DT), 0, 'B') >= date '2021-06-01'
 			and intnx('month', datepart(t1.SALES_DT), 0, 'B') <= date '2021-06-30'
	;
quit;



/* 2 */
proc fedsql sessref = casauto;
	create table casuser.score_inner_34_34{options replace=true} as 
		select scr.*
		from 
			casuser.score_distinct as scr
		inner join 
			casuser.lost_34 as t34
		on scr.PBO_LOCATION_ID = t34.PBO_LOCATION_ID 
	;
quit;

proc fedsql sessref = casauto;
	create table casuser.one_guy{options replace=true} as 
		select scr.*
		from 
			casuser.score_inner_34_34 as scr
		left join 
			casuser.score_inner_34 as t33
		on scr.PBO_LOCATION_ID = t33.PBO_LOCATION_ID 
		where t33.PBO_LOCATION_ID is null
	;
quit;

data casuser.one_guy;
	set casuser.one_guy;
	format PBO_LOCATION_ID best32.;
run;

/* Ищем в АМ */
proc casutil;
	  load data=IA.IA_ASSORT_MATRIX casout='ASSORT_MATRIX' outcaslib='casuser' replace;
run;

proc fedsql sessref=casauto noprint;
create table casuser.am_filt{options replace=true} 
as
select am.*
from casuser.ASSORT_MATRIX  as am
inner join casuser.lost_34 as l34
 on am.pbo_location_id = l34.pbo_location_id
    
;
quit;

data casuser.comp_list_jun;
	set casuser.comp_list;
	where mon_dt = '01jun2021'd;
run;

proc fedsql sessref=casauto;
	create table casuser.filt_abt16{options replace=true} as
	select distinct 
		  main.channel_cd
		, main.pbo_location_id
		, main.product_id
/* 		, main.sales_dt */

	from 
/* 		CASUSER.ABT16_ML as main */
		CASUSER.ALL_ML_SCORING as main
	where 
		main.sales_dt between date '2021-06-03' and date '2021-06-30'
		and channel_cd = 1
	;
quit;



proc fedsql sessref = casauto;
	create table casuser.comp_lost34_abt16{options replace=true} as 
		select scr.*
		from 
			casuser.filt_abt16 as scr
		inner join 
			casuser.lost_34 as t34
		on scr.PBO_LOCATION_ID = t34.PBO_LOCATION_ID 
	;
quit;



