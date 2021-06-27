cas casauto;
caslib _all_ assign;

%add_promotool_marks2(
		  mpOutCaslib		 = casuser
		, mpPtCaslib		 = pt
		, PromoCalculationRk =
	);



/* *************************************************************************************** */
/* ШАГ 0. ФИЛЬТРАЦИЯ НЕ COMP */
/* !!! Или она должна быть в конце ? !!! */
/* *************************************************************************************** */
%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
%include "&common_path./data_prep_pbo.sas"; 
%data_prep_pbo(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PBO_DICTIONARY
);
/* Календарь по месяцам */
data casuser.calendar(keep=mon_dt);
d1 = '1may2021'd;
d2 = '1sep2021'd;
format mon_dt date9.;
do i = 0 to intck('month', d1, d2);
	mon_dt = intnx('month', d1, i, 'B');
	output;
end;
run;

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

proc fedsql sessref = casauto;
	create table casuser.PMIX_DAYS_RESULT_COMP{options replace=true} as 
		select
			main.*
		from 
			MN_SHORT.PMIX_DAYS_RESULT as main
		inner join 
			CASUSER.comp_list as cmp
		on 
			main.pbo_location_id = cmp.pbo_location_id
			and intnx('month', main.SALES_DT, 0, 'B') = cmp.mon_dt
		;
quit;


/* *************************************************************************************** */
/* ШАГ 1. ПРИМЕНЕНИЕ PLM */
/* !!! простая обработка, в rtp_7 чото посложнее */
/* *************************************************************************************** */

proc fedsql sessref = casauto;
	create table casuser.plm_changed{options replace=true} as 
		select
			coalesce(t2.successor_product_id, t1.product_id) as PRODUCT_ID,
			t1.PBO_LOCATION_ID,
			t3.CHANNEL_CD,
			t1.SALES_DT,
			t1.p_sum_qty
		from 
/* 			MN_SHORT.PMIX_DAYS_RESULT as t1 */
			CASUSER.PMIX_DAYS_RESULT_COMP as t1
		left join 
			CASUSER.PRODUCT_CHAIN_ENH t2
		on 
			t2.predecessor_product_id = t1.product_id and
			t2.predecessor_dim2_id = t1.pbo_location_id and
			datepart(t2.successor_start_dt) <= t1.sales_dt and 
			t2.lifecycle_cd = 'T'
		inner join 
			MN_DICT.ENCODING_CHANNEL_CD as t3
		on t1.channel_cd = t3.channel_cd_id
		;
quit;

/**** Удаление ****/
proc fedsql sessref = casauto;
	create table casuser.plm_deleted{options replace=true} as 
		select *
		from 
			casuser.plm_changed as t1
		left join 
			CASUSER.PRODUCT_CHAIN_ENH t2
		on 
			t2.predecessor_product_id = t1.product_id and
			t2.predecessor_dim2_id = t1.pbo_location_id and
			t2.lifecycle_cd = 'D'
		where 
			t1.sales_dt <= coalesce(datepart(t2.predecessor_end_dt), date '2022-12-31')
;
quit;


data casuser.nodups;
	set casuser.plm_deleted;
	by channel_cd pbo_location_id product_id sales_dt;
	if first.sales_dt then output;
run;

/* *************************************************************************************** */
/* ШАГ 2. РЕКОНСИЛЯЦИЯ */
/* !!! По идее должны быть еще новинки !!! */
/* *************************************************************************************** */


%let pmix_table = casuser.nodups;
%let pbo_table 	= MN_DICT.PBO_FORECAST_RESTORED;

/* 1.2 Реконсилируем прогноз с ПБО на мастеркод */
proc fedsql sessref=casauto;
	/* 1.2.1 Считаем распределение прогноза на уровне мастеркода */
	create table casuser.percent{options replace=true} as
		select
			t1.*,
			t2.sum_prediction,
			case 
				when t1.p_sum_qty = 0 
				then 0 
				else t1.p_sum_qty / t2.sum_prediction
			end as pcnt_prediction
		from
			&pmix_table. as t1
		inner join
			(
			select
				t1.pbo_location_id,
				t1.sales_dt,
				sum(t1.p_sum_qty) as sum_prediction
			from
				&pmix_table. as t1
			where 
				t1.channel_cd = 'ALL'
			group by
				t1.pbo_location_id,
				t1.sales_dt
			) as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.sales_dt = t2.sales_dt
		where 
			t1.channel_cd = 'ALL'
	;
quit;


/* 1.2.2 Реконсилируем прогноз с ПБО на мастеркод */
proc fedsql sessref=casauto;
	create table casuser.fact_predict_cmp_net{options replace=true} as
		select
			t1.*,
			t2.pbo_fcst,
			coalesce(t1.pcnt_prediction * t2.pbo_fcst, t1.p_sum_qty) as p_rec_sum_qty

		from
			casuser.percent as t1
		left join
			&pbo_table. as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.sales_dt = t2.sales_dt
		where
			t2.CHANNEL_CD = 'ALL'

	;
quit;


/* *************************************************************************************** */
/* ШАГ 3. ДОБАВЛЕНИЕ ЦЕН */
/* !!! простая обработка, в rtp_7 чото посложнее */
/* *************************************************************************************** */

/*таблица с ценами в разрезе товар-пбо-интервал_дат*/
proc fedsql sessref=casauto;
	select min (START_DT) from MN_DICT.PRICE_REGULAR_FUTURE;
	select max (END_DT) from MN_DICT.PRICE_REGULAR_FUTURE;
quit;

data casuser.price_reg_future;
	set MN_DICT.PRICE_REGULAR_FUTURE;
	retain _past 0;
	if start_dt ne . and end_dt ne . then
	do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
	output;
	end;
run;

data casuser.prices_flat1;
	format period_dt date9. product_id pbo_location_id 32.;
	set /*casuser.price_reg_past*/ casuser.price_reg_future;
	by product_id pbo_location_id period_dt _past;
	if first.period_dt then output;
run;

data casuser.price_promo_future;
	set MN_DICT.PRICE_promo_future;
	drop channel_cd;
	retain _past 0;
	if start_dt ne . and end_dt ne . and upcase(channel_cd)='ALL' then
	do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
	output;
	end;
run;

data casuser.prices_flat2;
	format period_dt date9. product_id pbo_location_id 32.;
	set /*casuser.price_promo_past*/ casuser.price_promo_future;
	by product_id pbo_location_id promo_id period_dt _past;
	if first.period_dt then output;
run;

proc fedsql sessref=casauto;
	create table casuser.prices_flat2_nopromo{options replace=true} as
	select period_dt, product_id, pbo_location_id,
		avg(GROSS_PRICE_AMT) as A_GROSS_PRICE_AMT,
		avg(NET_PRICE_AMT) as A_NET_PRICE_AMT,
		min(GROSS_PRICE_AMT) as M_GROSS_PRICE_AMT,
		min(NET_PRICE_AMT) as M_NET_PRICE_AMT,
		count(*) as promo_ct
	from casuser.prices_flat2
	group by 1,2,3
	;
quit;

data casuser.price_feat;
  merge casuser.prices_flat1(rename=(net_price_amt=price_reg_net gross_price_amt=price_reg_gross)) 
		casuser.prices_flat2_nopromo(rename=( M_GROSS_PRICE_AMT = price_prom_gross M_NET_PRICE_AMT=price_prom_net));
	where period_dt <= '1oct2021'd;
  by product_id pbo_location_id period_dt;

  keep product_id pbo_location_id period_dt price_reg_gross price_prom_gross
  discount_rub discount_pct promo_ct price_fact_gross price_fact_net;
  if price_prom_gross>0 then do;
	price_fact_gross=price_prom_gross ;
	price_fact_net=price_prom_net ;
    discount_rub=max(0,price_reg_gross-price_prom_gross);
	discount_pct=divide(discount_rub,price_reg_gross);
  end;
  else do;
	price_fact_gross=price_reg_gross;
	price_fact_net=price_reg_net ;
    discount_rub=0;
	discount_pct=0;
  end;
  promo_ct=coalesce(promo_ct,0);
run;

data casuser.price_unfolded;
 set MN_SHORT.PRICE_ML; 
 where price_type='F';
 keep product_id pbo_location_id net_price_amt gross_price_amt sales_dt;
 format sales_dt date9.;
 do sales_dt=max(START_DT,'1may2021'd) to min(END_DT,'1oct2021'd);
   output;
 end;
run;

data casuser.price_nodup;
  set casuser.price_unfolded;
  by product_id pbo_location_id sales_dt;
  if first.sales_dt then output;
run;

proc casutil;
  droptable casdata="price_unfolded" incaslib="casuser" quiet;
run;
quit;
 
proc cas;
timeData.timeSeries result =r /
	series={{name="gross_price_amt", setmiss="prev"},
			{name="net_price_amt", setmiss="prev"}}
	tEnd= "&vf_fc_agg_end_dt" /*fc_start_dt+hor*/
	table={caslib="casuser",name="price_nodup", groupby={"PBO_LOCATION_ID","PRODUCT_ID"} }
	timeId="SALES_DT"
	trimId="LEFT"
	interval="day"
	casOut={caslib="casuser",name="TS_price_fact",replace=True}
	;
run;
quit;


/* Наложение полученнных цен на прогноз units */

proc casutil;
	droptable 
		casdata="FINAL_FCST" 
		incaslib="max_casl" 
		quiet
	;
run;


proc fedsql sessref=casauto;
create table max_casl.FINAL_FCST{options replace=true} as
	select t1.*
		,t1.p_rec_sum_qty as FCST_UNITS 
		,t1.p_rec_sum_qty * t2.net_price_amt as FCST_SALE
		,t2.net_price_amt as AVG_PRICE
	from casuser.fact_predict_cmp_net t1 
	left join casuser.ts_price_fact t2 
	on t1.product_id=t2.product_id 
		and t1.pbo_location_id=t2.pbo_location_id 
		and t1.sales_dt=t2.sales_dt
	;
quit;


proc casutil;
	promote 
		casdata="FINAL_FCST" 
		casout="FINAL_FCST"  
		incaslib="max_casl" 
		outcaslib="max_casl"
	;
run;