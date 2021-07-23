cas casauto;
caslib _all_ assign;


options casdatalimit=600000M;


%add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt,
							PromoCalculationRk=);

/* proc casutil; */
/*     droptable casdata="price_full_sku_pbo_day" incaslib="mn_dict" quiet; */
/* quit; */
/*  */
/* proc casutil; */
/*     load casdata="price_full_sku_pbo_day.sashdat" incaslib="mn_dict" casout="price_full_sku_pbo_day" outcaslib="mn_dict"; */
/* quit; */


/* Дата начала прогнозирования и текущая дата и дата начала скоринговой выборки:
'01dec2020'd							'31dec2020'd
'01jan2021'd		'26dec2020'd		'31jan2021'd
'27feb2021'd							'31mar2021'd
*/

/* %let ETL_CURRENT_DT      =  '26dec2020'd; */

%let START_DT   =  '01mar2021'd;
%let END_DT     =  '31mar2021'd;
%let mpMLPmixTabName= MAX_CASL.PMIX_DAYS_RESULT_MAR;
%let pbo_table  	= MAX_CASL.PBO_FORECAST_RESTORED_MAR;			
%let out_table  	= FCST_UNITS_MAR;			


/*************************************************************************************/

%let lmvStartDt = %str(date%')%sysfunc(putn(%sysfunc(intnx(day,&START_DT.,0)), yymmdd10.))%str(%');
%let lmvEndDt 	= %str(date%')%sysfunc(putn(%sysfunc(intnx(day,&END_DT.,0)), yymmdd10.))%str(%');

%let price_table	= MAX_CASL.KPI_PRICES;			
%let new_prod_table = MAX_CASL.NEW_PRODUCT_FCSTS;

%let pbo_dictionary = MN_SHORT.PBO_DICTIONARY;
%let prod_dictionary= MN_SHORT.PRODUCT_DICTIONARY;


proc fedsql sessref=casauto;
    create table casuser.pmix_daily{options replace=true} as
    select distinct t2.PBO_LOCATION_ID, t2.PRODUCT_ID, t2.sales_dt as period_dt, t3.channel_cd
        , cast(intnx('month',t2.sales_dt,0) as date) as mon_dt
        , t2.P_SUM_QTY as ff
		, . as promo
    from &mpMLPmixTabName. t2 
    inner join MN_DICT.ENCODING_CHANNEL_CD t3				/* Энкодим ID канала его наименованием из отдельного справочника */
        on t2.channel_cd=t3.channel_cd_id
    where t2.sales_dt between &lmvStartDt. and &lmvEndDt. 
		and t3.channel_cd = 'ALL'
;
quit;


/************************************************************************************
 *		ДОБАВЛЯЕМ НОВИНКИ С ПРИОРИТЕТОМ				*
 ************************************************************************************/
	
data casuser.npf_prediction;
	set &new_prod_table.;
	where SALES_DT between &START_DT. and &END_DT.;
run;

proc fedsql sessref=casauto;
	create table casuser.pmix_daily_new{options replace=true} as
	select 
		coalesce(t1.SALES_DT,t2.period_dt) as period_dt,
		coalesce(t1.product_id,t2.PRODUCT_ID) as product_id,
		coalesce(t1.channel_cd,t2.channel_cd) as channel_cd, 
		coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
		coalesce(cast(intnx('month',t1.sales_dt,0) as date),t2.mon_dt) as mon_dt,
		coalesce(t1.P_SUM_QTY, t2.ff) as ff
	from casuser.npf_prediction t1 
	full outer join casuser.pmix_daily t2
	 on t1.SALES_DT 		= t2.period_dt 
	and t1.product_id		= t2.product_id 
	and t1.channel_cd		= t2.channel_cd 
	and t1.pbo_location_id	= t2.pbo_location_id
	;
quit;




/************************************************************************************
 *		Реконсилируем прогноз с PBO до PBO-SKU										*
 ************************************************************************************/
/*			Здесь идет речь только о short term??? 
 *			Для повышения точности прогноза UNITS на уровне PBO-SKU-DAY используется 
 *		реконсиляция с уровня ресторана. Суммарные продажи UNITS на уровне ресторана
 *		прогнозируются отдельно и затем распределяются пропорционально на нижний
 *		уровень согласно ML прогнозу 
 */
	proc fedsql sessref=casauto;
/* ------------ Start. Считаем распределение прогноза на уровне PBO-SKU ----------- */
		create table casuser.percent{options replace=true} as
			select 
				  wplm.*
				, case 
					when wplm.FF = 0 
					then 0 
					else wplm.FF / sum.sum_ff
				end as fcst_pct
			from 
/* 				casuser.fc_w_plm as wplm */
				casuser.pmix_daily_new as wplm
			inner join
				(
				select 
					  channel_cd
					, pbo_location_id
					, period_dt
					, sum(FF) as sum_ff
				from 
/* 					casuser.fc_w_plm */
					casuser.pmix_daily_new 
				group by 
					  channel_cd
					, pbo_location_id
					, period_dt
				) as sum
					on wplm.pbo_location_id = sum.pbo_location_id 
					and wplm.period_dt = sum.period_dt
					and wplm.channel_cd = sum.channel_cd
		;
/* ------------ End. Считаем распределение прогноза на уровне PBO-SKU ------------- */


/* ------------ Start. Реконсилируем прогноз с PBO до PBO-SKU --------------------- */
		create table casuser.fcst_reconciled{options replace=true} as
			select
				  pct.CHANNEL_CD
				, pct.pbo_location_id
				, pct.product_id
				, pct.period_dt
				, pct.FF as FF_ML
/* 				, pct.fcst_pct */
/* 				, pct.promo */
				, coalesce(vf.pbo_fcst * pct.fcst_pct, pct.FF) as FF_REC_BPLM
				, pct.FF as FF
			from
				casuser.percent as pct
			left join 
				&pbo_table. as vf
			on      pct.pbo_location_id = vf.pbo_location_id 
				and pct.period_dt       = vf.sales_dt
				and pct.CHANNEL_CD 		= vf.CHANNEL_CD 
		;
	quit;
/* ------------ End. Реконсилируем прогноз с PBO до PBO-SKU ----------------------- */





/************************************************************************************
 *		ВРЕМЕННЫЕ и ПОСТОЯННЫЕ ДНИ ЗАКРЫТИЯ ПБО			*
 ************************************************************************************/


/* ------------ Start. Дни когда пбо будет уже закрыт (навсегда) ------------------ */
	data casuser.days_pbo_date_close;
		set MN_SHORT.PBO_DICTIONARY;
		format period_dt date9.;
		keep PBO_LOCATION_ID CHANNEL_CD period_dt;
		CHANNEL_CD = "ALL"; 
		if A_CLOSE_DATE ne . and A_CLOSE_DATE <= &END_DT. then 
		do period_dt = max(A_CLOSE_DATE, &START_DT.) to &END_DT.;
			output;
		end;
	run;
/* ------------ End. Дни когда пбо будет уже закрыт (навсегда) -------------------- */


/* ------------ Start. Дни когда пбо будет временно закрыт ------------------------ */
	data casuser.days_pbo_close;
		set MN_SHORT.PBO_CLOSE_PERIOD;
		format period_dt date9.;
		keep PBO_LOCATION_ID CHANNEL_CD period_dt;
		if channel_cd = "ALL" ;
		if (end_dt >= &START_DT. and end_dt <= &END_DT.) 
		or (start_dt >= &START_DT. and start_dt <= &END_DT.) 
		or (start_dt <= &START_DT. and &START_DT. <= end_dt)
		then
		do period_dt = max(start_dt, &START_DT.) to min(&END_DT., end_dt);
			output;
		end;
	run;
/* ------------ End. Дни когда пбо будет временно закрыт -------------------------- */


/* ------------ Start. Дни когда закрыто ПБО - никаких продаж быть не должно ------ */
	data casuser.days_pbo_close(append=force); 
	  set casuser.days_pbo_date_close;
	run;
/* ------------ End. Дни когда закрыто ПБО - никаких продаж быть не должно -------- */

	
/* ------------ Start. Убираем дубликаты ------------------------------------------ */
	proc fedsql sessref = casauto;
	create table casuser.days_pbo_close{options replace=true} as
	select distinct * from casuser.days_pbo_close;
	quit;
/* ------------ End. Убираем дубликаты -------------------------------------------- */


/************************************************************************************
 *		ОБРАБОТКА ЗАМЕН T	  								*
 ************************************************************************************/

	proc fedsql sessref=casauto;
		create table casuser.plm_t{options replace=true} as
		select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
			SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
			coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date)) as PREDECESSOR_END_DT, 
			SUCCESSOR_START_DT
	
		from CASUSER.PRODUCT_CHAIN_ENH
		where LIFECYCLE_CD='T' 
			and coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date))<= &lmvEndDt.	
			/* and successor_start_dt>=intnx('month',&vf_fc_start_dt,-3); */
			and successor_start_dt>=intnx('month',&vf_fc_start_dt,-8);
		/*фильтр, отсекающий "старые" замены 
		Замены случившиеся больше 3 мес назад отсекаются 
		Замены позднее fc_agg_end_dt отсекаем*/
	quit;

    /*predcessor будет продаваться до predecessor_end_dt (включ), все остальные даты ПОСЛЕ удаляем*/
    proc fedsql sessref=casauto; 
		create table casuser.predessor_periods_t{options replace=true} as
		select PREDECESSOR_DIM2_ID as pbo_location_id,
			PREDECESSOR_PRODUCT_ID as product_id,
			min(PREDECESSOR_END_DT) as end_dt
		from casuser.plm_t group by 1,2
		;
	quit;


/************************************************************************************
 *		ОБРАБОТКА ВЫВОДОВ D										*
 ************************************************************************************/

	proc fedsql sessref=casauto;
		create table casuser.plm_d{options replace=true} as
		select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
			SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
			PREDECESSOR_END_DT, SUCCESSOR_START_DT
		from CASUSER.PRODUCT_CHAIN_ENH
		where LIFECYCLE_CD = 'D'
			and predecessor_end_dt <= &lmvEndDt.
		;	
		/*старые выводы не отсекаем
		  выводы позднее fc_agg_end_dt отсекаем*/
	quit;



/************************************************************************************
 *		РАЗВЕРТКА PRODUCT_CHAIN					*
 ************************************************************************************/

/* ------------ Start. формирование таблицы товар-ПБО-день, которые должны 
							быть в прогнозе - на основании АМ --------------------- */
	proc fedsql sessref=casauto;
		create table casuser.plm_dist{options replace=true} as
		select successor_dim2_id as pbo_location_id
			, successor_product_id as product_id
			, successor_start_dt as start_dt
			, predecessor_end_dt as end_dt
		from casuser.PRODUCT_CHAIN_ENH
		where successor_start_dt <= &lmvEndDt.  
		  and predecessor_end_dt >= &lmvStartDt.  
		;
	quit;
/* ------------ End. формирование таблицы товар-ПБО-день, которые должны 
							быть в прогнозе - на основании АМ --------------------- */


/* ------------ Start. Дни когда товар должен продаваться по информации из АМ ----- */
/*			???Мы считаем, что на горизонте прогнозирования у товара не может быть
 *		плановых перерывов между продажами на уровне ресторан-товар?
 */	
	data casuser.days_prod_sale; 
	  set casuser.plm_dist;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID PRODUCT_ID period_dt;
	  do period_dt=max(start_dt,&START_DT.) to min(&END_DT.,end_dt);
	    output;
	  end;
	run;
/* ------------ End. Дни когда товар должен продаваться по информации из АМ ------- */


/* ------------ Start. удалить дубликаты ------------------------------------------ */
	data casuser.days_prod_sale1;
		set casuser.days_prod_sale;
		by PBO_LOCATION_ID PRODUCT_ID period_dt;
		if first.period_dt then output;
	run;
/* ------------ End. удалить дубликаты -------------------------------------------- */
	
	
/* ------------ Start. удалить отсюда периоды D  ---------------------------------- */
/* Какая логика удаления D??? */
	proc fedsql sessref=casauto;
		create table casuser.plm_sales_mask{options replace=true} as
		select t1.PBO_LOCATION_ID, t1.PRODUCT_ID, t1.period_dt
		from  casuser.days_prod_sale1 t1 
		left join casuser.plm_d t2
		   on  t1.product_id = t2.PREDECESSOR_PRODUCT_ID 
		   and t1.pbo_location_id = t2.PREDECESSOR_DIM2_ID
		where t1.period_dt <= &lmvEndDt. 
		;
	quit;
/* ------------ End. удалить отсюда периоды D  ------------------------------------ */


/* ------------ Start. удалить отсюда периоды временного и постоянного закрытия ПБО */
	proc fedsql sessref=casauto;
	  create table casuser.plm_sales_mask1{options replace=true} as
		  select t1.PBO_LOCATION_ID, t1.PRODUCT_ID, t1.period_dt
		  from  casuser.plm_sales_mask t1 left join casuser.DAYS_PBO_CLOSE t3
		  on t1.pbo_location_id=t3.pbo_location_id and t1.period_dt=t3.period_dt
		  /*пересечь с casuser.days_pbo_close - 
		  когда ПБО закрыт по любым причинам,
		  эти дни не должны попадать в неё по ключу ПБО - канал*/
		  left join casuser.predessor_periods_t t4
		  on t1.pbo_location_id=t4.pbo_location_id and t1.product_id=t4.product_id
		/*из plm_sales_mask1 удаляем для predcessor периоды с датой >end_dt*/
		  where t3.pbo_location_id is null and t3.period_dt is null
		  and ((t1.period_dt<=t4.end_dt and t4.end_dt is not null) or t4.end_dt=.)
		   /*если ряд есть в predcessor - оставляем всё <=даты вывода, если нет - не смотрим на дату*/
	;
	quit;
	


/* ------------ Start. Наложение plm на прогноз ----------------------------------- */
/*			Здесь идет речь об объединененном прогнозе short term + long term??? 
 */
	proc fedsql sessref=casauto;
	create table casuser.fc_w_plm{options replace=true} as 
		select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,t1.PRODUCT_ID,t1.period_dt
			, t1.FF_ML
			, t1.FF_REC_BPLM
		
		from casuser.fcst_reconciled /*casuser.pmix_daily_new*/ t1 
		inner join casuser.plm_sales_mask1 t2 						/* дни когда товар ДОЛЖЕН продаваться */
		on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID and t1.PRODUCT_ID=t2.PRODUCT_ID and t1.period_dt=t2.period_dt
		;
	quit;
/* ------------ End. Наложение plm на прогноз ------------------------------------- */





/************************************************************************************
 *		Реконсилируем прогноз с PBO до PBO-SKU										*
 ************************************************************************************/
/*			Здесь идет речь только о short term??? 
 *			Для повышения точности прогноза UNITS на уровне PBO-SKU-DAY используется 
 *		реконсиляция с уровня ресторана. Суммарные продажи UNITS на уровне ресторана
 *		прогнозируются отдельно и затем распределяются пропорционально на нижний
 *		уровень согласно ML прогнозу 
 */
	proc fedsql sessref=casauto;
/* ------------ Start. Считаем распределение прогноза на уровне PBO-SKU ----------- */
		create table casuser.percent2{options replace=true} as
			select 
				  wplm.*
				, case 
					when wplm.FF_ML = 0 
					then 0 
					else wplm.FF_ML / sum.sum_ff
				end as fcst_pct
			from 
				casuser.fc_w_plm as wplm
			inner join
				(
				select 
					  channel_cd
					, pbo_location_id
					, period_dt
					, sum(FF_ML) as sum_ff
				from 
					casuser.fc_w_plm					
				group by 
					  channel_cd
					, pbo_location_id
					, period_dt
				) as sum
					on wplm.pbo_location_id = sum.pbo_location_id 
					and wplm.period_dt = sum.period_dt
					and wplm.channel_cd = sum.channel_cd
		;
/* ------------ End. Считаем распределение прогноза на уровне PBO-SKU ------------- */


/* ------------ Start. Реконсилируем прогноз с PBO до PBO-SKU --------------------- */
		create table casuser.fcst_reconciled2{options replace=true} as
			select
				  pct.CHANNEL_CD
				, pct.pbo_location_id
				, pct.product_id
				, pct.period_dt
				, pct.FF_ML
				, pct.FF_REC_BPLM
/* 				, pct.promo */
				, coalesce(vf.pbo_fcst * pct.fcst_pct, pct.FF_ML) as FF_REC_APLM
			from
				casuser.percent2 as pct
			left join 
				&pbo_table. as vf
			on      pct.pbo_location_id = vf.pbo_location_id 
				and pct.period_dt       = vf.sales_dt
				and pct.CHANNEL_CD 		= vf.CHANNEL_CD 
		;
	quit;
/* ------------ End. Реконсилируем прогноз с PBO до PBO-SKU ----------------------- */


/************************************************************************************
*   СОХРАНЕНИЕ ФИНАЛЬНОЙ ТАБЛИЦЫ UNITS   *
 ************************************************************************************/

proc casutil;
	droptable 
		casdata		= "&out_table." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;    

proc fedsql sessref=casauto;
	create table MAX_CASL.&out_table.{options replace=true} as
	select distinct
		t1.product_id,
		t1.pbo_location_id,
		t1.period_dt as sales_dt,										
		t1.FF_ML as FINAL_FCST_UNITS_ML,
		t1.FF_REC_BPLM as FINAL_FCST_UNITS_REC_BPLM,					
		t1.FF_REC_APLM as FINAL_FCST_UNITS_REC_APLM					
		
	from casuser.fcst_reconciled2 t1 
	where t1.channel_cd='ALL' 
	;
quit;

                      
proc casutil;         
	promote           
		casdata		= "&out_table." 
		incaslib	= "MAX_CASL" 
		casout		= "&out_table."  
		outcaslib	= "MAX_CASL"
	;                 
run;                  
                      
/* proc casutil;          */
/* 	save               */
/* 		casdata		= "&out_table."  */
/* 		incaslib	= "MAX_CASL"  */
/* 		casout		= "&out_table."   */
/* 		outcaslib	= "MAX_CASL" */
/* 	; */
/* run; */


