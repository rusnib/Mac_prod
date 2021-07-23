%macro gc_alert_strange_seasonality(
		  mpLocLvl = LVL2_NM
		, mpBorderDt = 22319
		, mpSeason = qtr
		, mpAlertCriterionRelChange = 0.3
		, mpOutTableNm = ALERT_STRANGE_SEASONALITY
	);

	%macro mDummy;
	%mend mDummy;

	/* Переопределение граничной даты на начало полного сезона 
		и расчет количества сезонных циклов */	

/* 	%let mpSeason = qtr; */
/* 	%let mpBorderDt = 22646; */

	data _null_;
		border_dt = &mpBorderDt.;
		curr_season_start_dt = intnx(%tslit(&mpSeason.), border_dt, 0, 'B');
		next_season_start_dt = intnx(%tslit(&mpSeason.), border_dt, 1, 'B');
		new_border_dt = ifn(
			  curr_season_start_dt = border_dt
			, border_dt
			, next_season_start_dt
		);		
		call symputx('mpBorderDt', new_border_dt);

		if %tslit(&mpSeason.) = 'qtr' then year_shift = 4;
		if %tslit(&mpSeason.) = 'month' then year_shift = 12;
		if %tslit(&mpSeason.) = 'semiyear' then year_shift = 2;
		call symputx('mpYearShift', year_shift);
	run;
	%put &=mpBorderDt;
	%put &=mpYearShift;


	/* ************************************************************************************************ */
	/* Подготовка прогнозов */

	/* 1. Извлечь прогнозы и просуммировать до требуемого уровня */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_FCST {options replace=true} as
		select 
			  main.channel_cd
			, loc.&mpLocLvl.
			, main.sales_dt
			, intnx(%tslit(&mpSeason.), main.sales_dt, 0, 'B') as season_dt
			, count(distinct main.pbo_location_id) as count_loc		
			, sum(main.actual	) as sum_actual
			, sum(main.predict	) as sum_predict
		from 
			CASUSER.PBO_FCST as main
		left join casuser.pbo_dictionary as loc
			on main.pbo_location_id = loc.pbo_location_id
		group by 
			  main.channel_cd
			, loc.&mpLocLvl.
			, main.sales_dt
			, intnx(%tslit(&mpSeason.), main.sales_dt, 0, 'B') 
		;
	quit;

	/* 2. Прогнозы после граничной даты mpBorderDt на год вперед */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_FCST_YEAR {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_predict) as sum_predict_year
		from 
			CASUSER.PBO_SUM_FCST
		where 
			sales_dt >= &mpBorderDt.
			and sales_dt < intnx(%tslit(&mpSeason.), &mpBorderDt., &mpYearShift., 'B')
		group by 1,2,3,4
		;
	quit;

	/* 3. Часть прогноза до граничной даты mpBorderDt */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_FCST_APPENDIX {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_predict) as sum_predict
		from 
			CASUSER.PBO_SUM_FCST
		where 
			sales_dt < &mpBorderDt.
		group by 1,2,3,4
		;
	quit;

	/* ************************************************************************************************ */
	/* Подготовка фактических/исторических значений */
	
	/* 1. Извлечь факты из ABT */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST {options replace=true} as
		select 
			  main.channel_cd
			, loc.&mpLocLvl.
			, main.sales_dt
			, intnx(%tslit(&mpSeason.), main.sales_dt, 0, 'B') as season_dt 
			, count(distinct main.pbo_location_id) as count_loc
			, sum(main.RECEIPT_QTY	) as sum_actual
			, . as sum_predict
		from 
			MN_LONG.PBO_SAL_ABT as main
		left join casuser.pbo_dictionary as loc
			on main.pbo_location_id = loc.pbo_location_id
		where 
			main.sales_dt < cast(&mpBorderDt. as date)
		group by 
			  main.channel_cd		
			, loc.&mpLocLvl.
			,  main.sales_dt
			, intnx(%tslit(&mpSeason.), main.sales_dt, 0, 'B') 
		;
	quit;

	/* 2. Просуммировать факты за год назад */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST_PREVYEAR {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_actual) as sum_actual_prevyear
		from 
			CASUSER.PBO_SUM_HIST
		where 
			season_dt < &mpBorderDt.
			and season_dt >= intnx(%tslit(&mpSeason.), &mpBorderDt., - &mpYearShift., 'B')
		group by 1,2,3,4
		;
	quit;

	/* 3. Просуммировать факты за год, перед предыдущим */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST_PREVYEAR2 {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_actual) as sum_actual_prevyear2
		from 
			CASUSER.PBO_SUM_HIST
		where 
			season_dt < intnx(%tslit(&mpSeason.), &mpBorderDt., - &mpYearShift., 'B')
			and season_dt >= intnx(%tslit(&mpSeason.), &mpBorderDt., - 2 * &mpYearShift., 'B')
		group by 1,2,3,4
		;
	quit;


	/* 4. Вычислить средний факт за 3 прошлых года */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST_ALL {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_actual) as sum_actual
		from 
			CASUSER.PBO_SUM_HIST
		group by 1,2,3,4
		;
	quit;


	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_AVG_ALLHIST {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, season_num
			, avg(sum_actual) as avg_actual_allhist
		from 
			CASUSER.PBO_SUM_HIST_ALL
		group by 
			  channel_cd
			, &mpLocLvl.
			, season_num
		;
	quit;



/* ************************************************************************************************ */
 	/* Соединение и расчет алертов */	
	
	/* Соединить факты и прогноз за предыдущий год и рассчитать поля для сравнения с критериями*/
	/* Замечание: имеет смысл присоединение части прогноза, если mpBorderDt установлена не на начало прогноза, 
		а, например, на следующий финансовый/календарный год */

	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_YEAR {options replace=true} as
		select 
			  fcst.channel_cd
			, fcst.&mpLocLvl.
			, fcst.season_dt
			, fcst.season_num
	
			, fcst.sum_predict_year
			, hist.sum_actual_prevyear
			, avghist.avg_actual_allhist

			, apndx.sum_predict as sum_appendix_predict_year
			, sum(hist.sum_actual_prevyear, apndx.sum_predict) as sum_actual_n_appendix_prevyear
			, hist2.sum_actual_prevyear2

			, abs(fcst.sum_predict_year - sum(hist.sum_actual_prevyear, apndx.sum_predict)) / sum(hist.sum_actual_prevyear, apndx.sum_predict) 	
					as modrel_change_to_prevyear
			, abs(fcst.sum_predict_year - hist2.sum_actual_prevyear2) / hist2.sum_actual_prevyear2		
					as modrel_change_to_prevyear2
			, abs(fcst.sum_predict_year - avghist.avg_actual_allhist) / avghist.avg_actual_allhist
					as modrel_change_to_avghist


		from 
			CASUSER.PBO_SUM_FCST_YEAR as fcst

		left join 
			CASUSER.PBO_SUM_FCST_APPENDIX as apndx
		on fcst.channel_cd = apndx.channel_cd
			and fcst.&mpLocLvl. = apndx.&mpLocLvl.
			and fcst.season_num = apndx.season_num
		
		left join 
			CASUSER.PBO_SUM_HIST_PREVYEAR as hist
		on fcst.channel_cd = hist.channel_cd
			and fcst.&mpLocLvl. = hist.&mpLocLvl.
			and fcst.season_num = hist.season_num

		left join 
			CASUSER.PBO_SUM_HIST_PREVYEAR2 as hist2
		on fcst.channel_cd = hist2.channel_cd
			and fcst.&mpLocLvl. = hist2.&mpLocLvl.
			and fcst.season_num = hist2.season_num

		left join 
			CASUSER.PBO_AVG_ALLHIST as avghist
		on fcst.channel_cd = avghist.channel_cd
			and fcst.&mpLocLvl. = avghist.&mpLocLvl.
			and fcst.season_num = avghist.season_num

		;
	quit;


	/* Рассчитать алерты */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.ALERT_STRANGE_SEASONALITY {options replace=true} as
		select *
			  
			, case
				when modrel_change_to_prevyear > &mpAlertCriterionRelChange.
					and modrel_change_to_prevyear is not missing 
						then 1
				else 0
				end as alert_season_change_to_prevyear

			, case
				when modrel_change_to_prevyear2 > &mpAlertCriterionRelChange.
					and modrel_change_to_prevyear2 is not missing 
						then 1
				else 0
				end as alert_season_change_to_prevyear2

			, case
				when modrel_change_to_avghist > &mpAlertCriterionRelChange.
					and modrel_change_to_avghist is not missing 
						then 1
				else 0
				end as alert_season_change_to_avghist

		from 
			CASUSER.PBO_SUM_YEAR		
		;
	quit;

	proc casutil incaslib="DM_ALERT" ;
		droptable casdata = "&mpOutTableNm" quiet;
	run;

	data DM_ALERT.&mpOutTableNm.(promote=yes);
		set CASUSER.ALERT_STRANGE_SEASONALITY;
		format sum_predict_year commax15.;
		format sum_actual_prevyear commax15.;
		format sum_actual_prevyear2 commax15.;
		format season_dt ddmmyy.;
		format sum_appendix_predict_year commax15.;
		format sum_actual_n_appendix_prevyear commax15.;
		format avg_actual_allhist commax15.;
		format modrel_change_to_prevyear numx8.2;
		format modrel_change_to_prevyear2 numx8.2; 
		format modrel_change_to_avghist numx8.2; 
		label
			SUM_PREDICT_YEAR                = 'Чеки, прогноз на ближайший год'
			SUM_ACTUAL_PREVYEAR2            = 'Чеки, факт за позапрошлый год'
			AVG_ACTUAL_ALLHIST              = 'Чеки, среднегодовой факт за всю историю'
			SUM_ACTUAL_PREVYEAR             = 'Чеки, факт за прошлый год'
			SUM_APPENDIX_PREDICT_YEAR       = 'Чеки, часть прогноза за прошлый год'
			SUM_ACTUAL_N_APPENDIX_PREVYEAR  = 'Чеки, факт и часть прогноза за прошлый год'
			MODREL_CHANGE_TO_PREVYEAR       = 'Модуль отклонения к прошлому году' 
			MODREL_CHANGE_TO_PREVYEAR2      = 'Модуль отклонения к позапрошлому году' 	
			MODREL_CHANGE_TO_AVGHIST        = 'Модуль отклонения к среднему факту за всю историю'
			ALERT_SEASON_CHANGE_TO_PREVYEAR = 'Алерт - изменение к прошлому году!'	
			ALERT_SEASON_CHANGE_TO_PREVYEAR2= 'Алерт - изменение к позапрошлому году!'
			ALERT_SEASON_CHANGE_TO_AVGHIST  = 'Алерт - изменение к среднему факту!'
		;
	run;

	proc casutil incaslib="CASUSER" ;
		droptable casdata = "PBO_SUM_FCST" quiet;
		droptable casdata = "PBO_SUM_FCST_YEAR" quiet;
		droptable casdata = "PBO_SUM_HIST" quiet;
		droptable casdata = "PBO_SUM_HIST_PREVYEAR" quiet;
		droptable casdata = "PBO_SUM_HIST_PREVYEAR2" quiet;
		droptable casdata = "PBO_LOCATION" quiet;
		droptable casdata = "PBO_SUM_YEAR" quiet;
		droptable casdata = "ALERT_STRANGE_SEASONALITY" quiet;
	run;

%mend gc_alert_strange_seasonality;
