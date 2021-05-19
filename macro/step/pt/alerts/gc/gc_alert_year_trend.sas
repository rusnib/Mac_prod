/* Параметры макроса

Параметр				Возможные значения
mpLocLvl 			 	A_AGREEMENT_TYPE, LVL1_NM, LVL2_NM, LVL3_NM, pbo_location_nm
mpBorderDt 			 	дата в виде числа 22646, любая функция от даты, например intnx('year', today(), 0, 'B')
mpAlertCriterionDamp 	интервал от 0 до 1, исключая границы 
mpAlertCriterionGrowth 	интервал от 0 до 1, исключая границы 
mpOutTableNm 			имя таблицы, например, GC_ALERT_YEAR_TREND

*/


%macro gc_alert_year_trend(
		  mpLocLvl = LVL2_NM
		, mpBorderDt = 22333
		, mpAlertCriterionDamp = 0.05
		, mpAlertCriterionGrowth = 0.1
		, mpOutTableNm = GC_ALERT_YEAR_TREND
	);

	%macro mDummy;
	%mend mDummy;

	/* ************************************************************************************************ */
	/* Подготовка прогнозов */

	/* 1. Извлечь прогнозы и просуммировать до требуемого уровня */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_FCST {options replace=true} as
		select 
			  main.channel_cd
			, loc.&mpLocLvl.
			, main.sales_dt
			, count(distinct main.pbo_location_id) as count_loc		
			, sum(main.actual	) as sum_actual
			, sum(main.predict	) as sum_predict
		from 
			CASUSER.PBO_FCST as main
		left join casuser.pbo_dictionary as loc
			on main.pbo_location_id = loc.pbo_location_id
		where 
			main.sales_dt >= &mpBorderDt.
		group by 
			  main.channel_cd
			, loc.&mpLocLvl.
			, main.sales_dt
		;
	quit;

	/* 2. Просуммировать прогноз за год вперед после даты mpBorderDt */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_FCST_YEAR {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, sum(sum_predict) as sum_predict_year
		from 
			CASUSER.PBO_SUM_FCST
		where 
			sales_dt < intnx('day', &mpBorderDt., 365, 'B')
		group by 
			  channel_cd
			, &mpLocLvl.
		;
	quit;

	/* 3. Часть прогноза до даты mpBorderDt */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_FCST_APPENDIX {options replace=true} as
		select 
			  main.channel_cd
			, loc.&mpLocLvl.
/* 			, main.sales_dt */
			, count(distinct main.pbo_location_id) as count_loc		
			, sum(main.actual	) as sum_actual
			, sum(main.predict	) as sum_predict
		from 
			CASUSER.PBO_FCST as main
		left join casuser.pbo_dictionary as loc
			on main.pbo_location_id = loc.pbo_location_id
		where 
			main.sales_dt < &mpBorderDt.
		group by 
			  main.channel_cd
			, loc.&mpLocLvl.
/* 			, main.sales_dt */
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
			, count(distinct main.pbo_location_id) as count_loc
			, sum(main.RECEIPT_QTY	) as sum_actual
			, . as sum_predict
		from 
			MN_LONG.PBO_SAL_ABT as main
		

		left join casuser.pbo_dictionary as loc
			on main.pbo_location_id = loc.pbo_location_id

		where 
			main.sales_dt < &mpBorderDt.

		group by 
			  main.channel_cd		
			, loc.&mpLocLvl.
			, main.sales_dt
		;
	quit;

	/* 2. Просуммировать факты за год назад */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST_PREVYEAR {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, sum(sum_actual) as sum_actual_prevyear
		from 
			CASUSER.PBO_SUM_HIST
		where 
			sales_dt >= intnx('day', &mpBorderDt., -365, 'B')
		group by 
			  channel_cd
			, &mpLocLvl.
		;
	quit;

	/* 3. Просуммировать факты за год, перед предыдущим */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST_PREVYEAR2 {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, sum(sum_actual) as sum_actual_prevyear2
		from 
			CASUSER.PBO_SUM_HIST
		where 
			sales_dt >= intnx('day', &mpBorderDt., -365*2, 'B')
			and sales_dt < intnx('day', &mpBorderDt., -365, 'B')
		group by 
			  channel_cd
			, &mpLocLvl.
		;
	quit;


	/* 4. Вычислить средний факт за 3 прошлых года */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_AVG_ALLHIST {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, 52 * avg(sum_actual) as avg_actual_allhist
		from 
			CASUSER.PBO_SUM_HIST
		group by 
			  channel_cd
			, &mpLocLvl.
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
	
			, fcst.sum_predict_year
			, hist2.sum_actual_prevyear2
			, avghist.avg_actual_allhist

			, hist.sum_actual_prevyear
			, apndx.sum_predict as sum_appendix_predict_year
			, sum(hist.sum_actual_prevyear, apndx.sum_predict) as sum_actual_n_appendix_prevyear

			, (fcst.sum_predict_year - sum(hist.sum_actual_prevyear, apndx.sum_predict)) / sum(hist.sum_actual_prevyear, apndx.sum_predict) 	
					as rel_change_to_prevyear
			, (fcst.sum_predict_year - hist2.sum_actual_prevyear2) / hist2.sum_actual_prevyear2		
					as rel_change_to_prevyear2
			, (fcst.sum_predict_year - avghist.avg_actual_allhist) / avghist.avg_actual_allhist
					as rel_change_to_avghist

		from 
			CASUSER.PBO_SUM_FCST_YEAR as fcst

		left join 
			CASUSER.PBO_SUM_FCST_APPENDIX as apndx
		on fcst.channel_cd = apndx.channel_cd
			and fcst.&mpLocLvl. = apndx.&mpLocLvl.
		
		left join 
			CASUSER.PBO_SUM_HIST_PREVYEAR as hist
		on fcst.channel_cd = hist.channel_cd
			and fcst.&mpLocLvl. = hist.&mpLocLvl.

		left join 
			CASUSER.PBO_SUM_HIST_PREVYEAR2 as hist2
		on fcst.channel_cd = hist2.channel_cd
			and fcst.&mpLocLvl. = hist2.&mpLocLvl.

		left join 
			CASUSER.PBO_AVG_ALLHIST as avghist
		on fcst.channel_cd = avghist.channel_cd
			and fcst.&mpLocLvl. = avghist.&mpLocLvl.

		;
	quit;


	/* Рассчитать алерты */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.ALERT_YEAR_TREND {options replace=true} as
		select *
			  
			/* Прогноз на год вперед ниже, чем факт за прошлый год на X% */
			, case
				when rel_change_to_prevyear < - &mpAlertCriterionDamp.
					and rel_change_to_prevyear is not missing 
						then 1
				else 0
				end as alert_trend_damp_to_prevyear

			/* Прогноз на год вперед ниже, чем факт за прошлый год на X%, и ниже чем факт за позапрошлый год на X% */
			, case
				when rel_change_to_prevyear2 < - &mpAlertCriterionDamp.					
					and rel_change_to_prevyear2 is not missing 
						then 1
				else 0
				end as alert_trend_damp_to_prevyear2

			/* Прогноз на год вперед ниже, чем средний годовой факт за всю историю на X% */
			, case
				when rel_change_to_avghist < - &mpAlertCriterionDamp.					
					and rel_change_to_avghist is not missing 
						then 1
				else 0
				end as alert_trend_damp_to_avghist

			/* Прогноз на год вперед выше, чем факт за прошлый год на X% */
			, case
				when rel_change_to_prevyear > &mpAlertCriterionGrowth.
					and rel_change_to_prevyear is not missing 					
						then 1
				else 0
				end as alert_trend_growth_to_prevyear

			/* Прогноз на год вперед выше, чем факт за прошлый год на X%, и выше чем факт за позапрошлый год на X% */
			, case
				when rel_change_to_prevyear2 > &mpAlertCriterionGrowth.
					and rel_change_to_prevyear2 is not missing 
						then 1
				else 0
				end as alert_trend_growth_to_prevyear2

			/* Прогноз на год вперед выше, чем средний годовой факт за всю историю на X% */
			, case
				when rel_change_to_avghist > &mpAlertCriterionGrowth.				
					and rel_change_to_avghist is not missing 
						then 1
				else 0
				end as alert_trend_growth_to_avghist
			
		from 
			CASUSER.PBO_SUM_YEAR		
		;
	quit;


	proc casutil incaslib="DM_ALERT" ;
		droptable casdata = "&mpOutTableNm" quiet;
	run;

	data DM_ALERT.&mpOutTableNm.(promote=yes);
		set CASUSER.ALERT_YEAR_TREND;
		format sum_predict_year commax15.;
		format sum_actual_prevyear commax15.;
		format sum_actual_prevyear2 commax15.;
		format avg_actual_allhist commax15.; 
		format sum_appendix_predict_year commax15.;
		format sum_actual_n_appendix_prevyear commax15.;
		format rel_change_to_prevyear numx8.2;
		format rel_change_to_prevyear2 numx8.2;
		format rel_change_to_avghist numx8.2; 
		label 
			SUM_PREDICT_YEAR                = 'Чеки, прогноз на ближайший год'
			SUM_ACTUAL_PREVYEAR2            = 'Чеки, факт за позапрошлый год'
			AVG_ACTUAL_ALLHIST              = 'Чеки, среднегодовой факт за всю историю'
			SUM_ACTUAL_PREVYEAR             = 'Чеки, факт за прошлый год'
			SUM_APPENDIX_PREDICT_YEAR       = 'Чеки, часть прогноза за прошлый год'
			SUM_ACTUAL_N_APPENDIX_PREVYEAR  = 'Чеки, факт и часть прогноза за прошлый год'
			REL_CHANGE_TO_PREVYEAR			= 'Отклонение к прошлому году' 
			REL_CHANGE_TO_PREVYEAR2			= 'Отклонение к позапрошлому году' 	
			REL_CHANGE_TO_AVGHIST  			= 'Отклонение к среднегодовому факту за всю историю'
			ALERT_TREND_DAMP_TO_PREVYEAR	= 'Алерт - падение к прошлому году!'	
			ALERT_TREND_DAMP_TO_PREVYEAR2	= 'Алерт - падение к позапрошлому году!'
			ALERT_TREND_DAMP_TO_AVGHIST	    = 'Алерт - падение к среднегодовому факту!'
			ALERT_TREND_GROWTH_TO_PREVYEAR	= 'Алерт - рост к прошлому году!'	
			ALERT_TREND_GROWTH_TO_PREVYEAR2	= 'Алерт - рост к позапрошлому году!'
			ALERT_TREND_GROWTH_TO_AVGHIST   = 'Алерт - рост к среднегодовому факту!'
		;
	run;

	/****************************************************.
	/* График */
	data work.pbo_for_line_chart;
		set 
			casuser.pbo_sum_hist
			casuser.pbo_sum_fcst
		;
		channel_cd = compress(channel_cd);
		format curr_dt date9.;
		curr_dt = '8feb2021'd;
		up = 9000000;
		down = 5000000;
	run;
	
	proc sort data=work.pbo_for_line_chart;
	by 
		channel_cd 	
		%if &mpLocLvl. = ALL %then %do; %end; %else %do; &mpLocLvl. %end;
		sales_dt
		curr_dt
		up
		down
		;
	run;
	
	PROC TRANSPOSE DATA = work.pbo_for_line_chart
		OUT = WORK.PBO_FOR_LINE_CHART_T (rename=(col1=qty))
		NAME = qty_type
		;
		BY 
			channel_cd 
			%if &mpLocLvl. = ALL %then %do; %end; %else %do; &mpLocLvl. %end;
 			sales_dt
			curr_dt
			up
			down
		;
		VAR sum_actual sum_predict;
	
	RUN;

/* 	SYMBOL1 */
/* 		INTERPOL=JOIN */
/* 		HEIGHT=10pt */
/* 		VALUE=NONE */
/* 		LINE=1 */
/* 		WIDTH=2 */
/* 		CV = _STYLE_ */
/* 	; */
/* 	SYMBOL2 */
/* 		INTERPOL=JOIN */
/* 		HEIGHT=10pt */
/* 		VALUE=NONE */
/* 		LINE=1 */
/* 		WIDTH=2 */
/* 		CV = _STYLE_ */
/* 	; */
	
/* 	proc gplot data = WORK.PBO_FOR_LINE_CHART_T; */
/* 		plot qty * sales_dt = qty_type */
/* 		;  */
/* 		by  */
/* 			channel_cd */
/* 			%if &mpLocLvl. = ALL %then %do; %end; %else %do; &mpLocLvl. %end; */
/* 		; */
/* 	run; */
/* 	 */

	
	proc sgplot data=WORK.PBO_FOR_LINE_CHART_T;
		by
			channel_cd
			%if &mpLocLvl. = ALL %then %do; %end; %else %do; &mpLocLvl. %end;
		;
		series x=sales_dt y=qty / group=qty_type;
		refline curr_dt / axis=x lineattrs=(thickness=1 color=black);
		refline up / axis=y lineattrs=(thickness=1 color=black);
		refline down / axis=y lineattrs=(thickness=1 color=black);
		xaxis grid;
		yaxis grid;
	run;


	/* Clear CAS */
	proc casutil incaslib="CASUSER" ;
		droptable casdata = "PBO_SUM_FCST" quiet;
		droptable casdata = "PBO_SUM_FCST_YEAR" quiet;
		droptable casdata = "PBO_SUM_HIST" quiet;
		droptable casdata = "PBO_SUM_HIST_PREVYEAR" quiet;
		droptable casdata = "PBO_SUM_HIST_PREVYEAR2" quiet;
		droptable casdata = "PBO_LOCATION" quiet;
		droptable casdata = "LVLS" quiet;
		droptable casdata = "PBO_SUM_YEAR" quiet;
		droptable casdata = "ALERT_YEAR_TREND" quiet;
	run;

%mend gc_alert_year_trend;