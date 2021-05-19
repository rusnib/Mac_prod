%macro gc_alert_unprecedented_value(
	  mpLocLvl = LVL3_NM
	, mpTimeLvl = month
	, mpBorderDt = 22319
	, mpOutTableNm = ALERT_UNPRECEDENTED_VALUE
);

	%macro mDummy;
	%mend mDummy;

	/* Переопределение граничной даты на начало полного сезона 
		и расчет количества сезонных циклов */	

	data _null_;
		border_dt = &mpBorderDt.;
		curr_period_start_dt = intnx(%tslit(&mpTimeLvl.), border_dt, 0, 'B');
		next_period_start_dt = intnx(%tslit(&mpTimeLvl.), border_dt, 1, 'B');
		new_border_dt = ifn(
			  curr_period_start_dt = border_dt
			, border_dt
			, next_period_start_dt
		);		
		call symputx('mpBorderDt', new_border_dt);
	run;
	%put &=mpBorderDt;


	/* ************************************************************************************************ */
	/* Подготовка прогнозов */

	/* 1. Извлечь прогнозы и просуммировать до требуемого уровня */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_FCST_PRE {options replace=true} as
		select 
			  main.channel_cd
			, loc.&mpLocLvl.
			, main.sales_dt
			, intnx(%tslit(&mpTimeLvl.), main.sales_dt, 0, 'B') as period_dt
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

	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_FCST {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, period_dt
			, sum(sum_predict) as sum_predict
		from 
			CASUSER.PBO_SUM_FCST_PRE
		group by 
			  channel_cd
			, &mpLocLvl.
			, period_dt
		;
	quit;

	/* ************************************************************************************************ */
	/* Подготовка исторических продаж */

	/* 1. Извлечь факты из ABT */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST_PRE {options replace=true} as
		select 
			  main.channel_cd
			, loc.&mpLocLvl.
			, main.sales_dt
			, intnx(%tslit(&mpTimeLvl.), main.sales_dt, 0, 'B') as period_dt
			, count(distinct main.pbo_location_id) as count_loc
			, sum(main.RECEIPT_QTY	) as sum_actual
			, . as sum_predict
		from 
			MN_LONG.PBO_SAL_ABT as main

		left join casuser.pbo_dictionary as loc
			on main.pbo_location_id = loc.pbo_location_id

		where 
			intnx(%tslit(&mpTimeLvl.), main.sales_dt, 0, 'B') < &mpBorderDt.

		group by 
			  main.channel_cd		
			, loc.&mpLocLvl.
			, main.sales_dt
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, period_dt
			, sum(sum_actual) as sum_actual
		from 
			CASUSER.PBO_SUM_HIST_PRE
		group by 
			  channel_cd
			, &mpLocLvl.
			, period_dt
		;
	quit;

	/* 2. Вычислить максимум, минимум, медиану, стандартное отклонение на истории */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_HIST_METRICS {options replace=true} as
		select 
			  channel_cd
			, &mpLocLvl.
			, avg(sum_actual) as average
			, std(sum_actual) as std
			, min(sum_actual) as minimum
			, max(sum_actual) as maximum
		from 
			CASUSER.PBO_SUM_HIST
		group by 
			  channel_cd
			, &mpLocLvl.
		;
	quit;

	/* Соединить в одну таблицу */

	data CASUSER.PBO_FCST_N_HIST;
		set 
			CASUSER.PBO_SUM_HIST
			CASUSER.PBO_SUM_FCST
		;
	run;

	/* Рассчитать алерты */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_SUM_YEAR {options replace=true} as
		select 
			  fcst.channel_cd
			, fcst.&mpLocLvl.
			
			, fcst.period_dt
			, fcst.sum_predict
			, fcst.sum_actual

			, metr.average
			, metr.std
			, metr.minimum
			, metr.maximum
			
			, case
				when fcst.sum_predict > 0 and fcst.sum_predict > metr.maximum 
					then 1
				else 0
				end as alert_break_maximum
			
			, case
				when fcst.sum_predict > 0 and fcst.sum_predict < metr.minimum 
					then 1
				else 0
				end as alert_break_minimum

			, case				
				when fcst.sum_predict > 0 and (fcst.sum_predict > metr.average + 3 * metr.std or fcst.sum_predict < metr.average - 3 * metr.std)
					then 1
				else 0
				end as alert_break_avgn3std

		from 
			CASUSER.PBO_FCST_N_HIST as fcst

		inner join 
			CASUSER.PBO_SUM_HIST_METRICS as metr
		on fcst.channel_cd = metr.channel_cd
			and fcst.&mpLocLvl. = metr.&mpLocLvl.
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table CASUSER.ALERTS_LOC {options replace=true} as
		select distinct channel_cd
			, &mpLocLvl.		
		from CASUSER.PBO_SUM_YEAR
		where alert_break_maximum = 1 
			or alert_break_minimum = 1
			or alert_break_avgn3std = 1
		;
		create table CASUSER.PBO_FOR_LINE_CHART {options replace=true} as
		select main.*
		from CASUSER.PBO_SUM_YEAR as main
		inner join CASUSER.ALERTS_LOC as dloc
		on main.channel_cd = dloc.channel_cd
			and main.&mpLocLvl. = dloc.&mpLocLvl.			
		;
	quit;


	proc casutil incaslib="DM_ALERT" ;
		droptable casdata = "&mpOutTableNm" quiet;
	run;

	data DM_ALERT.&mpOutTableNm.(promote=yes);
		set CASUSER.PBO_FOR_LINE_CHART;
		where sum_predict >= 0;
		drop sum_actual;
		format sum_predict commax15.;
		format average commax15.;
		format std commax15.;
		format minimum commax15.;
		format maximum commax15.;
		format period_dt date9.;
		label
			SUM_PREDICT           = 'Чеки, прогноз на период'
			AVERAGE               = 'Чеки, среднее за историю'
			STD                   = 'Чеки, ст.отклонение за историю'
			MINIMUM               = 'Чеки, минимум за историю'
			MAXIMUM               = 'Чеки, максимум за историю'
			ALERT_BREAK_MAXIMUM   = 'Алерт - Максимум пробит!'
			ALERT_BREAK_MINIMUM   = 'Алерт - Минимум пробит!'
			ALERT_BREAK_AVGN3STD  = 'Алерт - Среднее +/- 3std пробито!'
		;
	run;
			


/********************************************************************/

	data WORK.PBO_FOR_LINE_CHART;
		set CASUSER.PBO_FOR_LINE_CHART;
	run;

	proc sort data=WORK.PBO_FOR_LINE_CHART;
	by 
		channel_cd 	
		&mpLocLvl.
		period_dt
	;
	run;
	
	PROC TRANSPOSE DATA = work.pbo_for_line_chart
		OUT = WORK.pbo_for_line_chart_t (rename=(col1=qty))
		NAME = qty_type
		;
		BY 
			channel_cd 
			&mpLocLvl.
 			period_dt
		;
		VAR sum_actual sum_predict;
	
	RUN;

	SYMBOL1
		INTERPOL=JOIN
		HEIGHT=10pt
		VALUE=NONE
		LINE=1
		WIDTH=2
		CV = _STYLE_
	;
	SYMBOL2
		INTERPOL=JOIN
		HEIGHT=10pt
		VALUE=NONE
		LINE=1
		WIDTH=2
		CV = _STYLE_
	;
	
	proc gplot data = WORK.PBO_FOR_LINE_CHART_T;
		plot qty * period_dt = qty_type
		; 
		by 
			channel_cd
			&mpLocLvl.
		;
	run;

	/* clear CAS */
	proc casutil incaslib="CASUSER" ;
		droptable casdata = "PBO_SUM_FCST" quiet;
		droptable casdata = "PBO_SUM_HIST" quiet;
		droptable casdata = "PBO_SUM_HIST_METRICS" quiet;
		droptable casdata = "PBO_LOCATION" quiet;
		droptable casdata = "LVLS" quiet;
		droptable casdata = "PBO_SUM_YEAR" quiet;
		droptable casdata = "ALERTS_LOC" quiet;
		droptable casdata = "PBO_FOR_LINE_CHART" quiet;
		droptable casdata = "PBO_FCST_N_HIST" quiet;
	run;

%mend gc_alert_unprecedented_value;