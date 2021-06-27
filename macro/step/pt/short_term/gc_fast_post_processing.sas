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

/*   intnx('month',mon_dt,-12,'b')>=  */
/*       case  */
/* 	   when day(A_OPEN_DATE)=1 then  */
/* 		 cast(A_OPEN_DATE as date) */
/* 	   else cast(intnx('month',A_OPEN_DATE,1,'b') as date) */
/*       end */
/*       and mon_dt<= */
/* 	  case */
/* 	   when A_CLOSE_DATE is null then cast(intnx('month',date %tslit(&VF_FC_AGG_END_DT),12) as date) */
/* 	   when A_CLOSE_DATE=intnx('month',A_CLOSE_DATE,0,'e') then cast(A_CLOSE_DATE as date) */
/*        else cast(intnx('month',A_CLOSE_DATE,-1,'e') as date) */
/* 	  end */


proc fedsql sessref=casauto;
	create table casuser.gc_only_comp{options replace=true} as
	select
		main.*
	from 
		casuser.gc_days as main
	inner join
		casuser.comp_list as comp
	on main.pbo_location_id = comp.pbo_location_id
		and intnx('month', main.SALES_DT, 0, 'B') = comp.mon_dt
	;
quit;