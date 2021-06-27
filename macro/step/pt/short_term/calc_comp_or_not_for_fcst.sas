cas casauto;
caslib _all_ assign;

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
		MAX_CASL.SHARE_FCST_GC as main
	inner join
		casuser.comp_list as comp
	on main.pbo_location_id = comp.pbo_location_id
		and intnx('month', main.SALES_DT, 0, 'B') = comp.mon_dt
	;
	create table casuser.gc_comp{options replace=true} as
	select
		main.pbo_location_id
		, main.sales_dt
		, main.FCST_GC as FCST_GC_BCOMP
		, case	
			when comp.pbo_location_id is null then 0
			else main.FCST_GC
		  end as FCST_GC_ACOMP
	from 
		MAX_CASL.SHARE_FCST_GC as main
	left join
		casuser.comp_list as comp
	on main.pbo_location_id = comp.pbo_location_id
		and intnx('month', main.SALES_DT, 0, 'B') = comp.mon_dt
	;
	create table casuser.aggr_gc_only_comp{options replace=true} as
	select sales_dt
		, sum(FCST_GC_BCOMP) as FCST_GC_BCOMP
		, sum(FCST_GC_ACOMP) as FCST_GC_ACOMP
	from casuser.gc_comp
	group by sales_dt
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.sale_only_comp{options replace=true} as
	select
		main.*
	from 
		MAX_CASL.SHARE_FCST_UNITS_N_SALE as main
	inner join
		casuser.comp_list as comp
	on main.pbo_location_id = comp.pbo_location_id
		and intnx('month', main.SALES_DT, 0, 'B') = comp.mon_dt
	;
	create table casuser.sale_comp{options replace=true} as
	select
		main.pbo_location_id
		, main.sales_dt
		, main.FCST_SALE as FCST_SALE_BCOMP
		, case	
			when comp.pbo_location_id is null then 0
			else main.FCST_SALE
		  end as FCST_SALE_ACOMP
	from 
		MAX_CASL.SHARE_FCST_UNITS_N_SALE as main
	left join
		casuser.comp_list as comp
	on main.pbo_location_id = comp.pbo_location_id
		and intnx('month', main.SALES_DT, 0, 'B') = comp.mon_dt
	;
	create table casuser.aggr_sale_only_comp{options replace=true} as
	select sales_dt
		, sum(FCST_SALE_BCOMP) as FCST_SALE_BCOMP
		, sum(FCST_SALE_ACOMP) as FCST_SALE_ACOMP
	from casuser.sale_comp
	group by sales_dt
	;

quit;



ods excel file="&common_path./MCD_SAS_TTLSTERMFCSTS_28052021.xlsx"  style=statistical;

ods excel options(sheet_interval = 'none' sheet_name = "GC"	);
proc print data = casuser.aggr_gc_only_comp 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "SALE" );
proc print data = casuser.aggr_sale_only_comp 	label; run;

ods excel close;

