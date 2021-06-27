%macro load_komp_matrix;
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=
						);
   %let lmvReportDttm=&ETL_CURRENT_DTTM.;
	 
	proc sql noprint;
		create table pbo_dt as
			select distinct pbo_location_id,
			input(pbo_loc_attr_value, DDMMYY10.) format=date9. as OPEN_DATE,
			PBO_LOC_ATTR_NM
			from etl_ia.pbo_loc_attributes
			where (PBO_LOC_ATTR_NM = 'OPEN_DATE' or PBO_LOC_ATTR_NM = 'CLOSE_DATE')
			and  valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			;
	quit;

	proc transpose data=pbo_dt out=pbo_open_dt;
		by pbo_location_id;
		var  open_date;
		id PBO_LOC_ATTR_NM;
	run;

	data casuser.pbo_open_dt (replace=yes drop=_name_);
		set pbo_open_dt;
	run;

	data casuser.calendar(drop=i);
		format day month week date9.;
		do i=intnx("year", date(), 0, "b") to intnx("year", date(), 3, "e");
			day = i;
			week = intnx("week.2", day, 0, "b");
			month = intnx("month", day, 0, "b");
			output;
		end;
	run;

	proc fedsql sessref=casauto;
		create table casuser.pbo_date_list{options replace=true} as
			select distinct pbo_location_id
						, OPEN_DATE
						, CLOSE_DATE
						, month
			from casuser.pbo_open_dt t1
			cross join
			(select month
			from casuser.calendar) t2
			;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.komp_matrix{options replace=true} as
			select pbo_location_id
					, month
					, (case when intnx('month', month,-12,'b')>=    
										  (case
											  when day(OPEN_DATE)=1 then
														   cast(OPEN_DATE as date)
											  else cast(intnx('month',OPEN_DATE,1,'b') as date)
											end) and month <= (case 	when CLOSE_DATE is null then cast(intnx('month',month,12) as date)
																		 when CLOSE_DATE=intnx('month', CLOSE_DATE,0,'e') then cast(CLOSE_DATE as date)
																else cast(intnx('month', CLOSE_DATE,-1,'e') as date)
																end) then 1 
						else 0 end) as KOMP_ATTRIB
			from casuser.pbo_date_list
			;
	quit;
%mend load_komp_matrix;
