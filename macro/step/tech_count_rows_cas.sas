%macro tech_count_rows_cas(mpInLibref=
							,mpInTableNm=
							,mpVar=mvCntLastDayTs);
							
	%global &mpVar.;
	
	proc fedsql sessref=casauto;
			create table casuser.tmp_count_rows{options replace=true} as
			select count(*) as CNT
			from &mpInLibref..&mpInTableNm.
	;
	quit;
	
	proc sql noprint;
		select cnt into :&mpVar.
		from casuser.tmp_count_rows
		;
	quit;
	
	proc casutil;
		droptable incaslib='CASUSER' casdata='tmp_count_rows' quiet;
	quit;
	
	%put &mpVar. = &&mpVar.;
	
%mend tech_count_rows_cas;