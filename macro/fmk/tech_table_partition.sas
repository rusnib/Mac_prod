%macro tech_table_partition(mpTableNm=,
							mpCasOut=,
							mpDateClmn=,
							mpPartsNum=,
							mpSaveFlg=,
							mpPromoteFlg=
							);
		
	%local lmvTableNm
			lmvDateClmn
			lmvPartsNum
			lmvSaveFlg 
			lmvPromoteFlg
			lmvDatSet
			lmvDatTo
			lmvDatFrom
			lmvDatFromC
			lmvDatToC
			lmvCasOut
	;
			
	%let lmvTableNm = %sysfunc(scan(&mpTableNm., -1, .));
	%let lmvCasIn = %sysfunc(scan(&mpTableNm., 1, .));
	%let lmvCasOut = &mpCasOut.;
	%let lmvPartsNum = &mpPartsNum.;
	%let lmvDateClmn = &mpDateClmn.;
	%let lmvSaveFlg = %sysfunc(upcase(&mpSaveFlg.));
	%let lmvPromoteFlg = %sysfunc(upcase(&mpPromoteFlg.));
	
    proc fedsql sessref=casauto;
        create table casuser.temp {options replace=true} as 
			select min(&lmvDateClmn.) as DatFrom
				, max(&lmvDateClmn.) as DatTo 
			from &lmvCasIn..&lmvTableNm.
		;
    quit;
        
    proc sql noprint;
        select ceil(DatTo*1)
			, ceil(DatFrom*1)
			, ceil((DatTo-DatFrom+1)/&lmvPartsNum.) 
		into :lmvDatTo
			, :lmvDatFrom
			, :lmvDatSet
		from casuser.temp
		;
    quit;
        
    %do i=1 %to &lmvPartsNum;
        %let lmvDatFromC = date%str(%')%sysfunc(putn(&lmvDatFrom.+&lmvDatSet.*(&i.-1), yymmdd10.))%str(%');
        %let lmvDatToC = date%str(%')%sysfunc(putn(&lmvDatFrom.+&lmvDatSet.*&i., yymmdd10.))%str(%');
        proc fedsql sessref=casauto;
            create table &lmvCasIn..&lmvTableNm._&i. {options replace=true} as
				select * 
				from &lmvCasIn..&lmvTableNm.
				where &lmvDatFromC. <= &lmvDateClmn.
					and &lmvDateClmn. < &lmvDatToC.
			;
        quit;
      
		proc casutil;
			%if &lmvPromoteFlg.=Y %then %do;
				promote casdata="&lmvTableNm.&i." incaslib="&lmvCasIn." outcaslib="&lmvCasOut.";
			%end;
			%if &lmvSaveFlg.=Y %then %do;
				save incaslib="&lmvCasIn." outcaslib="&lmvCasOut." casdata="&lmvTableNm.&i." casout="&lmvTableNm.&i..sashdat" replace;
			%end;
		quit;
    %end;
%mend tech_table_partition;