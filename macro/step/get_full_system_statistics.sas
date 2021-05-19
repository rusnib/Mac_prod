/* 
    АРГУМЕНТЫ:
		mpCasLibName - Наименование библиотеки CAS
		mpTableOut - Наименование таблицы вывода, в которую будет формироваться статистика
*/
%macro get_full_system_statistics(mpCasLibName=, mpTableOut=);
	%let lmvTableOut = &mpTableOut.;
	
	options nomprint nosymbolgen;
	cas;
	proc cas;
		accessControl.assumeRole / adminRole="superuser";
		accessControl.accessPersonalCaslibs;
		run;
		table.caslibinfo result=r / 
			showHidden=true, verbose=true;
			res=r.caslibinfo;
			saveresult res dataout=libinfo;
		run;
	quit;

	proc sql noprint;
		select distinct  name into :lmvLibNames separated by ' '
		from work.libinfo
		where name not like 'CASUSER%'
		;
		select count(*) as cnt into :lmvCnt
		from work.libinfo
		where name not like 'CASUSER%'
		;
	quit;
	
	/* Если имя CAS-либы было указано */
	%if %length(&mpCasLibName.) gt 0 %then %do;
		/* Если такая либа есть в списке имён */
		%if %index(&lmvLibNames., &mpCasLibName.) gt 0 %then %do;
			%let lmvLibNames = &mpCasLibName.;
			%let lmvCnt = 1;
		%end;
		%else %do;
			%put ERROR: CAS-library &mpCasLibName. does not exist.;
			%abort;
		%end;                                  
	%end;
	%put &=lmvLibNames; %put &=lmvCnt;
	
	/*Дроп выходной таблицы, если она раньше существовала*/
	%if %sysfunc(exist(&lmvTableOut.)) gt 0 %then %do;
		proc sql noprint;
			DROP TABLE &lmvTableOut;
		quit;
	%end;
		
		
	%do i=1 %to &lmvCnt;
		%let lmvTempLibName = %scan(&lmvLibNames., &i., %str( ));
		%put &=lmvTempLibName;
		proc cas;
			accessControl.assumeRole / adminRole="superuser";
			accessControl.accessPersonalCaslibs;
			run;
			table.tableinfo result=r /
				caslib="&lmvTempLibName.";
				if exists(r, "TableInfo") then do;
					t=r.tableinfo;
					saveresult t dataout=work.tblinfo; 
				end;
			run;
		quit;
		
		proc sql noprint;
			select distinct  name into :lmvTabNames separated by ' '
			from work.tblinfo
			where name not like 'CASUSERHDFS(%'
			;
			
			select count(*) as cnt into :lmvCntTab
			from work.tblinfo
			where name not like 'CASUSERHDFS(%'
			;
		quit;
		%put &=lmvTabNames; %put &=lmvCntTab;
		
		%do j=1 %to &lmvCntTab.;
			%let lmvTempTabName = %scan(&lmvTabNames., &j., %str( ));
			proc cas;
				accessControl.assumeRole / adminRole="superuser";
				accessControl.accessPersonalCaslibs;
				run;
				table.tabledetails result=r /
					caslib="&lmvTempLibName." name="&lmvTempTabName.";
					t=r.TableDetails;
					saveresult t dataout=work.tbldtls; 
				run;
			quit;
			
			/*оставляем только интересные поля*/
			proc sql;
				create table tbls_dtls as
				select "&lmvTempLibName." as LibName, "&lmvTempTabName." as TblName length=32, Rows, IndexSize, DataSize
				from WORK.TBLDTLS
				;
			quit; 
			%if %sysfunc(exist(&lmvTableOut.)) eq 0 %then %do;
				data &lmvTableOut.;
					set work.tbls_dtls;
					stop;
				run;
			%end;
			proc append base=&lmvTableOut. data=work.tbls_dtls force;
			quit;
		%end;
	%end;

	proc sql noprint;
		create table &lmvTableOut._sorted as
		select LibName
			,TblName
			,((DataSize/1048000)/1024) as InMemorySizeGB
			, IndexSize
			, Rows
			, sum(((DataSize/1048000)/1024)) as TotalInMemorySize
		from &lmvTableOut.
		order by DataSize desc;
	quit;
	
	proc sql noprint;
		create table &lmvTableOut._total as 
		select t1.*
				,sum(t1.totalSizeGb) as FullCaslibObjectSizeGb
		from (
			select sum(InMemorySizeGB) as totalSizeGb,
					libname
			from &lmvTableOut._sorted
			group by libname
			) t1
		order by t1.totalSizeGb desc
		;
	quit;

%mend get_full_system_statistics;