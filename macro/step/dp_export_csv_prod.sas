/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки csv файлов под DP
*	
*
*  ПАРАМЕТРЫ:
*	  mpInput       		- Наименование входной таблицы для экспорта
*     mpPath				- Наименование директории, в которую будет производиться экспорт
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %dp_export_csv_prod(mpInput=DM_ABT.PLAN_UPT_DAY
				, mpPath=/data/tmp/);
*
****************************************************************************
*  20-09-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro dp_export_csv_prod(mpInput=DM_ABT.PLAN_UPT_DAY, mpPath=/data/files/output/dp_files/);
		
	%local lmvOutLibref	
			lmvOutTabName
			;
		
	%member_names (mpTable=&mpInput, mpLibrefNameKey=lmvOutLibref, mpMemberNameKey=lmvOutTabName); 
	
	proc export data=&lmvOutLibref..&lmvOutTabName. (datalimit = all)
				outfile="&mpPath.&lmvOutTabName..csv"
				dbms=dlm
				replace
				;
				delimiter='|'
				;
	run;
					
%mend dp_export_csv_prod;