/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Загружает служебные таблицы в CAS
*
*  ПАРАМЕТРЫ:
*     Нет
*									
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
*	%load_common_tables;
*
****************************************************************************
*  25-08-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro load_common_tables(mpOutCaslib = mn_dict);
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	/* ENCODING_CHANNEL_CD into &mpOutCaslib. */
	proc casutil;
		droptable casdata="ENCODING_CHANNEL_CD" incaslib="&mpOutCaslib." quiet;
	run;
	
	data &mpOutCaslib..ENCODING_CHANNEL_CD(drop=channel_lvl channel_nm member_rk parent_channel_cd promote=yes);
		set ETL_IA.CHANNEL_LOOKUP;
		channel_cd=channel_cd;
		channel_cd_id=member_rk;
	run;

	proc casutil;
		save incaslib="&mpOutCaslib." outcaslib="&mpOutCaslib." casdata="ENCODING_CHANNEL_CD" casout="ENCODING_CHANNEL_CD.sashdat" replace; 
	run;
%mend load_common_tables;