/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в рамках сквозного процесса для оперпрогноза (мастеркоды)
*	
*
*  ПАРАМЕТРЫ:
*     Нет
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
*     %rtp002_load_data_mcode;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp002_load_data_mcode;
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_load_data_mcode);						
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_abt_pmix);
	
	%rtp_2_load_data_mastercode( mpMode=A,
							mpInputTableScore=mn_short.all_ml_scoring, 
							mpInputTableTrain=mn_short.all_ml_train,
							mpOutputTableScore = mn_short.master_code_score,
							mpOutputTableTrain = mn_short.master_code_train,
							mpWorkCaslib=mn_short
							);
	
	%tech_update_resource_status(mpStatus=L, mpResource=rtp_abt_pmix);
	%tech_open_resource(mpResource=rtp_abt_mc);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_load_data_mcode);	
	
%mend rtp002_load_data_mcode;