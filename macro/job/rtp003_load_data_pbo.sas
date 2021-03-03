/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки данных в рамках сквозного процесса для оперпрогноза (pbo)
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
*     %rtp003_load_data_pbo;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp003_load_data_pbo;
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_abt_pbo_prepare);
	/* 1. загрузка данных в CAS */
	%rtp_3_load_data_pbo(mpMode=A, 
							mpOutTableTrain=mn_short.pbo_train,
							mpOutTableScore=mn_short.pbo_score); 
	%tech_update_resource_status(mpStatus=L, mpResource=rtp_abt_pbo_prepare);
	%tech_open_resource(mpResource=rtp_abt_pbo);
	
	*%tech_cas_session(mpMode = end
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
	
%mend rtp003_load_data_pbo;