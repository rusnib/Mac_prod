/***************************************************************************
* Наименование:      
*   %tech_cas_session
*
* Назначение:
*   Скрипт для запуска и остановки сессий CAS
*
* Входные параметры:
*	mpCasSessNm=				+ Наименование CAS-сессии
*	mpAssignFlg=				+ Флаг о подключении caslib (y/n)
*	mpMode=				+ Статус сессии: запущен (start) или завершён (end)
* Выходные параметры:
*   нет
*
* Глобальные макропеременные:
*
* Пример вызова: 
*	%tech_cas_session(
*		mpCasSessNm= casauto,
*		mpAssignFlg= y,
*		mpMode= start
*	);
*
* Версия:
*   1	-	13.01.2021	- Alexey Samsonov, Initial version
***************************************************************************/

%macro tech_cas_session(mpMode= 
						,mpCasSessNm=
						,mpAssignFlg=
						);

	%local mpMode 
			mpAssignFlg
			mpCasSessNm
	;
	
	%let lmvMode = %upcase(&mpMode.);
	%let lmvAssignFlg = %upcase(&mpAssignFlg.);
	%let lmvCasSessName = %upcase(&mpCasSessNm.);

	%if &lmvMode. = START %then %do;
		cas &lmvCasSessName. sessopts=(metrics=true);
		%if &lmvAssignFlg. = Y %then %do;
			caslib _ALL_ ASSIGN SESSREF=&lmvCasSessName.;
		%end;
	%end;
	%if &lmvMode. = END %then %do;
		cas &lmvCasSessName. terminate;
	%end;

%mend tech_cas_session;