/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Поток для создания модели недельного профиля для разбивки
*	  по дням и переагрегации недель до месяцев
*
******************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
******************************************************************/
%etl_stream_start;

%vf100_001_train_week_profile;
%vf100_002_week_profile_gc;

%etl_stream_finish;