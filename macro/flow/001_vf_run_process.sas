/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Поток запуска процесса сквозного прогнозирования временными рядами
*
******************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
******************************************************************/
%etl_stream_start;

%vf000_001_load_data;
%vf000_002_prepare_ts_abt_pbo;
%vf000_003_run_project_pbo;
%vf000_004_prepare_ts_abt_pmix;
%vf000_005_run_project_pmix;
%vf000_006_month_aggregation;

%etl_stream_finish;