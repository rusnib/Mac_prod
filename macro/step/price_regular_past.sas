/*****************************************************************
*  ВЕРСИЯ:
*   $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*   Реализация алгоритма вычисления регулярных цен на прошлое согласно п.1 постановки McD_price_calculation_v6.
*
*  ПАРАМЕТРЫ:
*	mpPromoTable    	 	 - Наименование входящего справочника с промо разметкой
*	mpPromoPboTable    		 - Наименование входящего справочника с промо-пбо разметкой
*	mpPromoProdTable   		 - Наименование входящего справочника с промо-скю разметкой
*   mpProductAttrTable       - Наименование входящего справочника с атрибутами скю
*	mpPriceTable 		 	 - Наименование входящей таблицы с фактическими ценами на прошлое
*	mpVatTable 		 		 - Наименование входящего справочника НДС
*	mpOutTable 		 		 - Наименование выходящей таблицы с рассчитанными регулярными ценами на прошлое
*	mpBatchValue 		 	 - Количество ПБО, используемых в одном батче
*
******************************************************************
*  Использует: 
*	нет
*
*  Устанавливает макропеременные:
*   нет
*
******************************************************************
*  Пример использования:
    %price_regular_past(
        mpPromoTable    	 = CASUSER.PROMO
        , mpPromoPboTable    = CASUSER.PROMO_PBO
        , mpPromoProdTable   = CASUSER.PROMO_PROD
        , mpProductAttrTable = CASUSER.PRODUCT_ATTRIBUTES
        , mpPriceTable 		 = CASUSER.PRICE
        , mpVatTable 		 = ETL_IA.VAT
        , mpOutTable 		 = CASUSER.PRICE_REG_PAST
        , mpBatchValue 		 = 7
    );
*
****************************************************************************
*  30-04-2021 		Мугтасимов Данил 		Начальное кодирование
****************************************************************************/

%macro price_regular_past(
    mpPromoTable    	    = CASUSER.PROMO
    , mpPromoPboTable       = CASUSER.PROMO_PBO_UNFOLD
    , mpPromoProdTable      = CASUSER.PROMO_PROD
    , mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
    , mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
    , mpPriceTable 		    = CASUSER.PRICE
    , mpVatTable 		    = CASUSER.VAT
    , mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
    , mpOutTable 		    = DM_ABT.PRICE_REGULAR_PAST
    , mpBatchValue 		    = 7
    );

    %local lmvRegPastMaxDate
           lmvPromoList
           lmvPromoProductIds
           lmvIterCounter
           lmvPboUsedNum
           lmvPboTotalNum
           lmvOutTableName
           lmvOutTableCLib
           lmvBatchValue
           lmvCheckNobs
           lmvPriceBatchDays1
        ;

    %member_names(mpTable=&mpOutTable, mpLibrefNameKey=lmvOutTableCLib, mpMemberNameKey=lmvOutTableName);
    
    %let lmvBatchValue = &mpBatchValue.;
    
    %if %sysfunc(sessfound(casauto))=0 %then %do;
        cas casauto;
        caslib _all_ assign;
    %end;
    
    proc casutil;  
        droptable casdata="&lmvOutTableName" incaslib="&lmvOutTableCLib" quiet;
    run;

    %let lmvRegPastMaxDate = 22249; /*"30NOV2020"d*/

    %let lmvPromoList = ('DISCOUNT', 'TEMP PRICE REDUCTION (DISCOUNT)', 'BOGO / 1+1', 'N+1', '1+1%', 'BUNDLE', 'EVM/SET', 'EVM / SET', 'PAIRS', 'PAIRS (DIFFERENT CATEGORIES)');
    
    /* Формирование списка товаров, введенных под промо */

    proc sql noprint;
        select distinct PRODUCT_ID
            into
                :lmvPromoProductIds separated by ","
        from &mpProductAttrTable
        where PRODUCT_ATTR_NM = 'REGULAR_ID'
            and PRODUCT_ID <> input(PRODUCT_ATTR_VALUE, 4.)
            and missing(PRODUCT_ATTR_VALUE) = 0
        ;
    quit;

    /* Джойн со справочниками. Создание промо-разметки CHANNEL_CD - SKU - ПБО - период- Флаг_промо */

    proc fedsql sessref=casauto noprint;
        create table CASUSER.PROMO_FILT_SKU_PBO{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t3.PRODUCT_ID
                , t2.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , t1.PROMO_MECHANICS
                , 1 as PROMO_FLAG
            from &mpPromoTable t1

            inner join &mpPromoPboTable t2
                on t1.PROMO_ID = t2.PROMO_ID

            inner join &mpPromoProdTable t3
                on t1.PROMO_ID = t3.PROMO_ID

            where upper(t1.PROMO_MECHANICS) in &lmvPromoList.
                and t1.CHANNEL_CD = 'ALL'
        ;
    quit;

    /* Обработка таблицы Price Increase */
    data CASUSER.PRICE_INCREASE_MODIFIED_1(drop=L_START_DT);
        set &mpPriceIncreaseTable;
        by PRICE_AREA_NM descending START_DT;
        format L_START_DT END_DT date9.;
        retain L_START_DT;

        L_START_DT = lag(START_DT);

        if first.PRICE_AREA_NM then do;
            END_DT = &ETL_SCD_FUTURE_DT.;
            L_START_DT = .;
            output;
        end;
        else if last.PRICE_AREA_NM then do;
            END_DT = L_START_DT - 1;
            output;
            END_DT = START_DT - 1;
            START_DT = &ETL_SCD_PAST_DT.;
            output;
        end;
        else do;
            END_DT = L_START_DT - 1;
            output;
        end;
    run;

    data CASUSER.PRICE_INCREASE_MODIFIED_2(drop=PERCENT_INCREASE rename=(NEW_PERCENT_INCREASE=PERCENT_INCREASE));
        set CASUSER.PRICE_INCREASE_MODIFIED_1;
        by PRICE_AREA_NM START_DT;
        retain NEW_PERCENT_INCREASE;

        if first.PRICE_AREA_NM then NEW_PERCENT_INCREASE = 1;
        NEW_PERCENT_INCREASE = coalesce(NEW_PERCENT_INCREASE, 1) * coalesce(PERCENT_INCREASE, 1);
    run;

    /* Промо-товары, из справочника Product Attributes не участвуют в вычислениях регулярных цен на прошлое. */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.PRICE_FILT{options replace=true} as
            select t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , t1.GROSS_PRICE_AMT
            from &mpPriceTable t1
            where t1.PRODUCT_ID not in (&lmvPromoProductIds.)
        ;
    quit;

    /* Создание пустой таблицы айдишников ПБО, в которой будут храниться уже посчитанные */
    data CASUSER.PBO_USED(keep=PBO_LOCATION_ID USED_FLAG);
        set CASUSER.PRICE_FILT;
        where PBO_LOCATION_ID < -1000;
        USED_FLAG = 1;
    run;

    proc fedsql sessref=casauto noprint;
        create table CASUSER.PBO_LIST_TMP{options replace=true} as
            select distinct PBO_LOCATION_ID
            from CASUSER.PRICE_FILT
        ;
    quit;

    data _NULL_;
        if 0 then set CASUSER.PBO_USED nobs=n;
        call symputx('lmvPboUsedNum', n);
        stop;
    run;
    data _NULL_;
        if 0 then set CASUSER.PBO_LIST_TMP nobs=n;
        call symputx('lmvPboTotalNum', n);
        stop;
    run;
    %let lmvIterCounter = 1;

    %do %while (&lmvPboUsedNum. < &lmvPboTotalNum.);

        /* Создание батча PBO start */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PBO_LIST{options replace=true} as
                select t1.PBO_LOCATION_ID
                from CASUSER.PBO_LIST_TMP t1		
                left join CASUSER.PBO_USED t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                where t2.USED_FLAG = . /*исключение уже посчитанных*/
            ;
        quit;
        
        data CASUSER.PBO_LIST_BATCH;
            set CASUSER.PBO_LIST(obs=&lmvBatchValue.);
            USED_FLAG = 1;
        run;

        proc casutil;droptable casdata="PBO_LIST" incaslib="CASUSER" quiet;run;

        /* Добавление в список посчитанных айдишников ПБО */
        data CASUSER.PBO_USED(append=yes);
            set CASUSER.PBO_LIST_BATCH;
        run;

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_FILT_SKU_PBO_BATCH{options replace=true} as
                select t1.*
                from CASUSER.PROMO_FILT_SKU_PBO t1
                inner join CASUSER.PBO_LIST_BATCH t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
            ;
        quit;

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH{options replace=true} as
                select t1.*
                from CASUSER.PRICE_FILT t1
                inner join CASUSER.PBO_LIST_BATCH t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
            ;
        quit;

        proc casutil;droptable casdata="PBO_LIST_BATCH" incaslib="CASUSER" quiet;run;
        /* Создание батча PBO end */

        /* Переход от start_dt end_dt интеревалов к подневному списку в ПРОМО разметке*/
        data CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS(rename=(START_DT=DAY_DT) drop=END_DT END_DT_TMP);
            set CASUSER.PROMO_FILT_SKU_PBO_BATCH(where=(START_DT < min(END_DT, &lmvRegPastMaxDate.)));
            END_DT_TMP = min(END_DT, &lmvRegPastMaxDate.);
            output;
            do while (START_DT < END_DT_TMP);
                START_DT = intnx('days', START_DT, 1);
                output;
            end;
        run;

        proc casutil;droptable casdata="PROMO_FILT_SKU_PBO_BATCH" incaslib="CASUSER" quiet;run;

        /* Переход от start_dt end_dt интеревалов к подневному списку в ФАКТИЧЕСКИХ ценах */
        data CASUSER.PRICE_BATCH_DAYS(rename=(START_DT=DAY_DT) drop=END_DT END_DT_TMP);
            set CASUSER.PRICE_BATCH(where=(START_DT < min(END_DT, &lmvRegPastMaxDate.)));
            END_DT_TMP = min(END_DT, &lmvRegPastMaxDate.);
            output;
            do while (START_DT < END_DT_TMP);
                START_DT = intnx('days', START_DT, 1);
                output;
            end;
        run;

        proc casutil;droptable casdata="PRICE_BATCH" incaslib="CASUSER" quiet;run;
        
        /* Джойн с промо-разметкой и проставление миссингов на цены с промо-днем = 1; замена на миссинги цены во время промо*/
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH_DAYS_1{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.DAY_DT
                    , case
                            when missing(t2.PROMO_FLAG) = 1 then t1.GROSS_PRICE_AMT
                            else .
                        end as GROSS_PRICE_AMT_NEW
                    , t2.PROMO_FLAG
                from CASUSER.PRICE_BATCH_DAYS t1
                
                left join CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.DAY_DT = t2.DAY_DT
            ;
        quit;

        proc casutil;droptable casdata="PROMO_FILT_SKU_PBO_BATCH_DAYS" incaslib="CASUSER" quiet;run;
        
        /* Продление каждого ВР без лидирующих и хвостовых заполнений, т.е. trimId="BOTH" */

        data _NULL_;
            if 0 then set CASUSER.PRICE_BATCH_DAYS_1 nobs=n;
            call symputx('lmvPriceBatchDays1', n);
            stop;
        run;

        %if &lmvPriceBatchDays1 > 0 %then %do;
            proc cas;
                timeData.timeSeries result =r /
                series={{name="GROSS_PRICE_AMT_NEW", Acc="sum", setmiss="PREV"}}
                tEnd= "&lmvRegPastMaxDate"
                table={caslib="CASUSER" ,name="PRICE_BATCH_DAYS_1", groupby={"PBO_LOCATION_ID","PRODUCT_ID"}}
                timeId="DAY_DT"
                interval="days"
                trimId="BOTH"
                casOut={caslib="CASUSER",name="PRICE_BATCH_DAYS_2", replace=True}
                ;
            run;
            quit;
        %end;
        %else %do;
            data CASUSER.PRICE_BATCH_DAYS_2;
               set CASUSER.PRICE_BATCH_DAYS_1;
           run;
        %end;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS_1" incaslib="CASUSER" quiet;run;

        /* Обработка случая, когда товар продаётся только во время промо: в этом случае регулярная цена = фактической цене START*/
        /* Комментарий: Учитывая отсекание из расчетов промо товаров, данная обработка не будет как-либо изменять данные. Оставили на случай неправильных входных данных */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.ALL_DAYS_PROMO{options replace=true} as
                select t2.PRODUCT_ID
                    , t2.PBO_LOCATION_ID
                    , 1 as ALL_DAYS_PROMO_FLG
                from
                    (select PRODUCT_ID
                        , PBO_LOCATION_ID
                        , sum(GROSS_PRICE_AMT_NEW) as GROSS_PRICE_AMT_SUM
                    from CASUSER.PRICE_BATCH_DAYS_2
                    group by PRODUCT_ID
                        , PBO_LOCATION_ID) as t2

                where t2.GROSS_PRICE_AMT_SUM = . 
            ;
        quit;
        
        proc fedsql sessref=casauto noprint;
            create table CASUSER.ALL_DAYS_PROMO_1{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.DAY_DT
                    , t1.GROSS_PRICE_AMT as GROSS_PRICE_AMT_NEW
                from CASUSER.PRICE_BATCH_DAYS t1
                inner join CASUSER.ALL_DAYS_PROMO t2
                    on t1.PRODUCT_ID = t2.PRODUCT_ID 
                        and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
            ;
        quit;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS" incaslib="CASUSER" quiet;run;

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH_DAYS_3{options replace=true} as
                select t1.*
                from CASUSER.PRICE_BATCH_DAYS_2 t1
                
                left join CASUSER.ALL_DAYS_PROMO t2
                    on t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID

                where t2.all_days_promo_flg = .
            ;
        quit;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS_2" incaslib="CASUSER" quiet;run;
        proc casutil;droptable casdata="ALL_DAYS_PROMO" incaslib="CASUSER" quiet;run;

        data CASUSER.PRICE_BATCH_DAYS_4;
            set CASUSER.PRICE_BATCH_DAYS_3
                CASUSER.ALL_DAYS_PROMO_1;
        run;

        proc casutil;droptable casdata="ALL_DAYS_PROMO_1" incaslib="CASUSER" quiet;run;
        proc casutil;droptable casdata="PRICE_BATCH_DAYS_3" incaslib="CASUSER" quiet;run;

        /* Обработка случая, когда товар продаётся только во время промо: в этом случае регулярная цена = фактической цене END*/
        /* Обработка случая, когда товар вводится в промо и протягивать нечем, поэтому регулярная цена равна миссинг. В этом случае, рег цена первой немиссинговой факт цене START*/
        
        /*Создание справочника с минимальной датой продажи и немиссинговой ценой */
        data CASUSER.PRICE_BATCH_DAYS_4_1;
            set CASUSER.PRICE_BATCH_DAYS_4;
            by PBO_LOCATION_ID PRODUCT_ID DAY_DT;
            where GROSS_PRICE_AMT_NEW is not missing;
            if first.PRODUCT_ID then do;
                FIRST_NONMISS_GROSS_PRICE = GROSS_PRICE_AMT_NEW;
                output;
            end;
        run;

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH_DAYS_4_2{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.DAY_DT
                    , t1.GROSS_PRICE_AMT_NEW
                    , t2.FIRST_NONMISS_GROSS_PRICE
                    , case
                            when (t1.GROSS_PRICE_AMT_NEW is missing) and (t1.DAY_DT < t2.DAY_DT) then t2.FIRST_NONMISS_GROSS_PRICE
                            else t1.GROSS_PRICE_AMT_NEW
                        end as GROSS_PRICE_AMT
                from CASUSER.PRICE_BATCH_DAYS_4 t1
                
                left join CASUSER.PRICE_BATCH_DAYS_4_1 t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t2.PRODUCT_ID
            ;
        quit;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS_4" incaslib="CASUSER" quiet;run;
        proc casutil;droptable casdata="PRICE_BATCH_DAYS_4_1" incaslib="CASUSER" quiet;run;
        
        /* Обработка случая, когда товар вводится в промо и протягивать нечем, поэтому регулярная цена равна миссинг. В этом случае, рег цена первой немиссинговой факт цене END*/

        /* Идентификация скачков более чем на 5% и их замена на предыдущее значение цены */
        data CASUSER.PRICE_BATCH_DAYS_5(keep=PRODUCT_ID PBO_LOCATION_ID DAY_DT GROSS_PRICE_AMT);
            set CASUSER.PRICE_BATCH_DAYS_4_2;
            by PBO_LOCATION_ID PRODUCT_ID DAY_DT;
            retain PREV_GROSS;
        
            if first.PRODUCT_ID then do;
                PREV_GROSS = coalesce(GROSS_PRICE_AMT, -1000);
            end;
        
            if (PREV_GROSS > coalesce(GROSS_PRICE_AMT, 0)*(1.05)) then do;
                ALERT_FLAG = 1;
                GROSS_PRICE_AMT = PREV_GROSS;
            end;
        
            PREV_GROSS = max(PREV_GROSS, coalesce(GROSS_PRICE_AMT, 0));
        run;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS_4_2" incaslib="CASUSER" quiet;run;
        
        /* Округление регулярных гросс цен до целого числа и фильтрация дат по открытию или полному закрытию ПБО.*/

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH_DAYS_6{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.DAY_DT
                    , t2.A_OPEN_DATE
                    , t1.GROSS_PRICE_AMT
                from CASUSER.PRICE_BATCH_DAYS_5 t1

                left join CASUSER.PBO_DICTIONARY t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID

                where t2.A_OPEN_DATE is not null
                    and t1.DAY_DT between t2.A_OPEN_DATE
                    and coalesce(t2.A_CLOSE_DATE, date%str(%')&VF_FC_AGG_END_DT.%str(%'))
            ;
        quit;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS_5" incaslib="CASUSER" quiet;run;
        
        data _NULL_;
            if 0 then set CASUSER.PRICE_BATCH_DAYS_6 nobs=n;
            call symputx('lmvCheckNobs',n);
            stop;
        run;
        
        %if &lmvCheckNobs. > 0 %then %do;
            
            /* Переход от подневной гранулярности к периодной */

            data CASUSER.REG_INTERVALS(rename=(PRICE_GRO=GROSS_PRICE_AMT));
                set CASUSER.PRICE_BATCH_DAYS_6;
                by PBO_LOCATION_ID PRODUCT_ID A_OPEN_DATE DAY_DT;
                keep PBO_LOCATION_ID PRODUCT_ID A_OPEN_DATE START_DT END_DT PRICE_GRO;
                format START_DT END_DT date9.;
                retain START_DT END_DT PRICE_GRO L_GROSS_PRICE;
                
                L_GROSS_PRICE = lag(GROSS_PRICE_AMT);
                L_DAY_DT = lag(DAY_DT);
                
                /*первое наблюдение в ряду - сбрасываем хар-ки интервала*/
                if first.PRODUCT_ID then do;
                    START_DT = DAY_DT;
                    END_DT =.;
                    PRICE_GRO = GROSS_PRICE_AMT;
                    L_GROSS_PRICE = .z;
                    L_DAY_DT = .;
                end;
                
                /*сбрасываем текущий интервал, готовим следующий*/
                if (GROSS_PRICE_AMT ne L_GROSS_PRICE or L_DAY_DT ne DAY_DT - 1) and not first.PRODUCT_ID then do;
                    END_DT = L_DAY_DT;
                    output;
                    START_DT = DAY_DT;
                    END_DT = .;
                    PRICE_GRO = GROSS_PRICE_AMT;
                end;
                if last.PRODUCT_ID then do;
                    END_DT = DAY_DT;
                    output;
                end;
            run;

            proc casutil;droptable casdata="PRICE_BATCH_DAYS_6" incaslib="CASUSER" quiet;run;
            
            data WORK.REG_INTERVALS;
                set CASUSER.REG_INTERVALS;
            run;
            
            proc casutil;droptable casdata="REG_INTERVALS" incaslib="CASUSER" quiet;run;
            
            /*Обработка неоцифрованных промо во время открытия ПБО START*/
            proc sort data=WORK.REG_INTERVALS;
                by PBO_LOCATION_ID A_OPEN_DATE PRODUCT_ID START_DT END_DT;
            run;
            
            proc expand data=WORK.REG_INTERVALS out=CASUSER.REG_INTERVALS_1;
                convert GROSS_PRICE_AMT = LEAD_GROSS_PRICE /transformout = (lead 1);
                convert START_DT = LEAD_START_DT /transformout = (lead 1);
                by PBO_LOCATION_ID A_OPEN_DATE PRODUCT_ID;
            run;

            proc sql;drop table WORK.REG_INTERVALS;quit;  

            data CASUSER.REG_INTERVALS_2(drop=A_OPEN_DATE LEAD_GROSS_PRICE LEAD_START_DT);
                set CASUSER.REG_INTERVALS_1;
                by PBO_LOCATION_ID PRODUCT_ID START_DT END_DT;
                retain PROMO_OPEN_FLAG;

                if  START_DT = A_OPEN_DATE
                    and END_DT - START_DT < 6
                    and missing(GROSS_PRICE_AMT) = 0
                    and missing(LEAD_GROSS_PRICE) = 0
                    and missing(LEAD_START_DT) = 0
                    and intnx('day', LEAD_START_DT, -1) = END_DT
                    and GROSS_PRICE_AMT <= LEAD_GROSS_PRICE * 0.95
                    
                then PROMO_OPEN_FLAG = 1;
                        
                /*Сдвигаем начало интервала на дату открытия ПБО.*/
                if 'TIME'n = 1 and PROMO_OPEN_FLAG = 1 then do;
                    START_DT = A_OPEN_DATE;
                    PROMO_OPEN_FLAG = .;
                end;
            run;

            proc casutil;droptable casdata="REG_INTERVALS_1" incaslib="CASUSER" quiet;run;
            
            data WORK.REG_INTERVALS_3(drop='TIME'n);
                set CASUSER.REG_INTERVALS_2;
                
                /*Убираем интервалы, которые были перекрыты в предыдущем степе*/
                if 'TIME'n = 0 and PROMO_OPEN_FLAG = 1 then delete;
            run;

            proc casutil;droptable casdata="REG_INTERVALS_2" incaslib="CASUSER" quiet;run;
            
            /*Обработка неоцифрованных промо во время открытия ПБО END*/
            
            /*Обработка случайных колебаний цен вниз окном. Если есть скачки кратковременные скачки вниз, 
                а потом возвращение на прежнюю цену, то скачок цены игнорируется START*/
            proc sort data=WORK.REG_INTERVALS_3(drop=PROMO_OPEN_FLAG);
                by PBO_LOCATION_ID PRODUCT_ID START_DT END_DT;
            run;

            proc expand data=WORK.REG_INTERVALS_3 out=CASUSER.REG_INTERVALS_4;
                convert GROSS_PRICE_AMT = LAG_GROSS_PRICE /transformout = (lag 1);
                convert GROSS_PRICE_AMT = LEAD_GROSS_PRICE /transformout = (lead 1);
                by PBO_LOCATION_ID PRODUCT_ID;
            run;

            proc sql;drop table WORK.REG_INTERVALS_3;quit;

            data CASUSER.REG_INTERVALS_5;
                set CASUSER.REG_INTERVALS_4;
                by PBO_LOCATION_ID PRODUCT_ID START_DT END_DT;
                retain CHANGE_PRICE_FLAG;
                
                if first.PRODUCT_ID then CHANGE_PRICE_FLAG = 0;
                
                if missing(LAG_GROSS_PRICE) = 0
                   and missing(LEAD_GROSS_PRICE) = 0
                   and CHANGE_PRICE_FLAG = 0
                   and GROSS_PRICE_AMT ne LAG_GROSS_PRICE
                   and LAG_GROSS_PRICE - LEAD_GROSS_PRICE < 0.0001
                   and END_DT - START_DT le 3
                then do;
                   CHANGE_PRICE_FLAG = 1;
                   GROSS_PRICE_AMT = LAG_GROSS_PRICE;
                end;
                else CHANGE_PRICE_FLAG = 0;
            run;

            proc casutil;droptable casdata="REG_INTERVALS_4" incaslib="CASUSER" quiet;run;

            data CASUSER.REG_INTERVALS_DAYS(rename=(START_DT=DAY_DT) keep=PRODUCT_ID PBO_LOCATION_ID START_DT GROSS_PRICE_AMT);
                set CASUSER.REG_INTERVALS_5;
                output;
                do while (START_DT < END_DT);
                    START_DT = intnx('days', START_DT, 1);
                    output;
                end;
            run;

            proc casutil;droptable casdata="REG_INTERVALS_5" incaslib="CASUSER" quiet;run;
            
            data CASUSER.REG_INTERVALS(rename=(PRICE_GRO=GROSS_PRICE_AMT));
                set CASUSER.REG_INTERVALS_DAYS;
                by PBO_LOCATION_ID PRODUCT_ID DAY_DT;
                keep PBO_LOCATION_ID PRODUCT_ID START_DT END_DT PRICE_GRO;
                format START_DT END_DT date9.;
                retain START_DT END_DT PRICE_GRO L_GROSS_PRICE;
                
                L_GROSS_PRICE = lag(GROSS_PRICE_AMT);
                L_DAY_DT = lag(DAY_DT);
                
                /*первое наблюдение в ряду - сбрасываем хар-ки интервала*/
                if first.PRODUCT_ID then do;
                    START_DT = DAY_DT;
                    END_DT = .;
                    PRICE_GRO = GROSS_PRICE_AMT;
                    L_GROSS_PRICE = .z;
                    L_DAY_DT = .;
                end;
                
                /*сбрасываем текущий интервал, готовим следующий*/
                if (GROSS_PRICE_AMT ne L_GROSS_PRICE or L_DAY_DT ne DAY_DT - 1) and not first.PRODUCT_ID then do;
                    END_DT = L_DAY_DT;
                    output;
                    START_DT = DAY_DT;
                    END_DT = .;
                    PRICE_GRO = GROSS_PRICE_AMT;
                end;
                if last.PRODUCT_ID then do;
                    END_DT = DAY_DT;
                    output;
                end;
            run;

            proc casutil;droptable casdata="REG_INTERVALS_DAYS" incaslib="CASUSER" quiet;run;

            /*Обработка колебаний цен вниз окном. Если есть скачки кратковременные скачки вниз, 
                а потом возвращение на прежнюю цену, то скачок цены игнорируется END*/		

            /* Вычисление цен без НДС */

            proc fedsql sessref=casauto noprint;
                create table CASUSER.REG_INTERVALS_OUT_1{options replace=true} as
                    select 'ALL' as CHANNEL_CD
                        , t1.PRODUCT_ID
                        , t1.PBO_LOCATION_ID
                        , t1.START_DT as START_DT_PRICE
                        , t1.END_DT as END_DT_PRICE
                        , t2.START_DT as START_DT_VAT
                        , t2.END_DT as END_DT_VAT
                        , t2.VAT
                        , case 
                                when (t1.START_DT between t2.START_DT and t2.END_DT) and (t1.END_DT between t2.START_DT and t2.END_DT) then 1
                                when (t2.START_DT < t1.START_DT) and (t2.END_DT between t1.START_DT and t1.END_DT) then 2
                                when (t2.START_DT between t1.START_DT and t1.END_DT) and (t2.END_DT between t1.START_DT and t1.END_DT) then 3
                                when (t2.END_DT > t1.END_DT) and (t2.START_DT between t1.START_DT and t1.END_DT) then 4
                            end as INTERSECT_TYPE
                        , t1.GROSS_PRICE_AMT
                    from CASUSER.REG_INTERVALS t1

                    left join &mpVatTable t2
                        on  t1.PRODUCT_ID = t2.PRODUCT_ID
                            and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                            and ( ( (t1.START_DT between t2.START_DT and t2.END_DT) and (t1.END_DT between t2.START_DT and t2.END_DT) )
                            or ( (t2.START_DT < t1.START_DT) and (t2.END_DT between t1.START_DT and t1.END_DT) )
                            or ( (t2.START_DT between t1.START_DT and t1.END_DT) and (t2.END_DT between t1.START_DT and t1.END_DT) )
                            or ( (t2.END_DT > t1.END_DT) and (t2.START_DT between t1.START_DT and t1.END_DT) ) )
                ;
            quit;

            data CASUSER.REG_INTERVALS_OUT(keep=CHANNEL_CD PRODUCT_ID PBO_LOCATION_ID START_DT END_DT NET_PRICE_AMT GROSS_PRICE_AMT);
                format START_DT date9. END_DT date9.;
                set CASUSER.REG_INTERVALS_OUT_1;
                
                if INTERSECT_TYPE = 1 then do;
                    START_DT = START_DT_PRICE;
                    END_DT = END_DT_PRICE;
                end;
                if INTERSECT_TYPE = 2 then do;
                    START_DT = START_DT_PRICE;
                    END_DT = END_DT_VAT;
                end;
                if INTERSECT_TYPE = 3 then do;
                    START_DT = START_DT_VAT;
                    END_DT = END_DT_VAT;
                end;
                if INTERSECT_TYPE = 4 then do;
                    START_DT = START_DT_VAT;
                    END_DT = END_DT_PRICE;
                end;
                if missing(VAT) = 0 and missing(GROSS_PRICE_AMT) = 0 then NET_PRICE_AMT = divide(GROSS_PRICE_AMT, (1 + divide(VAT, 100)));
                else NET_PRICE_AMT = .;
            run;

            /* Применение таблицы с плановым повышением цен START */

            proc fedsql sessref=casauto noprint;
                create table CASUSER.UNION_MECHANICS_3{options replace=true} as
                    select t1.CHANNEL_CD
                        , t1.PRODUCT_ID
                        , t1.PBO_LOCATION_ID
                        , t1.START_DT as START_DT_PRICE
                        , t1.END_DT as END_DT_PRICE
                        , t3.START_DT as START_DT_INC
                        , t3.END_DT as END_DT_INC
                        , coalesce(t2.PBO_LOC_ATTR_VALUE, 'Price Regular') as PBO_LOC_ATTR_VALUE
                        , t3.PERCENT_INCREASE
                        , case 
                                when (t1.START_DT between t3.START_DT and t3.END_DT) and (t1.END_DT between t3.START_DT and t3.END_DT) then 1
                                when (t3.START_DT < t1.START_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) then 2
                                when (t3.START_DT between t1.START_DT and t1.END_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) then 3
                                when (t3.END_DT > t1.END_DT) and (t3.START_DT between t1.START_DT and t1.END_DT) then 4
                            end as INTERSECT_TYPE
                        , t1.NET_PRICE_AMT AS NET_PRICE_AMT_TMP
                        , t1.GROSS_PRICE_AMT AS GROSS_PRICE_AMT_TMP
                    from CASUSER.REG_INTERVALS_OUT t1

                    left join ( select PBO_LOCATION_ID
                                    , PBO_LOC_ATTR_VALUE
                                from &mpPboLocAttributes
                                where PBO_LOC_ATTR_NM = 'PRICE_AREA_NAME' ) t2
                        on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID

                    left join CASUSER.PRICE_INCREASE_MODIFIED_2 t3
                        on coalesce(t2.PBO_LOC_ATTR_VALUE, 'Price Regular') = t3.PRICE_AREA_NM
                            and ( ( (t1.START_DT between t3.START_DT and t3.END_DT) and (t1.END_DT between t3.START_DT and t3.END_DT) )
                            or ( (t3.START_DT < t1.START_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) )
                            or ( (t3.START_DT between t1.START_DT and t1.END_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) )
                            or ( (t3.END_DT > t1.END_DT) and (t3.START_DT between t1.START_DT and t1.END_DT) ) )
                ;
            quit;

            data CASUSER.REG_INTERVALS_OUT_2(keep=CHANNEL_CD PRODUCT_ID PBO_LOCATION_ID START_DT END_DT NET_PRICE_AMT GROSS_PRICE_AMT);
                format START_DT date9. END_DT date9.;
                set CASUSER.UNION_MECHANICS_3;
                
                if INTERSECT_TYPE = 1 then do;
                    START_DT = START_DT_PRICE;
                    END_DT = END_DT_PRICE;
                end;
                if INTERSECT_TYPE = 2 then do;
                    START_DT = START_DT_PRICE;
                    END_DT = END_DT_INC;
                end;
                if INTERSECT_TYPE = 3 then do;
                    START_DT = START_DT_INC;
                    END_DT = END_DT_INC;
                end;
                if INTERSECT_TYPE = 4 then do;
                    START_DT = START_DT_INC;
                    END_DT = END_DT_PRICE;
                end;

                if missing(PERCENT_INCREASE) = 0 and missing(GROSS_PRICE_AMT_TMP) = 0 and missing(NET_PRICE_AMT_TMP) = 0 then do;
                    GROSS_PRICE_AMT = round(GROSS_PRICE_AMT_TMP * PERCENT_INCREASE);
                    NET_PRICE_AMT = round(NET_PRICE_AMT_TMP * PERCENT_INCREASE, 0.01);
                end;
                else do;
                    GROSS_PRICE_AMT = .;
                    NET_PRICE_AMT = .;
                end;
            run;

            proc casutil;droptable casdata="UNION_MECHANICS_3" incaslib="CASUSER" quiet;run;

            /* Применение таблицы с плановым повышением цен END */

            data CASUSER.&lmvOutTableName(append=yes);
                set CASUSER.REG_INTERVALS_OUT_2;
            run;

            proc casutil;droptable casdata="REG_INTERVALS" incaslib="CASUSER" quiet;run;
            proc casutil;droptable casdata="REG_INTERVALS_OUT" incaslib="CASUSER" quiet;run;
            proc casutil;droptable casdata="REG_INTERVALS_OUT_1" incaslib="CASUSER" quiet;run;
            proc casutil;droptable casdata="REG_INTERVALS_OUT_2" incaslib="CASUSER" quiet;run;
        %end;

        %let lmvIterCounter = %eval(&lmvIterCounter. + 1);

        data _NULL_;
            if 0 then set CASUSER.PBO_USED nobs=n;
            call symputx('lmvPboUsedNum',n);
            stop;
        run;

    %end;

    proc casutil;
        promote casdata="&lmvOutTableName" incaslib="casuser" outcaslib="&lmvOutTableCLib";
		save incaslib="&lmvOutTableCLib." outcaslib="&lmvOutTableCLib." casdata="&lmvOutTableName." casout="&lmvOutTableName..sashdat" replace; 
    run;

    proc casutil;
        droptable casdata="PROMO_FILT_SKU_PBO" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_FILT" incaslib="CASUSER" quiet;
        droptable casdata="PBO_USED" incaslib="CASUSER" quiet;
        droptable casdata="PBO_LIST_TMP" incaslib="CASUSER" quiet;
        droptable casdata="PBO_LIST" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_INCREASE_MODIFIED_1" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_INCREASE_MODIFIED_2" incaslib="CASUSER" quiet;
    run;

%mend price_regular_past;

/*%price_regular_past();*/
