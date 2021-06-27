%tech_log_event(mpMode=START, mpProcess_Nm=tech_cfg_tables_updates);

	proc sql noprint;
        connect using etl_cfg;
        execute by etl_cfg 
        (
			insert into etl_cfg.cfg_cycle_id
				values (nextval('etl_cfg.cfg_cycle_id_seq'), now())
			;
        );
		disconnect from etl_cfg;
	quit;

	proc sql noprint;
        connect using etl_cfg;
        execute by etl_cfg 
        (
			insert into etl_cfg.cfg_status_table_hist
	            select resource_id, resource_nm, status_cd, processed_dttm
	            ,current_timestamp as transfer_dttm
				,retries_cnt
				/*,currval('etl_cfg.cfg_cycle_id_seq')*/
				,(select max(cycle_id) from etl_cfg.cfg_cycle_id)
          		from etl_cfg.cfg_status_table
          	;
        );
		disconnect from etl_cfg;
	quit;
	
	proc sql noprint;
        connect using etl_cfg;
        execute by etl_cfg 
        (
			
            truncate etl_cfg.cfg_status_table
            ;
        );
		disconnect from etl_cfg;
	quit;
	
	%tech_init_config_files(mpSchema=etl_cfg);

%tech_log_event(mpMode=END, mpProcess_Nm=tech_cfg_tables_updates);