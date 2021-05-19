%macro fmk100_load_etl_stg(mpResource=);
	%local lmvResource;
	%let lmvResource=&mpResource.;
	
	%tech_update_resource_status(mpStatus=P, mpResource=IA_&lmvResource.);
	
	%fmk_load_etl_stg(mpResource=&lmvResource);
	
	%tech_update_resource_status(mpStatus=L, mpResource=IA_&lmvResource.);
	
	%tech_open_resource(mpResource=STG_&lmvResource);
	
%mend fmk100_load_etl_stg;