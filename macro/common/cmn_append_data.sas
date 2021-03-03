%MACRO cmn_append_data(mpData=, mpBase=);

	%LET lmvData=&mpData.;
	%LET lmvBase=&mpBase.;

	PROC APPEND DATA=&lmvData. BASE=&lmvBase. FORCE;
	RUN;

%MEND cmn_append_data;