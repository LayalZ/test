--
-- PostgreSQL database dump
--

-- Dumped from database version 12.4
-- Dumped by pg_dump version 12.10

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: datamart; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA datamart;


ALTER SCHEMA datamart OWNER TO postgres;

--
-- Name: dblink; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS dblink WITH SCHEMA public;


--
-- Name: EXTENSION dblink; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION dblink IS 'connect to other PostgreSQL databases from within a database';


--
-- Name: postgres_fdw; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;


--
-- Name: EXTENSION postgres_fdw; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgres_fdw IS 'foreign-data wrapper for remote PostgreSQL servers';


--
-- Name: tablefunc; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS tablefunc WITH SCHEMA public;


--
-- Name: EXTENSION tablefunc; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION tablefunc IS 'functions that manipulate whole tables, including crosstab';


--
-- Name: convert_cron_expression(text); Type: FUNCTION; Schema: datamart; Owner: postgres
--

CREATE FUNCTION datamart.convert_cron_expression(exp text) RETURNS text[]
    LANGUAGE plpgsql
    AS $$
declare
    groups    text[];
	part_year int;
	part_month text;
	part_day   int;
	date_list  text[];  
	i integer;
begin
 
    if exp is null then
        -- raise notice 'invalid parameter "exp": must not be null';
		return date_list;
    end if;

    groups = regexp_split_to_array(trim(exp), '\s+');
	-- RAISE NOTICE 'groups: %', groups;
    if array_length(groups, 1) < 7 then
        -- raise notice 'invalid parameter "exp": seven space-separated fields expected';
		return date_list;
    end if;
    i=1;
	for part_year in select * from regexp_split_to_table(groups[7], ',')
    loop 
		for part_month in select * from regexp_split_to_table(groups[5], ',')
		loop 
			for part_day in select * from regexp_split_to_table(groups[4], ',')
			loop
				RAISE NOTICE 'Holiday: %', part_year || part_month || part_day;
				date_list[i]=part_year || '-' || UPPER(part_month) || '-'|| LPAD(part_day::text,2,'0');
				i=i+1;
			end loop;
		end loop; 
     end loop;

    return date_list;
end
$$;


ALTER FUNCTION datamart.convert_cron_expression(exp text) OWNER TO postgres;

--
-- Name: usp_createdateandtime(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_createdateandtime()
    LANGUAGE plpgsql
    AS $$
DECLARE vYear smallint;
DECLARE vMonth smallint;
DECLARE vDate timestamp;
DECLARE vdayInterval smallint;

BEGIN
	FOR vYear IN 2015..2045 by 1 LOOP 
		INSERT INTO datamart."Year"("Number")
		VALUES (vYEar);
		
		FOR vMonth IN 1..12 by 1 LOOP 
		INSERT INTO datamart."Month"("Number","Name","MONTH_YEAR")
		SELECT vMonth,TO_CHAR(TO_DATE (vMonth::text, 'MM'), 'Month'),
			CASE WHEN vMonth<10 THEN '0' || CAST(vMonth as varchar(1)) ELSE CAST(vMonth AS varchar(2)) END || '_' || CAST(vYear AS varchar(4));
    	END LOOP;
	END LOOP;
	
	--update ID_Year for Month Table
	UPDATE datamart."Month"
	SET "ID_YEAR" = yr."ID_Year"
	FROM datamart."Year" yr
	WHERE yr."Number" = CAST(substring("MONTH_YEAR",4,4) as integer);
	
	--Trading Period
	FOR vYear IN 2020..2035 by 1 LOOP 
	--vDate=To_TIMESTAMP(CAST(vYear AS varchar(4)) || '-01-01 00:00:00','YYYY-MM-dd HH24:mi:ss');
	vDate=To_DATE(CAST(vYear AS varchar(4)) || '-01-01','YYYY-MM-dd');	
	WHILE vDate <= TO_DATE(CAST(vYear AS varchar(4)) || '-12-31','YYYY-MM-dd') LOOP
		FOR vdayInterval IN 1..54 by 1 LOOP 
			INSERT INTO datamart."TRADING_PERIOD" ("DATE_KEY_OMAN","TRADING_PERIOD","DAY_NAME","DAY_OF_MONTH","YEAR","WEEKEND_OMAN")
			SELECT vDate, vdayInterval, to_char(vDate, 'Day'),EXTRACT(DAY FROM vDate), vYear,
					CASE WHEN to_char(vDate, 'Day') like 'Friday%' OR to_char(vDate, 'Day')like  'Saturday%' THEN CAST('true' AS BOOLEAN) else CAST('false' AS Boolean) END;
		END LOOP;
	vDate = vDate + (1 * interval '1 days');
	END LOOP;
	END LOOP;
	
	/*Trading Period update ID MONTH and Year*/
	UPDATE datamart."TRADING_PERIOD"
	SET "ID_Year"=yr."ID_Year",
		"ID_Month"=mnt."ID_Month"
	FROM datamart."Year" yr  
	JOIN datamart."Month" mnt on yr."ID_Year" = mnt."ID_YEAR" 
	WHERE "YEAR"=yr."Number" and mnt."Number"=EXTRACT(MONTH FROM "DATE_KEY_OMAN");
	
	
	UPDATE datamart."TRADING_PERIOD"
	set "DATE_KEY_UTC" = ("DATE_KEY_OMAN" - time '4:00')::date,
	    "QUARTER" = EXTRACT (QUARTER FROM "DATE_KEY_OMAN");
	
END;
$$;


ALTER PROCEDURE datamart.usp_createdateandtime() OWNER TO postgres;

--
-- Name: usp_importstandingdata(character varying); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_importstandingdata(tblname character varying)
    LANGUAGE plpgsql
    AS $$
DECLARE  sqlstr text;
DECLARE connstr text;

BEGIN
-- stored procedure body
connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';

sqlstr= 'SELECT "entitydef_name", "description", "eic_code" ' || 
		'FROM vw_schedulevaluestring svs'  ||  chr(13);

	IF tblName = 'STD_CORRIDOR' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''CORRIDOR'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_CORRIDOR"("Name","Description", "EIC_CODE")
		SELECT MP_Name, description, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (MP_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_CORRIDOR" MP on t1.mp_name=MP."Name"
		WHERE MP."Name" IS NULL;
	end if;

	IF tblName = 'STD_MARKET_PARTY' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''MARKETPARTY'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_MARKET_PARTY"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT MP_Name, description, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (MP_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_MARKET_PARTY" MP on t1.mp_name=MP."NAME"
		WHERE MP."NAME" IS NULL;
	end if;
	
	IF tblName = 'STD_PRODUCTION_FACILITY' THEN
		sqlstr:= 'SELECT entitydef_name, description, "PFAC_CAPACITY", "PFAC_TRANSLOSS" ' ||
			   ' FROM vw_xtabschedulevaluenumber';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_PRODUCTION_FACILITY"("NAME","DESCRIPTION", "REGISTERED_CAPACITY","TRANSMISSION_LOSS")
		SELECT MF_Name, description, REGISTERED_CAPACITY, TRANSMISSION_LOSS
		FROM dblink(connstr,sqlstr)
		AS t1 (MF_Name varchar(256), DESCRIPTION varchar(256), REGISTERED_CAPACITY numeric, TRANSMISSION_LOSS numeric)
		LEFT JOIN datamart."STD_PRODUCTION_FACILITY" MF on t1.mf_name=MF."NAME"
		WHERE MF."NAME" IS NULL;
		
		--update EIC_CODE(s)
		update datamart."STD_PRODUCTION_FACILITY"
		SET "EIC_CODE"=t1."EIC_CODE"
		FROM dblink(connstr
					,'SELECT entitydef_name, description, "EIC_OPRODFAC" FROM public.vw_xtabschedulevaluestring
					 WHERE "EIC_OPRODFAC" is not null')
		AS t1 (pf_name varchar(256), DESCRIPTION varchar(256), "EIC_CODE" varchar(256))
		WHERE datamart."STD_PRODUCTION_FACILITY"."NAME"=t1."pf_name";
	end if;
	
	IF tblName = 'STD_PRODUCTION_BLOCK' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''OPRODBLOCK'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_PRODUCTION_BLOCK"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT PB_Name, description, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (PB_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_PRODUCTION_BLOCK" PB on t1.pb_name=PB."NAME"
		WHERE PB."NAME" IS NULL;
		
		--update EIC_CODE(s)
		update datamart."STD_PRODUCTION_BLOCK"
		SET "REGISTERED_CAPACITY"=t1."PRODBLOCK_CAPACITY"
		FROM dblink(connstr
					,'SELECT entitydef_name, description, "PRODBLOCK_CAPACITY" FROM public.vw_xtabschedulevaluenumber
					 WHERE "PRODBLOCK_CAPACITY" is not null')
		AS t1 (PB_name varchar(256), DESCRIPTION varchar(256), "PRODBLOCK_CAPACITY" numeric)
		WHERE datamart."STD_PRODUCTION_BLOCK"."NAME"=t1."pb_name";
	end if;
	
	IF tblName = 'STD_CONTROL_AREA' THEN
		sqlstr:= sqlstr || '' || 'WHERE svs.primary_role_code= ''CONTROLAREA'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_CONTROL_AREA"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT CA_Name, DESCRIPTION, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (CA_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_CONTROL_AREA" ca on t1.ca_name=ca."NAME"
		WHERE ca."NAME" IS NULL;
	end if;
	
	IF tblName = 'STD_BID_ZONE' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''BZONE'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_BID_ZONE"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT BZ_Name, DESCRIPTION, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (BZ_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_BID_ZONE" bz on t1.bz_name=bz."NAME"
		WHERE bz."NAME" IS NULL;
	end if;
	
	IF tblName = 'STD_FUEL_TYPE' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''OFT'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_FUEL_TYPE"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT FT_Name, DESCRIPTION, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (FT_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_FUEL_TYPE" ft on t1.ft_name=ft."NAME"
		WHERE ft."NAME" IS NULL;
	end if;
	
	IF tblName = 'STD_PSU' THEN
		sqlstr:= 'SELECT entitydef_name, description, "EIC_PSU","PSU_PTYPE","PSU_TYPE" ' ||
				 'FROM vw_xtabschedulevaluestring ' ||
				 'WHERE "EIC_PSU" is not null;';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_PSU"("NAME", "DESCRIPTION", "EIC_CODE","PSU_PARTICIPATION_TYPE","PSU_TYPE")
		SELECT PSU_Name, DESCRIPTION, EIC_CODE, PSU_PTYPE, PSU_TYPE
		FROM dblink(connstr,sqlstr)
		AS t1 (PSU_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256), PSU_PTYPE varchar(256), PSU_TYPE varchar(256))
		LEFT JOIN datamart."STD_PSU" psu on t1.psu_name=psu."NAME"
		WHERE psu."NAME" IS NULL;
		
		--update Registered Capacity for PSU
		update datamart."STD_PSU"
		SET "PSU_REGISTERED_CAPACITY"=t1."PSU_CAPACITY"
		FROM dblink(connstr
					,'SELECT entitydef_name, description, "PSU_CAPACITY" FROM public.vw_xtabschedulevaluenumber
					 WHERE "PSU_CAPACITY" is not null')
		AS t1 (psu_name varchar(256), DESCRIPTION varchar(256), "PSU_CAPACITY" decimal(10,3))
		WHERE datamart."STD_PSU"."NAME"=t1."psu_name";	
	end if;
	
	IF tblName = 'STD_PSU_CONFIG' THEN
		sqlstr:= sqlstr || '' || 'WHERE svs.primary_role_code= ''OPSUCONF'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_PSU_CONFIG"("NAME", "DESCRIPTION", "EIC_CODE")
		SELECT PSU_Name, DESCRIPTION, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (PSU_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_PSU_CONFIG" psu on t1.psu_name=psu."NAME"
		WHERE psu."NAME" IS NULL;
		
		--update Registered Capacity for PSU_CONFIG
		update datamart."STD_PSU_CONFIG"
		SET "REGISTERED_CAPACITY"=t1."PSUCONF_CAPACITY"
		FROM dblink(connstr
					,'SELECT entitydef_name, description, "PSUCONF_CAPACITY" FROM public.vw_xtabschedulevaluenumber
					 WHERE "PSUCONF_CAPACITY" is not null')
		AS t1 (psu_name varchar(256), DESCRIPTION varchar(256), "PSUCONF_CAPACITY" decimal(10,3))
		WHERE datamart."STD_PSU_CONFIG"."NAME"=t1."psu_name";	
	end if;
		
	IF tblName = 'STD_TRANSITION' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''OTRANS'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_TRANSITION"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT FT_Name, DESCRIPTION, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (FT_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_TRANSITION" st on t1.ft_name=st."NAME"
		WHERE st."NAME" IS NULL;
	end if;
	
	IF tblName = 'STD_METER' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''OMETER'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_METER"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT FT_Name, DESCRIPTION, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (FT_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_METER" sm on t1.FT_Name=sm."NAME"
		WHERE sm."NAME" IS NULL;
	end if;
	
	IF tblName = 'STD_TRANSITION_MATRIX' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''OTRANSMATRX'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_TRANSITION_MATRIX"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT TMX_Name, DESCRIPTION, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (TMX_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_TRANSITION_MATRIX" tmx on t1.tmx_name=tmx."NAME"
		WHERE tmx."NAME" IS NULL;
	end if;
	IF tblName = 'STD_GENSET' THEN
		sqlstr:= sqlstr || '' || ' WHERE svs.primary_role_code= ''OGENSET'' AND svs.valuetype_name=''EIC Code''';
		RAISE NOTICE '%' , sqlstr;
		INSERT INTO datamart."STD_GENSET"("NAME","DESCRIPTION", "EIC_CODE")
		SELECT GS_Name, DESCRIPTION, EIC_CODE
		FROM dblink(connstr,sqlstr)
		AS t1 (GS_Name varchar(256), DESCRIPTION varchar(256), EIC_CODE varchar(256))
		LEFT JOIN datamart."STD_GENSET" gs on t1.gs_name=gs."NAME"
		WHERE gs."NAME" IS NULL
		ORDER BY t1.gs_name;
		
		--update Registered Capacity for GenSet
		UPDATE datamart."STD_GENSET"
		SET "REGISTERED_CAPACITY"=t1."GEN_CAPACITY"
		FROM dblink(connstr
					,'SELECT entitydef_name, description, "GEN_CAPACITY" FROM public.vw_xtabschedulevaluenumber
					 WHERE "GEN_CAPACITY" is not null')
		AS t1 (gs_name varchar(256), DESCRIPTION varchar(256), "GEN_CAPACITY" decimal(10,3))
		WHERE datamart."STD_GENSET"."NAME"=t1."gs_name";	
	end if;
end;
$$;


ALTER PROCEDURE datamart.usp_importstandingdata(tblname character varying) OWNER TO postgres;

--
-- Name: usp_msh_inputs_bidzone_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_msh_inputs_bidzone_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	-- updated on 2/2/2023
	connstr= 'dbname=OPWP_SCHD_BATCHINPUT_HIS';
	/* connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS'; */
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('MSH_INPUTS_BIDZONE_TP','sr_schedule_schd');
	 
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	DROP TABLE IF EXISTS public."tempData";
	
	CREATE TEMP TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''DEMSHEDDING'' OR st.code=''DEMFORECAST'' OR st.code=''MODFORECAST'' 
	      OR st.code=''EASPINRESERVE'' OR st.code=''EPSPINRESERVE'')
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)	
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	INNER JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	INNER JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."MSH_INPUTS_BIDZONE_TP" tgp on t1."schedule_id"=tgp."schedule_id"
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'MSH_INPUTS_BIDZONE_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;
	
		RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;	

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("DEMSHEDDING") AS "V1"
	, Sum("DEMFORECAST") AS "V2"
	, Sum("MODFORECAST") AS "V3"
	, Sum("EASPINRESERVE") AS "V4"
	, Sum("EPSPINRESERVE") AS "V5"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'DEMSHEDDING' UNION ALL
		   SELECT 'DEMFORECAST' UNION ALL
		   SELECT 'MODFORECAST' UNION ALL
		   SELECT 'EASPINRESERVE' UNION ALL
		   SELECT 'EPSPINRESERVE'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "DEMSHEDDING" numeric(15,5), "DEMFORECAST" numeric (15,5), "MODFORECAST" numeric (15,5)
	 , "EASPINRESERVE" numeric (15,5), "EPSPINRESERVE" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID", "Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."MSH_RESULTS_BLOCK_TP"
		INSERT INTO datamart."MSH_INPUTS_BIDZONE_TP" ("TRANSCO_DEMAND_SHEDDING","TRANSCO_DEMAND_FORECAST"
										   , "MARKET_OPERATOR_DEMAND_FORECAST","EXANTE_SPINNING_RESERVE"
										   , "EXPOST_SPINNING_RESERVE"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_BID_ZONE","schedule_id")
		SELECT "V1","V2","V3","V4","V5", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."MSH_INPUTS_BIDZONE_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='BZONE';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'MSH_INPUTS_BIDZONE_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		/*
		UPDATE datamart."MSH_INPUTS_FACILITY"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_INPUTS_FACILITY"."schedule_id"=tmp."schedule_id" AND datamart."MSH_INPUTS_FACILITY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."MSH_INPUTS_FACILITY"
		SET "ID_PRODUCTION_FACILITY"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_INPUTS_FACILITY"."schedule_id"=tmp."schedule_id" AND datamart."MSH_INPUTS_FACILITY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODFAC';
		
		*/

	UPDATE public.sr_schedule_schd schd
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE schd.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_msh_inputs_bidzone_tp() OWNER TO postgres;

--
-- Name: usp_msh_inputs_facility_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_msh_inputs_facility_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	-- updated on 2/2/2023
	connstr= 'dbname=OPWP_SCHD_BATCHINPUT_HIS';
	/* connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS'; */
	
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('MSH_INPUTS_FACILITY','sr_schedule_schd');
							 
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TEMP TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''WATERPROD'' OR st.code=''WEATHER_FC'' OR st.code=''TCMACF'' 
	      OR st.code=''MRUNAUXCONSF'' OR st.code=''EFP'')
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)	
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric (15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."MSH_INPUTS_FACILITY" tgp on t1."schedule_id"=tgp."schedule_id"
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'MSH_INPUTS_FACILITY','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;
	
		RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("WATERPROD") AS "V1"
	, Sum("WEATHER_FC") AS "V2"
	, Sum("TCMACF") AS "V3"
	, Sum("MRUNAUXCONSF") AS "V4"
	, Sum("EFP") AS "V5"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'WATERPROD' UNION ALL
		   SELECT 'WEATHER_FC' UNION ALL
		   SELECT 'TCMACF' UNION ALL
		   SELECT 'MRUNAUXCONSF' UNION ALL
		   SELECT 'EFP'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "WATERPROD" numeric (15,5), "WEATHER_FC" numeric (15,5), "TCMACF" numeric (15,5)
	 , "MRUNAUXCONSF" numeric (15,5), "EFP" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."MSH_RESULTS_BLOCK_TP"
		INSERT INTO datamart."MSH_INPUTS_FACILITY" ("WATER_PRODUCTION_REQUIREMENTS","WEATHER_FORECAST"
										   , "TRANSCO_MUST_RUN_AUXILIARY_FORECAST","MUST_RUN_AUXILIARY_FORECAST"
										   , "ECONOMIC_FUEL_PRICE" -- should be renamed
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."MSH_INPUTS_FACILITY" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'MSH_INPUTS_FACILITY','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."MSH_INPUTS_FACILITY"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_INPUTS_FACILITY"."schedule_id"=tmp."schedule_id" AND datamart."MSH_INPUTS_FACILITY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."MSH_INPUTS_FACILITY"
		SET "ID_PRODUCTION_FACILITY"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_INPUTS_FACILITY"."schedule_id"=tmp."schedule_id" AND datamart."MSH_INPUTS_FACILITY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODFAC';
		
		UPDATE datamart."MSH_INPUTS_FACILITY"
		SET "ID_FUEL_TYPE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_INPUTS_FACILITY"."schedule_id"=tmp."schedule_id" AND datamart."MSH_INPUTS_FACILITY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OFT';

	UPDATE public.sr_schedule_schd schd
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE schd.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_msh_inputs_facility_tp() OWNER TO postgres;

--
-- Name: usp_msh_inputs_psu_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_msh_inputs_psu_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	-- updated on 2/2/2023
	connstr= 'dbname=OPWP_SCHD_BATCHINPUT_HIS';
	/* connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS'; */
 
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('MSH_INPUTS_PSU','sr_schedule_schd');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''EARESHOLL'' OR st.code=''EARESHOLQ'' OR st.code=''EPRESHOLL'' 
	      OR st.code=''EPRESHOLQ'' OR st.code=''CURTAILMENT'' OR st.code=''MQ_MAI'' OR st.code=''MQ_MAC'')
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)	
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."MSH_INPUTS_PSU" tgp on t1."schedule_id"=tgp."schedule_id"
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'MSH_INPUTS_PSU','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("EARESHOLL") AS "V1"
	, Sum("EARESHOLQ") AS "V2"
	, Sum("EPRESHOLL") AS "V3"
	, Sum("EPRESHOLQ") AS "V4"
	, Sum("CURTAILMENT") AS "V5"
	, Sum("MQ_MAI") AS "V6"
	, Sum("MQ_MAC") AS "V7"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'EARESHOLL' UNION ALL
		   SELECT 'EARESHOLQ' UNION ALL
		   SELECT 'EPRESHOLL' UNION ALL
		   SELECT 'EPRESHOLQ' UNION ALL
		   SELECT 'CURTAILMENT'UNION ALL
		   SELECT 'MQ_MAI'UNION ALL
		   SELECT 'MQ_MAC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "EARESHOLL" numeric(15,5), "EARESHOLQ" numeric (15,5), "EPRESHOLL" numeric (15,5)
	 , "EPRESHOLQ" numeric (15,5), "CURTAILMENT" numeric (15,5), "MQ_MAI" numeric (15,5), "MQ_MAC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."MSH_RESULTS_BLOCK_TP"
		INSERT INTO datamart."MSH_INPUTS_PSU" ("EXANTE_RESERVE_HOLDING_LIMIT","EXANTE_RESERVE_HOLDING_QUANTITY"
										   , "EXPOST_RESERVE_HOLDING_LIMIT","EXPOST_RESERVE_HOLDING_QUANTITY"
										   , "CURTAILMENT_DATA","INDICATIVE_METER_QUANTITIES","CONFIRMED_METER_QUANTITIES"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5","V6","V7", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."MSH_INPUTS_PSU" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'MSH_INPUTS_PSU','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."MSH_INPUTS_PSU"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_INPUTS_PSU"."schedule_id"=tmp."schedule_id" AND datamart."MSH_INPUTS_PSU"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."MSH_INPUTS_PSU"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_INPUTS_PSU"."schedule_id"=tmp."schedule_id" AND datamart."MSH_INPUTS_PSU"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';
		
	UPDATE public.sr_schedule_schd sch
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE sch.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_msh_inputs_psu_tp() OWNER TO postgres;

--
-- Name: usp_msh_results_bid_zone_cb(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_msh_results_bid_zone_cb()
    LANGUAGE plpgsql
    AS $_$

DECLARE  sqlstr text;
DECLARE connstr text;

BEGIN
	connstr= 'dbname=OPWP_SCHD_BATCHINPUT_HIS';
	/* connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS'; */
	
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('MSH_RESULTS_BID_ZONE_TP','sr_schedule_schd');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."MSH_RESULTS_BID_ZONE_CB";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code in (''unknown_1'',''MSEAMR_ZSMP'',''MSPEACR_APP'',''MSEAMR_HPRC'',''MSPEACR_SP'',''MSPEACR_EASF'',
						''MSPEACR_MAR'',''MSPEACR_SAC'',''MSPEACR_SCR'',''unknown_2'',''unknown_3'',''MSEAMR_ZGEN'',
						''unknown_4'',''MSEPIMR_ZSMP'',''MSPEPICR_APP'',''MSEPIMR_HPRC'',''MSPEPIMR_SP'',''MSPEPICR_EPSF'',
						''MSPEPICR_MAR'',''MSPEPICR_SAC'',''MSPEPICR_SCR'',''unknown_5'',''unknown_6'',''MSEPIMR_ZGEN'',
						''unknown_7'',''MSEPCMR_ZSMP'',''MSPEPCCR_APP'',''MSEPCMR_HPRC'',''MSPEPCMR_SP'',''MSPEPCCR_EPSF'',
						''MSPEPCCR_MAR'',''MSPEPCCR_SAC'',''MSPEPCCR_SCR'',''unknown_8'',''unknown_9'',''MSEPCMR_ZGEN'')
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0)';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)	
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("unknown_1") AS "V1"
, Sum("MSEAMR_ZSMP") AS "V2"
, Sum("MSPEACR_APP") AS "V3"
, Sum("MSEAMR_HPRC") AS "V4"
, Sum("MSPEACR_SP") AS "V5"
, Sum("MSPEACR_EASF") AS "V6"
, Sum("MSPEACR_MAR") AS "V7"
, Sum("MSPEACR_SAC") AS "V8"
, Sum("MSPEACR_SCR") AS "V9"
, Sum("unknown_2") AS "V10"
, Sum("unknown_3") AS "V11"
, Sum("MSEAMR_ZGEN") AS "V12"
, Sum("unknown_4") AS "V13"
, Sum("MSEPIMR_ZSMP") AS "V14"
, Sum("MSPEPICR_APP") AS "V15"
, Sum("MSEPIMR_HPRC") AS "V16"
, Sum("MSPEPIMR_SP") AS "V17"
, Sum("MSPEPICR_EPSF") AS "V18"
, Sum("MSPEPICR_MAR") AS "V19"
, Sum("MSPEPICR_SAC") AS "V20"
, Sum("MSPEPICR_SCR") AS "V21"
, Sum("unknown_5") AS "V22"
, Sum("unknown_6") AS "V23"
, Sum("MSEPIMR_ZGEN") AS "V24"
, Sum("unknown_7") AS "V25"
, Sum("MSEPCMR_ZSMP") AS "V26"
, Sum("MSPEPCCR_APP") AS "V27"
, Sum("MSEPCMR_HPRC") AS "V28"
, Sum("MSPEPCMR_SP") AS "V29"
, Sum("MSPEPCCR_EPSF") AS "V30"
, Sum("MSPEPCCR_MAR") AS "V31"
, Sum("MSPEPCCR_SAC") AS "V32"
, Sum("MSPEPCCR_SCR") AS "V33"
, Sum("unknown_8") AS "V34"
, Sum("unknown_9") AS "V35"
, Sum("MSEPCMR_ZGEN") AS "V36"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ 	SELECT 'unknown_1' UNION ALL
			SELECT 'MSEAMR_ZSMP' UNION ALL
			SELECT 'MSPEACR_APP' UNION ALL
			SELECT 'MSEAMR_HPRC' UNION ALL
			SELECT 'MSPEACR_SP' UNION ALL
			SELECT 'MSPEACR_EASF' UNION ALL
			SELECT 'MSPEACR_MAR' UNION ALL
			SELECT 'MSPEACR_SAC' UNION ALL
			SELECT 'MSPEACR_SCR' UNION ALL
			SELECT 'unknown_2' UNION ALL
			SELECT 'unknown_3' UNION ALL
			SELECT 'MSEAMR_ZGEN' UNION ALL
			SELECT 'unknown_4' UNION ALL
			SELECT 'MSEPIMR_ZSMP' UNION ALL
			SELECT 'MSPEPICR_APP' UNION ALL
			SELECT 'MSEPIMR_HPRC' UNION ALL
			SELECT 'MSPEPIMR_SP' UNION ALL
			SELECT 'MSPEPICR_EPSF' UNION ALL
			SELECT 'MSPEPICR_MAR' UNION ALL
			SELECT 'MSPEPICR_SAC' UNION ALL
			SELECT 'MSPEPICR_SCR' UNION ALL
			SELECT 'unknown_5' UNION ALL
			SELECT 'unknown_6' UNION ALL
			SELECT 'MSEPIMR_ZGEN' UNION ALL
			SELECT 'unknown_7' UNION ALL
			SELECT 'MSEPCMR_ZSMP' UNION ALL
			SELECT 'MSPEPCCR_APP' UNION ALL
			SELECT 'MSEPCMR_HPRC' UNION ALL
			SELECT 'MSPEPCMR_SP' UNION ALL
			SELECT 'MSPEPCCR_EPSF' UNION ALL
			SELECT 'MSPEPCCR_MAR' UNION ALL
			SELECT 'MSPEPCCR_SAC' UNION ALL
			SELECT 'MSPEPCCR_SCR' UNION ALL
			SELECT 'unknown_8' UNION ALL
			SELECT 'unknown_9' UNION ALL
			SELECT 'MSEPCMR_ZGEN'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
		,"unknown_1"  numeric(15,5), "MSEAMR_ZSMP"  numeric(15,5), "MSPEACR_APP"  numeric(15,5), "MSEAMR_HPRC"  numeric(15,5), "MSPEACR_SP"  numeric(15,5), "MSPEACR_EASF"  numeric(15,5)
		,"MSPEACR_MAR"  numeric(15,5), "MSPEACR_SAC"  numeric(15,5), "MSPEACR_SCR"  numeric(15,5), "unknown_2"  numeric(15,5), "unknown_3"  numeric(15,5), "MSEAMR_ZGEN"  numeric(15,5)
		,"unknown_4"  numeric(15,5), "MSEPIMR_ZSMP"  numeric(15,5), "MSPEPICR_APP"  numeric(15,5), "MSEPIMR_HPRC"  numeric(15,5), "MSPEPIMR_SP"  numeric(15,5), "MSPEPICR_EPSF"  numeric(15,5)
		,"MSPEPICR_MAR"  numeric(15,5), "MSPEPICR_SAC"  numeric(15,5), "MSPEPICR_SCR"  numeric(15,5), "unknown_5"  numeric(15,5), "unknown_6"  numeric(15,5), "MSEPIMR_ZGEN"  numeric(15,5)
		,"unknown_7"  numeric(15,5), "MSEPCMR_ZSMP"  numeric(15,5), "MSPEPCCR_APP"  numeric(15,5), "MSEPCMR_HPRC"  numeric(15,5), "MSPEPCMR_SP"  numeric(15,5), "MSPEPCCR_EPSF"  numeric(15,5)
		,"MSPEPCCR_MAR"  numeric(15,5), "MSPEPCCR_SAC"  numeric(15,5), "MSPEPCCR_SCR"  numeric(15,5), "unknown_8"  numeric(15,5), "unknown_9"  numeric(15,5), "MSEPCMR_ZGEN"  numeric(15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
	
	CREATE TABLE IF NOT EXISTS datamart."MSH_RESULTS_BID_ZONE_CB"
(
    "EXANTE_CALCULATED_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXANTE_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXANTE_AGGREGATE_POOL_PRICE" numeric(15,5),
    "EXANTE_HIGHEST_OFFER_PRICE" numeric(15,5),
    "EXANTE_SCARCITY_PRICE" numeric(15,5),
    "EXANTE_SCARCITY_FACTOR" numeric(15,5),
    "EXANTE_MARGIN" numeric(15,5),
    "EXANTE_SYSTEM_AVAILABILITY_CAPACITY" numeric(15,5),
    "EXANTE_SYSTEM_CAPACITY_REQUIREMENT" numeric(15,5),
    "EXANTE_POOL_DEMAND" numeric(15,5),
    "EXANTE_ZONE_PRICE" numeric(15,5),
    "EXANTE_ZONE_GENERATION" numeric(15,5),
    "EXPOST_INDICATIVE_CALCULATED_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_AGGREGATE_POOL_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_HIGHEST_OFFER_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_SCARCITY_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_SCARCITY_FACTOR" numeric(15,5),
    "EXPOST_INDICATIVE_MARGIN" numeric(5,5),
    "EXPOST_INDICATIVE_SYSTEM_AVAILABILITY_CAPACITY" numeric(15,5),
    "EXPOST_INDICATIVE_SYSTEM_CAPACITY_REQUIREMENT" numeric(15,5),
    "EXPOST_INDICATIVE_POOL_DEMAND" numeric(15,5),
    "EXPOST_INDICATIVE_ZONE_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_ZONE_GENERATION" numeric(15,5),
    "EXPOST_CONFIRMED_CALCULATED_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_AGGREGATE_POOL_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_HIGHEST_OFFER_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_SCARCITY_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_SCARCITY_FACTOR" numeric(15,5),
    "EXPOST_CONFIRMED_MARGIN" numeric(15,5),
    "EXPOST_CONFIRMED_SYSTEM_AVAILABILITY_CAPACITY" numeric(15,5),
    "EXPOST_CONFIRMED_SYSTEM_CAPACITY_REQUIREMENT" numeric(15,5),
    "EXPOST_CONFIRMED_POOL_DEMAND" numeric(15,5),
    "EXPOST_CONFIRMED_ZONE_GENERATION" numeric(15,5),
    "EXPOST_CONFIRMED_ZONE_PRICE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    schedule_id integer,
    "ID_MARKET_PARTY" integer
);
	
		-- Insert/update Datamart Table datamart."MSH_RESULTS_BID_ZONE_CB"
		INSERT INTO datamart."MSH_RESULTS_BID_ZONE_CB" ("EXANTE_CALCULATED_SYSTEM_MARGINAL_PRICE","EXANTE_SYSTEM_MARGINAL_PRICE","EXANTE_AGGREGATE_POOL_PRICE","EXANTE_HIGHEST_OFFER_PRICE","EXANTE_SCARCITY_PRICE","EXANTE_SCARCITY_FACTOR",
"EXANTE_MARGIN","EXANTE_SYSTEM_AVAILABILITY_CAPACITY","EXANTE_SYSTEM_CAPACITY_REQUIREMENT","EXANTE_POOL_DEMAND","EXANTE_ZONE_PRICE","EXANTE_ZONE_GENERATION","EXPOST_INDICATIVE_CALCULATED_SYSTEM_MARGINAL_PRICE",
"EXPOST_INDICATIVE_SYSTEM_MARGINAL_PRICE","EXPOST_INDICATIVE_AGGREGATE_POOL_PRICE","EXPOST_INDICATIVE_HIGHEST_OFFER_PRICE","EXPOST_INDICATIVE_SCARCITY_PRICE","EXPOST_INDICATIVE_SCARCITY_FACTOR",
"EXPOST_INDICATIVE_MARGIN","EXPOST_INDICATIVE_SYSTEM_AVAILABILITY_CAPACITY","EXPOST_INDICATIVE_SYSTEM_CAPACITY_REQUIREMENT","EXPOST_INDICATIVE_POOL_DEMAND",
"EXPOST_INDICATIVE_ZONE_PRICE","EXPOST_INDICATIVE_ZONE_GENERATION","EXPOST_CONFIRMED_CALCULATED_SYSTEM_MARGINAL_PRICE","EXPOST_CONFIRMED_SYSTEM_MARGINAL_PRICE",
"EXPOST_CONFIRMED_AGGREGATE_POOL_PRICE","EXPOST_CONFIRMED_HIGHEST_OFFER_PRICE","EXPOST_CONFIRMED_SCARCITY_PRICE","EXPOST_CONFIRMED_SCARCITY_FACTOR","EXPOST_CONFIRMED_MARGIN",
"EXPOST_CONFIRMED_SYSTEM_AVAILABILITY_CAPACITY","EXPOST_CONFIRMED_SYSTEM_CAPACITY_REQUIREMENT","EXPOST_CONFIRMED_POOL_DEMAND","EXPOST_CONFIRMED_ZONE_PRICE","EXPOST_CONFIRMED_ZONE_GENERATION"	
									 	   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5","V6","V7","V8","V9"
			  , "V10","V11","V12","V13","V14","V15","V16","V17","V18"
			  , "V19","V20","V21","V22","V23","V24","V25","V26","V27"
			  , "V28","V29","V30","V31","V32","V33","V34","V35","V36"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."MSH_RESULTS_BID_ZONE_CB" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';

		UPDATE datamart."MSH_RESULTS_BID_ZONE_CB"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_BID_ZONE_CB"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_BID_ZONE_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

--update final TABLE

INSERT INTO datamart."MSH_RESULTS_BID_ZONE_TP"(
	"EXANTE_CALCULATED_SYSTEM_MARGINAL_PRICE", "EXANTE_SYSTEM_MARGINAL_PRICE", "EXANTE_AGGREGATE_POOL_PRICE", "EXANTE_HIGHEST_OFFER_PRICE", "EXANTE_SCARCITY_PRICE", 
	"EXANTE_SCARCITY_FACTOR", "EXANTE_MARGIN", "EXANTE_SYSTEM_AVAILABILITY_CAPACITY", "EXANTE_SYSTEM_CAPACITY_REQUIREMENT", "EXANTE_POOL_DEMAND", "EXANTE_ZONE_PRICE", 
	"EXANTE_ZONE_GENERATION", "EXPOST_INDICATIVE_CALCULATED_SYSTEM_MARGINAL_PRICE", "EXPOST_INDICATIVE_SYSTEM_MARGINAL_PRICE", "EXPOST_INDICATIVE_AGGREGATE_POOL_PRICE", 
	"EXPOST_INDICATIVE_HIGHEST_OFFER_PRICE", "EXPOST_INDICATIVE_SCARCITY_PRICE", "EXPOST_INDICATIVE_SCARCITY_FACTOR", "EXPOST_INDICATIVE_MARGIN", 
	"EXPOST_INDICATIVE_SYSTEM_AVAILABILITY_CAPACITY", "EXPOST_INDICATIVE_SYSTEM_CAPACITY_REQUIREMENT", "EXPOST_INDICATIVE_POOL_DEMAND", 
	"EXPOST_INDICATIVE_ZONE_PRICE", "EXPOST_INDICATIVE_ZONE_GENERATION", "EXPOST_CONFIRMED_CALCULATED_SYSTEM_MARGINAL_PRICE", 
	"EXPOST_CONFIRMED_SYSTEM_MARGINAL_PRICE", "EXPOST_CONFIRMED_AGGREGATE_POOL_PRICE", "EXPOST_CONFIRMED_HIGHEST_OFFER_PRICE", 
	"EXPOST_CONFIRMED_SCARCITY_PRICE", "EXPOST_CONFIRMED_SCARCITY_FACTOR", "EXPOST_CONFIRMED_MARGIN", "EXPOST_CONFIRMED_SYSTEM_AVAILABILITY_CAPACITY", 
	"EXPOST_CONFIRMED_SYSTEM_CAPACITY_REQUIREMENT", "EXPOST_CONFIRMED_POOL_DEMAND", "EXPOST_CONFIRMED_ZONE_GENERATION", "EXPOST_CONFIRMED_ZONE_PRICE", 
	"DATE_KEY_OMAN", "TRADING_PERIOD", "ID_BID_ZONE")
SELECT "EXANTE_CALCULATED_SYSTEM_MARGINAL_PRICE","EXANTE_SYSTEM_MARGINAL_PRICE","EXANTE_AGGREGATE_POOL_PRICE","EXANTE_HIGHEST_OFFER_PRICE","EXANTE_SCARCITY_PRICE",
"EXANTE_SCARCITY_FACTOR","EXANTE_MARGIN","EXANTE_SYSTEM_AVAILABILITY_CAPACITY","EXANTE_SYSTEM_CAPACITY_REQUIREMENT","EXANTE_POOL_DEMAND","EXANTE_ZONE_PRICE",
"EXANTE_ZONE_GENERATION","EXPOST_INDICATIVE_CALCULATED_SYSTEM_MARGINAL_PRICE","EXPOST_INDICATIVE_SYSTEM_MARGINAL_PRICE","EXPOST_INDICATIVE_AGGREGATE_POOL_PRICE",
"EXPOST_INDICATIVE_HIGHEST_OFFER_PRICE","EXPOST_INDICATIVE_SCARCITY_PRICE","EXPOST_INDICATIVE_SCARCITY_FACTOR","EXPOST_INDICATIVE_MARGIN",
"EXPOST_INDICATIVE_SYSTEM_AVAILABILITY_CAPACITY","EXPOST_INDICATIVE_SYSTEM_CAPACITY_REQUIREMENT","EXPOST_INDICATIVE_POOL_DEMAND",
"EXPOST_INDICATIVE_ZONE_PRICE","EXPOST_INDICATIVE_ZONE_GENERATION","EXPOST_CONFIRMED_CALCULATED_SYSTEM_MARGINAL_PRICE",
"EXPOST_CONFIRMED_SYSTEM_MARGINAL_PRICE","EXPOST_CONFIRMED_AGGREGATE_POOL_PRICE","EXPOST_CONFIRMED_HIGHEST_OFFER_PRICE",
"EXPOST_CONFIRMED_SCARCITY_PRICE","EXPOST_CONFIRMED_SCARCITY_FACTOR","EXPOST_CONFIRMED_MARGIN","EXPOST_CONFIRMED_SYSTEM_AVAILABILITY_CAPACITY",
"EXPOST_CONFIRMED_SYSTEM_CAPACITY_REQUIREMENT","EXPOST_CONFIRMED_POOL_DEMAND","EXPOST_CONFIRMED_ZONE_GENERATION","EXPOST_CONFIRMED_ZONE_PRICE", 
"DATE_KEY_OMAN","TRADING_PERIOD","ID_BID_ZONE" from datamart."MSH_RESULTS_BID_ZONE_CB"	;

/*		UPDATE datamart."MSH_RESULTS_BID_ZONE_CB"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_BID_ZONE_CB"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_BID_ZONE_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';
		
		UPDATE datamart."MSH_RESULTS_BID_ZONE_CB"
		SET "ID_PRODUCTION_FACILITY"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_BID_ZONE_CB"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_BID_ZONE_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODFAC';
		
		UPDATE datamart."MSH_RESULTS_BID_ZONE_CB"
		SET "ID_PRODUCTION_BLOCK"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_BID_ZONE_CB"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_BID_ZONE_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODBLOCK';
*/

	UPDATE public.sr_schedule_schd schd
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE schd.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."MSH_RESULTS_BID_ZONE_CB";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_msh_results_bid_zone_cb() OWNER TO postgres;

--
-- Name: usp_msh_results_bid_zone_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_msh_results_bid_zone_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
 
	connstr= 'dbname=OPWP_SCHD_BATCHINPUT_HIS';
	
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('MSH_RESULTS_BID_ZONE_TP','sr_schedule_schd');
	
	DROP TABLE  IF EXISTS public."xtabtempdata";
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData"; 
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );	

	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY en on sp.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	JOIN SD_ENTITY_DEF ed on en.entity_id=ed.entity_id
	WHERE (st.code in (''MSEAMR_ZGEN'',''MSEPIMR_HPRC'',''MSEPIMR_ZGEN'',''MSEPCMR_ZGEN'',''MSEPCMR_HPRC'',''MSEAMR_HPRC'' )
	AND sch.is_actual_version=1
	AND sch.datamart_fetched = false 
	AND sch.deletion_time is null 
	AND sch.draft=0
	AND ed.draft=0)';
	-- AND sch.datamart_fetched = false
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)	
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	INNER JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	INNER JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."MSH_RESULTS_BID_ZONE_TP" tgp on t1."schedule_id"= tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	 
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	 
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'MSH_RESULTS_BID_ZONE_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count; 	
	 
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;
     
	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
    , Sum("MSEAMR_ZGEN") AS "V1" 
    , Sum("MSEPIMR_HPRC") AS "V2"
    , Sum("MSEPIMR_ZGEN") AS "V3" 
    , Sum("MSEPCMR_ZGEN") AS "V4" 
    , Sum("MSEPCMR_HPRC") AS "V5" 
	, Sum("MSEAMR_HPRC") AS "V6"
	, Sum("MSEAMR_ZDEFICIT") AS "V7"
	, Sum("MSEAMR_ZSURPLUS") AS "V8"
	, Sum("MSEAMR_ZDEMAND") AS "V9"
	, Sum("MSEPIMR_ZDEFICIT") AS "V10"
	, Sum("MSEPIMR_ZSURPLUS") AS "V11"
	, Sum("MSEPIMR_ZDEMAND") AS "V12"
	, Sum("MSEPCMR_ZDEFICIT") AS "V13"
	, Sum("MSEPCMR_ZSURPLUS") AS "V14"
	, Sum("MSEPCMR_ZDEMAND") AS "V15" 
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ 	SELECT 'MSEAMR_ZGEN' UNION ALL
			SELECT 'MSEPIMR_HPRC' UNION ALL
			SELECT 'MSEPIMR_ZGEN' UNION ALL
			SELECT 'MSEPCMR_ZGEN' UNION ALL
			SELECT 'MSEPCMR_HPRC'  UNION ALL
			SELECT 'MSEAMR_HPRC' UNION ALL
			SELECT 'MSEAMR_ZDEFICIT' UNION ALL
			SELECT 'MSEAMR_ZSURPLUS' UNION ALL
			SELECT 'MSEAMR_ZDEMAND' UNION ALL
			SELECT 'MSEPIMR_ZDEFICIT' UNION ALL
			SELECT 'MSEPIMR_ZSURPLUS' UNION ALL
			SELECT 'MSEPIMR_ZDEMAND' UNION ALL
			SELECT 'MSEPCMR_ZDEFICIT' UNION ALL
			SELECT 'MSEPCMR_ZSURPLUS' UNION ALL
			SELECT 'MSEPCMR_ZDEMAND'  $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
		,"MSEAMR_ZGEN"  numeric(15,5), "MSEPIMR_HPRC"  numeric(15,5), "MSEPIMR_ZGEN"  numeric(15,5),
	     "MSEPCMR_ZGEN"  numeric(15,5), "MSEPCMR_HPRC"  numeric(15,5), "MSEAMR_HPRC"  numeric(15,5),
	     "MSEAMR_ZDEFICIT" numeric(15,5),"MSEAMR_ZSURPLUS" numeric(15,5),"MSEAMR_ZDEMAND" numeric(15,5),
	     "MSEPIMR_ZDEFICIT" numeric(15,5),"MSEPIMR_ZSURPLUS" numeric(15,5),"MSEPIMR_ZDEMAND" numeric(15,5),
	     "MSEPCMR_ZDEFICIT" numeric(15,5),"MSEPCMR_ZSURPLUS" numeric(15,5),"MSEPCMR_ZDEMAND" numeric(15,5)
	 
	 )
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval; 
  
   		 
	-- Insert/update Datamart Table datamart."MSH_RESULTS_BID_ZONE_TP"
	INSERT INTO datamart."MSH_RESULTS_BID_ZONE_TP" (
		 "EXANTE_ZONE_GENERATION"
		,"EXPOST_INDICATIVE_HIGHEST_OFFER_PRICE"
		,"EXPOST_INDICATIVE_ZONE_GENERATION" 
		,"EXPOST_CONFIRMED_ZONE_GENERATION"	
		,"EXPOST_CONFIRMED_HIGHEST_OFFER_PRICE"	
		,"EXANTE_HIGHEST_OFFER_PRICE"
		,"EXANTE_ZONE_GENERATION_DEFICIT"
		,"EXANTE_ZONE_GENERATION_SURPLUS"
		,"EXANTE_ZONE_GENERATION_DEMAND"
		,"EXPOST_INDICATIVE_ZONE_GENERATION_DEFICIT"
		,"EXPOST_INDICATIVE_ZONE_GENERATION_SURPLUS"
		,"EXPOST_INDICATIVE_ZONE_GENERATION_DEMAND"
		,"EXPOST_CONFIRMED_ZONE_GENERATION_DEFICIT"
		,"EXPOST_CONFIRMED_ZONE_GENERATION_SURPLUS"
		,"EXPOST_CONFIRMED_ZONE_GENERATION_DEMAND" 
		,"DATE_KEY_OMAN","TRADING_PERIOD", "ID_BID_ZONE","schedule_id")

	SELECT "V1","V2","V3","V4","V5", "V6","V7","V8"
	, "V9", "V10", "V11", "V12", "V13", "V14", "V15", 
	tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
	  FROM "xtabTempData" tmp
	  LEFT JOIN datamart."MSH_RESULTS_BID_ZONE_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
	 WHERE tgp.schedule_id IS NULL
	   AND tmp."Primary_Role_Code"='BZONE';
  		 
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	 
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'MSH_RESULTS_BID_ZONE_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
	 
	RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;		
     
	UPDATE public.sr_schedule_schd schd
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE schd.schedule_id = tmp.schedule_id; 
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData"; 
END;
$_$;


ALTER PROCEDURE datamart.usp_msh_results_bid_zone_tp() OWNER TO postgres;

--
-- Name: usp_msh_results_block_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_msh_results_block_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_SCHD_BATCHINPUT_HIS';
	/*connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';*/

	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('MSH_RESULTS_BLOCK_TP','sr_schedule_schd');
							
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''MSEAMR_TRNS'' OR st.code=''MSEAMR_PRCT'' OR st.code=''MSEPIMR_TRNS'' 
	      OR st.code=''MSEPIMR_PRCT'' OR st.code=''MSEPCMR_TRNS'' OR st.code=''MSEPCMR_PRCT'')
    AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."MSH_RESULTS_BLOCK_TP" tgp on t1."schedule_id"= tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'MSH_RESULTS_BLOCK_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("MSEAMR_TRANSCOST") AS "V1"
	, Sum("MSEAMR_PRCT") AS "V2"
	, Sum("MSEPIMR_TRANSCOST") AS "V3"
	, Sum("MSEPIMR_PRCT") AS "V4"
	, Sum("MSEPCMR_TRANSCOST") AS "V5"
	, Sum("MSEPCMR_PRCT") AS "V6"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'MSEAMR_TRANSCOST' UNION ALL
		   SELECT 'MSEAMR_PRCT' UNION ALL
		   SELECT 'MSEPIMR_TRANSCOST' UNION ALL
		   SELECT 'MSEPIMR_PRCT' UNION ALL
		   SELECT 'MSEPCMR_TRANSCOST'UNION ALL
		   SELECT 'MSEPCMR_PRCT'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "MSEAMR_TRANSCOST" numeric(15,5), "MSEAMR_PRCT" numeric (15,5), "MSEPIMR_TRANSCOST" numeric (15,5)
	 , "MSEPIMR_PRCT" numeric (15,5), "MSEPCMR_TRANSCOST" numeric (15,5), "MSEPCMR_PRCT" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."MSH_RESULTS_BLOCK_TP"
		INSERT INTO datamart."MSH_RESULTS_BLOCK_TP" ("EXANTE_TRANSITION_COST","EXANTE_PRODUCTION_COST"
										   , "EXPOST_INDICATIVE_TRANSITION_COST","EXPOST_INDICATIVE_PRODUCTION_COST"
										   , "EXPOST_CONFIRMED_TRANSITION_COST","EXPOST_CONFIRMED_PRODUCTION_COST"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5","V6", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."MSH_RESULTS_BLOCK_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
	
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'MSH_RESULTS_BLOCK_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;			

		UPDATE datamart."MSH_RESULTS_BLOCK_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_BLOCK_TP"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_BLOCK_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."MSH_RESULTS_BLOCK_TP"
		SET "ID_PRODUCTION_BLOCK"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_BLOCK_TP"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_BLOCK_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODBLOCK';
		
	UPDATE public.sr_schedule_schd sch
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE sch.schedule_id = tmp.schedule_id;

	DROP TABLE "tmpStandingData";
	DROP TABLE "tempData";
	DROP TABLE "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_msh_results_block_tp() OWNER TO postgres;

--
-- Name: usp_msh_results_psu_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_msh_results_psu_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_SCHD_BATCHINPUT_HIS';
	/*connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS'; */

	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('MSH_RESULTS_PSU_TP','sr_schedule_schd');
							
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code in (''MSEACR_CMSCH'',''MSEAMR_MSCH'',''MSEAMR_NOLOADCOST'',''MSEAMR_HOUP'',''MSEAMR_INITGEN''
					, ''MSEACR_LSLK'',''MSEAMR_COST'',''MSEAMR_MCOM'',''MSEACR_TRNS'',''MSEPIMR_MSCH''
					, ''MSEPCCR_CMSCH'',''MSEPIMR_NLCT'',''MSEPIMR_HOUP'',''MSEPIMR_INITGEN'',''MSEPICR_LSLK''
					, ''MSEPIMR_COST'',''MSEPIMR_CCA'',''MSEPIMR_MCOM'',''MSEPICR_TRNS''
					, ''MSEPCMR_NLCT'',''MSEPCMR_HOUP'',''MSEPCCR_LSLK'',''MSEPCMR_COST'',''MSEPCMR_CCA''
					, ''MSEPCMR_MSCH'',''MSEPCMR_MCOM'',''MSEPCCR_TRNS'',''MSEPCMR_INITGEN'',''MSEAMR_TRNS'')
    
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	--AND sch.datamart_fetched = false
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."MSH_RESULTS_PSU_TP" tgp on t1."schedule_id"= tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	 
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'MSH_RESULTS_PSU_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;
    
	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("MSEACR_CMSCH") AS "V1"
	, Sum("MSEAMR_MSCH") AS "V2"
	, Sum("MSEAMR_NOLOADCOST") AS "V3"
	, Sum("MSEAMR_HOUP") AS "V4"
	, Sum("MSEAMR_INITGEN") AS "V5"
	, Sum("MSEACR_UNITSURPLUS") AS "V6"
	, Sum("MSEACR_UNITDEFICIT") AS "V7"
	, Sum("MSEACR_UNITSURPLUSRAMP") AS "V8"
	, Sum("MSEAMR_TRANSCOST") AS "V9"
	, Sum("MSEAMR_MCOM") AS "V10"	
	, Sum("MSEACR_TRANSCOST") AS "V11"
	, Sum("MSEPIMR_MSCH") AS "V12"
	, Sum("MSEPCCR_CMSCH") AS "V13"
	, Sum("MSEPIMR_NLCT") AS "V14"
	, Sum("MSEPIMR_HOUP") AS "V15"
	, Sum("MSEPIMR_INITGEN") AS "V16"
	, Sum("MSEPICR_UNITSURPLUS") AS "V17"
	, Sum("MSEPICR_UNITDEFICIT") AS "V18"
	, Sum("MSEPICR_UNITSURPLUSRAMP") AS "V19"
	, Sum("MSEPIMR_NOLOADCOST") AS "V20"	
	, Sum("MSEPIMR_CCA") AS "V21"
	, Sum("MSEPIMR_MCOM") AS "V22"
	, Sum("MSEPICR_TRANSCOST") AS "V23"
	-- , Sum("unknown") AS "V24"
	, Sum("MSEPCMR_MSCH") AS "V25"
	, Sum("MSEPCMR_NLCT") AS "V26"
	, Sum("MSEPCMR_HOUP") AS "V27"
	, Sum("MSEPCMR_INITGEN") AS "V28"
	, Sum("MSEPCCR_UNITSURPLUS") AS "V29"
	, Sum("MSEPCCR_UNITDEFICIT") AS "V30"
	, Sum("MSEPCCR_UNITSURPLUSRAMP") AS "V31"
	, Sum("MSEPCMR_NOLOADCOST") AS "V32"
	, Sum("MSEPCMR_CCA") AS "V33"
	, Sum("MSEPCMR_MCOM") AS "V34"
	, Sum("MSEPCCR_TRANSCOST") AS "V35"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'MSEACR_CMSCH' UNION ALL
		   SELECT 'MSEAMR_MSCH' UNION ALL
		   SELECT 'MSEAMR_NOLOADCOST' UNION ALL
		   SELECT 'MSEAMR_HOUP' UNION ALL				  
		   SELECT 'MSEAMR_INITGEN' UNION ALL
		   SELECT 'MSEACR_UNITSURPLUS' UNION ALL
		   SELECT 'MSEACR_UNITDEFICIT' UNION ALL
		   SELECT 'MSEACR_UNITSURPLUSRAMP' UNION ALL		  
		   SELECT 'MSEAMR_TRANSCOST' UNION ALL
		   SELECT 'MSEAMR_MCOM' UNION ALL	  
		   SELECT 'MSEACR_TRANSCOST' UNION ALL
		   SELECT 'MSEPIMR_MSCH' UNION ALL			  
		   SELECT 'MSEPCCR_CMSCH' UNION ALL
		   SELECT 'MSEPIMR_NLCT' UNION ALL
		   SELECT 'MSEPIMR_HOUP' UNION ALL
		   SELECT 'MSEPIMR_INITGEN' UNION ALL			  
		   SELECT 'MSEPICR_UNITSURPLUS' UNION ALL
		   SELECT 'MSEPICR_UNITDEFICIT' UNION ALL
		   SELECT 'MSEPICR_UNITSURPLUSRAMP' UNION ALL
		   SELECT 'MSEPIMR_NOLOADCOST' UNION ALL			  
		   SELECT 'MSEPIMR_CCA' UNION ALL
		   SELECT 'MSEPIMR_MCOM' UNION ALL
		   SELECT 'MSEPICR_TRANSCOST' UNION ALL
		 --  SELECT 'unknown' UNION ALL				  
		   SELECT 'MSEPCMR_MSCH' UNION ALL
		   SELECT 'MSEPCMR_NLCT' UNION ALL
		   SELECT 'MSEPCMR_HOUP' UNION ALL
		   SELECT 'MSEPCMR_INITGEN' UNION ALL			  
		   SELECT 'MSEPCCR_UNITSURPLUS' UNION ALL
		   SELECT 'MSEPCCR_UNITDEFICIT' UNION ALL			
		   SELECT 'MSEPCCR_UNITSURPLUSRAMP' UNION ALL
		   SELECT 'MSEPCMR_NOLOADCOST' UNION ALL				  
		   SELECT 'MSEPCMR_CCA' UNION ALL
		   SELECT 'MSEPCMR_MCOM' UNION ALL
		   SELECT 'MSEPCCR_TRANSCOST'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "MSEACR_CMSCH" numeric(15,5), "MSEAMR_MSCH" numeric (15,5), "MSEAMR_NOLOADCOST" numeric (15,5), "MSEAMR_HOUP" numeric (15,5)
	 , "MSEAMR_INITGEN" numeric(15,5), "MSEACR_UNITSURPLUS" numeric (15,5), "MSEACR_UNITDEFICIT" numeric (15,5), "MSEACR_UNITSURPLUSRAMP" numeric (15,5)
	 , "MSEAMR_TRANSCOST" numeric(15,5), "MSEAMR_MCOM" numeric (15,5), "MSEACR_TRANSCOST" numeric (15,5), "MSEPIMR_MSCH" numeric (15,5)
	 , "MSEPCCR_CMSCH" numeric(15,5), "MSEPIMR_NLCT" numeric (15,5), "MSEPIMR_HOUP" numeric (15,5), "MSEPIMR_INITGEN" numeric (15,5)
	 , "MSEPICR_UNITSURPLUS" numeric(15,5), "MSEPICR_UNITDEFICIT" numeric (15,5), "MSEPICR_UNITSURPLUSRAMP" numeric (15,5), "MSEPIMR_NOLOADCOST" numeric (15,5)
	 , "MSEPIMR_CCA" numeric(15,5), "MSEPIMR_MCOM" numeric (15,5), "MSEPICR_TRANSCOST" numeric (15,5)--, "unknown" numeric (15,5)
	 , "MSEPCMR_MSCH" numeric(15,5), "MSEPCMR_NLCT" numeric (15,5), "MSEPCMR_HOUP" numeric (15,5), "MSEPCMR_INITGEN" numeric (15,5)
	 , "MSEPCCR_UNITSURPLUS" numeric(15,5), "MSEPCCR_UNITDEFICIT" numeric (15,5), "MSEPCCR_UNITSURPLUSRAMP" numeric (15,5), "MSEPCMR_NOLOADCOST" numeric (15,5)
	 , "MSEPCMR_CCA" numeric (15,5), "MSEPCMR_MCOM" numeric (15,5), "MSEPCCR_TRANSCOST" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
	BEGIN
		-- Insert/update Datamart Table datamart."MSH_RESULTS_BLOCK_TP"
		INSERT INTO datamart."MSH_RESULTS_PSU_TP" ("EXANTE_CALCULATED_MARKET_SCHEDULE","EXANTE_MARKET_SCHEDULE","EXANTE_NO_LOAD_COST"
										   , "EXANTE_HOURS_UP","EXANTE_INITIAL_GENERATION","EXANTE_LIMIT_SLACK_MAX"
										   , "EXANTE_LIMIT_SLACK_MIN","EXANTE_LIMIT_SLACK_RAMPRATE","EXANTE_COST"
									       , "EXANTE_MARKET_COMMITTED","EXANTE_TRANSITION","EXPOST_INDICATIVE_MARKET_SCHEDULE"
										   , "EXPOST_CALCULATED_MARKET_SCHEDULE","EXPOST_INDICATIVE_NO_LOAD_COST","EXPOST_INDICATIVE_HOURS_UP"
									       , "EXPOST_INDICATIVE_INITIAL_GENERATION","EXPOST_INDICATIVE_LIMIT_SLACK_MAX","EXPOST_INDICATIVE_LIMIT_SLACK_MIN"
										   , "EXPOST_INDICATIVE_LIMIT_SLACK_RAMPRATE","EXPOST_INDICATIVE_COST","EXPOST_INDICATIVE_CORRECTED_CERTIFIED_AVAILABILITY"
										   , "EXPOST_INDICATIVE_MARKET_COMMITTED","EXPOST_INDICATIVE_TRANSITION","EXPOST_CALCULATED_CONFIRMED_MARKET_SCHEDULE"
									       , "EXPOST_CONFIRMED_MARKET_SCHEDULE","EXPOST_CONFIRMED_NO_LOAD_COST","EXPOST_CONFIRMED_HOURS_UP"
										   , "EXPOST_CONFIRMED_INITIAL_GENERATION","EXPOST_CONFIRMED_LIMIT_SLACK_MAX","EXPOST_CONFIRMED_LIMIT_SLACK_MIN"
									       , "EXPOST_CONFIRMED_LIMIT_SLACK_RAMPRATE","EXPOST_CONFIRMED_COST","EXPOST_CONFIRMED_CORRECTED_CERTIFIED_AVAILABILITY"	
									       , "EXPOST_CONFIRMED_MARKET_COMMITTED","EXPOST_CONFIRMED_TRANSITION"	
									 	   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5","V6","V7","V8","V9"
			  , "V10","V11","V12","V13","V14","V15","V16","V17","V18"
			  , "V19","V20","V21","V22","V23","V25","V25","V26","V27"
			  , "V28","V29","V30","V31","V32","V33","V34","V35"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."MSH_RESULTS_PSU_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		 
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'MSH_RESULTS_PSU_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		 
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;		

         
		UPDATE datamart."MSH_RESULTS_PSU_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_PSU_TP"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_PSU_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
       -- RAISE NOTICE 'Updated MSH_RESULTS_PSU_TP - ID_BID_ZONE';
		
		UPDATE datamart."MSH_RESULTS_PSU_TP"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_PSU_TP"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_PSU_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';
		-- RAISE NOTICE 'Updated MSH_RESULTS_PSU_TP - ID_PSU';
		/*
		UPDATE public.sr_schedule_schd sch
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE sch.schedule_id = tmp.schedule_id;
		RAISE NOTICE 'Updated sr_schedule_schd - datamart_fetched';
	 */
	END;	
		/*
		UPDATE datamart."MSH_RESULTS_PSU_TP"
		SET "ID_PRODUCTION_FACILITY"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_PSU_TP"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_PSU_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODFAC';
		
		UPDATE datamart."MSH_RESULTS_PSU_TP"
		SET "ID_PRODUCTION_BLOCK"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."MSH_RESULTS_PSU_TP"."schedule_id"=tmp."schedule_id" AND datamart."MSH_RESULTS_PSU_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODBLOCK';
		*/
		
		
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_msh_results_psu_tp() OWNER TO postgres;

--
-- Name: usp_par_bidzone(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_par_bidzone()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';

	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('PAR_BIDZONE','sr_schedule_trak');
							
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "start_date" date
	 , "stop_date" date
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 , "ValueText" text
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value, NULL as "ValueText" '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''SCCA'',''MSCC'',''ODDF'',''UDPF'',''PPFLOOR'',''PPCAP'',''RELPRICE'',''CER'')
	AND sch.datamart_fetched=false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0
	UNION
	SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", NULL as value, svn.value as "ValueText"'||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_matrix svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''SFT'')
	AND sch.datamart_fetched=false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0
	';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","start_date","stop_date","ValueTypeCode","ScheduleTypeCode","Value", "ValueText")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate", t1."start_date"
	,"stop_date",t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value", t1."ValueText"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5), "ValueText" text)
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN 
		(SELECT DISTINCT "DATE_KEY_OMAN" FROM  datamart."TRADING_PERIOD") tp on t1.marketdate = tp."DATE_KEY_OMAN";
		
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'PAR_BIDZONE','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;
	
	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "start_date","stop_date","SD_ID","Primary_Role_Code"
	, Sum("SCCA") AS "V1"
	, Sum("MSCC") AS "V2"
	, Sum("ODDF") AS "V3"
	, Sum("UDPF") AS "V4"
	, Sum("PPFLOOR") AS "V5"
	, Sum("PPCAP") AS "V6"
	, Sum("RELPRICE") AS "V7"
	, MAX("SFT") AS "V8"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC) AS "Nr_Crt",schedule_id, marketdate,"start_date","stop_date"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, CASE WHEN "ValueTypeCode" like 'SFT' THEN MAX("ValueText") ELSE SUM("Value")::text  END AS "Valoare"  
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","start_date","stop_date","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"Primary_Role_Code"$$,
		$$ SELECT 'SCCA' UNION ALL
		   SELECT 'MSCC' UNION ALL
		   SELECT 'ODDF' UNION ALL
		   SELECT 'UDPF' UNION ALL
		   SELECT 'PPFLOOR' UNION ALL
		   SELECT 'PPCAP' UNION ALL
		   SELECT 'RELPRICE' UNION ALL
		   SELECT 'SFT'$$) 
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "start_date" date,"stop_date" date,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "SCCA" numeric(15,5), "MSCC" numeric (15,5), "ODDF" numeric(15,5), "UDPF" numeric (15,5)
	 , "PPFLOOR" numeric (15,5),"PPCAP" numeric (15,5), "RELPRICE" numeric(15,5), "SFT" text)
	GROUP BY schedule_id, marketdate, "start_date","stop_date","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id;
	
		-- Insert/update Datamart Table datamart."TRA_OFFER_DATA_DAY"
		INSERT INTO datamart."PAR_BIDZONE" ("ANNUAL_SCARCITY_CREDIT_CAP","MONTHLY_SCARCITY_CREDIT_CAP"
							, "OVER_DELIVERY_DISCOUNT_FACTOR","UNDER_DELIVERY_PREMIUM_FACTOR","POOL_PRICE_FLOOR"
							, "POOL_PRICE_CAP","RELIABILITY_PRICE","SCARCITY_FACTOR_TABLE"
							, "DATE_KEY_OMAN","ID_BID_ZONE","START_DATE","STOP_DATE","schedule_id")
		SELECT "V1","V2","V3","V4"
		, "V5","V6","V7","V8",tmp."marketdate",tmp."SD_ID",tmp."start_date",tmp."stop_date",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."PAR_BIDZONE" tgp on tmp."schedule_id"=tgp."schedule_id"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='BZONE';
    		
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'PAR_BIDZONE','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
	RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;		
	
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_par_bidzone() OWNER TO postgres;

--
-- Name: usp_par_bidzone_adm_price(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_par_bidzone_adm_price()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('PAR_BIDZONE_ADM_PRICE','sr_schedule_trak');		
							
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	DROP TABLE IF EXISTS public."tempData";
	
	CREATE TEMP TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN sd_entity en on sp.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	JOIN SD_ENTITY_DEF ed on en.entity_id=ed.entity_id
	WHERE st.code=''ADMPRICE'' 
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)	
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	INNER JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	INNER JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."PAR_BIDZONE_ADM_PRICE" tgp on t1."schedule_id"=tgp."schedule_id"
	WHERE tgp.schedule_id IS NULL
	;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'PAR_BIDZONE_ADM_PRICE','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	
	IF v_rows_count= 0 then
		RAISE NOTICE 'No new records selected from HIS: ( % )', v_rows_count;
	ELSE
		RAISE NOTICE 'Number of new records selected from HIS: ( % )', v_rows_count;
	END IF;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("ADMPRICE") AS "V1"
		FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'ADMPRICE' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "ADMPRICE" numeric(15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID", "Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."MSH_RESULTS_BLOCK_TP"
		INSERT INTO datamart."PAR_BIDZONE_ADM_PRICE" ("ADMINISTERED_PRICE"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_BID_ZONE","schedule_id")
		SELECT "V1", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."PAR_BIDZONE_ADM_PRICE" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='BZONE';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'PAR_BIDZONE_ADM_PRICE','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_par_bidzone_adm_price() OWNER TO postgres;

--
-- Name: usp_set_annual_scarcity_facility_cb(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_annual_scarcity_facility_cb()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;
BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';

	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_ANNUAL_SCARCITY_FACILITY','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_ANNUAL_SCARCITY_FACILITY_CB";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''PFASSC_SI'',''PFASSC_SC'')
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	 
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT distinct t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN"; -- and t1.interval=tp."TRADING_PERIOD";
 
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_ANNUAL_SCARCITY_FACILITY_CB','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
 
	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("PFASSC_SI") AS "V1"
	, Sum("PFASSC_SC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'PFASSC_SI' UNION ALL
		   SELECT 'PFASSC_SC' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "PFASSC_SI" numeric(15,5), "PFASSC_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
	CREATE TABLE IF NOT EXISTS datamart."SET_ANNUAL_SCARCITY_FACILITY_CB"
(
    "INDICATIVE_FACILITY_ANNUAL_SCARCITY_CREDIT" numeric(15,5),
    "CONFIRMED_FACILITY_ANNUAL_SCARCITY_CREDIT" numeric(15,5),
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_FACILITY" integer,
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint NOT NULL,
    schedule_id integer
);
	
		-- Insert/update Datamart Table datamart."SET_ANNUAL_SCARCITY_FACILITY_CB"
		INSERT INTO datamart."SET_ANNUAL_SCARCITY_FACILITY_CB" ("INDICATIVE_FACILITY_ANNUAL_SCARCITY_CREDIT","CONFIRMED_FACILITY_ANNUAL_SCARCITY_CREDIT"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_ANNUAL_SCARCITY_FACILITY_CB" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY'
		AND NOT EXISTS (SELECT 1 
						  FROM datamart."SET_ANNUAL_SCARCITY_FACILITY" asf JOIN datamart."TRADING_PERIOD" tp ON tp."DATE_KEY_OMAN"=tmp."marketdate"
						 WHERE asf."ID_Year" = tp."ID_Year"  
						   AND asf."ID_MARKET_PARTY" = tmp."SD_ID");
 
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_ANNUAL_SCARCITY_FACILITY_CB','Inserted into Datamart', CURRENT_TIMESTAMP(0),v_rows_count;
	 	
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;
		UPDATE datamart."SET_ANNUAL_SCARCITY_FACILITY_CB"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_ANNUAL_SCARCITY_FACILITY_CB"."schedule_id"=tmp."schedule_id" AND datamart."SET_ANNUAL_SCARCITY_FACILITY_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
		
		UPDATE datamart."SET_ANNUAL_SCARCITY_FACILITY_CB" acf
		set "ID_Year"=tp."ID_Year"
		from datamart."TRADING_PERIOD" tp
		where acf."DATE_KEY_OMAN"=tp."DATE_KEY_OMAN";
		--and acf."TRADING_PERIOD"=tp."TRADING_PERIOD";

--update final table

INSERT INTO datamart."SET_ANNUAL_SCARCITY_FACILITY"(
	"INDICATIVE_FACILITY_ANNUAL_SCARCITY_CREDIT", "CONFIRMED_FACILITY_ANNUAL_SCARCITY_CREDIT", "ID_Year", "ID_BID_ZONE", "ID_MARKET_PARTY", "ID_PRODUCTION_FACILITY")
SELECT "INDICATIVE_FACILITY_ANNUAL_SCARCITY_CREDIT","CONFIRMED_FACILITY_ANNUAL_SCARCITY_CREDIT", "ID_Year", "ID_BID_ZONE","ID_MARKET_PARTY", "ID_PRODUCTION_FACILITY"
from datamart."SET_ANNUAL_SCARCITY_FACILITY_CB";
		
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;		

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_ANNUAL_SCARCITY_FACILITY_CB";
END;
$_$;


ALTER PROCEDURE datamart.usp_set_annual_scarcity_facility_cb() OWNER TO postgres;

--
-- Name: usp_set_daily_energy_psu_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_daily_energy_psu_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_DAILY_ENERGY_PSU_TD','sr_schedule_trak');

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''MSEC_EI'',''UNINSTIMBC_EI'',''UNINSTIMBD_EI'',''DQ_EI'',''MSCH_EI''
				  	,''MSEC_EC'',''UNINSTIMBC_EC'',''UNINSTIMBD_EC'',''DQ_EC'',''MSCH_EC'')
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."SET_DAILY_ENERGY_PSU_TD" tgp on t1."schedule_id"=tgp."schedule_id"
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_DAILY_ENERGY_PSU_TD','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("MSEC_EI") AS "V1"
	, Sum("UNINSTIMBC_EI") AS "V2"
	, Sum("UNINSTIMBD_EI") AS "V3"
	, Sum("DQ_EI") AS "V4"
	, Sum("MSCH_EI") AS "V5"
	, Sum("MSEC_EC") AS "V6"
	, Sum("UNINSTIMBC_EC") AS "V7"
	, Sum("UNINSTIMBD_EC") AS "V8"
	, Sum("DQ_EC") AS "V9"
	, Sum("MSCH_EC") AS "V10"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'MSEC_EI' UNION ALL
		   SELECT 'UNINSTIMBC_EI' UNION ALL
		   SELECT 'UNINSTIMBD_EI' UNION ALL
		   SELECT 'DQ_EI' UNION ALL
		   SELECT 'MSCH_EI' UNION ALL
		   SELECT 'MSEC_EC' UNION ALL
		   SELECT 'UNINSTIMBC_EC' UNION ALL
		   SELECT 'UNINSTIMBD_EC' UNION ALL
		   SELECT 'DQ_EC' UNION ALL
		   SELECT 'MSCH_EC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "MSEC_EI" numeric(15,5), "UNINSTIMBC_EI" numeric (15,5), "UNINSTIMBD_EI" numeric (15,5), "DQ_EI" numeric (15,5)
	 , "MSCH_EI" numeric (15,5),"MSEC_EC" numeric (15,5), "UNINSTIMBC_EC" numeric (15,5), "UNINSTIMBD_EC" numeric (15,5)
	 , "DQ_EC" numeric (15,5), "MSCH_EC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."SET_DAILY_ENERGY_PSU_TD" ("INDICATIVE_ENERGY_CREDIT","INDICATIVE_UNINSTRUCTED_IMBALANCE_CREDIT"
										   , "INDICATIVE_UNINSTRUCTED_IMBALANCE_DEBIT","INDICATIVE_DISPATCH_QUANTITY"
										   , "INDICATIVE_MARKET_SCHEDULE","CONFIRMED_ENERGY_CREDIT"
										   , "CONFIRMED_UNINSTRUCTED_IMBALANCE_CREDIT","CONFIRMED_UNINSTRUCTED_IMBALANCE_DEBIT"
										   , "CONFIRMED_DISPATCH_QUANTITY","CONFIRMED_MARKET_SCHEDULE"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5","V6","V7","V8","V9","V10",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_DAILY_ENERGY_PSU_TD" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';

		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_DAILY_ENERGY_PSU_TD','Inserted into Datamart', CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."SET_DAILY_ENERGY_PSU_TD"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_ENERGY_PSU_TD"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_ENERGY_PSU_TD"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
		
		UPDATE datamart."SET_DAILY_ENERGY_PSU_TD"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_ENERGY_PSU_TD"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_ENERGY_PSU_TD"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_daily_energy_psu_tp() OWNER TO postgres;

--
-- Name: usp_set_daily_facility_day(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_daily_facility_day()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_DAILY_FACILITY_DAY','sr_schedule_trak');

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''PFDEC_EI'' OR st.code=''FPAD_EI'' OR st.code=''PFDEC_EC'' OR st.code=''FPAD_EC'')
	AND sch.is_actual_version=1
	AND sch.datamart_fetched = false
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	INNER JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	INNER JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."SET_DAILY_FACILITY_DAY" tgp ON t1.schedule_id=tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_DAILY_FACILITY_DAY','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	IF v_rows_count= 0 then
		RAISE NOTICE 'No new records selected from HIS: ( % )', v_rows_count;
	ELSE
		RAISE NOTICE 'Number of new records selected from HIS: ( % )', v_rows_count;
	END IF;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("PFDEC_EI") AS "V1"
	, Sum("FPAD_EI") AS "V2"
	, Sum("PFDEC_EC") AS "V3"
	, Sum("FPAD_EC") AS "V4"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'PFDEC_EI' UNION ALL
		   SELECT 'FPAD_EI' UNION ALL
		   SELECT 'PFDEC_EC' UNION ALL
		   SELECT 'FPAD_EC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"PFDEC_EI" numeric(15,5), "FPAD_EI" numeric (15,5),"PFDEC_EC" numeric (15,5),"FPAD_EC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."SET_DAILY_FACILITY_DAY" ("INDICATIVE_FACILITY_DAILY_ENERGY_CREDIT","INDICATIVE_FUEL_PRICE_ADJUSTMENT_DEBIT" 
										   , "CONFIRMED_FACILITY_DAILY_ENERGY_CREDIT","CONFIRMED_FUEL_PRICE_ADJUSTMENT_DEBIT"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_DAILY_FACILITY_DAY" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_DAILY_FACILITY_DAY','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;		

		UPDATE datamart."SET_DAILY_FACILITY_DAY"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_FACILITY_DAY"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_FACILITY_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."SET_DAILY_FACILITY_DAY"
		SET "ID_PRODUCTION_FACILITY"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_FACILITY_DAY"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_FACILITY_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODFAC';

	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE "tmpStandingData";
	DROP TABLE "tempData";
	DROP TABLE "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_daily_facility_day() OWNER TO postgres;

--
-- Name: usp_set_daily_production_block_day(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_daily_production_block_day()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_DAILY_PRODUCTION_BLOCK_DAY','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''UNINSTIMBDC_EI'',''UNINSTIMBDD_EI'',''MSPC_EI'',''CONONC_EI'',''MSMWC_EI'',''MSECD_EI'',''DAD_EI'',''UNINSTIMBDC_EC'',''UNINSTIMBDD_EC'',''MSPC_EC'',''CONONC_EC'',''MSMWC_EC'',''MSECD_EC'',''DAD_EC'')
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	INNER JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	INNER JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" tgp on t1."schedule_id"=tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;

	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_DAILY_PRODUCTION_BLOCK_DAY','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of new records selected from HIS: ( % )', v_rows_count;
		
	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("UNINSTIMBDC_EI") AS "V1"
	, Sum("UNINSTIMBDD_EI") AS "V2"
	, Sum("MSPC_EI") AS "V3"
	, Sum("CONONC_EI") AS "V4"
	, Sum("MSMWC_EI") AS "V5"
	, Sum("MSECD_EI") AS "V6"
	, Sum("DAD_EI") AS "V7"
	, Sum("UNINSTIMBDC_EC") AS "V8"
	, Sum("UNINSTIMBDD_EC") AS "V9"
	, Sum("MSPC_EC") AS "V10"
	, Sum("CONONC_EC") AS "V11"
	, Sum("MSMWC_EC") AS "V12"
	, Sum("MSECD_EC") AS "V13"
	, Sum("DAD_EC") AS "V14"

	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ 	SELECT 'UNINSTIMBDC_EI' UNION ALL
			SELECT 'UNINSTIMBDD_EI' UNION ALL
			SELECT 'MSPC_EI' UNION ALL
			SELECT 'CONONC_EI' UNION ALL
			SELECT 'MSMWC_EI' UNION ALL
			SELECT 'MSECD_EI' UNION ALL
			SELECT 'DAD_EI' UNION ALL
			SELECT 'UNINSTIMBDC_EC' UNION ALL
			SELECT 'UNINSTIMBDD_EC' UNION ALL
			SELECT 'MSPC_EC' UNION ALL
			SELECT 'CONONC_EC' UNION ALL
			SELECT 'MSMWC_EC' UNION ALL
			SELECT 'MSECD_EC' UNION ALL
			SELECT 'DAD_EC' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"UNINSTIMBDC_EI" numeric(15,5),"UNINSTIMBDD_EI" numeric(15,5),"MSPC_EI" numeric(15,5),"CONONC_EI" numeric(15,5),"MSMWC_EI" numeric(15,5),"MSECD_EI" numeric(15,5),"DAD_EI" numeric(15,5),
	"UNINSTIMBDC_EC" numeric(15,5),"UNINSTIMBDD_EC" numeric(15,5),"MSPC_EC" numeric(15,5),"CONONC_EC" numeric(15,5),"MSMWC_EC" numeric(15,5),"MSECD_EC" numeric(15,5),"DAD_EC" numeric(15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" ("INDICATIVE_UNINSTRUCTED_IMBALANCE_DAILY_CREDIT", "INDICATIVE_UNINSTRUCTED_IMBALANCE_DAILY_DEBIT", "INDICATIVE_PRODUCTION_COST", "INDICATIVE_CONSTRAINED_ON_CREDIT", "INDICATIVE_MAKE_WHOLE_CREDIT", 
															"INDICATIVE_DAILY_ENERGY_CREDIT", "INDICATIVE_DISPATCH_ADJUSTMENT_DEBIT", "CONFIRMED_UNINSTRUCTED_IMBALANCE_DAILY_CREDIT", "CONFIRMED_UNINSTRUCTED_IMBALANCE_DAILY_DEBIT", "CONFIRMED_PRODUCTION_COST", 
															"CONFIRMED_CONSTRAINED_ON_CREDIT", "CONFIRMED_MAKE_WHOLE_CREDIT", "CONFIRMED_DAILY_ENERGY_CREDIT", "CONFIRMED_DISPATCH_ADJUSTMENT_DEBIT"
															, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5","V6","V7","V8","V9","V10","V11","V12","V13","V14",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_DAILY_PRODUCTION_BLOCK_DAY','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
	
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" 
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_PRODUCTION_BLOCK_DAY"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_PRODUCTION_BLOCK_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
		
		UPDATE datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" 
		SET "ID_PRODUCTION_BLOCK"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_PRODUCTION_BLOCK_DAY"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_PRODUCTION_BLOCK_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODBLOCK';
				
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
END;
$_$;


ALTER PROCEDURE datamart.usp_set_daily_production_block_day() OWNER TO postgres;

--
-- Name: usp_set_daily_production_block_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_daily_production_block_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_DAILY_PRODUCTION_BLOCK_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" varchar(100)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_string svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''DISPATCHCONF_EI'' OR st.code=''DISPATCHCONF_EC'' )
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" varchar(100))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_DAILY_PRODUCTION_BLOCK_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, MAX("DISPATCHCONF_EI") AS "V1"
	, MAX("DISPATCHCONF_EC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, MAX("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'DISPATCHCONF_EI' UNION ALL
		   SELECT 'DISPATCHCONF_EC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"DISPATCHCONF_EI" varchar(100), "DISPATCHCONF_EC" varchar(100))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."SET_DAILY_PRODUCTION_BLOCK_TP" ("INDICATIVE_DISPATCHED_CONFIGURATION","CONFIRMED_DISPATCHED_CONFIGURATION"
										   ,"DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_DAILY_PRODUCTION_BLOCK_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_DAILY_PRODUCTION_BLOCK_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;		

		UPDATE datamart."SET_DAILY_PRODUCTION_BLOCK_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_PRODUCTION_BLOCK_TP"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_PRODUCTION_BLOCK_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."SET_DAILY_PRODUCTION_BLOCK_TP"
		SET "ID_PRODUCTION_BLOCK"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_PRODUCTION_BLOCK_TP"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_PRODUCTION_BLOCK_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODBLOCK';

	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE "tmpStandingData";
	DROP TABLE "tempData";
	DROP TABLE "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_daily_production_block_tp() OWNER TO postgres;

--
-- Name: usp_set_daily_scarcity_bid_zone_cb(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_daily_scarcity_bid_zone_cb()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_DAILY_SCARCITY_BID_ZONE_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_DAILY_SCARCITY_BID_ZONE_CB";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''SP_SI'',''SP_SC'')
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("SP_SI") AS "V1"
	, Sum("SP_SC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'SP_SI' UNION ALL
		   SELECT 'SP_SC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"SP_SI" numeric(15,5), "SP_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
	CREATE TABLE IF NOT EXISTS datamart."SET_DAILY_SCARCITY_BID_ZONE_CB"
(
    "INDICATIVE_SCARCITY_PRICE" numeric(15,5),
    "CONFIRMED_SCARCITY_PRICE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    schedule_id integer
);

	
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."SET_DAILY_SCARCITY_BID_ZONE_CB" ("INDICATIVE_SCARCITY_PRICE","CONFIRMED_SCARCITY_PRICE"
										   			   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_DAILY_SCARCITY_BID_ZONE_CB" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';

		UPDATE datamart."SET_DAILY_SCARCITY_BID_ZONE_CB"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_SCARCITY_BID_ZONE_CB"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_SCARCITY_BID_ZONE_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
--UPDATE FINAL TABLE

INSERT INTO datamart."SET_DAILY_SCARCITY_BID_ZONE_TP"(
	"INDICATIVE_SCARCITY_PRICE", "CONFIRMED_SCARCITY_PRICE", "DATE_KEY_OMAN", "TRADING_PERIOD", "ID_BID_ZONE")
SELECT "INDICATIVE_SCARCITY_PRICE", "CONFIRMED_SCARCITY_PRICE", "DATE_KEY_OMAN", "TRADING_PERIOD", "ID_BID_ZONE" from datamart."SET_DAILY_SCARCITY_BID_ZONE_CB";		
	
	
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_DAILY_SCARCITY_BID_ZONE_CB";
END;
$_$;


ALTER PROCEDURE datamart.usp_set_daily_scarcity_bid_zone_cb() OWNER TO postgres;

--
-- Name: usp_set_daily_scarcity_bid_zone_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_daily_scarcity_bid_zone_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_DAILY_SCARCITY_BID_ZONE_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''SP_SI'',''SP_SC'')
	AND sch.datamart_fetched =false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_DAILY_SCARCITY_BID_ZONE_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;
	
	RAISE NOTICE 'Number of new records selected from HIS: ( % )', v_rows_count;
	
	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("SP_SI") AS "V1"
	, Sum("SP_SC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'SP_SI' UNION ALL
		   SELECT 'SP_SC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"SP_SI" numeric(15,5), "SP_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."SET_DAILY_SCARCITY_BID_ZONE_TP"
		INSERT INTO datamart."SET_DAILY_SCARCITY_BID_ZONE_TP" ("INDICATIVE_SCARCITY_PRICE","CONFIRMED_SCARCITY_PRICE"
										   			   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_BID_ZONE","schedule_id")
		SELECT "V1","V2",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_DAILY_SCARCITY_BID_ZONE_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='BZONE';

	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_DAILY_SCARCITY_BID_ZONE_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
	
	RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;
	
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_daily_scarcity_bid_zone_tp() OWNER TO postgres;

--
-- Name: usp_set_daily_scarcity_psu_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_daily_scarcity_psu_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_DAILY_SCARCITY_PSU_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code in (''EAQ_SI'',''EAQ_SC'',''SPC_SI'',''SPC_SC'',''PSC_SI'',''PSC_SC'',''ASC_SI'',''ASC_SC'' )
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."SET_DAILY_SCARCITY_PSU_TP" tgp on t1."schedule_id"= tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_DAILY_SCARCITY_PSU_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;
	

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("ASC_SI") AS "V1"
	, Sum("EAQ_SI") AS "V2"
	, Sum("PSC_SI") AS "V3"
	, Sum("SPC_SI") AS "V4"
	, Sum("ASC_SC") AS "V5"
	, Sum("EAQ_SC") AS "V6"
	, Sum("PSC_SC") AS "V7"
	, Sum("SPC_SC") AS "V8"	
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'ASC_SI' UNION ALL
		   SELECT 'EAQ_SI' UNION ALL
		   SELECT 'PSC_SI' UNION ALL
		   SELECT 'SPC_SI' UNION ALL
		   SELECT 'ASC_SC' UNION ALL
		   SELECT 'EAQ_SC' UNION ALL
		   SELECT 'PSC_SC' UNION ALL
		   SELECT 'SPC_SC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "ASC_SI" numeric(15,5), "EAQ_SI" numeric(15,5), "PSC_SI" numeric(15,5), "SPC_SI" numeric(15,5)
	 , "ASC_SC" numeric(15,5), "EAQ_SC" numeric(15,5), "PSC_SC" numeric(15,5), "SPC_SC" numeric(15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."SET_DAILY_SCARCITY_PSU_TP"
		INSERT INTO datamart."SET_DAILY_SCARCITY_PSU_TP" ("INDICATIVE_ADJUSTED_SCARCITY_CREDIT","INDICATIVE_ELIGIBILITY_QUANTITY"
										   ,"INDICATIVE_PRELIMINARY_SCARCITY_CREDIT","INDICATIVE_SCARCITY_PRICE_COEFFICIENT"
										   ,"CONFIRMED_ADJUSTED_SCARCITY_CREDIT","CONFIRMED_ELIGIBILITY_QUANTITY"
										   ,"CONFIRMED_PRELIMINARY_SCARCITY_CREDIT","CONFIRMED_SCARCITY_PRICE_COEFFICIENT"
										   ,"DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5","V6","V7","V8", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_DAILY_SCARCITY_PSU_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_DAILY_SCARCITY_PSU_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."SET_DAILY_SCARCITY_PSU_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_SCARCITY_PSU_TP"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_SCARCITY_PSU_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."SET_DAILY_SCARCITY_PSU_TP"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_DAILY_SCARCITY_PSU_TP"."schedule_id"=tmp."schedule_id" AND datamart."SET_DAILY_SCARCITY_PSU_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';

	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE  IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_daily_scarcity_psu_tp() OWNER TO postgres;

--
-- Name: usp_set_meter_data_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_meter_data_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_METER_DATA_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
 
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''METDATA'')
	AND sch.is_actual_version=1
	AND sch.datamart_fetched = false
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."SET_METER_DATA_TP" tgp on t1."schedule_id"= tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_METER_DATA_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("METDATA") AS "V1"
	, Sum("METDATAQ") AS "V2"
	, Sum("WATERFLAG") AS "V3"
	, Sum("AUXFLAG") AS "V4"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'METDATA' UNION ALL
		   SELECT 'METDATAQ' UNION ALL
		   SELECT 'WATERFLAG' UNION ALL
		   SELECT 'AUXFLAG'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"METDATA" numeric(15,5), "METDATAQ" numeric (15,5), "WATERFLAG" numeric (15,5), "AUXFLAG" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."SET_METER_DATA_TP" ("METER_DATA_QUANTITY","METER_DATA_QUALITY_FLAG"
										   ,"WATER_FLAG","AUXILIARY_FLAG"
										   ,"DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_METER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_METER_DATA_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."SET_METER_DATA_TP"
		SET "ID_GENSET"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_METER_DATA_TP"."schedule_id"=tmp."schedule_id" AND datamart."SET_METER_DATA_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OGENSET';

		UPDATE datamart."SET_METER_DATA_TP"
		SET "ID_METER"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_METER_DATA_TP"."schedule_id"=tmp."schedule_id" AND datamart."SET_METER_DATA_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OMETER';

	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_meter_data_tp() OWNER TO postgres;

--
-- Name: usp_set_monthly_energy_facility(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_monthly_energy_facility()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_MONTHLY_ENERGY_FACILITY','sr_schedule_trak');
	
	DROP TABLE IF EXISTS public."xtabtempdata";
	DROP TABLE IF EXISTS public."tmpStandingData";
	DROP TABLE IF EXISTS public."tempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "ID_Month" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''PFDEC_EI'',''DSMEC_SI'',''PFDEC_EC'',''DSMEC_SC'')
	 
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	--AND sch.datamart_fetched = false
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","ID_Month","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",tp."ID_Month"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, "interval" int, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1."interval"=tp."TRADING_PERIOD";
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_MONTHLY_ENERGY_FACILITY','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;
	
	IF v_rows_count= 0 then
		RAISE NOTICE 'No new records selected from HIS: ( % )', v_rows_count;
	ELSE
		RAISE NOTICE 'Number of new records selected from HIS: ( % )', v_rows_count;
	END IF;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "ID_Month","SD_ID","Primary_Role_Code"
	, Sum("PFDEC_EI") AS "V1"
	, Sum("DSMEC_SI") AS "V2"
	, Sum("PFDEC_EC") AS "V3"
	, Sum("DSMEC_SC") AS "V4"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,"ID_Month" ASC) AS "Nr_Crt",schedule_id, marketdate,"ID_Month"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","ID_Month","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"ID_Month","Primary_Role_Code"$$,
		$$ SELECT 'PFDEC_EI' UNION ALL
		   SELECT 'DSMEC_SI' UNION ALL
		   SELECT 'PFDEC_EC' UNION ALL
		   SELECT 'DSMEC_SC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "ID_Month" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "PFDEC_EI" numeric(15,5), "DSMEC_SI" numeric (15,5), "PFDEC_EC" numeric (15,5), "DSMEC_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "ID_Month","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, "ID_Month";

	
	-- Insert/update Datamart Table datamart."SET_MONTHLY_ENERGY_FACILITY"
	INSERT INTO datamart."SET_MONTHLY_ENERGY_FACILITY" ("INDICATIVE_FACILITY_MONTHLY_ENERGY_CREDIT","INDICATIVE_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT"
									   , "CONFIRMED_FACILITY_MONTHLY_ENERGY_CREDIT","CONFIRMED_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT"
									   , "ID_Month","ID_Year","ID_MARKET_PARTY","schedule_id")
	SELECT "V1","V2","V3","V4",mnt."ID_Month",mnt."ID_YEAR",tmp."SD_ID",tmp."schedule_id"
	FROM "xtabTempData" tmp
	INNER JOIN datamart."Month" mnt on tmp."ID_Month" = mnt."ID_Month"
	LEFT JOIN datamart."SET_MONTHLY_ENERGY_FACILITY" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."ID_Month"=tgp."ID_Month"
	WHERE tgp.schedule_id IS NULL
	AND tmp."Primary_Role_Code"='MARKETPARTY';
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_MONTHLY_ENERGY_FACILITY','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
	
	RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;	

		UPDATE datamart."SET_MONTHLY_ENERGY_FACILITY"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_ENERGY_FACILITY"."schedule_id"=tmp."schedule_id" 
		AND tmp."Primary_Role_Code"='BZONE';
		
		UPDATE datamart."SET_MONTHLY_ENERGY_FACILITY"
		SET "ID_PRODUCTION_FACILITY"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_ENERGY_FACILITY"."schedule_id"=tmp."schedule_id" 
		AND tmp."Primary_Role_Code"='OPRODFAC';
 /*
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	*/
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_monthly_energy_facility() OWNER TO postgres;

--
-- Name: usp_set_monthly_energy_facility_cb(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_monthly_energy_facility_cb()
    LANGUAGE plpgsql
    AS $_$

DECLARE  sqlstr text;
DECLARE connstr text;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_MONTHLY_ENERGY_FACILITY','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_MONTHLY_ENERGY_FACILITY_CB";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''PFDEC_EI'',''DSMEC_SI'',''PFDEC_EC'',''DSMEC_SC'')
	AND sch.is_actual_version=1
	AND sch.deletion_time is null 
	AND sch.draft=0
	AND ed.draft=0';
	-- AND sch.datamart_fetched = false
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("PFDEC_EI") AS "V1"
	, Sum("DSMEC_SI") AS "V2"
	, Sum("PFDEC_EC") AS "V3"
	, Sum("DSMEC_SC") AS "V4"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'PFDEC_EI' UNION ALL
		   SELECT 'DSMEC_SI' UNION ALL
		   SELECT 'PFDEC_EC' UNION ALL
		   SELECT 'DSMEC_SC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "PFDEC_EI" numeric(15,5), "DSMEC_SI" numeric (15,5), "PFDEC_EC" numeric (15,5), "DSMEC_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
	CREATE TABLE IF NOT EXISTS datamart."SET_MONTHLY_ENERGY_FACILITY_CB"
(
    "INDICATIVE_FACILITY_MONTHLY_ENERGY_CREDIT" numeric(15,5),
    "INDICATIVE_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT" numeric(15,5),
    "CONFIRMED_FACILITY_MONTHLY_ENERGY_CREDIT" numeric(15,5),
    "CONFIRMED_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT" numeric(15,5),
    "ID_Month" integer,
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PSU" integer,
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    schedule_id integer
);
	
	
		-- Insert/update Datamart Table datamart."SET_MONTHLY_ENERGY_FACILITY_CB"
		INSERT INTO datamart."SET_MONTHLY_ENERGY_FACILITY_CB" ("INDICATIVE_FACILITY_MONTHLY_ENERGY_CREDIT","INDICATIVE_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT"
										   , "CONFIRMED_FACILITY_MONTHLY_ENERGY_CREDIT","CONFIRMED_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_MONTHLY_ENERGY_FACILITY_CB" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';

		UPDATE datamart."SET_MONTHLY_ENERGY_FACILITY_CB"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_ENERGY_FACILITY_CB"."schedule_id"=tmp."schedule_id" AND datamart."SET_MONTHLY_ENERGY_FACILITY_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BIDZONE';
		
		UPDATE datamart."SET_MONTHLY_ENERGY_FACILITY_CB"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_ENERGY_FACILITY_CB"."schedule_id"=tmp."schedule_id" AND datamart."SET_MONTHLY_ENERGY_FACILITY_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';
		
		UPDATE datamart."SET_MONTHLY_ENERGY_FACILITY_CB" acf
		set "ID_Year"=tp."ID_Year"
		from datamart."TRADING_PERIOD" tp
		where acf."DATE_KEY_OMAN"=tp."DATE_KEY_OMAN"
		and acf."TRADING_PERIOD"=tp."TRADING_PERIOD";
		
		UPDATE datamart."SET_MONTHLY_ENERGY_FACILITY_CB" acf
		set "ID_Month"=tp."ID_Month"
		from datamart."TRADING_PERIOD" tp
		where acf."DATE_KEY_OMAN"=tp."DATE_KEY_OMAN"
		and acf."TRADING_PERIOD"=tp."TRADING_PERIOD";		

--UPDATE FINAL TABLE	
INSERT INTO datamart."SET_MONTHLY_ENERGY_FACILITY"(
	"INDICATIVE_FACILITY_MONTHLY_ENERGY_CREDIT", "INDICATIVE_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT", "CONFIRMED_FACILITY_MONTHLY_ENERGY_CREDIT",
	"CONFIRMED_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT", "ID_Month", "ID_Year", "ID_BID_ZONE", "ID_MARKET_PARTY", "ID_PSU")
SELECT 	"INDICATIVE_FACILITY_MONTHLY_ENERGY_CREDIT", "INDICATIVE_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT", "CONFIRMED_FACILITY_MONTHLY_ENERGY_CREDIT",
	"CONFIRMED_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT", "ID_Month", "ID_Year", "ID_BID_ZONE", "ID_MARKET_PARTY", "ID_PSU" from datamart."SET_MONTHLY_ENERGY_FACILITY_CB";	
 /*
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	*/
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_MONTHLY_ENERGY_FACILITY_CB";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_monthly_energy_facility_cb() OWNER TO postgres;

--
-- Name: usp_set_monthly_scarcity_bid_zone(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_monthly_scarcity_bid_zone()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_MONTHLY_SCARCITY_BID_ZONE','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "ID_Month" int
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''PSSF_SI'',''PSSF_SC'')
	AND sch.datamart_fetched=false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","ID_Month","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",tp."ID_Month"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN (SELECT DISTINCT "DATE_KEY_OMAN","ID_Month","ID_Year"
		 	FROM datamart."TRADING_PERIOD") tp  on t1.marketdate = tp."DATE_KEY_OMAN"; 
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_MONTHLY_SCARCITY_BID_ZONE','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;
	
	IF v_rows_count= 0 then
		RAISE NOTICE 'No new records selected from HIS: ( % )', v_rows_count;
	ELSE
		RAISE NOTICE 'Number of new records selected from HIS: ( % )', v_rows_count;
	END IF;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "ID_Month","SD_ID","Primary_Role_Code"
	, Sum("PSSF_SI") AS "V1"
	, Sum("PSSF_SC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,"ID_Month" ASC) AS "Nr_Crt",schedule_id, marketdate,"ID_Month"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","ID_Month","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"ID_Month","Primary_Role_Code"$$,
		$$ SELECT 'PSSF_SI' UNION ALL
		   SELECT 'PSSF_SC' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "ID_Month" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "PSSF_SI" numeric(15,5), "PSSF_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "ID_Month","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, "ID_Month";

	
		-- Insert/update Datamart Table datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB"
		INSERT INTO datamart."SET_MONTHLY_SCARCITY_BID_ZONE" ("INDICATIVE_PRELIMINARY_SCARCITY_SCALING_FACTOR","CONFIRMED_PRELIMINARY_SCARCITY_SCALING_FACTOR"
										   , "ID_Month","ID_Year","ID_BID_ZONE","schedule_id")
		SELECT "V1","V2",mnt."ID_Month",mnt."ID_YEAR",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		INNER JOIN datamart."Month" mnt on tmp."ID_Month" = mnt."ID_Month"
		LEFT JOIN datamart."SET_MONTHLY_SCARCITY_BID_ZONE" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."ID_Month"=tgp."ID_Month"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='BZONE';

		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_MONTHLY_SCARCITY_BID_ZONE','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;	
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_monthly_scarcity_bid_zone() OWNER TO postgres;

--
-- Name: usp_set_monthly_scarcity_bid_zone_cb(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_monthly_scarcity_bid_zone_cb()
    LANGUAGE plpgsql
    AS $_$

DECLARE  sqlstr text;
DECLARE connstr text;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_MONTHLY_SCARCITY_BID_ZONE','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''PSSF_SI'',''PSSF_SC'')
	AND sch.is_actual_version=1
	AND sch.datamart_fetched = false
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("PSSF_SI") AS "V1"
	, Sum("PSSF_SC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'PSSF_SI' UNION ALL
		   SELECT 'PSSF_SC' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "PSSF_SI" numeric(15,5), "PSSF_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
	CREATE TABLE IF NOT EXISTS datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB"
(
    "INDICATIVE_PRELIMINARY_SCARCITY_SCALING_FACTOR" numeric(5,5),
    "CONFIRMED_PRELIMINARY_SCARCITY_SCALING_FACTOR" numeric(5,5),
    "ID_Month" integer,
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" integer,
    schedule_id integer,
    "ID_MARKET_PARTY" integer
);
	
	
		-- Insert/update Datamart Table datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB"
		INSERT INTO datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB" ("INDICATIVE_PRELIMINARY_SCARCITY_SCALING_FACTOR","CONFIRMED_PRELIMINARY_SCARCITY_SCALING_FACTOR"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';

		UPDATE datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB"."schedule_id"=tmp."schedule_id" AND datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BIDZONE';
		
		UPDATE datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB" acf
		set "ID_Year"=tp."ID_Year"
		from datamart."TRADING_PERIOD" tp
		where acf."DATE_KEY_OMAN"=tp."DATE_KEY_OMAN"
		and acf."TRADING_PERIOD"=tp."TRADING_PERIOD";
		
		UPDATE datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB" acf
		set "ID_Month"=tp."ID_Month"
		from datamart."TRADING_PERIOD" tp
		where acf."DATE_KEY_OMAN"=tp."DATE_KEY_OMAN"
		and acf."TRADING_PERIOD"=tp."TRADING_PERIOD";

--UPDATE FINAL TABLE	
INSERT INTO datamart."SET_MONTHLY_SCARCITY_BID_ZONE"(
	"INDICATIVE_PRELIMINARY_SCARCITY_SCALING_FACTOR", "CONFIRMED_PRELIMINARY_SCARCITY_SCALING_FACTOR", "ID_Month", "ID_Year", "ID_BID_ZONE")
SELECT "INDICATIVE_PRELIMINARY_SCARCITY_SCALING_FACTOR", "CONFIRMED_PRELIMINARY_SCARCITY_SCALING_FACTOR", "ID_Month", "ID_Year", "ID_BID_ZONE" from datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB";

	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_MONTHLY_SCARCITY_BID_ZONE_CB";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_monthly_scarcity_bid_zone_cb() OWNER TO postgres;

--
-- Name: usp_set_monthly_scarcity_block(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_monthly_scarcity_block()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_MONTHLY_SCARCITY_BLOCK','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "ID_Month" int
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''MASC_SI'',''MASC_SC'')
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","ID_Month","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",tp."ID_Month"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, "ID_Month" int, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN (SELECT DISTINCT "DATE_KEY_OMAN","ID_Month","ID_Year"
		 	FROM datamart."TRADING_PERIOD") tp  on t1.marketdate = tp."DATE_KEY_OMAN"; 

	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_MONTHLY_SCARCITY_BLOCK','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;
	
	IF v_rows_count= 0 then
		RAISE NOTICE 'No new records selected from HIS: ( % )', v_rows_count;
	ELSE
		RAISE NOTICE 'Number of new records selected from HIS: ( % )', v_rows_count;
	END IF;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "ID_Month","SD_ID","Primary_Role_Code"
	, Sum("MASC_SI") AS "V1"
	, Sum("MASC_SC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,"ID_Month" ASC) AS "Nr_Crt",schedule_id, marketdate,"ID_Month"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","ID_Month","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"ID_Month","Primary_Role_Code"$$,
		$$ SELECT 'MASC_SI' UNION ALL
		   SELECT 'MASC_SC' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "ID_Month" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "MASC_SI" numeric(15,5), "MASC_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "ID_Month","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, "ID_Month";
	
	
		-- Insert/update Datamart Table datamart."SET_MONTHLY_SCARCITY_BLOCK"
		INSERT INTO datamart."SET_MONTHLY_SCARCITY_BLOCK" ("INDICATIVE_MONTHLY_SCARCITY_CREDIT","CONFIRMED_MONTHLY_SCARCITY_CREDIT"
										   , "ID_Month","ID_Year","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2",mnt."ID_Month",mnt."ID_YEAR",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		INNER JOIN datamart."Month" mnt on tmp."ID_Month" = mnt."ID_Month"
		LEFT JOIN datamart."SET_MONTHLY_SCARCITY_BLOCK" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."ID_Month"=tgp."ID_Month"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_MONTHLY_SCARCITY_BLOCK','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;		

		UPDATE datamart."SET_MONTHLY_SCARCITY_BLOCK"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_SCARCITY_BLOCK"."schedule_id"=tmp."schedule_id" --AND datamart."SET_MONTHLY_SCARCITY_BLOCK"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
		
		UPDATE datamart."SET_MONTHLY_SCARCITY_BLOCK"
		SET "ID_PRODUCTION_BLOCK"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_SCARCITY_BLOCK"."schedule_id"=tmp."schedule_id" --AND datamart."SET_MONTHLY_SCARCITY_BLOCK"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODBLOCK';

		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_monthly_scarcity_block() OWNER TO postgres;

--
-- Name: usp_set_monthly_scarcity_block_cb(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_monthly_scarcity_block_cb()
    LANGUAGE plpgsql
    AS $_$

DECLARE  sqlstr text;
DECLARE connstr text;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_MONTHLY_SCARCITY_BLOCK','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_MONTHLY_SCARCITY_BLOCK_CB";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''MASC_SI'',''MASC_SC'')
	AND sch.is_actual_version=1
	AND sch.datamart_fetched = false
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("MASC_SI") AS "V1"
	, Sum("MASC_SC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'MASC_SI' UNION ALL
		   SELECT 'MASC_SC' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "MASC_SI" numeric(15,5), "MASC_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
	CREATE TABLE IF NOT EXISTS datamart."SET_MONTHLY_SCARCITY_BLOCK_CB"
(
    "INDICATIVE_MONTHLY_SCARCITY_CREDIT" numeric(15,5),
    "CONFIRMED_MONTHLY_SCARCITY_CREDIT" numeric(15,5),
    "ID_Month" integer,
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_BLOCK" integer,
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" integer,
    schedule_id integer
);
	
		-- Insert/update Datamart Table datamart."SET_MONTHLY_SCARCITY_BLOCK_CB"
		INSERT INTO datamart."SET_MONTHLY_SCARCITY_BLOCK_CB" ("INDICATIVE_MONTHLY_SCARCITY_CREDIT","CONFIRMED_MONTHLY_SCARCITY_CREDIT"
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."SET_MONTHLY_SCARCITY_BLOCK_CB" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';

		UPDATE datamart."SET_MONTHLY_SCARCITY_BLOCK_CB"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_SCARCITY_BLOCK_CB"."schedule_id"=tmp."schedule_id" AND datamart."SET_MONTHLY_SCARCITY_BLOCK_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BIDZONE';

		UPDATE datamart."SET_MONTHLY_SCARCITY_BLOCK_CB" acf
		set "ID_Year"=tp."ID_Year"
		from datamart."TRADING_PERIOD" tp
		where acf."DATE_KEY_OMAN"=tp."DATE_KEY_OMAN"
		and acf."TRADING_PERIOD"=tp."TRADING_PERIOD";
		
		UPDATE datamart."SET_MONTHLY_SCARCITY_BLOCK_CB" acf
		set "ID_Month"=tp."ID_Month"
		from datamart."TRADING_PERIOD" tp
		where acf."DATE_KEY_OMAN"=tp."DATE_KEY_OMAN"
		and acf."TRADING_PERIOD"=tp."TRADING_PERIOD";

--UPDATE FINAL TABLE	

INSERT INTO datamart."SET_MONTHLY_SCARCITY_BLOCK"(
	"INDICATIVE_MONTHLY_SCARCITY_CREDIT", "CONFIRMED_MONTHLY_SCARCITY_CREDIT", "ID_Month", "ID_Year", "ID_BID_ZONE", "ID_MARKET_PARTY", "ID_PRODUCTION_BLOCK")
SELECT "INDICATIVE_MONTHLY_SCARCITY_CREDIT", "CONFIRMED_MONTHLY_SCARCITY_CREDIT", "ID_Month", "ID_Year", "ID_BID_ZONE", "ID_MARKET_PARTY", "ID_PRODUCTION_BLOCK" from datamart."SET_MONTHLY_SCARCITY_BLOCK_CB";

	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
		
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS datamart."SET_MONTHLY_SCARCITY_BLOCK_CB";
END;
$_$;


ALTER PROCEDURE datamart.usp_set_monthly_scarcity_block_cb() OWNER TO postgres;

--
-- Name: usp_set_monthly_scarcity_facility(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_set_monthly_scarcity_facility()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('SET_MONTHLY_SCARCITY_FACILITY','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "ID_Month" int
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''PFMSC_SI'',''PFMSC_SC'')
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","ID_Month","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",tp."ID_Month"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, "ID_Month" smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN (SELECT DISTINCT "DATE_KEY_OMAN","ID_Month","ID_Year"
		 	FROM datamart."TRADING_PERIOD") tp  on t1.marketdate = tp."DATE_KEY_OMAN"; 
			
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'SET_MONTHLY_SCARCITY_FACILITY','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;
	
	IF v_rows_count= 0 then
		RAISE NOTICE 'No new records selected from HIS: ( % )', v_rows_count;
	ELSE
		RAISE NOTICE 'Number of new records selected from HIS: ( % )', v_rows_count;
	END IF;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "ID_Month","SD_ID","Primary_Role_Code"
	, Sum("PFMSC_SI") AS "V1"
	, Sum("PFMSC_SC") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,"ID_Month" ASC) AS "Nr_Crt",schedule_id, marketdate,"ID_Month"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","ID_Month","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"ID_Month","Primary_Role_Code"$$,
		$$ SELECT 'PFMSC_SI' UNION ALL
		   SELECT 'PFMSC_SC' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "ID_Month" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "PFMSC_SI" numeric(15,5), "PFMSC_SC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "ID_Month","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, "ID_Month";

	
		-- Insert/update Datamart Table datamart."SET_MONTHLY_SCARCITY_FACILITY"
		INSERT INTO datamart."SET_MONTHLY_SCARCITY_FACILITY" ("INDICATIVE_FACILITY_MONTHLY_SCARCITY_CREDIT","CONFIRMED_FACILITY_MONTHLY_SCARCITY_CREDIT"
										   , "ID_Month","ID_Year","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2",mnt."ID_Month",mnt."ID_YEAR",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		INNER JOIN datamart."Month" mnt on tmp."ID_Month" = mnt."ID_Month"
		LEFT JOIN datamart."SET_MONTHLY_SCARCITY_FACILITY" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."ID_Month"=tgp."ID_Month"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'SET_MONTHLY_SCARCITY_FACILITY','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."SET_MONTHLY_SCARCITY_FACILITY"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_SCARCITY_FACILITY"."schedule_id"=tmp."schedule_id" --AND datamart."SET_MONTHLY_SCARCITY_FACILITY_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
		
		UPDATE datamart."SET_MONTHLY_SCARCITY_FACILITY"
		SET "ID_PRODUCTION_FACILITY"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."SET_MONTHLY_SCARCITY_FACILITY"."schedule_id"=tmp."schedule_id" --AND datamart."SET_MONTHLY_SCARCITY_FACILITY_CB"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODFAC';
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;
		
			
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_set_monthly_scarcity_facility() OWNER TO postgres;

--
-- Name: usp_sync_nonactual_schedule(character varying, character varying); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_sync_nonactual_schedule(table_name_datamart character varying, table_name_his character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
  EXECUTE 'delete from datamart."'|| table_name_datamart ||'" ' ||
	 ' where schedule_id in (select schedule_id ' ||
						   '  from public."'|| table_name_his ||'" ' ||
						   ' where is_actual_version=0);';
END;
$$;


ALTER PROCEDURE datamart.usp_sync_nonactual_schedule(table_name_datamart character varying, table_name_his character varying) OWNER TO postgres;

--
-- Name: usp_tra_block_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_block_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_BLOCK_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS public."xtabTempData";
	DROP TABLE IF EXISTS public."tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''FCS'')
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";
	--WHERE t1."Value"<>0;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_BLOCK_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("FCS") AS "V1"
	--, Sum("GENSDISPATCH") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'FCS' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"FCS" numeric(15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."MSH_RESULTS_BLOCK_TP"
		INSERT INTO datamart."TRA_BLOCK_TP" ("FUEL_CONSUMPTION_SCHEDULE","DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_BLOCK_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_BLOCK_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."TRA_BLOCK_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_BLOCK_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_BLOCK_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."TRA_BLOCK_TP"
		SET "ID_PRODUCTION_BLOCK"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_BLOCK_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_BLOCK_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODBLOCK';
		
		UPDATE datamart."TRA_BLOCK_TP"
		SET "ID_FUEL_TYPE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_BLOCK_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_BLOCK_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OFT';
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_block_tp() OWNER TO postgres;

--
-- Name: usp_tra_exports_corridor_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_exports_corridor_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_EXPORTS_CORRIDOR_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''FSE'' OR st.code=''ASE'')
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_EXPORTS_CORRIDOR_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("FSE") AS "V1", Sum("ASE") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'FSE' UNION ALL
		   SELECT 'ASE' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"FSE" numeric(15,5), "ASE" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."TRA_EXPORTS_CORRIDOR_TP" ("FORECAST_SYSTEM_EXPORTS","ACTUAL_SYSTEM_EXPORTS","DATE_KEY_OMAN","TRADING_PERIOD","ID_CORRIDOR","schedule_id")
		SELECT "V1","V2",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_EXPORTS_CORRIDOR_TP" tec on tmp."schedule_id"=tec."schedule_id" and tmp."interval"=tec."TRADING_PERIOD"
		WHERE tec.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='CORRIDOR';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_EXPORTS_CORRIDOR_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;
    
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE "tmpStandingData";
	DROP TABLE "tempData";
	DROP TABLE "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_exports_corridor_tp() OWNER TO postgres;

--
-- Name: usp_tra_genset_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_genset_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_GENSET_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''GENSDISPATCH'' OR st.code=''GENSACTAVAIL'')
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_GENSET_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("GENSACTAVAIL") AS "V1", Sum("GENSDISPATCH") AS "V2"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'GENSACTAVAIL' UNION ALL
		   SELECT 'GENSDISPATCH' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"GENSACTAVAIL" numeric(15,5), "GENSDISPATCH" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."TRA_GENSET_TP" ("GENSET_ACTUAL_AVAILABILITY","GENSET_DISPATCH","DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_GENSET_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_GENSET_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."TRA_GENSET_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_GENSET_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_GENSET_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."TRA_GENSET_TP"
		SET "ID_GENSET"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_GENSET_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_GENSET_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OGENSET';
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_genset_tp() OWNER TO postgres;

--
-- Name: usp_tra_inputs_facility_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_inputs_facility_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS'; 
	
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_INPUTS_FACILITY','sr_schedule_trak');
							 
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TEMP TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''WATERPROD'' OR st.code=''WEATHER_FC'' OR st.code=''TCMACF'' 
	      OR st.code=''MRUNAUXCONSF'' OR st.code=''EFP'')
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)	
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric (15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."TRA_INPUTS_FACILITY" tgp on t1."schedule_id"=tgp."schedule_id"
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_INPUTS_FACILITY','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;
	
		RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("WATERPROD") AS "V1"
	, Sum("WEATHER_FC") AS "V2"
	, Sum("TCMACF") AS "V3"
	, Sum("MRUNAUXCONSF") AS "V4"
	, Sum("EFP") AS "V5"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'WATERPROD' UNION ALL
		   SELECT 'WEATHER_FC' UNION ALL
		   SELECT 'TCMACF' UNION ALL
		   SELECT 'MRUNAUXCONSF' UNION ALL
		   SELECT 'EFP'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "WATERPROD" numeric (15,5), "WEATHER_FC" numeric (15,5), "TCMACF" numeric (15,5)
	 , "MRUNAUXCONSF" numeric (15,5), "EFP" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_INPUTS_FACILITY"
		INSERT INTO datamart."TRA_INPUTS_FACILITY" ("WATER_PRODUCTION_REQUIREMENTS","WEATHER_FORECAST"
										   , "TRANSCO_MUST_RUN_AUXILIARY_FORECAST","MUST_RUN_AUXILIARY_FORECAST"
										   , "ECONOMIC_FUEL_PRICE" -- should be renamed
										   , "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_INPUTS_FACILITY" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_INPUTS_FACILITY','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."TRA_INPUTS_FACILITY"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_INPUTS_FACILITY"."schedule_id"=tmp."schedule_id" AND datamart."TRA_INPUTS_FACILITY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."TRA_INPUTS_FACILITY"
		SET "ID_PRODUCTION_FACILITY"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_INPUTS_FACILITY"."schedule_id"=tmp."schedule_id" AND datamart."TRA_INPUTS_FACILITY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODFAC';
		
		UPDATE datamart."TRA_INPUTS_FACILITY"
		SET "ID_FUEL_TYPE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_INPUTS_FACILITY"."schedule_id"=tmp."schedule_id" AND datamart."TRA_INPUTS_FACILITY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OFT';

	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_inputs_facility_tp() OWNER TO postgres;

--
-- Name: usp_tra_offer_data_day(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_offer_data_day()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_OFFER_DATA_DAY','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''AFP'',''AFPB'',''RESMO'',''RESRUR'',''RESRDR'',''RESMAXON'',''RESMINON'',''RESMINOFF'',''AUTOGENSTATUS'')
	AND sch.datamart_fetched = false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD";
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_OFFER_DATA_DAY','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("AFP") AS "V1"
	, Sum("AFPB") AS "V2"
	, Sum("RESMO") AS "V3"
	, Sum("RESRUR") AS "V4"
	, Sum("RESRDR") AS "V5"
	, Sum("RESMAXON") AS "V6"
	, Sum("RESMINON") AS "V7"
	, Sum("RESMINOFF") AS "V8"
	, Sum("AUTOGENSTATUS") AS "V9"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'AFP' UNION ALL
		   SELECT 'AFPB' UNION ALL
		   SELECT 'RESMO' UNION ALL
		   SELECT 'RESRUR' UNION ALL
		   SELECT 'RESRDR' UNION ALL
		   SELECT 'RESMAXON' UNION ALL
		   SELECT 'RESMINON' UNION ALL
		   SELECT 'RESMINOFF' UNION ALL
		   SELECT 'AUTOGENSTATUS'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "AFP" numeric(15,5), "AFPB" numeric (15,5), "RESMO" numeric(15,5), "RESRUR" numeric (15,5)
	 , "RESRDR" numeric (15,5),"RESMAXON" numeric (15,5), "RESMINON" numeric(15,5), "RESMINOFF" numeric (15,5)
	, "AUTOGENSTATUS" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_OFFER_DATA_DAY"
		INSERT INTO datamart."TRA_OFFER_DATA_DAY" ("ACTUAL_FUEL_PRICE","ACTUAL_FUEL_PRICE_BACKUP"
							, "MINIMUM_OUTPUT","RAMP_UP_RATE","RAMP_DOWN_RATE"
							, "MAX_ON_TIME","MIN_ON_TIME","MIN_OFF_TIME","AUTOGENERATION_STATUS" 
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4"
		, "V5","V6","V7","V8","V9",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_DAY" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_OFFER_DATA_DAY','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;	
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;
		
		UPDATE datamart."TRA_OFFER_DATA_DAY"
		SET "ID_FUEL_TYPE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_OFFER_DATA_DAY"."schedule_id"=tmp."schedule_id" AND datamart."TRA_OFFER_DATA_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OFT';

		UPDATE datamart."TRA_OFFER_DATA_DAY"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_OFFER_DATA_DAY"."schedule_id"=tmp."schedule_id" AND datamart."TRA_OFFER_DATA_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
		
		UPDATE datamart."TRA_OFFER_DATA_DAY"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_OFFER_DATA_DAY"."schedule_id"=tmp."schedule_id" AND datamart."TRA_OFFER_DATA_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';
		
		UPDATE datamart."TRA_OFFER_DATA_DAY"
		SET "ACTUAL_FUEL_TYPE" = "ID_FUEL_TYPE"
		WHERE "ID_FUEL_TYPE" IS NOT NULL AND "ACTUAL_FUEL_PRICE" IS NOT NULL;
		
		UPDATE datamart."TRA_OFFER_DATA_DAY"
		SET "ACTUAL_FUEL_BACKUP_TYPE" = "ID_FUEL_TYPE"
		WHERE "ID_FUEL_TYPE" IS NOT NULL AND "ACTUAL_FUEL_PRICE_BACKUP" IS NOT NULL;
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_offer_data_day() OWNER TO postgres;

--
-- Name: usp_tra_offer_data_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_offer_data_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE sqlstr text;
DECLARE sqlstep text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_OFFER_DATA_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS public."xtabtempdata";
	DROP TABLE IF EXISTS public."tmpStandingData";
	DROP TABLE IF EXISTS public."tempData";
	DROP TABLE IF EXISTS "tmpStepNumber";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "StepNumber" numeric(5,0)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstep='SELECT  sch.schedule_id, en.primary_role_code
	, (sch.start_Date + time ''04:00'') as MarketDate, svn.value 
	FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''ENERO'' OR st.code=''RAO'') 
	AND vt.code in (''STEPNUMBER'',''RAOSTEP'')
	AND en.primary_role_code=''MARKETPARTY''
	AND sch.datamart_fetched=false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	--Create temp table with ENERO Step Number 
	CREATE TEMP TABLE "tmpStepNumber" AS 
	SELECT schedule_id, primary_role_code,"MarketDate", "Value"
	FROM dblink(connstr,sqlstep)
	AS t2 (schedule_id integer,primary_role_code varchar(80),"MarketDate" Date, "Value" numeric(15,5));
	
	--Main Extract FROM BATCHINPUT
	
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'')  as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''UTS'',''ENERO'',''NLCOSTFE'',''NLCOSTNF'',''ROA'',''RNS'',''RAO'')
	AND sch.is_actual_version=1
	AND sch.datamart_fetched=false
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value", "StepNumber")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	, t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value", sn."Value" AS "StepNumber"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on t1."schedule_id"= tgp.schedule_id
	LEFT JOIN "tmpStepNumber" sn on t1.schedule_id=sn.schedule_id  /*and t1.marketdate=sn."MarketDate";*/
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_OFFER_DATA_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code", "StepNumber"
	, Sum("UTAS") AS "V1"
	, Sum("UTF") AS "V2"
	, Sum("PPCF") AS "V3"
	, Sum("TCCF") AS "V4"
	, Sum("ENEROQTY") AS "V5"
	, Sum("ENEROPRICE") AS "V6"
	, Sum("ENERONFPRICE") AS "V7"
	, Sum("ENEROMRUN") AS "V8"
	, Sum("NLCOSTFE") AS "V9"
	, Sum("NLCOSTNF") AS "V10"
	, Sum("ROA") AS "V11"
	, Sum("RNS") AS "V12"
	, Sum("REVISED_RNS") AS "V13"
	, Sum("RAOQTY") AS "V14"
	, Sum("RAOPRICE") AS "V15"	
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","StepNumber","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","StepNumber","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'UTAS' UNION ALL
		   SELECT 'UTF' UNION ALL
		   SELECT 'PPCF' UNION ALL
		   SELECT 'TCCF' UNION ALL
		   SELECT 'ENEROQTY' UNION ALL
		   SELECT 'ENEROPRICE' UNION ALL
		   SELECT 'ENERONFPRICE' UNION ALL
		   SELECT 'ENEROMRUN' UNION ALL	   
		   SELECT 'NLCOSTFE' UNION ALL
		   SELECT 'NLCOSTNF' UNION ALL
		   SELECT 'ROA' UNION ALL
		   SELECT 'RNS' UNION ALL
		   SELECT 'REVISED_RNS' UNION ALL
		   SELECT 'RAOQTY' UNION ALL
		   SELECT 'RAOPRICE'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50), "StepNumber" numeric (5,0)
	 , "UTAS" numeric(15,5), "UTF" numeric (15,5), "PPCF" numeric(15,5), "TCCF" numeric (15,5)
	 , "ENEROQTY" numeric (15,5),"ENEROPRICE" numeric (15,5), "ENERONFPRICE" numeric(15,5), "ENEROMRUN" numeric (15,5)
	 , "NLCOSTFE" numeric (15,5), "NLCOSTNF" numeric (15,5), "ROA" numeric(15,5), "RNS" numeric (15,5),"REVISED_RNS" numeric (15,5)
	 , "RAOQTY" numeric (15,5),"RAOPRICE" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code", "StepNumber"
	ORDER BY schedule_id, interval;
	
	CREATE INDEX "xtabTempData_IX_PrimaryRoleCode" ON "xtabTempData" ("Primary_Role_Code" ASC, "StepNumber" ASC );
	CREATE INDEX "xtabTempData_IX_StepNumber" ON "xtabTempData" ("StepNumber" ASC, "Primary_Role_Code" ASC);
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."TRA_OFFER_DATA_TP" ("QUANTITY_1","PRICE_1", "NON_FUEL_PRICE_1","MUST_RUN"
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V5","V6","V7","V8"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL AND tmp."Primary_Role_Code"='MARKETPARTY'
		AND tmp."StepNumber" = 1;
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_OFFER_DATA_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;		
		
		INSERT INTO datamart."TRA_OFFER_DATA_TP" ("QUANTITY_2","PRICE_2", "NON_FUEL_PRICE_2"
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V5","V6","V7"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL AND tmp."Primary_Role_Code"='MARKETPARTY'
		AND tmp."StepNumber" = 2;
		
		INSERT INTO datamart."TRA_OFFER_DATA_TP" ("QUANTITY_3","PRICE_3", "NON_FUEL_PRICE_3"
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V5","V6","V7"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL AND tmp."Primary_Role_Code"='MARKETPARTY'
		AND tmp."StepNumber" = 3;

		INSERT INTO datamart."TRA_OFFER_DATA_TP" ("QUANTITY_4","PRICE_4", "NON_FUEL_PRICE_4"
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V5","V6","V7"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL AND tmp."Primary_Role_Code"='MARKETPARTY'
		AND tmp."StepNumber" = 4;

		INSERT INTO datamart."TRA_OFFER_DATA_TP" ("QUANTITY_5","PRICE_5", "NON_FUEL_PRICE_5"
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V5","V6","V7"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL AND tmp."Primary_Role_Code"='MARKETPARTY'
		AND tmp."StepNumber" = 5;
		
		INSERT INTO datamart."TRA_OFFER_DATA_TP" ("QUANTITY_6","PRICE_6", "NON_FUEL_PRICE_6"
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V5","V6","V7"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL AND tmp."Primary_Role_Code"='MARKETPARTY'
		AND tmp."StepNumber" = 6;
		
		INSERT INTO datamart."TRA_OFFER_DATA_TP" ("QUANTITY_7","PRICE_7", "NON_FUEL_PRICE_7"
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V5","V6","V7"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL AND tmp."Primary_Role_Code"='MARKETPARTY'
		AND tmp."StepNumber" = 7;

		INSERT INTO datamart."TRA_OFFER_DATA_TP" ("UTS","UTF", "PPCF","TCCF","NO_LOAD_COST_FUEL_ECONOMIC"
							, "NO_LOAD_COST_NON_FUEL", "OFFERED_AVAILABILITY","NOMINATED_QUANTITY","REVISED_OFFER_FLAG"
							, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V9","V10","V11","V12","V13"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_OFFER_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL 
		--AND tmp."StepNumber"=1
		AND tmp."Primary_Role_Code"='MARKETPARTY';

		UPDATE datamart."TRA_OFFER_DATA_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_OFFER_DATA_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_OFFER_DATA_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
		
		UPDATE datamart."TRA_OFFER_DATA_TP"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_OFFER_DATA_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_OFFER_DATA_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	DROP TABLE IF EXISTS "tmpStepNumber";
	
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_offer_data_tp() OWNER TO postgres;

--
-- Name: usp_tra_psu_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_psu_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_PSU_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''RAA'' OR st.code=''TCRAA'' OR st.code=''AAAS'' OR st.code=''NLCOSTFA'' OR st.code=''DSNLC'')
	AND sch.datamart_fetched=false
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."TRA_PSU_TP" tgp on t1."schedule_id"= tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_PSU_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("RAA") AS "V1"
	, Sum("TCRAA") AS "V2"
	, Sum("AAAS") AS "V3"
	, Sum("NLCOSTFA") AS "V4"
	, Sum("DSNLC") AS "V5"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'RAA' UNION ALL
		   SELECT 'TCRAA' UNION ALL
		   SELECT 'AAAS' UNION ALL
		   SELECT 'NLCOSTFA' UNION ALL
		   SELECT 'DSNLC'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"RAA" numeric(15,5), "TCRAA" numeric (15,5), "AAAS" numeric (15,5), "NLCOSTFA" numeric (15,5),"DSNLC" numeric (15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."TRA_PSU_TP" ("ACTUAL_AVAILABILITY","TRANSCO_ACTUAL_AVAILABILITY"
										   ,"AMBIENT_AIR_SCHEDULE","NO_LOAD_COST_FUEL_ACTUAL","DISPATCH_SCHEDULE_NO_LOAD_COST"
										   ,"DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5", tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_PSU_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_PSU_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."TRA_PSU_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_PSU_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_PSU_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';

		UPDATE datamart."TRA_PSU_TP"
		SET "ID_PSU"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_PSU_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_PSU_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPSU';
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_psu_tp() OWNER TO postgres;

--
-- Name: usp_tra_results_bid_zone_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_results_bid_zone_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_RESULTS_BID_ZONE_TP','sr_schedule_trak');
	
	DROP TABLE  IF EXISTS public."xtabtempdata";
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData"; 
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );	

	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY en on sp.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	JOIN SD_ENTITY_DEF ed on en.entity_id=ed.entity_id
	WHERE (st.code in (''MSEAMR_ZSMP'',''MSPEAMR_APP'',''MSEAMR_HPRC'',''MSPEAMR_SP'',''MSPEAMR_EASF'',
						''MSPEAMR_MAR'',''MSPEAMR_SAC'',''MSPEAMR_SCR'',''MSEAMR_ZGEN'',
						''MSEPIMR_ZSMP'',''MSPEPIMR_APP'',''MSEPIMR_HPRC'',''MSPEPIMR_SP'',''MSPEPIMR_EPSF'',
						''MSPEPIMR_MAR'',''MSPEPIMR_SAC'',''MSPEPIMR_SCR'',''MSEPIMR_PD'',''MSEPIMR_ZGEN'',
						''MSEPCMR_ZSMP'',''MSPEPCMR_APP'',''MSEPCMR_HPRC'',''MSPEPCMR_SP'',''MSPEPCMR_EPSF'',
						''MSPEPCMR_MAR'',''MSPEPCMR_SAC'',''MSPEPCMR_SCR'',''MSEPIMR_PD'',''MSEPCMR_ZGEN'',  ''MSEPCMR_PD'' )
	AND sch.is_actual_version=1
	AND sch.deletion_time is null
	AND sch.datamart_fetched = false
	AND sch.draft=0
	AND ed.draft=0)';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)	
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	INNER JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	INNER JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."TRA_RESULTS_BID_ZONE_TP" tgp on t1."schedule_id"= tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	 
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_RESULTS_BID_ZONE_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	 
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
, Sum("MSEAMR_ZSMP") AS "V2"
, Sum("MSPEAMR_APP") AS "V3"
--, Sum("MSEAMR_HPRC") AS "V4"
, Sum("MSPEAMR_SP") AS "V5"
, Sum("MSPEAMR_EASF") AS "V6"
, Sum("MSPEAMR_MAR") AS "V7"
, Sum("MSPEAMR_SAC") AS "V8"
, Sum("MSPEAMR_SCR") AS "V9"
, Sum("MSEAMR_ZGEN") AS "V12"
, Sum("MSEPIMR_ZSMP") AS "V14"
, Sum("MSPEPIMR_APP") AS "V15"
, Sum("MSEPIMR_HPRC") AS "V16"
, Sum("MSPEPIMR_SP") AS "V17"
, Sum("MSPEPIMR_EPSF") AS "V18"
, Sum("MSPEPIMR_MAR") AS "V19"
, Sum("MSPEPIMR_SAC") AS "V20"
, Sum("MSPEPIMR_SCR") AS "V21"
, Sum("MSEPIMR_PD") AS "V22"
, Sum("MSEPIMR_ZGEN") AS "V24"
, Sum("MSEPCMR_ZSMP") AS "V26"
, Sum("MSPEPCMR_APP") AS "V27"
, Sum("MSEPCMR_HPRC") AS "V28"
, Sum("MSPEPCMR_SP") AS "V29"
, Sum("MSPEPCMR_EPSF") AS "V30"
, Sum("MSPEPCMR_MAR") AS "V31"
, Sum("MSPEPCMR_SAC") AS "V32"
, Sum("MSPEPCMR_SCR") AS "V33"
, Sum("MSEPCMR_PD") AS "V34"  
, Sum("MSEPCMR_ZGEN") AS "V36"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ 	SELECT 'MSEAMR_ZSMP' UNION ALL
			SELECT 'MSPEAMR_APP' UNION ALL
			SELECT 'MSEAMR_HPRC' UNION ALL
			SELECT 'MSPEAMR_SP' UNION ALL
			SELECT 'MSPEAMR_EASF' UNION ALL
			SELECT 'MSPEAMR_MAR' UNION ALL
			SELECT 'MSPEAMR_SAC' UNION ALL
			SELECT 'MSPEAMR_SCR' UNION ALL
			SELECT 'MSEAMR_ZGEN' UNION ALL
			SELECT 'MSEPIMR_ZSMP' UNION ALL
			SELECT 'MSPEPIMR_APP' UNION ALL
			SELECT 'MSEPIMR_HPRC' UNION ALL
			SELECT 'MSPEPIMR_SP' UNION ALL
			SELECT 'MSPEPIMR_EPSF' UNION ALL
			SELECT 'MSPEPIMR_MAR' UNION ALL
			SELECT 'MSPEPIMR_SAC' UNION ALL
			SELECT 'MSPEPIMR_SCR' UNION ALL
			SELECT 'MSEPIMR_PD' UNION ALL
			SELECT 'MSEPIMR_ZGEN' UNION ALL
			SELECT 'MSEPCMR_ZSMP' UNION ALL
			SELECT 'MSPEPCMR_APP' UNION ALL
			SELECT 'MSEPCMR_HPRC' UNION ALL
			SELECT 'MSPEPCMR_SP' UNION ALL
			SELECT 'MSPEPCMR_EPSF' UNION ALL
			SELECT 'MSPEPCMR_MAR' UNION ALL
			SELECT 'MSPEPCMR_SAC' UNION ALL
			SELECT 'MSPEPCMR_SCR' UNION ALL
			SELECT 'MSEPCMR_PD' UNION ALL
			SELECT 'MSEPCMR_ZGEN'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
		,"MSEAMR_ZSMP"  numeric(15,5), "MSPEAMR_APP"  numeric(15,5), "MSEAMR_HPRC"  numeric(15,5), "MSPEAMR_SP"  numeric(15,5), "MSPEAMR_EASF"  numeric(15,5)
		,"MSPEAMR_MAR"  numeric(15,5), "MSPEAMR_SAC"  numeric(15,5), "MSPEAMR_SCR"  numeric(15,5), "MSEAMR_ZGEN"  numeric(15,5)
		,"MSEPIMR_ZSMP"  numeric(15,5), "MSPEPIMR_APP"  numeric(15,5), "MSEPIMR_HPRC"  numeric(15,5), "MSPEPIMR_SP"  numeric(15,5), "MSPEPIMR_EPSF"  numeric(15,5)
		,"MSPEPIMR_MAR"  numeric(15,5), "MSPEPIMR_SAC"  numeric(15,5), "MSPEPIMR_SCR"  numeric(15,5), "MSEPIMR_PD"  numeric(15,5),  "MSEPIMR_ZGEN"  numeric(15,5)
		,"MSEPCMR_ZSMP"  numeric(15,5), "MSPEPCMR_APP"  numeric(15,5), "MSEPCMR_HPRC"  numeric(15,5), "MSPEPCMR_SP"  numeric(15,5), "MSPEPCMR_EPSF"  numeric(15,5)
		,"MSPEPCMR_MAR"  numeric(15,5), "MSPEPCMR_SAC"  numeric(15,5), "MSPEPCMR_SCR"  numeric(15,5), "MSEPCMR_PD"  numeric(15,5),  "MSEPCMR_ZGEN"  numeric(15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_RESULTS_BID_ZONE_TP"
		INSERT INTO datamart."TRA_RESULTS_BID_ZONE_TP" ("EXANTE_SYSTEM_MARGINAL_PRICE",
														"EXANTE_AGGREGATE_POOL_PRICE",
												--		"EXANTE_HIGHEST_OFFER_PRICE",
														"EXANTE_SCARCITY_PRICE",
														"EXANTE_SCARCITY_FACTOR",
"EXANTE_MARGIN","EXANTE_SYSTEM_AVAILABILITY_CAPACITY","EXANTE_SYSTEM_CAPACITY_REQUIREMENT",
--"EXANTE_ZONE_GENERATION",
"EXPOST_INDICATIVE_SYSTEM_MARGINAL_PRICE","EXPOST_INDICATIVE_AGGREGATE_POOL_PRICE",
--"EXPOST_INDICATIVE_HIGHEST_OFFER_PRICE",
"EXPOST_INDICATIVE_SCARCITY_PRICE","EXPOST_INDICATIVE_SCARCITY_FACTOR",
"EXPOST_INDICATIVE_MARGIN","EXPOST_INDICATIVE_SYSTEM_AVAILABILITY_CAPACITY","EXPOST_INDICATIVE_SYSTEM_CAPACITY_REQUIREMENT",
"EXPOST_INDICATIVE_POOL_DEMAND", --"EXPOST_INDICATIVE_ZONE_GENERATION",
														"EXPOST_CONFIRMED_SYSTEM_MARGINAL_PRICE",
"EXPOST_CONFIRMED_AGGREGATE_POOL_PRICE",--"EXPOST_CONFIRMED_HIGHEST_OFFER_PRICE",
														"EXPOST_CONFIRMED_SCARCITY_PRICE",
"EXPOST_CONFIRMED_SCARCITY_FACTOR","EXPOST_CONFIRMED_MARGIN",
"EXPOST_CONFIRMED_SYSTEM_AVAILABILITY_CAPACITY","EXPOST_CONFIRMED_SYSTEM_CAPACITY_REQUIREMENT",
"EXPOST_CONFIRMED_POOL_DEMAND"--,"EXPOST_CONFIRMED_ZONE_GENERATION"	
		 , "DATE_KEY_OMAN","TRADING_PERIOD", "ID_BID_ZONE","schedule_id")
		SELECT "V2","V3",--"V4",
		"V5","V6","V7","V8","V9"
			--  ,"V12"
			,"V14","V15",--"V16",
			"V17","V18"
			  , "V19","V20","V21","V22",--"V24",
			  "V26","V27"
			 -- , "V28"
			 ,"V29","V30","V31","V32","V33","V34"--,"V36"
		, tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_RESULTS_BID_ZONE_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='BZONE';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	 
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_RESULTS_BID_ZONE_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		 
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;		

		/*
		UPDATE datamart."TRA_RESULTS_BID_ZONE_TP"
		SET "ID_BID_ZONE"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_RESULTS_BID_ZONE_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_RESULTS_BID_ZONE_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='BZONE';
		*/
	UPDATE public.sr_schedule_trak trk
	SET datamart_fetched = true
	FROM "xtabTempData" tmp
	WHERE trk.schedule_id = tmp.schedule_id;
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	 
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_results_bid_zone_tp() OWNER TO postgres;

--
-- Name: usp_tra_transition_data_day(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_transition_data_day()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_TRANSITION_DATA_DAY','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE st.code IN (''TRCOSTFE'',''TRCOSTNF'', ''TDE'')
	AND sch.datamart_fetched = false
	AND sch.deletion_time is null
	AND sch.is_actual_version=1
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	INNER JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	INNER JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."TRA_TRANSITION_DATA_DAY" tgp on t1.schedule_id=tgp.schedule_id
	WHERE tgp.schedule_id IS NULL 
	--AND t1."Value"<>0
	;
	
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_TRANSITION_DATA_DAY','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("TRCOSTFE") AS "V1"
	, Sum("TRCOSTNF") AS "V2"
	, Sum("TDECOMBO") AS "V3"
	, Sum("TDECOMBD") AS "V4"
	, Sum("TDERMPD1") AS "V5"
	, Sum("TDEMIDBO") AS "V6"
	, Sum("TDEMIDBD") AS "V7"
	, Sum("TDERMPD2") AS "V8"
	, Sum("TDECMPBO") AS "V9"
	, Sum("TDECMPBD") AS "V10"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'TRCOSTFE' UNION ALL
		   SELECT 'TRCOSTNF' UNION ALL			  
		   SELECT 'TDECOMBO' UNION ALL
		   SELECT 'TDECOMBD' UNION ALL
		   SELECT 'TDERMPD1' UNION ALL
		   SELECT 'TDEMIDBO' UNION ALL
		   SELECT 'TDEMIDBD' UNION ALL
		   SELECT 'TDERMPD2' UNION ALL
		   SELECT 'TDECMPBO' UNION ALL
		   SELECT 'TDECMPBD' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"TRCOSTFE" numeric(15,5), "TRCOSTNF" numeric (15,5)
	 , "TDECOMBO" numeric (15,5), "TDECOMBD" numeric (15,5), "TDERMPD1" numeric (15,5)
	 , "TDEMIDBO" numeric (15,5), "TDEMIDBD" numeric (15,5), "TDERMPD2" numeric (15,5)
	 , "TDECMPBO" numeric (15,5), "TDECMPBD" numeric (15,5)
	)
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."TRA_TRANSITION_DATA_DAY" 
			("TRANSITION_COST_FUEL_ECONOMIC","TRANSITION_COST_NON_FUEL"
			, "TDE_COMMENCE_BANKING_OUTPUT", "TDE_COMMENCE_BANKING_DURATION","TDE_RAMP_ONE_DURATION"
			, "TDE_MIDDLE_BANKING_OUTPUT", "TDE_MIDDLE_BANKING_DURATION","TDE_RAMP_TWO_DURATION"
			, "TDE_COMPLETE_BANKING_OUTPUT", "TDE_COMPLETE_BANKING_DURATION"
			, "DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1","V2","V3","V4","V5","V6","V7","V8","V9","V10"
		,tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_TRANSITION_DATA_DAY" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_TRANSITION_DATA_DAY','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."TRA_TRANSITION_DATA_DAY"
		SET "ID_TRANSITION"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_TRANSITION_DATA_DAY"."schedule_id"=tmp."schedule_id" AND datamart."TRA_TRANSITION_DATA_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OTRANS';
		
		UPDATE datamart."TRA_TRANSITION_DATA_DAY"
		SET "ID_PRODUCTION_BLOCK"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_TRANSITION_DATA_DAY"."schedule_id"=tmp."schedule_id" AND datamart."TRA_TRANSITION_DATA_DAY"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OPRODBLOCK';
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_transition_data_day() OWNER TO postgres;

--
-- Name: usp_tra_transition_data_dst_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_transition_data_dst_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_TRANSITION_DATA_DST_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_boolean svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN SD_ENTITY_DEF ed on sp.entity_id=ed.entity_id
	JOIN SD_ENTITY en on ed.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	WHERE (st.code=''DST'')
	AND sch.is_actual_version=1
	AND sch.datamart_fetched = false
	AND sch.deletion_time is null
	AND sch.draft=0
	AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."TRA_TRANSITION_DATA_DST_TP" tgp on t1.schedule_id=tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_TRANSITION_DATA_DST_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, MAX("DST") AS "V1"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, MAX("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'DST'$$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 , "DST" numeric(15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."TRA_TRANSITION_DATA_DST_TP" ("DISPATCH_SCHEDULE_TRANSITION_INTEGER"
										   ,"DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_TRANSITION_DATA_DST_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_TRANSITION_DATA_DST_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."TRA_TRANSITION_DATA_DST_TP"
		SET "ID_TRANSITION"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_TRANSITION_DATA_DST_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_TRANSITION_DATA_DST_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OTRANS';
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		RAISE NOTICE 'Number of datamart_fetched updated: ( % )', v_rows_count;
		
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_transition_data_dst_tp() OWNER TO postgres;

--
-- Name: usp_tra_transition_data_tp(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_tra_transition_data_tp()
    LANGUAGE plpgsql
    AS $_$
DECLARE  sqlstr text;
DECLARE connstr text;
DECLARE v_rows_count int;

BEGIN
	
	connstr= 'dbname=OPWP_TRAK_BATCHINPUT_HIS';
	-- delete non actual schedules from datamart
	CALL datamart.usp_sync_nonactual_schedule('TRA_TRANSITION_DATA_TP','sr_schedule_trak');
	
	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
	CREATE TABLE "tmpStandingData"
	("SD_ID" int
	, "NAME" varchar(256)
	, "DESCRIPTION" varchar (256)
	, "StandingData_Type" varchar(256)
	);
	
	CREATE INDEX "IX_Name" on "tmpStandingData" ("NAME");
	
	INSERT INTO "tmpStandingData"("SD_ID","NAME","DESCRIPTION","StandingData_Type")
	SELECT "SD_ID","NAME","DESCRIPTION","StandingDataType"
	FROM datamart."vw_Standing_Data";
	
	CREATE TABLE "tempData"
	(schedule_id int
	 , "SD_ID" int
	 , "Entity_Name" varchar(256)
	 , "Entity_Description" varchar(256)
	 , "Primary_Role_Code" varchar(50)
	 , "marketdate" date
	 , "interval" smallint
	 , "ValueTypeCode" varchar(30)
	 , "ScheduleTypeCode" varchar(30)
	 , "Value" numeric (15,5)
	 );
	 
	sqlstr= 'SELECT sch.schedule_id, en.primary_role_code' || 
	', ed.Name AS "Entity_Name", Ed.Description AS "Entity_Description" ' ||
	', (sch.start_Date + time ''04:00'') as MarketDate, svn.start_date, svn.stop_date ' ||
	', EXTRACT(EPOCH FROM svn.stop_date-sch.start_Date)/1800 as Interval '
	', vt.code AS "ValueTypeCode", vt.description, st.code AS "ScheduleTypeCode", svn.value '||
	' FROM sr_schedule sch
	JOIN sr_schedule_type st on sch.schedule_type_id=st.schedule_type_id
	JOIN sr_schedule_value_number svn on sch.schedule_id=svn.schedule_id
	JOIN sr_value_type vt on svn.value_type_id=vt.value_type_id
	JOIN sr_schedule_parameter sp on sch.schedule_id=sp.schedule_id
	JOIN sd_entity en on sp.entity_id=en.entity_id and sp.parameter_type_code=en.primary_role_code
	JOIN SD_ENTITY_DEF ed on en.entity_id=ed.entity_id
	WHERE (st.code=''OTTRANS'')
	  AND sch.datamart_fetched = false
	  AND sch.is_actual_version=1
	  AND sch.deletion_time is null
	  AND sch.draft=0
	  AND ed.draft=0';
	
	INSERT INTO "tempData" ("schedule_id", "SD_ID", "Entity_Name","Entity_Description", "Primary_Role_Code"
							, "marketdate","interval","ValueTypeCode","ScheduleTypeCode","Value")
	SELECT t1."schedule_id", tmp."SD_ID", t1."entity_name", t1."entity_description",t1."primary_role_code", t1."marketdate",t1."interval"
	,t1."ValueTypeCode", t1."ScheduleTypeCode", t1."Value"
 	FROM dblink(connstr,sqlstr)
	AS t1 (schedule_id integer, primary_role_code varchar(50), entity_name varchar(256), entity_description varchar(256)
		   , marketdate date, start_date timestamp, stop_date timestamp, interval smallint, "ValueTypeCode" varchar(30)
		   , "ValueTypeDescription" varchar(100), "ScheduleTypeCode" varchar(30), "Value" numeric(15,5))
	JOIN "tmpStandingData" tmp on t1."entity_name" = tmp."NAME" and t1.primary_role_code=tmp."StandingData_Type"
	JOIN datamart."TRADING_PERIOD" tp on t1.marketdate = tp."DATE_KEY_OMAN" and t1.interval=tp."TRADING_PERIOD"
	LEFT JOIN datamart."TRA_TRANSITION_DATA_TP" tgp on t1."schedule_id"= tgp.schedule_id
	WHERE tgp.schedule_id IS NULL;
	
	GET DIAGNOSTICS v_rows_count=ROW_COUNT;
	INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
	SELECT 'TRA_TRANSITION_DATA_TP','Extracted FROM HIS',CURRENT_TIMESTAMP(0),v_rows_count;	
	
	RAISE NOTICE 'Number of NEW records extracted from HIS: ( % )', v_rows_count;

	--crostab table tempData into xtabTempData
	CREATE TEMP TABLE "xtabTempData" AS
	SELECT schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	, Sum("OTTRANS") AS "V1"
	FROM CROSSTAB($$ SELECT row_number() OVER (ORDER BY schedule_id ASC,interval ASC) AS "Nr_Crt",schedule_id, marketdate,"interval"
			, "SD_ID","Primary_Role_Code","ValueTypeCode"
			, SUM("Value") AS "Valoare"
			FROM "tempData"
		GROUP BY schedule_id,"marketdate","interval","SD_ID","Primary_Role_Code","ValueTypeCode"
		ORDER BY schedule_id,"interval","Primary_Role_Code"$$,
		$$ SELECT 'OTTRANS' $$)
	AS ptd
	("Nr_Crt" int,schedule_id int, "marketdate" Date, "interval" int,"SD_ID" int, "Primary_Role_Code" varchar(50)
	 ,"OTTRANS" numeric(15,5))
	GROUP BY schedule_id, marketdate, "interval","SD_ID","Primary_Role_Code"
	ORDER BY schedule_id, interval;
	
--	CREATE INDEX "IX_ScheduleId" on "xtabTempData" ("schedule_id");
	
		-- Insert/update Datamart Table datamart."TRA_GENSET_TP"
		INSERT INTO datamart."TRA_TRANSITION_DATA_TP" ("OUTTURN_TRANSITIONS"
										   ,"DATE_KEY_OMAN","TRADING_PERIOD","ID_MARKET_PARTY","schedule_id")
		SELECT "V1",tmp."marketdate",tmp."interval",tmp."SD_ID",tmp."schedule_id"
		FROM "xtabTempData" tmp
		LEFT JOIN datamart."TRA_TRANSITION_DATA_TP" tgp on tmp."schedule_id"=tgp."schedule_id" and tmp."interval"=tgp."TRADING_PERIOD"
		WHERE tgp.schedule_id IS NULL
		AND tmp."Primary_Role_Code"='MARKETPARTY';
		
		GET DIAGNOSTICS v_rows_count=ROW_COUNT;
		INSERT INTO datamart."Extract_LOG" ("TableName","Measure","ExtractionDate","NumberOfRecords")
		SELECT 'TRA_TRANSITION_DATA_TP','Inserted into Datamart',CURRENT_TIMESTAMP(0),v_rows_count;
		
		RAISE NOTICE 'Number of new records inserted: ( % )', v_rows_count;

		UPDATE datamart."TRA_TRANSITION_DATA_TP"
		SET "ID_TRANSITION"=tmp."SD_ID"
		FROM "xtabTempData" tmp
		WHERE datamart."TRA_TRANSITION_DATA_TP"."schedule_id"=tmp."schedule_id" AND datamart."TRA_TRANSITION_DATA_TP"."TRADING_PERIOD"=tmp."interval"
		AND tmp."Primary_Role_Code"='OTRANS';
		
		
		UPDATE public.sr_schedule_trak trk
		SET datamart_fetched = true
		FROM "xtabTempData" tmp
		WHERE trk.schedule_id = tmp.schedule_id;

	DROP TABLE IF EXISTS "tmpStandingData";
	DROP TABLE IF EXISTS "tempData";
	DROP TABLE IF EXISTS "xtabTempData";
	
END;
$_$;


ALTER PROCEDURE datamart.usp_tra_transition_data_tp() OWNER TO postgres;

--
-- Name: usp_updateholidays(); Type: PROCEDURE; Schema: datamart; Owner: postgres
--

CREATE PROCEDURE datamart.usp_updateholidays()
    LANGUAGE plpgsql
    AS $$
DECLARE 
    date_key_oman_min date;
	date_key_oman_max date; 
BEGIN
 
 CREATE TEMP TABLE tmp_dates(date_key_oman date);
 
 INSERT INTO tmp_dates("date_key_oman")
 SELECT "DATE_KEY_OMAN"  
   FROM datamart."TRADING_PERIOD" p JOIN  datamart."Month" m ON p."ID_Month" = m."ID_Month"
  WHERE "YEAR" || '-' || substring(UPPER(trim("Name")),1,3) || '-' || LPAD("DAY_OF_MONTH"::text,2,'0')
     IN ( SELECT  unnest(array_agg(date_match))  
  FROM ( 
   SELECT  unnest(datamart.convert_cron_expression(cronexpression))  as date_match
     FROM public.croncalendarrule_trak 
  ) as t WHERE date_match IS NOT null);
  
  UPDATE datamart."TRADING_PERIOD"
     SET "HOLIDAY_OMAN" = 'f'
   WHERE  "DATE_KEY_OMAN" >= (SELECT MIN("date_key_oman") FROM tmp_dates);
   
   UPDATE datamart."TRADING_PERIOD"
      SET "HOLIDAY_OMAN" = 't'
	WHERE "DATE_KEY_OMAN" IN (SELECT "date_key_oman" FROM tmp_dates);
										 
	DROP TABLE tmp_dates;								 
    
end;
$$;


ALTER PROCEDURE datamart.usp_updateholidays() OWNER TO postgres;

--
-- Name: OPWP_SCHD_BATCHINPUT_HIS; Type: SERVER; Schema: -; Owner: postgres
--

CREATE SERVER "OPWP_SCHD_BATCHINPUT_HIS" FOREIGN DATA WRAPPER dblink_fdw;


ALTER SERVER "OPWP_SCHD_BATCHINPUT_HIS" OWNER TO postgres;

--
-- Name: USER MAPPING postgres SERVER OPWP_SCHD_BATCHINPUT_HIS; Type: USER MAPPING; Schema: -; Owner: postgres
--

CREATE USER MAPPING FOR postgres SERVER "OPWP_SCHD_BATCHINPUT_HIS";


--
-- Name: opwp_trak_nextgen; Type: SERVER; Schema: -; Owner: postgres
--

CREATE SERVER opwp_trak_nextgen FOREIGN DATA WRAPPER postgres_fdw OPTIONS (
    dbname 'OPWP_TRAK_NEXTGEN'
);


ALTER SERVER opwp_trak_nextgen OWNER TO postgres;

--
-- Name: USER MAPPING postgres SERVER opwp_trak_nextgen; Type: USER MAPPING; Schema: -; Owner: postgres
--

CREATE USER MAPPING FOR postgres SERVER opwp_trak_nextgen;


--
-- Name: schd_batchinput_his; Type: SERVER; Schema: -; Owner: postgres
--

CREATE SERVER schd_batchinput_his FOREIGN DATA WRAPPER postgres_fdw OPTIONS (
    dbname 'OPWP_SCHD_BATCHINPUT_HIS'
);


ALTER SERVER schd_batchinput_his OWNER TO postgres;

--
-- Name: USER MAPPING postgres SERVER schd_batchinput_his; Type: USER MAPPING; Schema: -; Owner: postgres
--

CREATE USER MAPPING FOR postgres SERVER schd_batchinput_his;


--
-- Name: trak_batchinput_his; Type: SERVER; Schema: -; Owner: postgres
--

CREATE SERVER trak_batchinput_his FOREIGN DATA WRAPPER postgres_fdw OPTIONS (
    dbname 'OPWP_TRAK_BATCHINPUT_HIS'
);


ALTER SERVER trak_batchinput_his OWNER TO postgres;

--
-- Name: USER MAPPING postgres SERVER trak_batchinput_his; Type: USER MAPPING; Schema: -; Owner: postgres
--

CREATE USER MAPPING FOR postgres SERVER trak_batchinput_his;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: Extract_LOG; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."Extract_LOG" (
    "ExtractLogId" integer NOT NULL,
    "TableName" character varying(200),
    "Measure" character varying(128),
    "ExtractionDate" timestamp with time zone,
    "NumberOfRecords" integer
);


ALTER TABLE datamart."Extract_LOG" OWNER TO postgres;

--
-- Name: DailyExtractLOG; Type: VIEW; Schema: datamart; Owner: postgres
--

CREATE VIEW datamart."DailyExtractLOG" AS
 WITH cte_extactlog AS (
         SELECT "Extract_LOG"."TableName",
            "Extract_LOG"."ExtractionDate",
                CASE
                    WHEN (("Extract_LOG"."Measure")::text = 'Extracted FROM HIS'::text) THEN "Extract_LOG"."NumberOfRecords"
                    ELSE 0
                END AS "[Extracted FROM HIS]",
                CASE
                    WHEN (("Extract_LOG"."Measure")::text = 'Inserted into Datamart'::text) THEN "Extract_LOG"."NumberOfRecords"
                    ELSE 0
                END AS "[Inserted into Datamart]"
           FROM datamart."Extract_LOG"
        )
 SELECT row_number() OVER (PARTITION BY ((cte_extactlog."ExtractionDate")::date) ORDER BY cte_extactlog."TableName") AS crtno,
    cte_extactlog."TableName",
    cte_extactlog."ExtractionDate",
    sum(cte_extactlog."[Extracted FROM HIS]") AS "[Extracted FROM HIS]",
    sum(cte_extactlog."[Inserted into Datamart]") AS "[Inserted into Datamart]"
   FROM cte_extactlog
  WHERE ((cte_extactlog."ExtractionDate")::date = CURRENT_DATE)
  GROUP BY cte_extactlog."TableName", cte_extactlog."ExtractionDate";


ALTER TABLE datamart."DailyExtractLOG" OWNER TO postgres;

--
-- Name: Extract_LOG_ExtractLogId_seq; Type: SEQUENCE; Schema: datamart; Owner: postgres
--

ALTER TABLE datamart."Extract_LOG" ALTER COLUMN "ExtractLogId" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME datamart."Extract_LOG_ExtractLogId_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: MSH_INPUTS_BIDZONE_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."MSH_INPUTS_BIDZONE_TP" (
    "TRANSCO_DEMAND_SHEDDING" numeric(15,5),
    "TRANSCO_DEMAND_FORECAST" numeric(15,5),
    "MARKET_OPERATOR_DEMAND_FORECAST" numeric(15,5),
    "EXANTE_SPINNING_RESERVE" numeric(15,5),
    "EXPOST_SPINNING_RESERVE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    schedule_id integer
);


ALTER TABLE datamart."MSH_INPUTS_BIDZONE_TP" OWNER TO postgres;

--
-- Name: MSH_INPUTS_FACILITY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."MSH_INPUTS_FACILITY" (
    "WATER_PRODUCTION_REQUIREMENTS" numeric(15,5),
    "WEATHER_FORECAST" numeric(15,5),
    "TRANSCO_MUST_RUN_AUXILIARY_FORECAST" numeric(15,5),
    "MUST_RUN_AUXILIARY_FORECAST" numeric(15,5),
    "ECONOMIC_FUEL_PRICE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_FACILITY" integer,
    "ID_FUEL_TYPE" integer,
    schedule_id integer
);


ALTER TABLE datamart."MSH_INPUTS_FACILITY" OWNER TO postgres;

--
-- Name: MSH_INPUTS_PSU; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."MSH_INPUTS_PSU" (
    "EXANTE_RESERVE_HOLDING_LIMIT" numeric(15,5),
    "EXANTE_RESERVE_HOLDING_QUANTITY" numeric(15,5),
    "EXPOST_RESERVE_HOLDING_LIMIT" numeric(15,5),
    "EXPOST_RESERVE_HOLDING_QUANTITY" numeric(15,5),
    "CURTAILMENT_DATA" numeric(15,5),
    "INDICATIVE_METER_QUANTITIES" numeric(15,5),
    "CONFIRMED_METER_QUANTITIES" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PSU" integer,
    schedule_id integer
);


ALTER TABLE datamart."MSH_INPUTS_PSU" OWNER TO postgres;

--
-- Name: MSH_RESULTS_BID_ZONE_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."MSH_RESULTS_BID_ZONE_TP" (
    "EXANTE_ZONE_GENERATION" numeric(15,5),
    "EXPOST_INDICATIVE_HIGHEST_OFFER_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_ZONE_GENERATION" numeric(15,5),
    "EXPOST_CONFIRMED_ZONE_GENERATION" numeric(15,5),
    "EXANTE_HIGHEST_OFFER_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_HIGHEST_OFFER_PRICE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint NOT NULL,
    "ID_BID_ZONE" integer,
    schedule_id integer NOT NULL,
    "EXANTE_ZONE_GENERATION_DEFICIT" numeric(15,5),
    "EXANTE_ZONE_GENERATION_SURPLUS" numeric(15,5),
    "EXANTE_ZONE_GENERATION_DEMAND" numeric(15,5),
    "EXPOST_INDICATIVE_ZONE_GENERATION_DEFICIT" numeric(15,5),
    "EXPOST_INDICATIVE_ZONE_GENERATION_SURPLUS" numeric(15,5),
    "EXPOST_INDICATIVE_ZONE_GENERATION_DEMAND" numeric(15,5),
    "EXPOST_CONFIRMED_ZONE_GENERATION_DEFICIT" numeric(15,5),
    "EXPOST_CONFIRMED_ZONE_GENERATION_SURPLUS" numeric(15,5),
    "EXPOST_CONFIRMED_ZONE_GENERATION_DEMAND" numeric(15,5)
);


ALTER TABLE datamart."MSH_RESULTS_BID_ZONE_TP" OWNER TO postgres;

--
-- Name: MSH_RESULTS_BLOCK_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."MSH_RESULTS_BLOCK_TP" (
    "EXANTE_TRANSITION_COST" numeric(15,5),
    "EXANTE_PRODUCTION_COST" numeric(15,5),
    "EXPOST_INDICATIVE_TRANSITION_COST" numeric(15,5),
    "EXPOST_INDICATIVE_PRODUCTION_COST" numeric(15,5),
    "EXPOST_CONFIRMED_TRANSITION_COST" numeric(15,5),
    "EXPOST_CONFIRMED_PRODUCTION_COST" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_BLOCK" integer,
    schedule_id integer
);


ALTER TABLE datamart."MSH_RESULTS_BLOCK_TP" OWNER TO postgres;

--
-- Name: MSH_RESULTS_PSU_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."MSH_RESULTS_PSU_TP" (
    "EXANTE_CALCULATED_MARKET_SCHEDULE" numeric(15,5),
    "EXANTE_MARKET_SCHEDULE" numeric(15,5),
    "EXANTE_NO_LOAD_COST" numeric(15,5),
    "EXANTE_HOURS_UP" numeric(15,5),
    "EXANTE_INITIAL_GENERATION" numeric(15,5),
    "EXANTE_LIMIT_SLACK_MAX" numeric(15,5),
    "EXANTE_LIMIT_SLACK_MIN" numeric(15,5),
    "EXANTE_LIMIT_SLACK_RAMPRATE" numeric(15,5),
    "EXANTE_COST" numeric(15,5),
    "EXANTE_MARKET_COMMITTED" numeric(15,5),
    "EXANTE_TRANSITION" numeric(15,5),
    "EXPOST_INDICATIVE_MARKET_SCHEDULE" numeric(15,5),
    "EXPOST_CALCULATED_MARKET_SCHEDULE" numeric(15,5),
    "EXPOST_INDICATIVE_NO_LOAD_COST" numeric(15,5),
    "EXPOST_INDICATIVE_HOURS_UP" numeric(15,5),
    "EXPOST_INDICATIVE_INITIAL_GENERATION" numeric(15,5),
    "EXPOST_INDICATIVE_LIMIT_SLACK_MAX" numeric(15,5),
    "EXPOST_INDICATIVE_LIMIT_SLACK_MIN" numeric(15,5),
    "EXPOST_INDICATIVE_LIMIT_SLACK_RAMPRATE" numeric(15,5),
    "EXPOST_INDICATIVE_COST" numeric(15,5),
    "EXPOST_INDICATIVE_CORRECTED_CERTIFIED_AVAILABILITY" numeric(15,5),
    "EXPOST_INDICATIVE_MARKET_COMMITTED" numeric(15,5),
    "EXPOST_INDICATIVE_TRANSITION" numeric(15,5),
    "EXPOST_CALCULATED_CONFIRMED_MARKET_SCHEDULE" numeric(15,5),
    "EXPOST_CONFIRMED_MARKET_SCHEDULE" numeric(15,5),
    "EXPOST_CONFIRMED_NO_LOAD_COST" numeric(15,5),
    "EXPOST_CONFIRMED_HOURS_UP" numeric(15,5),
    "EXPOST_CONFIRMED_INITIAL_GENERATION" numeric(15,5),
    "EXPOST_CONFIRMED_LIMIT_SLACK_MAX" numeric(15,5),
    "EXPOST_CONFIRMED_LIMIT_SLACK_MIN" numeric(15,5),
    "EXPOST_CONFIRMED_LIMIT_SLACK_RAMPRATE" numeric(15,5),
    "EXPOST_CONFIRMED_COST" numeric(15,5),
    "EXPOST_CONFIRMED_CORRECTED_CERTIFIED_AVAILABILITY" numeric(15,5),
    "EXPOST_CONFIRMED_MARKET_COMMITTED" numeric(15,5),
    "EXPOST_CONFIRMED_TRANSITION" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint NOT NULL,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PSU" integer,
    schedule_id integer NOT NULL
);


ALTER TABLE datamart."MSH_RESULTS_PSU_TP" OWNER TO postgres;

--
-- Name: Month; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."Month" (
    "ID_Month" integer NOT NULL,
    "Number" smallint,
    "Name" character varying(15),
    "MONTH_YEAR" character varying(10),
    "ID_YEAR" integer
);


ALTER TABLE datamart."Month" OWNER TO postgres;

--
-- Name: Month_ID_Month_seq; Type: SEQUENCE; Schema: datamart; Owner: postgres
--

CREATE SEQUENCE datamart."Month_ID_Month_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE datamart."Month_ID_Month_seq" OWNER TO postgres;

--
-- Name: Month_ID_Month_seq; Type: SEQUENCE OWNED BY; Schema: datamart; Owner: postgres
--

ALTER SEQUENCE datamart."Month_ID_Month_seq" OWNED BY datamart."Month"."ID_Month";


--
-- Name: PAR_BIDZONE; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."PAR_BIDZONE" (
    "ANNUAL_SCARCITY_CREDIT_CAP" numeric(15,5),
    "MONTHLY_SCARCITY_CREDIT_CAP" numeric(15,5),
    "OVER_DELIVERY_DISCOUNT_FACTOR" numeric(15,5),
    "UNDER_DELIVERY_PREMIUM_FACTOR" numeric(15,5),
    "POOL_PRICE_FLOOR" numeric(15,5),
    "POOL_PRICE_CAP" numeric(15,5),
    "RELIABILITY_PRICE" numeric(15,5),
    "SCARCITY_FACTOR_TABLE" text,
    "CURRENCY_EXCHANGE_RATE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "ID_BID_ZONE" integer,
    "START_DATE" date,
    "STOP_DATE" date,
    schedule_id integer
);


ALTER TABLE datamart."PAR_BIDZONE" OWNER TO postgres;

--
-- Name: PAR_BIDZONE_ADM_PRICE; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."PAR_BIDZONE_ADM_PRICE" (
    "ADMINISTERED_PRICE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" integer NOT NULL,
    "ID_BID_ZONE" integer,
    schedule_id integer NOT NULL
);


ALTER TABLE datamart."PAR_BIDZONE_ADM_PRICE" OWNER TO postgres;

--
-- Name: SET_ANNUAL_SCARCITY_FACILITY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_ANNUAL_SCARCITY_FACILITY" (
    "INDICATIVE_FACILITY_ANNUAL_SCARCITY_CREDIT" numeric(15,5),
    "CONFIRMED_FACILITY_ANNUAL_SCARCITY_CREDIT" numeric(15,5),
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_FACILITY" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_ANNUAL_SCARCITY_FACILITY" OWNER TO postgres;

--
-- Name: SET_DAILY_ENERGY_PSU_TD; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_DAILY_ENERGY_PSU_TD" (
    "INDICATIVE_ENERGY_CREDIT" numeric(15,5),
    "INDICATIVE_UNINSTRUCTED_IMBALANCE_CREDIT" numeric(15,5),
    "INDICATIVE_UNINSTRUCTED_IMBALANCE_DEBIT" numeric(15,5),
    "INDICATIVE_DISPATCH_QUANTITY" numeric(15,5),
    "INDICATIVE_MARKET_SCHEDULE" numeric(15,5),
    "CONFIRMED_ENERGY_CREDIT" numeric(15,5),
    "CONFIRMED_UNINSTRUCTED_IMBALANCE_CREDIT" numeric(15,5),
    "CONFIRMED_UNINSTRUCTED_IMBALANCE_DEBIT" numeric(15,5),
    "CONFIRMED_DISPATCH_QUANTITY" numeric(15,5),
    "CONFIRMED_MARKET_SCHEDULE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PSU" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_DAILY_ENERGY_PSU_TD" OWNER TO postgres;

--
-- Name: SET_DAILY_FACILITY_DAY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_DAILY_FACILITY_DAY" (
    "INDICATIVE_FACILITY_DAILY_ENERGY_CREDIT" numeric(15,5),
    "INDICATIVE_FUEL_PRICE_ADJUSTMENT_DEBIT" numeric(15,5),
    "CONFIRMED_FACILITY_DAILY_ENERGY_CREDIT" numeric(15,5),
    "CONFIRMED_FUEL_PRICE_ADJUSTMENT_DEBIT" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_FACILITY" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_DAILY_FACILITY_DAY" OWNER TO postgres;

--
-- Name: SET_DAILY_PRODUCTION_BLOCK_DAY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" (
    "INDICATIVE_UNINSTRUCTED_IMBALANCE_DAILY_CREDIT" numeric(15,5),
    "INDICATIVE_UNINSTRUCTED_IMBALANCE_DAILY_DEBIT" numeric(15,5),
    "INDICATIVE_PRODUCTION_COST" numeric(15,5),
    "INDICATIVE_CONSTRAINED_ON_CREDIT" numeric(15,5),
    "INDICATIVE_MAKE_WHOLE_CREDIT" numeric(15,5),
    "INDICATIVE_DAILY_ENERGY_CREDIT" numeric(15,5),
    "INDICATIVE_DISPATCH_ADJUSTMENT_DEBIT" numeric(15,5),
    "CONFIRMED_UNINSTRUCTED_IMBALANCE_DAILY_CREDIT" numeric(15,5),
    "CONFIRMED_UNINSTRUCTED_IMBALANCE_DAILY_DEBIT" numeric(15,5),
    "CONFIRMED_PRODUCTION_COST" numeric(15,5),
    "CONFIRMED_CONSTRAINED_ON_CREDIT" numeric(15,5),
    "CONFIRMED_MAKE_WHOLE_CREDIT" numeric(15,5),
    "CONFIRMED_DAILY_ENERGY_CREDIT" numeric(15,5),
    "CONFIRMED_DISPATCH_ADJUSTMENT_DEBIT" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_BLOCK" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" OWNER TO postgres;

--
-- Name: SET_DAILY_PRODUCTION_BLOCK_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_DAILY_PRODUCTION_BLOCK_TP" (
    "INDICATIVE_DISPATCHED_CONFIGURATION" character varying(100),
    "CONFIRMED_DISPATCHED_CONFIGURATION" character varying(100),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_BLOCK" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_DAILY_PRODUCTION_BLOCK_TP" OWNER TO postgres;

--
-- Name: SET_DAILY_SCARCITY_BID_ZONE_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_DAILY_SCARCITY_BID_ZONE_TP" (
    "INDICATIVE_SCARCITY_PRICE" numeric(15,5),
    "CONFIRMED_SCARCITY_PRICE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_DAILY_SCARCITY_BID_ZONE_TP" OWNER TO postgres;

--
-- Name: SET_DAILY_SCARCITY_PSU_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_DAILY_SCARCITY_PSU_TP" (
    "INDICATIVE_ADJUSTED_SCARCITY_CREDIT" numeric(15,5),
    "INDICATIVE_ELIGIBILITY_QUANTITY" numeric(15,5),
    "INDICATIVE_PRELIMINARY_SCARCITY_CREDIT" numeric(15,5),
    "INDICATIVE_SCARCITY_PRICE_COEFFICIENT" numeric(15,5),
    "CONFIRMED_ADJUSTED_SCARCITY_CREDIT" numeric(15,5),
    "CONFIRMED_ELIGIBILITY_QUANTITY" numeric(15,5),
    "CONFIRMED_PRELIMINARY_SCARCITY_CREDIT" numeric(15,5),
    "CONFIRMED_SCARCITY_PRICE_COEFFICIENT" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PSU" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_DAILY_SCARCITY_PSU_TP" OWNER TO postgres;

--
-- Name: SET_METER_DATA_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_METER_DATA_TP" (
    "METER_DATA_QUANTITY" numeric(15,5),
    "METER_DATA_QUALITY_FLAG" smallint,
    "WATER_FLAG" smallint,
    "AUXILIARY_FLAG" smallint,
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_MARKET_PARTY" integer,
    "ID_GENSET" integer,
    "ID_METER" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_METER_DATA_TP" OWNER TO postgres;

--
-- Name: SET_MONTHLY_ENERGY_FACILITY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_MONTHLY_ENERGY_FACILITY" (
    "INDICATIVE_FACILITY_MONTHLY_ENERGY_CREDIT" numeric(15,5),
    "INDICATIVE_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT" numeric(15,5),
    "CONFIRMED_FACILITY_MONTHLY_ENERGY_CREDIT" numeric(15,5),
    "CONFIRMED_DEMAND_SIDE_MONTHLY_ENERGY_CREDIT" numeric(15,5),
    "ID_Month" integer,
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_FACILITY" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_MONTHLY_ENERGY_FACILITY" OWNER TO postgres;

--
-- Name: SET_MONTHLY_SCARCITY_BID_ZONE; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_MONTHLY_SCARCITY_BID_ZONE" (
    "INDICATIVE_PRELIMINARY_SCARCITY_SCALING_FACTOR" numeric(15,5),
    "CONFIRMED_PRELIMINARY_SCARCITY_SCALING_FACTOR" numeric(15,5),
    "ID_Month" integer,
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_MONTHLY_SCARCITY_BID_ZONE" OWNER TO postgres;

--
-- Name: SET_MONTHLY_SCARCITY_BLOCK; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_MONTHLY_SCARCITY_BLOCK" (
    "INDICATIVE_MONTHLY_SCARCITY_CREDIT" numeric(15,5),
    "CONFIRMED_MONTHLY_SCARCITY_CREDIT" numeric(15,5),
    "ID_Month" integer,
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_BLOCK" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_MONTHLY_SCARCITY_BLOCK" OWNER TO postgres;

--
-- Name: SET_MONTHLY_SCARCITY_FACILITY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."SET_MONTHLY_SCARCITY_FACILITY" (
    "INDICATIVE_FACILITY_MONTHLY_SCARCITY_CREDIT" numeric(15,5),
    "CONFIRMED_FACILITY_MONTHLY_SCARCITY_CREDIT" numeric(15,5),
    "ID_Month" integer,
    "ID_Year" integer,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_FACILITY" integer,
    schedule_id integer
);


ALTER TABLE datamart."SET_MONTHLY_SCARCITY_FACILITY" OWNER TO postgres;

--
-- Name: STD_BID_ZONE_ID_BID_ZONE_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_BID_ZONE_ID_BID_ZONE_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_BID_ZONE_ID_BID_ZONE_seq" OWNER TO postgres;

--
-- Name: STD_BID_ZONE_ID_BID_ZONE_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_BID_ZONE_ID_BID_ZONE_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_BID_ZONE_ID_BID_ZONE_seq" OWNER TO postgres;

--
-- Name: STD_BID_ZONE; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_BID_ZONE" (
    "ID_BID_ZONE" integer DEFAULT nextval('public."STD_BID_ZONE_ID_BID_ZONE_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "ID_CONTROL_AREA" integer,
    "CONTROL_AREA" character varying(25)
);


ALTER TABLE datamart."STD_BID_ZONE" OWNER TO postgres;

--
-- Name: STD_CONTROL_AREA_ID_CONTROL_AREA_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_CONTROL_AREA_ID_CONTROL_AREA_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_CONTROL_AREA_ID_CONTROL_AREA_seq" OWNER TO postgres;

--
-- Name: STD_CONTROL_AREA; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_CONTROL_AREA" (
    "ID_CONTROL_AREA" integer DEFAULT nextval('public."STD_CONTROL_AREA_ID_CONTROL_AREA_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256)
);


ALTER TABLE datamart."STD_CONTROL_AREA" OWNER TO postgres;

--
-- Name: STD_CORRIDOR; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_CORRIDOR" (
    "ID_CORRIDOR" integer NOT NULL,
    "Name" character varying(256),
    "Description" character varying(256),
    "EIC_CODE" character varying(256),
    "ID_BID_ZONE_OUT" integer,
    "BID_ZONE_OUT" character varying(25),
    "ID_CONTROL_AREA_OUT" integer,
    "CONTROL_AREA_OUT" character varying(25),
    "ID_CONTROL_AREA_IN" integer,
    "CONTROL_AREA_IN" character varying(25)
);


ALTER TABLE datamart."STD_CORRIDOR" OWNER TO postgres;

--
-- Name: STD_CORRIDOR_ID_CORRIDOR_seq; Type: SEQUENCE; Schema: datamart; Owner: postgres
--

CREATE SEQUENCE datamart."STD_CORRIDOR_ID_CORRIDOR_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE datamart."STD_CORRIDOR_ID_CORRIDOR_seq" OWNER TO postgres;

--
-- Name: STD_CORRIDOR_ID_CORRIDOR_seq; Type: SEQUENCE OWNED BY; Schema: datamart; Owner: postgres
--

ALTER SEQUENCE datamart."STD_CORRIDOR_ID_CORRIDOR_seq" OWNED BY datamart."STD_CORRIDOR"."ID_CORRIDOR";


--
-- Name: STD_FUEL_TYPE_ID_FUEL_TYPE_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_FUEL_TYPE_ID_FUEL_TYPE_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_FUEL_TYPE_ID_FUEL_TYPE_seq" OWNER TO postgres;

--
-- Name: STD_FUEL_TYPE; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_FUEL_TYPE" (
    "ID_FUEL_TYPE" integer DEFAULT nextval('public."STD_FUEL_TYPE_ID_FUEL_TYPE_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256)
);


ALTER TABLE datamart."STD_FUEL_TYPE" OWNER TO postgres;

--
-- Name: STD_GENSET_ID_GENSET_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_GENSET_ID_GENSET_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_GENSET_ID_GENSET_seq" OWNER TO postgres;

--
-- Name: STD_GENSET; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_GENSET" (
    "ID_GENSET" integer DEFAULT nextval('public."STD_GENSET_ID_GENSET_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "REGISTERED_CAPACITY" numeric(10,2),
    "ID_PRODUCTION_BLOCK" integer,
    "PRODUCTION_BLOCK" character varying(25),
    "ID_MARKET_PARTY" integer,
    "MARKET_PARTY" character varying(25),
    "ID_BID_ZONE" integer,
    "BID_ZONE" character varying(25)
);


ALTER TABLE datamart."STD_GENSET" OWNER TO postgres;

--
-- Name: STD_MARKET_PARTY_ID_MARKET_PARTY_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_MARKET_PARTY_ID_MARKET_PARTY_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_MARKET_PARTY_ID_MARKET_PARTY_seq" OWNER TO postgres;

--
-- Name: STD_MARKET_PARTY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_MARKET_PARTY" (
    "ID_MARKET_PARTY" integer DEFAULT nextval('public."STD_MARKET_PARTY_ID_MARKET_PARTY_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "EIC_CODE" character varying(256),
    "DESCRIPTION" character varying(256),
    "ID_BID_ZONE" integer,
    "BID_ZONE" character varying(25),
    "ID_CONTROL_AREA" integer,
    "CONTROL_AREA" character varying(25)
);


ALTER TABLE datamart."STD_MARKET_PARTY" OWNER TO postgres;

--
-- Name: STD_METER_ID_METER_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_METER_ID_METER_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_METER_ID_METER_seq" OWNER TO postgres;

--
-- Name: STD_METER; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_METER" (
    "ID_METER" integer DEFAULT nextval('public."STD_METER_ID_METER_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "ID_GENSET" integer,
    "GENSET" character varying(25),
    "ID_PRODUCTION_FACILITY" integer,
    "PRODUCTION_FACILITY" character varying(25),
    "ID_MARKET_PARTY" integer,
    "MARKET_PARTY" character varying(25)
);


ALTER TABLE datamart."STD_METER" OWNER TO postgres;

--
-- Name: STD_PRODUCTION_BLOCK_ID_PRODUCTION_BLOCK_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_PRODUCTION_BLOCK_ID_PRODUCTION_BLOCK_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_PRODUCTION_BLOCK_ID_PRODUCTION_BLOCK_seq" OWNER TO postgres;

--
-- Name: STD_PRODUCTION_BLOCK; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_PRODUCTION_BLOCK" (
    "ID_PRODUCTION_BLOCK" integer DEFAULT nextval('public."STD_PRODUCTION_BLOCK_ID_PRODUCTION_BLOCK_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "REGISTERED_CAPACITY" numeric,
    "ID_PRODUCTION_FACILITY" integer,
    "PRODUCTION_FACILITY" character varying(25),
    "ID_MARKET_PARTY" integer,
    "MARKET_PARTY" character varying(25),
    "ID_BID_ZONE" integer,
    "BID_ZONE" character varying(25)
);


ALTER TABLE datamart."STD_PRODUCTION_BLOCK" OWNER TO postgres;

--
-- Name: STD_PRODUCTION_FACILITY_ID_PRODUCTION_FACILITY_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_PRODUCTION_FACILITY_ID_PRODUCTION_FACILITY_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_PRODUCTION_FACILITY_ID_PRODUCTION_FACILITY_seq" OWNER TO postgres;

--
-- Name: STD_PRODUCTION_FACILITY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_PRODUCTION_FACILITY" (
    "ID_PRODUCTION_FACILITY" integer DEFAULT nextval('public."STD_PRODUCTION_FACILITY_ID_PRODUCTION_FACILITY_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "REGISTERED_CAPACITY" smallint,
    "TRANSMISSION_LOSS" numeric(10,3),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "ID_MARKET_PARTY" integer,
    "MARKET_PARTY" character varying(25),
    "ID_BID_ZONE" integer,
    "BID_ZONE" character varying(25),
    "ID_FUEL_TYPE_ALT" integer,
    "FUEL_TYPE_ALT" character varying(25),
    "ID_FUEL_TYPE_PRM" integer,
    "FUEL_TYPE_PRM" character varying(25)
);


ALTER TABLE datamart."STD_PRODUCTION_FACILITY" OWNER TO postgres;

--
-- Name: STD_PSU_ID_PSU_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_PSU_ID_PSU_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_PSU_ID_PSU_seq" OWNER TO postgres;

--
-- Name: STD_PSU; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_PSU" (
    "ID_PSU" integer DEFAULT nextval('public."STD_PSU_ID_PSU_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "PSU_TYPE" character varying(256),
    "PSU_PARTICIPATION_TYPE" character varying(256),
    "PSU_REGISTERED_CAPACITY" numeric(12,3),
    "ID_PRODUCTION_BLOCK" integer,
    "PRODUCTION_BLOCK" character varying(25),
    "ID_MARKET_PARTY" integer,
    "MARKET_PARTY" character varying(25),
    "ID_BID_ZONE" integer,
    "BID_ZONE" character varying(25)
);


ALTER TABLE datamart."STD_PSU" OWNER TO postgres;

--
-- Name: STD_PSU_CONFIG_ID_PSU_CONFIG_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_PSU_CONFIG_ID_PSU_CONFIG_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_PSU_CONFIG_ID_PSU_CONFIG_seq" OWNER TO postgres;

--
-- Name: STD_PSU_CONFIG; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_PSU_CONFIG" (
    "ID_PSU_CONFIG" integer DEFAULT nextval('public."STD_PSU_CONFIG_ID_PSU_CONFIG_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "REGISTERED_CAPACITY" numeric(10,3),
    "ID_PRODUCTION_BLOCK" integer,
    "PRODUCTION_BLOCK" character varying(25),
    "ID_MARKET_PARTY" integer,
    "MARKET_PARTY" character varying(25),
    "ID_BID_ZONE" integer,
    "BID_ZONE" character varying(25)
);


ALTER TABLE datamart."STD_PSU_CONFIG" OWNER TO postgres;

--
-- Name: STD_PSU_CONFIG_PSU_REL; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_PSU_CONFIG_PSU_REL" (
    "ID_PSU_CONFIG" integer NOT NULL,
    "PSU_CONFIG" character varying(25),
    "ID_PSU" integer NOT NULL,
    "PSU" character varying(25),
    "TYPE" character(1)
);


ALTER TABLE datamart."STD_PSU_CONFIG_PSU_REL" OWNER TO postgres;

--
-- Name: STD_PSU_GENSET_REL; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_PSU_GENSET_REL" (
    "ID_GENSET" integer NOT NULL,
    "GENSET" character varying(25),
    "ID_PSU" integer NOT NULL,
    "PSU" character varying(25)
);


ALTER TABLE datamart."STD_PSU_GENSET_REL" OWNER TO postgres;

--
-- Name: STD_RELATIONSHIP_TYPE; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_RELATIONSHIP_TYPE" (
    "TABLE_NAME" character varying(50),
    "CHILD_ROLE" character varying(25),
    "PARENT_ROLE" character varying(25),
    "RELATIONSHIP_TYPE" character varying(30),
    "PARENT_ROLE_ALIAS" character varying(25),
    "ACTION" character(1) DEFAULT 'U'::bpchar,
    "SUFFIX" boolean DEFAULT false
);


ALTER TABLE datamart."STD_RELATIONSHIP_TYPE" OWNER TO postgres;

--
-- Name: STD_ROLE_COLUMN_MAP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_ROLE_COLUMN_MAP" (
    "PRIMARY_ROLE" character varying(25),
    "COLUMN_NAME" character varying(25),
    "TABLE_NAME" character varying(25)
);


ALTER TABLE datamart."STD_ROLE_COLUMN_MAP" OWNER TO postgres;

--
-- Name: STD_TRANSITION_ID_TRANSITION_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_TRANSITION_ID_TRANSITION_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_TRANSITION_ID_TRANSITION_seq" OWNER TO postgres;

--
-- Name: STD_TRANSITION; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_TRANSITION" (
    "ID_TRANSITION" integer DEFAULT nextval('public."STD_TRANSITION_ID_TRANSITION_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "ID_PRODUCTION_BLOCK" integer,
    "PRODUCTION_BLOCK" character varying(25),
    "ID_MARKET_PARTY" integer,
    "MARKET_PARTY" character varying(25)
);


ALTER TABLE datamart."STD_TRANSITION" OWNER TO postgres;

--
-- Name: STD_TRANSITION_MATRIX_ID_TRANSITION_MATRIX_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."STD_TRANSITION_MATRIX_ID_TRANSITION_MATRIX_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;


ALTER TABLE public."STD_TRANSITION_MATRIX_ID_TRANSITION_MATRIX_seq" OWNER TO postgres;

--
-- Name: STD_TRANSITION_MATRIX; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_TRANSITION_MATRIX" (
    "ID_TRANSITION_MATRIX" integer DEFAULT nextval('public."STD_TRANSITION_MATRIX_ID_TRANSITION_MATRIX_seq"'::regclass) NOT NULL,
    "NAME" character varying(256),
    "DESCRIPTION" character varying(256),
    "EIC_CODE" character varying(256),
    "ID_PRODUCTION_BLOCK" integer,
    "PRODUCTION_BLOCK" character varying(25),
    "ID_MARKET_PARTY" integer,
    "MARKET_PARTY" character varying(25)
);


ALTER TABLE datamart."STD_TRANSITION_MATRIX" OWNER TO postgres;

--
-- Name: STD_TRANSITION_MATRIX_PSU_CONFIG_REL; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_TRANSITION_MATRIX_PSU_CONFIG_REL" (
    "ID_TRANSITION_MATRIX" integer NOT NULL,
    "TRANSITION_MATRIX" character varying(25),
    "ID_PSU_CONFIG" integer NOT NULL,
    "PSU_CONFIG" character varying(25),
    "TYPE" character varying(4)
);


ALTER TABLE datamart."STD_TRANSITION_MATRIX_PSU_CONFIG_REL" OWNER TO postgres;

--
-- Name: STD_TRANSITION_MATRIX_TRANSITION_REL; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_TRANSITION_MATRIX_TRANSITION_REL" (
    "ID_TRANSITION_MATRIX" integer NOT NULL,
    "TRANSITION_MATRIX" character varying(25),
    "ID_TRANSITION" integer NOT NULL,
    "TRANSITION" character varying(25)
);


ALTER TABLE datamart."STD_TRANSITION_MATRIX_TRANSITION_REL" OWNER TO postgres;

--
-- Name: STD_TRANSITION_PSU_CONFIG_REL; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."STD_TRANSITION_PSU_CONFIG_REL" (
    "ID_TRANSITION" integer NOT NULL,
    "TRANSITION" character varying(25),
    "ID_PSU_CONFIG" integer NOT NULL,
    "PSU_CONFIG" character varying(25),
    "TYPE" character varying(4)
);


ALTER TABLE datamart."STD_TRANSITION_PSU_CONFIG_REL" OWNER TO postgres;


--
-- Name: TRADING_PERIOD; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRADING_PERIOD" (
    "DATE_KEY_OMAN" date NOT NULL,
    "DATE_KEY_UTC" date,
    "TRADING_PERIOD" smallint NOT NULL,
    "TRADING_DAY" character varying(30),
    "DAY_NAME" character varying(30),
    "DAY_OF_MONTH" smallint,
    "MONTH_NAME" character varying(30),
    "MONTH_YEAR" character varying(30),
    "YEAR" smallint,
    "QUARTER" smallint,
    "WEEKEND_OMAN" boolean,
    "HOLIDAY_OMAN" boolean,
    "ID_Year" integer,
    "ID_Month" integer
);


ALTER TABLE datamart."TRADING_PERIOD" OWNER TO postgres;

--
-- Name: TRA_BLOCK_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_BLOCK_TP" (
    "RESERVE_HOLDING_THRESHOLD" numeric(15,5),
    "FUEL_CONSUMPTION_SCHEDULE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_BLOCK" integer,
    "ID_FUEL_TYPE" integer,
    schedule_id integer
);


ALTER TABLE datamart."TRA_BLOCK_TP" OWNER TO postgres;

--
-- Name: TRA_EXPORTS_CORRIDOR_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_EXPORTS_CORRIDOR_TP" (
    "FORECAST_SYSTEM_EXPORTS" numeric(15,5),
    "ACTUAL_SYSTEM_EXPORTS" numeric(15,5),
    "INTER_ZONAL_CONTRAINT" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_CORRIDOR" integer,
    schedule_id integer
);


ALTER TABLE datamart."TRA_EXPORTS_CORRIDOR_TP" OWNER TO postgres;

--
-- Name: TRA_GENSET_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_GENSET_TP" (
    "GENSET_ACTUAL_AVAILABILITY" numeric(15,5),
    "GENSET_DISPATCH" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_GENSET" integer,
    schedule_id integer
);


ALTER TABLE datamart."TRA_GENSET_TP" OWNER TO postgres;

--
-- Name: TRA_INPUTS_FACILITY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_INPUTS_FACILITY" (
    "WATER_PRODUCTION_REQUIREMENTS" numeric(15,5),
    "WEATHER_FORECAST" numeric(15,5),
    "TRANSCO_MUST_RUN_AUXILIARY_FORECAST" numeric(15,5),
    "MUST_RUN_AUXILIARY_FORECAST" numeric(15,5),
    "ECONOMIC_FUEL_PRICE" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_FACILITY" integer,
    "ID_FUEL_TYPE" integer,
    schedule_id integer
);


ALTER TABLE datamart."TRA_INPUTS_FACILITY" OWNER TO postgres;

--
-- Name: TRA_OFFER_DATA_DAY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_OFFER_DATA_DAY" (
    "ACTUAL_FUEL_PRICE" numeric(15,5),
    "ACTUAL_FUEL_TYPE" integer,
    "ACTUAL_FUEL_PRICE_BACKUP" numeric(15,5),
    "ACTUAL_FUEL_BACKUP_TYPE" integer,
    "MINIMUM_OUTPUT" numeric(15,5),
    "RAMP_UP_RATE" numeric(15,5),
    "RAMP_DOWN_RATE" numeric(15,5),
    "MAX_ON_TIME" numeric(15,5),
    "MIN_ON_TIME" numeric(15,5),
    "MIN_OFF_TIME" numeric(15,5),
    "AUTOGENERATION_STATUS" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint NOT NULL,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PSU" integer,
    "ID_FUEL_TYPE" integer,
    schedule_id integer NOT NULL
);


ALTER TABLE datamart."TRA_OFFER_DATA_DAY" OWNER TO postgres;

--
-- Name: TRA_OFFER_DATA_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_OFFER_DATA_TP" (
    "UTS" numeric(15,5),
    "UTF" numeric(15,5),
    "PPCF" numeric(15,5),
    "TCCF" numeric(15,5),
    "QUANTITY_1" numeric(15,5),
    "PRICE_1" numeric(15,5),
    "NON_FUEL_PRICE_1" numeric(15,5),
    "MUST_RUN" numeric(15,5),
    "QUANTITY_2" numeric(15,5),
    "PRICE_2" numeric(15,5),
    "NON_FUEL_PRICE_2" numeric(15,5),
    "QUANTITY_3" numeric(15,5),
    "PRICE_3" numeric(15,5),
    "NON_FUEL_PRICE_3" numeric(15,5),
    "QUANTITY_4" numeric(15,5),
    "PRICE_4" numeric(15,5),
    "NON_FUEL_PRICE_4" numeric(15,5),
    "QUANTITY_5" numeric(15,5),
    "PRICE_5" numeric(15,5),
    "NON_FUEL_PRICE_5" numeric(15,5),
    "QUANTITY_6" numeric(15,5),
    "PRICE_6" numeric(15,5),
    "NON_FUEL_PRICE_6" numeric(15,5),
    "QUANTITY_7" numeric(15,5),
    "PRICE_7" numeric(15,5),
    "NON_FUEL_PRICE_7" numeric(15,5),
    "QUANTITY_8" numeric(15,5),
    "PRICE_8" numeric(15,5),
    "NON_FUEL_PRICE_8" numeric(15,5),
    "QUANTITY_9" numeric(15,5),
    "PRICE_9" numeric(15,5),
    "NON_FUEL_PRICE_9" numeric(15,5),
    "QUANTITY_10" numeric(15,5),
    "PRICE_10" numeric(15,5),
    "NON_FUEL_PRICE_10" numeric(15,5),
    "NO_LOAD_COST_FUEL_ECONOMIC" numeric(15,5),
    "NO_LOAD_COST_NON_FUEL" numeric(15,5),
    "OFFERED_AVAILABILITY" numeric(15,5),
    "NOMINATED_QUANTITY" numeric(15,5),
    "REVISED_OFFER_FLAG" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PSU" integer,
    schedule_id integer
);


ALTER TABLE datamart."TRA_OFFER_DATA_TP" OWNER TO postgres;

--
-- Name: TRA_PSU_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_PSU_TP" (
    "ACTUAL_AVAILABILITY" numeric(15,5),
    "TRANSCO_ACTUAL_AVAILABILITY" numeric(15,5),
    "AMBIENT_AIR_SCHEDULE" numeric(15,5),
    "NO_LOAD_COST_FUEL_ACTUAL" numeric(15,5),
    "DISPATCH_SCHEDULE_NO_LOAD_COST" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_BID_ZONE" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PSU" integer,
    schedule_id integer
);


ALTER TABLE datamart."TRA_PSU_TP" OWNER TO postgres;

--
-- Name: TRA_RESULTS_BID_ZONE_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_RESULTS_BID_ZONE_TP" (
    "EXANTE_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXANTE_AGGREGATE_POOL_PRICE" numeric(15,5),
    "EXANTE_SCARCITY_PRICE" numeric(15,5),
    "EXANTE_SCARCITY_FACTOR" numeric(15,5),
    "EXANTE_MARGIN" numeric(15,5),
    "EXANTE_SYSTEM_AVAILABILITY_CAPACITY" numeric(15,5),
    "EXANTE_SYSTEM_CAPACITY_REQUIREMENT" numeric(15,5),
    "EXPOST_INDICATIVE_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_AGGREGATE_POOL_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_SCARCITY_PRICE" numeric(15,5),
    "EXPOST_INDICATIVE_SCARCITY_FACTOR" numeric(15,5),
    "EXPOST_INDICATIVE_MARGIN" numeric(15,5),
    "EXPOST_INDICATIVE_SYSTEM_AVAILABILITY_CAPACITY" numeric(15,5),
    "EXPOST_INDICATIVE_SYSTEM_CAPACITY_REQUIREMENT" numeric(15,5),
    "EXPOST_INDICATIVE_POOL_DEMAND" numeric(15,5),
    "EXPOST_CONFIRMED_SYSTEM_MARGINAL_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_AGGREGATE_POOL_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_SCARCITY_PRICE" numeric(15,5),
    "EXPOST_CONFIRMED_SCARCITY_FACTOR" numeric(15,5),
    "EXPOST_CONFIRMED_MARGIN" numeric(15,5),
    "EXPOST_CONFIRMED_SYSTEM_AVAILABILITY_CAPACITY" numeric(15,5),
    "EXPOST_CONFIRMED_SYSTEM_CAPACITY_REQUIREMENT" numeric(15,5),
    "EXPOST_CONFIRMED_POOL_DEMAND" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint NOT NULL,
    "ID_BID_ZONE" integer,
    schedule_id integer NOT NULL
);


ALTER TABLE datamart."TRA_RESULTS_BID_ZONE_TP" OWNER TO postgres;

--
-- Name: TRA_TRANSITION_DATA_DAY; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_TRANSITION_DATA_DAY" (
    "TRANSITION_COST_FUEL_ECONOMIC" numeric(15,5),
    "TRANSITION_COST_NON_FUEL" numeric(15,5),
    "TDE_COMMENCE_BANKING_OUTPUT" numeric(15,5),
    "TDE_COMMENCE_BANKING_DURATION" numeric(15,5),
    "TDE_RAMP_ONE_DURATION" numeric(15,5),
    "TDE_MIDDLE_BANKING_OUTPUT" numeric(15,5),
    "TDE_MIDDLE_BANKING_DURATION" numeric(15,5),
    "TDE_RAMP_TWO_DURATION" numeric(15,5),
    "TDE_COMPLETE_BANKING_OUTPUT" numeric(15,5),
    "TDE_COMPLETE_BANKING_DURATION" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_TRANSITION" integer,
    "ID_MARKET_PARTY" integer,
    "ID_PRODUCTION_BLOCK" integer,
    schedule_id integer
);


ALTER TABLE datamart."TRA_TRANSITION_DATA_DAY" OWNER TO postgres;

--
-- Name: TRA_TRANSITION_DATA_DST_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_TRANSITION_DATA_DST_TP" (
    "DISPATCH_SCHEDULE_TRANSITION_INTEGER" smallint,
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint NOT NULL,
    "ID_MARKET_PARTY" integer,
    "ID_TRANSITION" integer,
    schedule_id integer NOT NULL
);


ALTER TABLE datamart."TRA_TRANSITION_DATA_DST_TP" OWNER TO postgres;

--
-- Name: TRA_TRANSITION_DATA_TP; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."TRA_TRANSITION_DATA_TP" (
    "OUTTURN_TRANSITIONS" numeric(15,5),
    "DATE_KEY_OMAN" date,
    "TRADING_PERIOD" smallint,
    "ID_MARKET_PARTY" integer,
    "ID_TRANSITION" integer,
    schedule_id integer
);


ALTER TABLE datamart."TRA_TRANSITION_DATA_TP" OWNER TO postgres;

--
-- Name: Year; Type: TABLE; Schema: datamart; Owner: postgres
--

CREATE TABLE datamart."Year" (
    "ID_Year" integer NOT NULL,
    "Number" smallint
);


ALTER TABLE datamart."Year" OWNER TO postgres;

--
-- Name: Year_ID_Year_seq; Type: SEQUENCE; Schema: datamart; Owner: postgres
--

CREATE SEQUENCE datamart."Year_ID_Year_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE datamart."Year_ID_Year_seq" OWNER TO postgres;

--
-- Name: Year_ID_Year_seq; Type: SEQUENCE OWNED BY; Schema: datamart; Owner: postgres
--

ALTER SEQUENCE datamart."Year_ID_Year_seq" OWNED BY datamart."Year"."ID_Year";


--
-- Name: vw_Standing_Data; Type: VIEW; Schema: datamart; Owner: postgres
--

CREATE VIEW datamart."vw_Standing_Data" AS
 SELECT "STD_BID_ZONE"."ID_BID_ZONE" AS "SD_ID",
    "STD_BID_ZONE"."NAME",
    "STD_BID_ZONE"."DESCRIPTION",
    'BZONE'::text AS "StandingDataType"
   FROM datamart."STD_BID_ZONE"
UNION ALL
 SELECT "STD_CONTROL_AREA"."ID_CONTROL_AREA" AS "SD_ID",
    "STD_CONTROL_AREA"."NAME",
    "STD_CONTROL_AREA"."DESCRIPTION",
    'CONTROLAREA'::text AS "StandingDataType"
   FROM datamart."STD_CONTROL_AREA"
UNION ALL
 SELECT "STD_CORRIDOR"."ID_CORRIDOR" AS "SD_ID",
    "STD_CORRIDOR"."Name" AS "NAME",
    "STD_CORRIDOR"."Description" AS "DESCRIPTION",
    'CORRIDOR'::text AS "StandingDataType"
   FROM datamart."STD_CORRIDOR"
UNION ALL
 SELECT "STD_FUEL_TYPE"."ID_FUEL_TYPE" AS "SD_ID",
    "STD_FUEL_TYPE"."NAME",
    "STD_FUEL_TYPE"."DESCRIPTION",
    'OFT'::text AS "StandingDataType"
   FROM datamart."STD_FUEL_TYPE"
UNION ALL
 SELECT "STD_GENSET"."ID_GENSET" AS "SD_ID",
    "STD_GENSET"."NAME",
    "STD_GENSET"."DESCRIPTION",
    'OGENSET'::text AS "StandingDataType"
   FROM datamart."STD_GENSET"
UNION ALL
 SELECT "STD_MARKET_PARTY"."ID_MARKET_PARTY" AS "SD_ID",
    "STD_MARKET_PARTY"."NAME",
    "STD_MARKET_PARTY"."DESCRIPTION",
    'MARKETPARTY'::text AS "StandingDataType"
   FROM datamart."STD_MARKET_PARTY"
UNION ALL
 SELECT "STD_METER"."ID_METER" AS "SD_ID",
    "STD_METER"."NAME",
    "STD_METER"."DESCRIPTION",
    'OMETER'::text AS "StandingDataType"
   FROM datamart."STD_METER"
UNION ALL
 SELECT "STD_PRODUCTION_BLOCK"."ID_PRODUCTION_BLOCK" AS "SD_ID",
    "STD_PRODUCTION_BLOCK"."NAME",
    "STD_PRODUCTION_BLOCK"."DESCRIPTION",
    'OPRODBLOCK'::text AS "StandingDataType"
   FROM datamart."STD_PRODUCTION_BLOCK"
UNION ALL
 SELECT "STD_PRODUCTION_FACILITY"."ID_PRODUCTION_FACILITY" AS "SD_ID",
    "STD_PRODUCTION_FACILITY"."NAME",
    "STD_PRODUCTION_FACILITY"."DESCRIPTION",
    'OPRODFAC'::text AS "StandingDataType"
   FROM datamart."STD_PRODUCTION_FACILITY"
UNION ALL
 SELECT "STD_PSU"."ID_PSU" AS "SD_ID",
    "STD_PSU"."NAME",
    "STD_PSU"."DESCRIPTION",
    'OPSU'::text AS "StandingDataType"
   FROM datamart."STD_PSU"
UNION ALL
 SELECT "STD_PSU_CONFIG"."ID_PSU_CONFIG" AS "SD_ID",
    "STD_PSU_CONFIG"."NAME",
    "STD_PSU_CONFIG"."DESCRIPTION",
    'OPSUCONF'::text AS "StandingDataType"
   FROM datamart."STD_PSU_CONFIG"
UNION ALL
 SELECT "STD_TRANSITION"."ID_TRANSITION" AS "SD_ID",
    "STD_TRANSITION"."NAME",
    "STD_TRANSITION"."DESCRIPTION",
    'OTRANS'::text AS "StandingDataType"
   FROM datamart."STD_TRANSITION"
UNION ALL
 SELECT "STD_TRANSITION_MATRIX"."ID_TRANSITION_MATRIX" AS "SD_ID",
    "STD_TRANSITION_MATRIX"."NAME",
    "STD_TRANSITION_MATRIX"."DESCRIPTION",
    '"OTRANSMATRX"'::text AS "StandingDataType"
   FROM datamart."STD_TRANSITION_MATRIX";


ALTER TABLE datamart."vw_Standing_Data" OWNER TO postgres;

--
-- Name: vw_TRA_OFFER_DATA_TP; Type: VIEW; Schema: datamart; Owner: postgres
--

CREATE VIEW datamart."vw_TRA_OFFER_DATA_TP" AS
 SELECT sum("TRA_OFFER_DATA_TP"."UTS") AS "UTS",
    sum("TRA_OFFER_DATA_TP"."UTF") AS "UTF",
    sum("TRA_OFFER_DATA_TP"."PPCF") AS "PPCF",
    sum("TRA_OFFER_DATA_TP"."TCCF") AS "TCCF",
    sum("TRA_OFFER_DATA_TP"."MUST_RUN") AS "MUST_RUN",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_1") AS "QUANTITY_1",
    sum("TRA_OFFER_DATA_TP"."PRICE_1") AS "PRICE_1",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_1") AS "NON_FUEL_PRICE_1",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_2") AS "QUANTITY_2",
    sum("TRA_OFFER_DATA_TP"."PRICE_2") AS "PRICE_2",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_2") AS "NON_FUEL_PRICE_2",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_3") AS "QUANTITY_3",
    sum("TRA_OFFER_DATA_TP"."PRICE_3") AS "PRICE_3",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_3") AS "NON_FUEL_PRICE_3",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_4") AS "QUANTITY_4",
    sum("TRA_OFFER_DATA_TP"."PRICE_4") AS "PRICE_4",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_4") AS "NON_FUEL_PRICE_4",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_5") AS "QUANTITY_5",
    sum("TRA_OFFER_DATA_TP"."PRICE_5") AS "PRICE_5",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_5") AS "NON_FUEL_PRICE_5",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_6") AS "QUANTITY_6",
    sum("TRA_OFFER_DATA_TP"."PRICE_6") AS "PRICE_6",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_6") AS "NON_FUEL_PRICE_6",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_7") AS "QUANTITY_7",
    sum("TRA_OFFER_DATA_TP"."PRICE_7") AS "PRICE_7",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_7") AS "NON_FUEL_PRICE_7",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_8") AS "QUANTITY_8",
    sum("TRA_OFFER_DATA_TP"."PRICE_8") AS "PRICE_8",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_8") AS "NON_FUEL_PRICE_8",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_9") AS "QUANTITY_9",
    sum("TRA_OFFER_DATA_TP"."PRICE_9") AS "PRICE_9",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_9") AS "NON_FUEL_PRICE_9",
    sum("TRA_OFFER_DATA_TP"."QUANTITY_10") AS "QUANTITY_10",
    sum("TRA_OFFER_DATA_TP"."PRICE_10") AS "PRICE_10",
    sum("TRA_OFFER_DATA_TP"."NON_FUEL_PRICE_10") AS "NON_FUEL_PRICE_10",
    sum("TRA_OFFER_DATA_TP"."NO_LOAD_COST_FUEL_ECONOMIC") AS "NO_LOAD_COST_FUEL_ECONOMIC",
    sum("TRA_OFFER_DATA_TP"."NO_LOAD_COST_NON_FUEL") AS "NO_LOAD_COST_NON_FUEL",
    sum("TRA_OFFER_DATA_TP"."OFFERED_AVAILABILITY") AS "OFFERED_AVAILABILITY",
    sum("TRA_OFFER_DATA_TP"."NOMINATED_QUANTITY") AS "NOMINATED_QUANTITY",
    sum("TRA_OFFER_DATA_TP"."REVISED_OFFER_FLAG") AS "REVISED_OFFER_FLAG",
    (("TRA_OFFER_DATA_TP"."DATE_KEY_OMAN" + '1 day'::interval))::date AS "DATE_KEY_OMAN",
    "TRA_OFFER_DATA_TP"."TRADING_PERIOD",
    "TRA_OFFER_DATA_TP"."ID_BID_ZONE",
    "TRA_OFFER_DATA_TP"."ID_MARKET_PARTY",
    "TRA_OFFER_DATA_TP"."ID_PSU"
   FROM datamart."TRA_OFFER_DATA_TP"
  GROUP BY "TRA_OFFER_DATA_TP"."DATE_KEY_OMAN", "TRA_OFFER_DATA_TP"."TRADING_PERIOD", "TRA_OFFER_DATA_TP"."ID_BID_ZONE", "TRA_OFFER_DATA_TP"."ID_MARKET_PARTY", "TRA_OFFER_DATA_TP"."ID_PSU";


ALTER TABLE datamart."vw_TRA_OFFER_DATA_TP" OWNER TO postgres;

--
-- Name: SeriesValues; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."SeriesValues" (
    "ValueName" character varying(80)
);


ALTER TABLE public."SeriesValues" OWNER TO postgres;

--
-- Name: croncalendarrule_trak; Type: FOREIGN TABLE; Schema: public; Owner: postgres
--

CREATE FOREIGN TABLE public.croncalendarrule_trak (
    id bigint NOT NULL,
    cronexpression character varying(255),
    name character varying(255),
    calendar_id bigint
)
SERVER opwp_trak_nextgen
OPTIONS (
    schema_name 'public',
    table_name 'croncalendarrule'
);


ALTER FOREIGN TABLE public.croncalendarrule_trak OWNER TO postgres;

--
-- Name: sr_schedule_schd; Type: FOREIGN TABLE; Schema: public; Owner: postgres
--

CREATE FOREIGN TABLE public.sr_schedule_schd (
    schedule_id numeric(10,0) NOT NULL,
    start_date timestamp without time zone NOT NULL,
    stop_date timestamp without time zone NOT NULL,
    is_actual_version numeric(1,0) NOT NULL,
    creation_time timestamp without time zone NOT NULL,
    version numeric(10,0) NOT NULL,
    log_context numeric(10,0),
    schedule_origin character varying(20),
    status character varying(40),
    schedule_ident character varying(40),
    deletion_time timestamp without time zone,
    document_id numeric(10,0),
    schedule_type_id numeric(10,0) NOT NULL,
    workspace_id numeric(10,0),
    draft numeric(1,0) DEFAULT 0 NOT NULL,
    template numeric(1,0) DEFAULT 0 NOT NULL,
    composite_key character varying(40) NOT NULL,
    last_modified_date timestamp without time zone NOT NULL,
    values_hash character varying(40),
    parameters_hash character varying(40),
    creation_timestamp timestamp without time zone DEFAULT clock_timestamp() NOT NULL,
    datamart_fetched boolean DEFAULT false
)
SERVER schd_batchinput_his
OPTIONS (
    schema_name 'public',
    table_name 'sr_schedule'
);


ALTER FOREIGN TABLE public.sr_schedule_schd OWNER TO postgres;

--
-- Name: sr_schedule_trak; Type: FOREIGN TABLE; Schema: public; Owner: postgres
--

CREATE FOREIGN TABLE public.sr_schedule_trak (
    schedule_id numeric(10,0) NOT NULL,
    start_date timestamp without time zone NOT NULL,
    stop_date timestamp without time zone NOT NULL,
    is_actual_version numeric(1,0) NOT NULL,
    creation_time timestamp without time zone NOT NULL,
    version numeric(10,0) NOT NULL,
    log_context numeric(10,0),
    schedule_origin character varying(20),
    status character varying(40),
    schedule_ident character varying(40),
    deletion_time timestamp without time zone,
    document_id numeric(10,0),
    schedule_type_id numeric(10,0) NOT NULL,
    workspace_id numeric(10,0),
    draft numeric(1,0) DEFAULT 0 NOT NULL,
    template numeric(1,0) DEFAULT 0 NOT NULL,
    composite_key character varying(40) NOT NULL,
    last_modified_date timestamp without time zone NOT NULL,
    values_hash character varying(40),
    parameters_hash character varying(40),
    creation_timestamp timestamp without time zone DEFAULT clock_timestamp() NOT NULL,
    datamart_fetched boolean DEFAULT false
)
SERVER trak_batchinput_his
OPTIONS (
    schema_name 'public',
    table_name 'sr_schedule'
);


ALTER FOREIGN TABLE public.sr_schedule_trak OWNER TO postgres;

--
-- Name: vw_xtabschedulevaluenumber; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.vw_xtabschedulevaluenumber AS
 SELECT svn.entity_id,
    svn.entitydef_name,
    svn.description,
    svn."PFAC_CAPACITY",
    svn."PFAC_TRANSLOSS",
    svn."PSU_CAPACITY",
    svn."PSUCONF_CAPACITY",
    svn."GEN_CAPACITY"
   FROM public.crosstab('Select ed.entity_id,
    ed.name AS entitydef_name, ed.Description, 
    vt.Code, SUM(svn.value)
FROM SD_ENTITY en
JOIN SD_ENTITY_DEF ed on en.entity_id=ed.entity_id
JOIN sr_schedule_parameter sp ON en.entity_id = sp.entity_id
JOIN sr_schedule_value_number svn ON sp.schedule_id = svn.schedule_id
JOIN sr_value_type vt ON svn.value_type_id = vt.value_type_id
WHERE vt.code in (''PFAC_CAPACITY'',''PFAC_TRANSLOSS'',''PSU_CAPACITY'',''PSUCONF_CAPACITY'',''GEN_CAPACITY'')
AND	(en.primary_role_code=''OPRODFAC'' OR en.primary_role_code=''OPSU'' OR en.primary_role_code=''OPSUCONF'' OR en.primary_role_code=''OGENSET'' ) 
GROUP BY ed.entity_id, ed.name, ed.Description,vt.code
ORDER BY ed.entity_id'::text, 'SELECT ''PFAC_CAPACITY'' UNION ALL
SELECT ''PFAC_TRANSLOSS'' UNION ALL
SELECT ''PSU_CAPACITY'' UNION ALL
SELECT ''PSUCONF_CAPACITY'' UNION ALL
SELECT ''GEN_CAPACITY'''::text) svn(entity_id integer, entitydef_name character varying(256), description character varying(256), "PFAC_CAPACITY" numeric, "PFAC_TRANSLOSS" numeric, "PSU_CAPACITY" numeric, "PSUCONF_CAPACITY" numeric(10,3), "GEN_CAPACITY" numeric);


ALTER TABLE public.vw_xtabschedulevaluenumber OWNER TO postgres;

--
-- Name: vw_xtabschedulevaluestring; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.vw_xtabschedulevaluestring AS
 SELECT svn.entity_id,
    svn.entitydef_name,
    svn.description,
    svn."EIC_MARKETPARTY",
    svn."EIC_BZONE",
    svn."EIC_CONTROLAREA",
    svn."EIC_OMETER",
    svn."EIC_OTRANS",
    svn."EIC_OPRODFAC",
    svn."EIC_OFT",
    svn."EIC_OGENSET",
    svn."EIC_PSU",
    svn."EIC_PSUCONF",
    svn."PSU_PTYPE",
    svn."PSU_TYPE"
   FROM public.crosstab('Select en.entity_id,
    ed.name AS entitydef_name, ed.Description, 
    ft.Code, fv.value_string
FROM SD_ENTITY en
JOIN SD_ENTITY_DEF ed on en.entity_id=ed.entity_id
JOIN sd_factor_value fv ON en.entity_id=fv.entity_id
JOIN sd_factor_type ft ON fv.factor_type_id=ft.factor_type_id
WHERE (ft.Code like ''EIC%'' OR ft.Code =''PSU_PTYPE'' OR ft.Code=''PSU_TYPE'')
--AND	en.primary_role_code in (''MARKETPARTY'',''CONTROLAREA'',''BZONE'',''OFT'',''OTRANS'',''OMETER'',''OPRODFAC'',''OPSU'',''OPSUCONF'',''OGENSET'')
--GROUP BY ed.entity_id, ed.name, ed.Description,vt.code
ORDER BY ed.entity_id'::text, 'SELECT ''EIC_MARKETPARTY'' UNION ALL
SELECT ''EIC_BZONE'' UNION ALL
SELECT ''EIC_CONTROLAREA'' UNION ALL
SELECT ''EIC_OMETER'' UNION ALL
SELECT ''EIC_OTRANS'' UNION ALL
SELECT ''EIC_OPRODFAC'' UNION ALL
SELECT ''EIC_OFT'' UNION ALL
SELECT ''EIC_OGENSET'' UNION ALL
SELECT ''EIC_OPSU'' UNION ALL
SELECT ''EIC_OPSUCONF'' UNION ALL
SELECT ''PSU_PTYPE'' UNION ALL
SELECT ''PSU_TYPE'''::text) svn(entity_id integer, entitydef_name character varying(256), description character varying(256), "EIC_MARKETPARTY" character varying(256), "EIC_BZONE" character varying(256), "EIC_CONTROLAREA" character varying(256), "EIC_OMETER" character varying(256), "EIC_OTRANS" character varying(256), "EIC_OPRODFAC" character varying(256), "EIC_OFT" character varying(256), "EIC_OGENSET" character varying(256), "EIC_PSU" character varying(256), "EIC_PSUCONF" character varying(256), "PSU_PTYPE" character varying(256), "PSU_TYPE" character varying(256));


ALTER TABLE public.vw_xtabschedulevaluestring OWNER TO postgres;

--
-- Name: Month ID_Month; Type: DEFAULT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."Month" ALTER COLUMN "ID_Month" SET DEFAULT nextval('datamart."Month_ID_Month_seq"'::regclass);


--
-- Name: STD_CORRIDOR ID_CORRIDOR; Type: DEFAULT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_CORRIDOR" ALTER COLUMN "ID_CORRIDOR" SET DEFAULT nextval('datamart."STD_CORRIDOR_ID_CORRIDOR_seq"'::regclass);


--
-- Name: Year ID_Year; Type: DEFAULT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."Year" ALTER COLUMN "ID_Year" SET DEFAULT nextval('datamart."Year_ID_Year_seq"'::regclass);


--
-- Name: STD_BID_ZONE BID_ZONE_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_BID_ZONE"
    ADD CONSTRAINT "BID_ZONE_PK" PRIMARY KEY ("ID_BID_ZONE");


--
-- Name: STD_CONTROL_AREA CONTROL_AREA_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_CONTROL_AREA"
    ADD CONSTRAINT "CONTROL_AREA_PK" PRIMARY KEY ("ID_CONTROL_AREA");


--
-- Name: STD_CORRIDOR CORRIDOR_pk; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_CORRIDOR"
    ADD CONSTRAINT "CORRIDOR_pk" PRIMARY KEY ("ID_CORRIDOR");


--
-- Name: TRADING_PERIOD DATE_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRADING_PERIOD"
    ADD CONSTRAINT "DATE_PK" PRIMARY KEY ("DATE_KEY_OMAN", "TRADING_PERIOD");


--
-- Name: STD_FUEL_TYPE FUEL_TYPE_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_FUEL_TYPE"
    ADD CONSTRAINT "FUEL_TYPE_PK" PRIMARY KEY ("ID_FUEL_TYPE");


--
-- Name: STD_GENSET GENSET_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_GENSET"
    ADD CONSTRAINT "GENSET_PK" PRIMARY KEY ("ID_GENSET");


--
-- Name: STD_MARKET_PARTY MARKET_PARTY_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_MARKET_PARTY"
    ADD CONSTRAINT "MARKET_PARTY_PK" PRIMARY KEY ("ID_MARKET_PARTY");


--
-- Name: STD_METER METER_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_METER"
    ADD CONSTRAINT "METER_PK" PRIMARY KEY ("ID_METER");


--
-- Name: Month Month_pk; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."Month"
    ADD CONSTRAINT "Month_pk" PRIMARY KEY ("ID_Month");


--
-- Name: MSH_RESULTS_BID_ZONE_TP PK_MSH_RESULTS_BID_ZONE_TP_1; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_BID_ZONE_TP"
    ADD CONSTRAINT "PK_MSH_RESULTS_BID_ZONE_TP_1" PRIMARY KEY (schedule_id, "TRADING_PERIOD");


--
-- Name: MSH_RESULTS_PSU_TP PK_MSH_RESULTS_PSU_TP; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_PSU_TP"
    ADD CONSTRAINT "PK_MSH_RESULTS_PSU_TP" PRIMARY KEY (schedule_id, "TRADING_PERIOD");


--
-- Name: PAR_BIDZONE_ADM_PRICE PK_PAR_BIDZONE_ADM_PRICE; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."PAR_BIDZONE_ADM_PRICE"
    ADD CONSTRAINT "PK_PAR_BIDZONE_ADM_PRICE" PRIMARY KEY (schedule_id, "TRADING_PERIOD");


--
-- Name: TRA_OFFER_DATA_DAY PK_TRA_OFFER_DATA_DAY; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_DAY"
    ADD CONSTRAINT "PK_TRA_OFFER_DATA_DAY" PRIMARY KEY (schedule_id, "TRADING_PERIOD");


--
-- Name: TRA_RESULTS_BID_ZONE_TP PK_TRA_RESULTS_BID_ZONE_TP; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_RESULTS_BID_ZONE_TP"
    ADD CONSTRAINT "PK_TRA_RESULTS_BID_ZONE_TP" PRIMARY KEY (schedule_id, "TRADING_PERIOD");


--
-- Name: TRA_TRANSITION_DATA_DST_TP PK_TRA_TRANSITION_DATA_DST_TP; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_DST_TP"
    ADD CONSTRAINT "PK_TRA_TRANSITION_DATA_DST_TP" PRIMARY KEY (schedule_id, "TRADING_PERIOD");


--
-- Name: STD_PRODUCTION_BLOCK PRODUCTION_BLOCK_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_PRODUCTION_BLOCK"
    ADD CONSTRAINT "PRODUCTION_BLOCK_PK" PRIMARY KEY ("ID_PRODUCTION_BLOCK");


--
-- Name: STD_PRODUCTION_FACILITY PRODUCTION_FACILITY_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_PRODUCTION_FACILITY"
    ADD CONSTRAINT "PRODUCTION_FACILITY_PK" PRIMARY KEY ("ID_PRODUCTION_FACILITY");


--
-- Name: STD_PSU_CONFIG PSU_CONFIG_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_PSU_CONFIG"
    ADD CONSTRAINT "PSU_CONFIG_PK" PRIMARY KEY ("ID_PSU_CONFIG");


--
-- Name: STD_PSU PSU_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_PSU"
    ADD CONSTRAINT "PSU_PK" PRIMARY KEY ("ID_PSU");


--
-- Name: STD_TRANSITION_MATRIX TRANSITION_MATRIX_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_TRANSITION_MATRIX"
    ADD CONSTRAINT "TRANSITION_MATRIX_PK" PRIMARY KEY ("ID_TRANSITION_MATRIX");


--
-- Name: STD_TRANSITION TRANSITION_PK; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."STD_TRANSITION"
    ADD CONSTRAINT "TRANSITION_PK" PRIMARY KEY ("ID_TRANSITION");


--
-- Name: Year Year_pk; Type: CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."Year"
    ADD CONSTRAINT "Year_pk" PRIMARY KEY ("ID_Year");


--
-- Name: DSPT_IX_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "DSPT_IX_Schedule_Id" ON datamart."SET_DAILY_SCARCITY_PSU_TP" USING btree (schedule_id);


--
-- Name: GenSET_IX_ScheduleId; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "GenSET_IX_ScheduleId" ON datamart."TRA_GENSET_TP" USING btree (schedule_id);


--
-- Name: IX_DATE_KEY_OMAN; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_DATE_KEY_OMAN" ON datamart."TRADING_PERIOD" USING btree ("DATE_KEY_OMAN");


--
-- Name: IX_DPBD_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_DPBD_Schedule_Id" ON datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" USING btree (schedule_id);


--
-- Name: IX_DPBT_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_DPBT_Schedule_Id" ON datamart."SET_DAILY_PRODUCTION_BLOCK_TP" USING btree (schedule_id);


--
-- Name: IX_DSBZT_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_DSBZT_Schedule_Id" ON datamart."SET_DAILY_SCARCITY_BID_ZONE_TP" USING btree (schedule_id);


--
-- Name: IX_DailyLog_ExtractionDate; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_DailyLog_ExtractionDate" ON datamart."Extract_LOG" USING btree ("ExtractionDate");


--
-- Name: IX_DailyLog_Measure; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_DailyLog_Measure" ON datamart."Extract_LOG" USING btree ("Measure");


--
-- Name: IX_MIB_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_MIB_Schedule_Id" ON datamart."MSH_INPUTS_BIDZONE_TP" USING btree (schedule_id);


--
-- Name: IX_MRBT_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_MRBT_Schedule_Id" ON datamart."MSH_RESULTS_BLOCK_TP" USING btree (schedule_id);


--
-- Name: IX_ScheduleId_MSH_RESULTS_PSU_TP; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_ScheduleId_MSH_RESULTS_PSU_TP" ON datamart."MSH_RESULTS_PSU_TP" USING btree (schedule_id);


--
-- Name: IX_TBP_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_TBP_Schedule_Id" ON datamart."TRA_BLOCK_TP" USING btree (schedule_id);


--
-- Name: IX_TPT_ScheduleId; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_TPT_ScheduleId" ON datamart."TRA_PSU_TP" USING btree (schedule_id);


--
-- Name: IX_TTDD_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "IX_TTDD_Schedule_Id" ON datamart."TRA_TRANSITION_DATA_DST_TP" USING btree (schedule_id);


--
-- Name: MIP_IX_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "MIP_IX_Schedule_Id" ON datamart."MSH_INPUTS_PSU" USING btree (schedule_id);


--
-- Name: OfferDataDay_IX_ScheduleId; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "OfferDataDay_IX_ScheduleId" ON datamart."TRA_OFFER_DATA_DAY" USING btree (schedule_id);


--
-- Name: SDEPT_IX_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "SDEPT_IX_Schedule_Id" ON datamart."SET_DAILY_ENERGY_PSU_TD" USING btree (schedule_id);


--
-- Name: TODP_IX_Schedule_Id; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "TODP_IX_Schedule_Id" ON datamart."TRA_OFFER_DATA_TP" USING btree (schedule_id);


--
-- Name: TransactionDataTP_IX_ScheduleID; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "TransactionDataTP_IX_ScheduleID" ON datamart."TRA_TRANSITION_DATA_TP" USING btree (schedule_id);


--
-- Name: TransitionDataDay_IX_ScheduleId; Type: INDEX; Schema: datamart; Owner: postgres
--

CREATE INDEX "TransitionDataDay_IX_ScheduleId" ON datamart."TRA_TRANSITION_DATA_DAY" USING btree (schedule_id);


--
-- Name: SET_ANNUAL_SCARCITY_FACILITY FK_STD_BID_ZONE; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_ANNUAL_SCARCITY_FACILITY"
    ADD CONSTRAINT "FK_STD_BID_ZONE" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_DAY ID_FUEL_TYPE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_DAY"
    ADD CONSTRAINT "ID_FUEL_TYPE_fk" FOREIGN KEY ("ID_FUEL_TYPE") REFERENCES datamart."STD_FUEL_TYPE"("ID_FUEL_TYPE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRADING_PERIOD Month_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRADING_PERIOD"
    ADD CONSTRAINT "Month_fk" FOREIGN KEY ("ID_Month") REFERENCES datamart."Month"("ID_Month") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_BID_ZONE Month_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_BID_ZONE"
    ADD CONSTRAINT "Month_fk" FOREIGN KEY ("ID_Month") REFERENCES datamart."Month"("ID_Month") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_BLOCK Month_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_BLOCK"
    ADD CONSTRAINT "Month_fk" FOREIGN KEY ("ID_Month") REFERENCES datamart."Month"("ID_Month") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_FACILITY Month_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_FACILITY"
    ADD CONSTRAINT "Month_fk" FOREIGN KEY ("ID_Month") REFERENCES datamart."Month"("ID_Month") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_ENERGY_FACILITY Month_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_ENERGY_FACILITY"
    ADD CONSTRAINT "Month_fk" FOREIGN KEY ("ID_Month") REFERENCES datamart."Month"("ID_Month") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: PAR_BIDZONE STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."PAR_BIDZONE"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_GENSET_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_GENSET_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_BIDZONE_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_BIDZONE_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_FACILITY STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_FACILITY"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_PSU STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_PSU"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_PSU_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_PSU_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_BLOCK_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_BLOCK_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_ENERGY_PSU_TD STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_ENERGY_PSU_TD"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_PRODUCTION_BLOCK_DAY STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_PRODUCTION_BLOCK_DAY"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_SCARCITY_PSU_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_SCARCITY_PSU_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_BID_ZONE STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_BID_ZONE"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_BLOCK STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_BLOCK"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_FACILITY STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_FACILITY"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_PRODUCTION_BLOCK_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_PRODUCTION_BLOCK_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_FACILITY_DAY STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_FACILITY_DAY"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_ENERGY_FACILITY STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_ENERGY_FACILITY"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_PSU_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_PSU_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_BLOCK_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_BLOCK_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_DAY STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_DAY"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_SCARCITY_BID_ZONE_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_SCARCITY_BID_ZONE_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_RESULTS_BID_ZONE_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_RESULTS_BID_ZONE_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_BID_ZONE_TP STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_BID_ZONE_TP"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_INPUTS_FACILITY STD_BID_ZONE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_INPUTS_FACILITY"
    ADD CONSTRAINT "STD_BID_ZONE_fk" FOREIGN KEY ("ID_BID_ZONE") REFERENCES datamart."STD_BID_ZONE"("ID_BID_ZONE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_EXPORTS_CORRIDOR_TP STD_CORRIDOR_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_EXPORTS_CORRIDOR_TP"
    ADD CONSTRAINT "STD_CORRIDOR_fk" FOREIGN KEY ("ID_CORRIDOR") REFERENCES datamart."STD_CORRIDOR"("ID_CORRIDOR") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_BLOCK_TP STD_FUEL_TYPE_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_BLOCK_TP"
    ADD CONSTRAINT "STD_FUEL_TYPE_fk" FOREIGN KEY ("ID_FUEL_TYPE") REFERENCES datamart."STD_FUEL_TYPE"("ID_FUEL_TYPE") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_GENSET_TP STD_GENSET_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_GENSET_TP"
    ADD CONSTRAINT "STD_GENSET_fk" FOREIGN KEY ("ID_GENSET") REFERENCES datamart."STD_GENSET"("ID_GENSET") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_METER_DATA_TP STD_GENSET_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_METER_DATA_TP"
    ADD CONSTRAINT "STD_GENSET_fk" FOREIGN KEY ("ID_GENSET") REFERENCES datamart."STD_GENSET"("ID_GENSET") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_GENSET_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_GENSET_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_FACILITY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_FACILITY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_PSU STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_PSU"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_PSU_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_PSU_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_BLOCK_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_BLOCK_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_ENERGY_PSU_TD STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_ENERGY_PSU_TD"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_PRODUCTION_BLOCK_DAY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_PRODUCTION_BLOCK_DAY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_SCARCITY_PSU_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_SCARCITY_PSU_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_BLOCK STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_BLOCK"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_FACILITY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_FACILITY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_METER_DATA_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_METER_DATA_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_DST_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_DST_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_PRODUCTION_BLOCK_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_PRODUCTION_BLOCK_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_FACILITY_DAY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_FACILITY_DAY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_ENERGY_FACILITY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_ENERGY_FACILITY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_ANNUAL_SCARCITY_FACILITY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_ANNUAL_SCARCITY_FACILITY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_PSU_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_PSU_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_BLOCK_TP STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_BLOCK_TP"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_DAY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_DAY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_DAY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_DAY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_INPUTS_FACILITY STD_MARKET_PARTY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_INPUTS_FACILITY"
    ADD CONSTRAINT "STD_MARKET_PARTY_fk" FOREIGN KEY ("ID_MARKET_PARTY") REFERENCES datamart."STD_MARKET_PARTY"("ID_MARKET_PARTY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_METER_DATA_TP STD_METER_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_METER_DATA_TP"
    ADD CONSTRAINT "STD_METER_fk" FOREIGN KEY ("ID_METER") REFERENCES datamart."STD_METER"("ID_METER") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_ENERGY_FACILITY STD_OPRODFAC_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_ENERGY_FACILITY"
    ADD CONSTRAINT "STD_OPRODFAC_fk" FOREIGN KEY ("ID_PRODUCTION_FACILITY") REFERENCES datamart."STD_PRODUCTION_FACILITY"("ID_PRODUCTION_FACILITY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_BLOCK_TP STD_PRODUCTION_BLOCK_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_BLOCK_TP"
    ADD CONSTRAINT "STD_PRODUCTION_BLOCK_fk" FOREIGN KEY ("ID_PRODUCTION_BLOCK") REFERENCES datamart."STD_PRODUCTION_BLOCK"("ID_PRODUCTION_BLOCK") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_PRODUCTION_BLOCK_DAY STD_PRODUCTION_BLOCK_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_PRODUCTION_BLOCK_DAY"
    ADD CONSTRAINT "STD_PRODUCTION_BLOCK_fk" FOREIGN KEY ("ID_PRODUCTION_BLOCK") REFERENCES datamart."STD_PRODUCTION_BLOCK"("ID_PRODUCTION_BLOCK") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_BLOCK STD_PRODUCTION_BLOCK_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_BLOCK"
    ADD CONSTRAINT "STD_PRODUCTION_BLOCK_fk" FOREIGN KEY ("ID_PRODUCTION_BLOCK") REFERENCES datamart."STD_PRODUCTION_BLOCK"("ID_PRODUCTION_BLOCK") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_PRODUCTION_BLOCK_TP STD_PRODUCTION_BLOCK_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_PRODUCTION_BLOCK_TP"
    ADD CONSTRAINT "STD_PRODUCTION_BLOCK_fk" FOREIGN KEY ("ID_PRODUCTION_BLOCK") REFERENCES datamart."STD_PRODUCTION_BLOCK"("ID_PRODUCTION_BLOCK") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_BLOCK_TP STD_PRODUCTION_BLOCK_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_BLOCK_TP"
    ADD CONSTRAINT "STD_PRODUCTION_BLOCK_fk" FOREIGN KEY ("ID_PRODUCTION_BLOCK") REFERENCES datamart."STD_PRODUCTION_BLOCK"("ID_PRODUCTION_BLOCK") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_FACILITY STD_PRODUCTION_FACILITY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_FACILITY"
    ADD CONSTRAINT "STD_PRODUCTION_FACILITY_fk" FOREIGN KEY ("ID_PRODUCTION_FACILITY") REFERENCES datamart."STD_PRODUCTION_FACILITY"("ID_PRODUCTION_FACILITY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_FACILITY STD_PRODUCTION_FACILITY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_FACILITY"
    ADD CONSTRAINT "STD_PRODUCTION_FACILITY_fk" FOREIGN KEY ("ID_PRODUCTION_FACILITY") REFERENCES datamart."STD_PRODUCTION_FACILITY"("ID_PRODUCTION_FACILITY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_FACILITY_DAY STD_PRODUCTION_FACILITY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_FACILITY_DAY"
    ADD CONSTRAINT "STD_PRODUCTION_FACILITY_fk" FOREIGN KEY ("ID_PRODUCTION_FACILITY") REFERENCES datamart."STD_PRODUCTION_FACILITY"("ID_PRODUCTION_FACILITY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_ANNUAL_SCARCITY_FACILITY STD_PRODUCTION_FACILITY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_ANNUAL_SCARCITY_FACILITY"
    ADD CONSTRAINT "STD_PRODUCTION_FACILITY_fk" FOREIGN KEY ("ID_PRODUCTION_FACILITY") REFERENCES datamart."STD_PRODUCTION_FACILITY"("ID_PRODUCTION_FACILITY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_INPUTS_FACILITY STD_PRODUCTION_FACILITY_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_INPUTS_FACILITY"
    ADD CONSTRAINT "STD_PRODUCTION_FACILITY_fk" FOREIGN KEY ("ID_PRODUCTION_FACILITY") REFERENCES datamart."STD_PRODUCTION_FACILITY"("ID_PRODUCTION_FACILITY") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_DAY STD_PROD_BLOCK_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_DAY"
    ADD CONSTRAINT "STD_PROD_BLOCK_fk" FOREIGN KEY ("ID_PRODUCTION_BLOCK") REFERENCES datamart."STD_PRODUCTION_BLOCK"("ID_PRODUCTION_BLOCK") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_TP STD_PSU_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_TP"
    ADD CONSTRAINT "STD_PSU_fk" FOREIGN KEY ("ID_PSU") REFERENCES datamart."STD_PSU"("ID_PSU") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_PSU STD_PSU_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_PSU"
    ADD CONSTRAINT "STD_PSU_fk" FOREIGN KEY ("ID_PSU") REFERENCES datamart."STD_PSU"("ID_PSU") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_PSU_TP STD_PSU_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_PSU_TP"
    ADD CONSTRAINT "STD_PSU_fk" FOREIGN KEY ("ID_PSU") REFERENCES datamart."STD_PSU"("ID_PSU") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_ENERGY_PSU_TD STD_PSU_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_ENERGY_PSU_TD"
    ADD CONSTRAINT "STD_PSU_fk" FOREIGN KEY ("ID_PSU") REFERENCES datamart."STD_PSU"("ID_PSU") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_SCARCITY_PSU_TP STD_PSU_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_SCARCITY_PSU_TP"
    ADD CONSTRAINT "STD_PSU_fk" FOREIGN KEY ("ID_PSU") REFERENCES datamart."STD_PSU"("ID_PSU") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_PSU_TP STD_PSU_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_PSU_TP"
    ADD CONSTRAINT "STD_PSU_fk" FOREIGN KEY ("ID_PSU") REFERENCES datamart."STD_PSU"("ID_PSU") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_DAY STD_PSU_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_DAY"
    ADD CONSTRAINT "STD_PSU_fk" FOREIGN KEY ("ID_PSU") REFERENCES datamart."STD_PSU"("ID_PSU") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_TP STD_TRANSITION_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_TP"
    ADD CONSTRAINT "STD_TRANSITION_fk" FOREIGN KEY ("ID_TRANSITION") REFERENCES datamart."STD_TRANSITION"("ID_TRANSITION") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_DST_TP STD_TRANSITION_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_DST_TP"
    ADD CONSTRAINT "STD_TRANSITION_fk" FOREIGN KEY ("ID_TRANSITION") REFERENCES datamart."STD_TRANSITION"("ID_TRANSITION") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_DAY STD_TRANSITION_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_DAY"
    ADD CONSTRAINT "STD_TRANSITION_fk" FOREIGN KEY ("ID_TRANSITION") REFERENCES datamart."STD_TRANSITION"("ID_TRANSITION") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_GENSET_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_GENSET_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_EXPORTS_CORRIDOR_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_EXPORTS_CORRIDOR_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_BIDZONE_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_BIDZONE_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_FACILITY TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_FACILITY"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_INPUTS_PSU TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_INPUTS_PSU"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_PSU_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_PSU_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_BLOCK_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_BLOCK_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_ENERGY_PSU_TD TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_ENERGY_PSU_TD"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_PRODUCTION_BLOCK_DAY TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_PRODUCTION_BLOCK_DAY"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_SCARCITY_PSU_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_SCARCITY_PSU_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_METER_DATA_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_METER_DATA_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_DST_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_DST_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_PRODUCTION_BLOCK_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_PRODUCTION_BLOCK_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_FACILITY_DAY TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_FACILITY_DAY"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_PSU_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_PSU_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("TRADING_PERIOD", "DATE_KEY_OMAN") REFERENCES datamart."TRADING_PERIOD"("TRADING_PERIOD", "DATE_KEY_OMAN") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_BLOCK_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_BLOCK_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_TRANSITION_DATA_DAY TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_TRANSITION_DATA_DAY"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_OFFER_DATA_DAY TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_OFFER_DATA_DAY"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_DAILY_SCARCITY_BID_ZONE_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_DAILY_SCARCITY_BID_ZONE_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_RESULTS_BID_ZONE_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_RESULTS_BID_ZONE_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: MSH_RESULTS_BID_ZONE_TP TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."MSH_RESULTS_BID_ZONE_TP"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRA_INPUTS_FACILITY TRADING_PERIOD_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRA_INPUTS_FACILITY"
    ADD CONSTRAINT "TRADING_PERIOD_fk" FOREIGN KEY ("DATE_KEY_OMAN", "TRADING_PERIOD") REFERENCES datamart."TRADING_PERIOD"("DATE_KEY_OMAN", "TRADING_PERIOD") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: TRADING_PERIOD Year_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."TRADING_PERIOD"
    ADD CONSTRAINT "Year_fk" FOREIGN KEY ("ID_Year") REFERENCES datamart."Year"("ID_Year") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_BID_ZONE Year_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_BID_ZONE"
    ADD CONSTRAINT "Year_fk" FOREIGN KEY ("ID_Year") REFERENCES datamart."Year"("ID_Year") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_BLOCK Year_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_BLOCK"
    ADD CONSTRAINT "Year_fk" FOREIGN KEY ("ID_Year") REFERENCES datamart."Year"("ID_Year") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_SCARCITY_FACILITY Year_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_SCARCITY_FACILITY"
    ADD CONSTRAINT "Year_fk" FOREIGN KEY ("ID_Year") REFERENCES datamart."Year"("ID_Year") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_MONTHLY_ENERGY_FACILITY Year_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_MONTHLY_ENERGY_FACILITY"
    ADD CONSTRAINT "Year_fk" FOREIGN KEY ("ID_Year") REFERENCES datamart."Year"("ID_Year") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: SET_ANNUAL_SCARCITY_FACILITY Year_fk; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."SET_ANNUAL_SCARCITY_FACILITY"
    ADD CONSTRAINT "Year_fk" FOREIGN KEY ("ID_Year") REFERENCES datamart."Year"("ID_Year") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: Month fk_year; Type: FK CONSTRAINT; Schema: datamart; Owner: postgres
--

ALTER TABLE ONLY datamart."Month"
    ADD CONSTRAINT fk_year FOREIGN KEY ("ID_YEAR") REFERENCES datamart."Year"("ID_Year");


--
-- Name: SCHEMA datamart; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA datamart TO readonly;
GRANT USAGE ON SCHEMA datamart TO rorole;


--
-- Name: TABLE "Extract_LOG"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."Extract_LOG" TO rorole;


--
-- Name: TABLE "DailyExtractLOG"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."DailyExtractLOG" TO rorole;


--
-- Name: TABLE "MSH_INPUTS_BIDZONE_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."MSH_INPUTS_BIDZONE_TP" TO rorole;
GRANT SELECT ON TABLE datamart."MSH_INPUTS_BIDZONE_TP" TO readonly;


--
-- Name: TABLE "MSH_INPUTS_FACILITY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."MSH_INPUTS_FACILITY" TO rorole;
GRANT SELECT ON TABLE datamart."MSH_INPUTS_FACILITY" TO readonly;


--
-- Name: TABLE "MSH_INPUTS_PSU"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."MSH_INPUTS_PSU" TO rorole;
GRANT SELECT ON TABLE datamart."MSH_INPUTS_PSU" TO readonly;


--
-- Name: TABLE "MSH_RESULTS_BID_ZONE_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."MSH_RESULTS_BID_ZONE_TP" TO readonly;
GRANT SELECT ON TABLE datamart."MSH_RESULTS_BID_ZONE_TP" TO rorole;


--
-- Name: TABLE "MSH_RESULTS_BLOCK_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."MSH_RESULTS_BLOCK_TP" TO rorole;
GRANT SELECT ON TABLE datamart."MSH_RESULTS_BLOCK_TP" TO readonly;


--
-- Name: TABLE "MSH_RESULTS_PSU_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."MSH_RESULTS_PSU_TP" TO rorole;
GRANT SELECT ON TABLE datamart."MSH_RESULTS_PSU_TP" TO readonly;


--
-- Name: TABLE "Month"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."Month" TO rorole;
GRANT SELECT ON TABLE datamart."Month" TO readonly;


--
-- Name: TABLE "PAR_BIDZONE"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."PAR_BIDZONE" TO rorole;
GRANT SELECT ON TABLE datamart."PAR_BIDZONE" TO readonly;


--
-- Name: TABLE "PAR_BIDZONE_ADM_PRICE"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."PAR_BIDZONE_ADM_PRICE" TO rorole;


--
-- Name: TABLE "SET_ANNUAL_SCARCITY_FACILITY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_ANNUAL_SCARCITY_FACILITY" TO readonly;
GRANT SELECT ON TABLE datamart."SET_ANNUAL_SCARCITY_FACILITY" TO rorole;


--
-- Name: TABLE "SET_DAILY_ENERGY_PSU_TD"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_DAILY_ENERGY_PSU_TD" TO rorole;
GRANT SELECT ON TABLE datamart."SET_DAILY_ENERGY_PSU_TD" TO readonly;


--
-- Name: TABLE "SET_DAILY_FACILITY_DAY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_DAILY_FACILITY_DAY" TO readonly;
GRANT SELECT ON TABLE datamart."SET_DAILY_FACILITY_DAY" TO rorole;


--
-- Name: TABLE "SET_DAILY_PRODUCTION_BLOCK_DAY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" TO rorole;
GRANT SELECT ON TABLE datamart."SET_DAILY_PRODUCTION_BLOCK_DAY" TO readonly;


--
-- Name: TABLE "SET_DAILY_PRODUCTION_BLOCK_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_DAILY_PRODUCTION_BLOCK_TP" TO rorole;
GRANT SELECT ON TABLE datamart."SET_DAILY_PRODUCTION_BLOCK_TP" TO readonly;


--
-- Name: TABLE "SET_DAILY_SCARCITY_BID_ZONE_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_DAILY_SCARCITY_BID_ZONE_TP" TO readonly;
GRANT SELECT ON TABLE datamart."SET_DAILY_SCARCITY_BID_ZONE_TP" TO rorole;


--
-- Name: TABLE "SET_DAILY_SCARCITY_PSU_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_DAILY_SCARCITY_PSU_TP" TO rorole;
GRANT SELECT ON TABLE datamart."SET_DAILY_SCARCITY_PSU_TP" TO readonly;


--
-- Name: TABLE "SET_METER_DATA_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_METER_DATA_TP" TO rorole;
GRANT SELECT ON TABLE datamart."SET_METER_DATA_TP" TO readonly;


--
-- Name: TABLE "SET_MONTHLY_ENERGY_FACILITY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_MONTHLY_ENERGY_FACILITY" TO readonly;
GRANT SELECT ON TABLE datamart."SET_MONTHLY_ENERGY_FACILITY" TO rorole;


--
-- Name: TABLE "SET_MONTHLY_SCARCITY_BID_ZONE"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_MONTHLY_SCARCITY_BID_ZONE" TO rorole;
GRANT SELECT ON TABLE datamart."SET_MONTHLY_SCARCITY_BID_ZONE" TO readonly;


--
-- Name: TABLE "SET_MONTHLY_SCARCITY_BLOCK"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_MONTHLY_SCARCITY_BLOCK" TO rorole;
GRANT SELECT ON TABLE datamart."SET_MONTHLY_SCARCITY_BLOCK" TO readonly;


--
-- Name: TABLE "SET_MONTHLY_SCARCITY_FACILITY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."SET_MONTHLY_SCARCITY_FACILITY" TO rorole;
GRANT SELECT ON TABLE datamart."SET_MONTHLY_SCARCITY_FACILITY" TO readonly;


--
-- Name: TABLE "STD_BID_ZONE"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_BID_ZONE" TO rorole;
GRANT SELECT ON TABLE datamart."STD_BID_ZONE" TO readonly;


--
-- Name: TABLE "STD_CONTROL_AREA"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_CONTROL_AREA" TO rorole;
GRANT SELECT ON TABLE datamart."STD_CONTROL_AREA" TO readonly;


--
-- Name: TABLE "STD_CORRIDOR"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_CORRIDOR" TO rorole;
GRANT SELECT ON TABLE datamart."STD_CORRIDOR" TO readonly;


--
-- Name: TABLE "STD_FUEL_TYPE"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_FUEL_TYPE" TO rorole;
GRANT SELECT ON TABLE datamart."STD_FUEL_TYPE" TO readonly;


--
-- Name: TABLE "STD_GENSET"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_GENSET" TO rorole;
GRANT SELECT ON TABLE datamart."STD_GENSET" TO readonly;


--
-- Name: TABLE "STD_MARKET_PARTY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_MARKET_PARTY" TO rorole;
GRANT SELECT ON TABLE datamart."STD_MARKET_PARTY" TO readonly;


--
-- Name: TABLE "STD_METER"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_METER" TO rorole;
GRANT SELECT ON TABLE datamart."STD_METER" TO readonly;


--
-- Name: TABLE "STD_PRODUCTION_BLOCK"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_PRODUCTION_BLOCK" TO rorole;
GRANT SELECT ON TABLE datamart."STD_PRODUCTION_BLOCK" TO readonly;


--
-- Name: TABLE "STD_PRODUCTION_FACILITY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_PRODUCTION_FACILITY" TO rorole;
GRANT SELECT ON TABLE datamart."STD_PRODUCTION_FACILITY" TO readonly;


--
-- Name: TABLE "STD_PSU"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_PSU" TO rorole;
GRANT SELECT ON TABLE datamart."STD_PSU" TO readonly;


--
-- Name: TABLE "STD_PSU_CONFIG"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_PSU_CONFIG" TO rorole;
GRANT SELECT ON TABLE datamart."STD_PSU_CONFIG" TO readonly;


--
-- Name: TABLE "STD_TRANSITION"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_TRANSITION" TO rorole;
GRANT SELECT ON TABLE datamart."STD_TRANSITION" TO readonly;


--
-- Name: TABLE "STD_TRANSITION_MATRIX"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."STD_TRANSITION_MATRIX" TO rorole;
GRANT SELECT ON TABLE datamart."STD_TRANSITION_MATRIX" TO readonly;


--
-- Name: TABLE "TRADING_PERIOD"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRADING_PERIOD" TO rorole;
GRANT SELECT ON TABLE datamart."TRADING_PERIOD" TO readonly;


--
-- Name: TABLE "TRA_BLOCK_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_BLOCK_TP" TO readonly;
GRANT SELECT ON TABLE datamart."TRA_BLOCK_TP" TO rorole;


--
-- Name: TABLE "TRA_EXPORTS_CORRIDOR_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_EXPORTS_CORRIDOR_TP" TO rorole;
GRANT SELECT ON TABLE datamart."TRA_EXPORTS_CORRIDOR_TP" TO readonly;


--
-- Name: TABLE "TRA_GENSET_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_GENSET_TP" TO rorole;
GRANT SELECT ON TABLE datamart."TRA_GENSET_TP" TO readonly;


--
-- Name: TABLE "TRA_INPUTS_FACILITY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_INPUTS_FACILITY" TO readonly;
GRANT SELECT ON TABLE datamart."TRA_INPUTS_FACILITY" TO rorole;


--
-- Name: TABLE "TRA_OFFER_DATA_DAY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_OFFER_DATA_DAY" TO readonly;
GRANT SELECT ON TABLE datamart."TRA_OFFER_DATA_DAY" TO rorole;


--
-- Name: TABLE "TRA_OFFER_DATA_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_OFFER_DATA_TP" TO rorole;
GRANT SELECT ON TABLE datamart."TRA_OFFER_DATA_TP" TO readonly;


--
-- Name: TABLE "TRA_PSU_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_PSU_TP" TO readonly;
GRANT SELECT ON TABLE datamart."TRA_PSU_TP" TO rorole;


--
-- Name: TABLE "TRA_RESULTS_BID_ZONE_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_RESULTS_BID_ZONE_TP" TO readonly;
GRANT SELECT ON TABLE datamart."TRA_RESULTS_BID_ZONE_TP" TO rorole;


--
-- Name: TABLE "TRA_TRANSITION_DATA_DAY"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_TRANSITION_DATA_DAY" TO readonly;
GRANT SELECT ON TABLE datamart."TRA_TRANSITION_DATA_DAY" TO rorole;


--
-- Name: TABLE "TRA_TRANSITION_DATA_DST_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_TRANSITION_DATA_DST_TP" TO rorole;
GRANT SELECT ON TABLE datamart."TRA_TRANSITION_DATA_DST_TP" TO readonly;


--
-- Name: TABLE "TRA_TRANSITION_DATA_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."TRA_TRANSITION_DATA_TP" TO rorole;
GRANT SELECT ON TABLE datamart."TRA_TRANSITION_DATA_TP" TO readonly;


--
-- Name: TABLE "Year"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."Year" TO rorole;
GRANT SELECT ON TABLE datamart."Year" TO readonly;


--
-- Name: TABLE "vw_Standing_Data"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."vw_Standing_Data" TO rorole;
GRANT SELECT ON TABLE datamart."vw_Standing_Data" TO readonly;


--
-- Name: TABLE "vw_TRA_OFFER_DATA_TP"; Type: ACL; Schema: datamart; Owner: postgres
--

GRANT SELECT ON TABLE datamart."vw_TRA_OFFER_DATA_TP" TO readonly;
GRANT SELECT ON TABLE datamart."vw_TRA_OFFER_DATA_TP" TO rorole;


--
-- Name: TABLE vw_xtabschedulevaluenumber; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.vw_xtabschedulevaluenumber TO rorole;


--
-- Name: TABLE vw_xtabschedulevaluestring; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.vw_xtabschedulevaluestring TO rorole;


--
-- PostgreSQL database dump complete
--

