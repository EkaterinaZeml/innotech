
CREATE TABLE oltp_src_system.order_data (
	order_id int4 NULL,
	status_nm text NULL,
	create_dttm timestamp NULL,
	update_dttm timestamp NULL
);

-- Table Triggers

create trigger order_data_changes after
insert
    or
delete
    or
update
    on
    oltp_src_system.order_data for each row execute function oltp_cdc_src_system.order_data_changes();

-- Permissions

ALTER TABLE oltp_src_system.order_data OWNER TO "Student09";
GRANT ALL ON TABLE oltp_src_system.order_data TO "Student09";
GRANT ALL ON TABLE oltp_src_system.order_data TO airflow;

/**********************************************************************************************************************************/
/**********************************************************************************************************************************/
CREATE OR REPLACE FUNCTION oltp_src_system.create_order()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
BEGIN
insert into oltp_src_system.order_data (order_id,status_nm,create_dttm, update_dttm)
select order_id
     , 'created' status_nm
     , now() create_dttm
     , now() update_dttm
  from (select n order_id
          from generate_series((select coalesce((select max(order_id) 
                                  from oltp_src_system.order_data) + 1, 1)
                                )
                              , (select coalesce((select max(order_id) 
                                                    from oltp_src_system.order_data) + 1, 1)
                              ) + round(random()*4)::int
                    ) n) nn;

    get diagnostics v_rc = row_count;           
    raise notice '% rows inserted into order_data',v_rc;
    return true;

end $function$
;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/
CREATE OR REPLACE FUNCTION oltp_src_system.delete_order()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
BEGIN                   
	delete from oltp_src_system.order_data
	where order_id in (
			select order_id
               from (select order_id 
                          , round(random()*10) rnd /*формируем метку на удаление*/
                       from oltp_src_system.order_data
                    ) rnd_tbl
              where (rnd - floor(rnd/10)) = 1 /*and status_nm = 'closed'*/ 
            );
	get diagnostics v_rc = row_count;

	raise notice '% rows deleted into order_data',v_rc;
	
	if v_rc > 0 then
		return true;
	else
		return false;
	end if;

end $function$
;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/
CREATE OR REPLACE FUNCTION oltp_src_system.update_order()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
BEGIN                   
	update oltp_src_system.order_data
	set status_nm = case (floor(random() * (5 - 1 + 1) + 1)::int)
	                     when 1 then 'formed'
	                     when 2 then 'sent'
	                     when 3 then 'delivered'
	                     when 4 then 'cancellation'
	                     when 5 then 'delay'
	                     else null
	                 end 
	    , update_dttm = now()
	where order_id in ( select order_id
						from 
						(
							select order_id 
			              		, round(random()*10) rnd
							from oltp_src_system.order_data
			          		where status_nm not in ('delivered', 'cancellation')
			        	) rnd_tbl
						where (rnd - floor(rnd/10)) = 1);
	
	get diagnostics v_rc = row_count;           
	raise notice '% rows updated order_data',v_rc;
	
	if v_rc > 0 then
		return true;
	else
		return false;
	end if;

end $function$
;

/**********************************************************************************************************************************/
/**********************************************************************************************************************************/

CREATE TABLE oltp_cdc_src_system.order_data_changes (
	order_id int4 NULL,
	status_nm text NULL,
	create_dttm timestamp NULL,
	update_dttm timestamp NULL,
	operation_cd bpchar(1) NOT NULL,
	updated_dttm timestamp NOT NULL
);

-- Permissions

ALTER TABLE oltp_cdc_src_system.order_data_changes OWNER TO "Student09";
GRANT ALL ON TABLE oltp_cdc_src_system.order_data_changes TO "Student09";
GRANT ALL ON TABLE oltp_cdc_src_system.order_data_changes TO airflow;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/
CREATE OR REPLACE FUNCTION oltp_cdc_src_system.order_data_changes()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    begin
        if (tg_op = 'DELETE') then
            insert into oltp_cdc_src_system.order_data_changes select old.*, 'D', now();
            return old;
        elsif (tg_op = 'UPDATE') then
            insert into oltp_cdc_src_system.order_data_changes select new.*, 'U', now();
            return new;
        elsif (tg_op = 'INSERT') then
            insert into oltp_cdc_src_system.order_data_changes select new.*, 'I', now();
            return new;
        end if;
        return null;
    end;
$function$
;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/
-- DROP TABLE dwh_stage.order_data_dwh_stage;

CREATE TABLE dwh_stage.order_data_dwh_stage (
	order_id int4 NULL,
	status_nm text NULL,
	create_dttm timestamp NULL,
	update_dttm timestamp NULL,
	operation_cd bpchar(1) NULL,
	updated_dttm timestamp NULL,
	hash bytea NULL GENERATED ALWAYS AS (digest(((((COALESCE(order_id::text, '#$%^&'::text) || COALESCE(status_nm, '#$%^&'::text)) || COALESCE(date_part('epoch'::text, create_dttm)::text, '#$%^&'::text)) || COALESCE(date_part('epoch'::text, update_dttm)::text, '#$%^&'::text)) || COALESCE(operation_cd, '#$%^&'::bpchar)::text) || COALESCE(date_part('epoch'::text, updated_dttm)::text, '#$%^&'::text), 'sha256'::text)) STORED
);

-- Permissions

ALTER TABLE dwh_stage.order_data_dwh_stage OWNER TO "Student09";
GRANT ALL ON TABLE dwh_stage.order_data_dwh_stage TO "Student09";
GRANT ALL ON TABLE dwh_stage.order_data_dwh_stage TO airflow;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/ добавить
CREATE OR REPLACE FUNCTION dwh_stage.load_cdc_order_data_changes()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    begin  
	    truncate table dwh_stage.order_data_dwh_stage;
        insert into dwh_stage.order_data_dwh_stage
        (
			order_id
			,status_nm
			,create_dttm
			,update_dttm
			,operation_cd
			,updated_dttm
		)
        select 
			order_id
			,status_nm
			,create_dttm
			,update_dttm
			,operation_cd
			,updated_dttm
		from oltp_cdc_src_system.order_data_changes t
		except
		select 
			order_id
			,status_nm
			,create_dttm
			,update_dttm
			,operation_cd
			,updated_dttm
		from dwh_ods.order_data_hist;             
        return true;
    end
$function$
;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/
-- DROP TABLE dwh_ods.order_data_hist;

CREATE TABLE dwh_ods.order_data_hist (
	order_id int4 NULL,
	status_nm text NULL,
	create_dttm timestamp NULL,
	update_dttm timestamp NULL,
	operation_cd bpchar(1) NULL,
	updated_dttm timestamp NULL,
	hash bytea NULL,
	valid_from_dttm timestamp NULL,
	valid_to_dttm timestamp NULL,
	deleted_flg bpchar(1) NULL,
	deleted_dttm timestamp NULL
);

-- Permissions

ALTER TABLE dwh_ods.order_data_hist OWNER TO "Student09";
GRANT ALL ON TABLE dwh_ods.order_data_hist TO "Student09";
GRANT ALL ON TABLE dwh_ods.order_data_hist TO airflow;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/
-- DROP TABLE dwh_ods.dim_date;

CREATE TABLE dwh_ods.dim_date (
	date_key int4 NOT NULL,
	date_actual date NOT NULL,
	"year" int4 NOT NULL,
	quarter int4 NOT NULL,
	"month" int4 NOT NULL,
	week int4 NOT NULL,
	day_of_month int4 NOT NULL,
	day_of_week int4 NOT NULL,
	is_weekday bool NOT NULL,
	is_holiday bool NOT NULL,
	fiscal_year int4 NOT NULL,
	CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);

-- Permissions

ALTER TABLE dwh_ods.dim_date OWNER TO "Student09";
GRANT ALL ON TABLE dwh_ods.dim_date TO "Student09";
GRANT SELECT ON TABLE dwh_ods.order_data_hist TO airflow;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/ добавить
CREATE OR REPLACE FUNCTION dwh_ods.load_order_data_hist()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$

declare

	v_load_dttm timestamp = now();
	
begin

	update dwh_ods.order_data_hist dst
		set valid_to_dttm = st.update_dttm 
	from dwh_stage.order_data_dwh_stage st
	where dst.order_id = st.order_id
		and st.operation_cd in ('U', 'D')
		and dst.valid_from_dttm = (select max(valid_from_dttm) from dwh_ods.order_data_hist where dst.order_id = order_id);	
	
	insert into dwh_ods.order_data_hist
	(
		order_id
		,status_nm
		,create_dttm
		,update_dttm
		,operation_cd
		,updated_dttm
		,hash
		,valid_from_dttm
		,valid_to_dttm
		,deleted_flg
		,deleted_dttm
	)
		select 
		st.order_id
		,st.status_nm
		,st.create_dttm
		,st.update_dttm
		,st.operation_cd
		,st.updated_dttm
		,st.hash	
		,st.update_dttm as valid_from_dttm
		,timestamp'2999-12-31 23:59:59' valid_to_dttm
		,case operation_cd
			when 'D' then '1'
			else '0'
		end deleted_flg
		,case operation_cd
			when 'D' then st.update_dttm
			else null
		end deleted_dttm
	from dwh_stage.order_data_dwh_stage st;
	
	return true;
end
$function$
;


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/


/**********************************************************************************************************************************/
/**********************************************************************************************************************************/