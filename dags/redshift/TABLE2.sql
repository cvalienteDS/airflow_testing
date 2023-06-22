SELECT * FROM integrations.dwh_delay_hist
where TO_CHAR(Load_update_date,'YYYY-MM-DD') BETWEEN '{{ (ti.xcom_pull(task_ids="init", key="DATE_BEFORE_{}".format(params.table))) }}' AND '{{ data_interval_end | ds}}';

