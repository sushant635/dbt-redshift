{{
    config(
        materialized='incremental',
        alias='stg_dim_dates',
        schema=var('silver_schema'),
        unique_key = 'date_id',
        incremental_stragey='delete+insert'
    )
}}


select 
date_id,
"day",
"month",
"year",
quarter,
weekday,
getdate() as created_at
from 
    {{var('bronze_schema')}}.ext_dates