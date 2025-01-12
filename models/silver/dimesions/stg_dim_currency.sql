{{
    config(
        materialized='incremental',
        alias='stg_dim_currency',
        schema=var('silver_schema'),
        unique_key = 'currency_id',
        incremental_stragey='delete+insert'
    )
}}


select 
currency_id ,name,
iso3_code,
active,
getdate() as created_at
from 
    {{var('bronze_schema')}}.ext_currency