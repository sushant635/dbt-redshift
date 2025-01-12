{{
    config(
        materialized='incremental',
        alias='stg_dim_locations',
        schema=var('silver_schema'),
        unique_key = 'location_id',
        incremental_stragey='delete+insert'
    )
}}


select 
location_id,address,city,state,country,getdate() as created_at
from 
    {{var('bronze_schema')}}.ext_locations