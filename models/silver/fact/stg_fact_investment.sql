{{
    config(
        materialized='incremental',
        alias='stg_fact_investment',
        schema=var('silver_schema'),
        unique_key = 'investment_id',
        incremental_stragey='delete+insert',
        primary_key='investment_id',
        sort_key='investment_id',
        distribution='even'
    )
}}


select 
investment_id,date_id,investment_type_id,account_id,amount,investment_return,
getdate() as created_at
from {{var('bronze_schema')}}.ext_fact_investment