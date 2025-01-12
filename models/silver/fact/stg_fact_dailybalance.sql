{{
    config(
        materialized='incremental',
        alias='stg_fact_dailybalance',
        schema=var('silver_schema'),
        unique_key = 'balance_id',
        incremental_stragey='delete+insert',
        primary_key='balance_id',
        sort_key='balance_id',
        distribution='even'
    )
}}


select 
balance_id,date_id,account_id,opening_balance,closing_balance,average_balance,
getdate() as created_at
from {{var('bronze_schema')}}.ext_fact_dailybalance