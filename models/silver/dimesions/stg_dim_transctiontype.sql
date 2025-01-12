{{
    config(
        materialized='incremental',
        alias='stg_dim_transactiontypes',
        schema=var('silver_schema'),
        unique_key = 'transaction_type_id',
        incremental_stragey='delete+insert'
    )
}}


select 
transaction_type_id,description, getdate() as created_at
from 
    {{var('bronze_schema')}}.ext_transactiontypes