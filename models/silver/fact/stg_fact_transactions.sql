{{
    config(
        materialized='incremental',
        alias='stg_fact_transaction',
        schema=var('silver_schema'),
        unique_key = 'transaction_id',
        incremental_stragey='delete+insert',
        primary_key='transaction_id',
        sort_key='transaction_id',
        distribution='even'
    )
}}


select 
transaction_id,
customer_id,
date_id,
channel_id,
account_id,
transaction_type_id,
location_id,
currency_id,
txn_amount,
txn_status,
getdate() as created_at
from {{var('bronze_schema')}}.ext_fact_transaction