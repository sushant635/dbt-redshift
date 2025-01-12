{{
    config(
        materialized='incremental',
        alias='stg_fact_loanpayment',
        schema=var('silver_schema'),
        unique_key = 'payment_id',
        incremental_stragey='delete+insert',
        primary_key='payment_id',
        sort_key='payment_id',
        distribution='even'
    )
}}


select 
payment_id,date_id,loan_id,customer_id,payment_amount,payment_status,
getdate() as created_at
from {{var('bronze_schema')}}.ext_fact_loanpayment