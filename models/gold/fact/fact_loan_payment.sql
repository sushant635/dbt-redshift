{{
    config(
        materialized='incremental',
        alias='fact_loanPayments',
        schema=var('gold_schema'),
        unique_key = 'loan_id',
        incremental_stragey='delete+insert',
        primary_key='loan_id',
        sort_key='loan_id',
        distribution='even'
    )
}}


select sdc.full_name,sdl.loan_id ,sdc.phone_number,sdl.loan_type ,sdl.loan_amount ,sdl.interest_rate,sum(sfl.payment_amount ) as total_amount_paind   from dev_bronze_dev_silver.stg_fact_loanpayment sfl 
join dev_bronze_dev_silver.stg_dim_customer sdc  on sfl.customer_id = sdc.customer_id 
join dev_bronze_dev_silver.stg_dim_loans sdl on sfl.loan_id = sdl.loan_id 
where sfl.payment_status ='Completed'
group by 1,2,3,4,5,6