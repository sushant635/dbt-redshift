{{
    config(
        materialized='incremental',
        alias='fact_investment',
        schema=var('gold_schema'),
        unique_key = 'account_number',
        incremental_stragey='delete+insert',
        primary_key='account_number',
        sort_key='account_number',
        distribution='even'
    )
}}


select sdc.full_name , sda.account_number ,sda.account_type ,sda.account_balance,sdit.investment_type_name,
sfi.amount ,sfi.investment_return , (sfi.amount  + sfi.investment_return ) as total_investment_amount,(sfi.amount + sda.account_balance) as total_amount
from {{var('silver_schema')}}.stg_fact_investment sfi 
join {{var('silver_schema')}}.stg_dim_account sda  on sfi.account_id = sda.account_id 
join {{var('silver_schema')}}.stg_dim_customer sdc on sda.customer_id  = sdc.customer_id 
join {{var('silver_schema')}}.stg_dim_investment_types sdit  on sfi.investment_type_id = sdit.investment_type_id 