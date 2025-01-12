{{
    config(
        materialized='incremental',
        alias='fact_dailybalance',
        schema=var('gold_schema'),
        unique_key = 'account_number',
        incremental_stragey='delete+insert',
        primary_key='account_number',
        sort_key='account_number',
        distribution='even'
    )
}}

select sdc.full_name ,sda.account_number,sda.account_type ,sda.account_balance,
sum(sfd.opening_balance ) over(partition by sfd.account_id order by sdd."day"  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_opening,
sum(sfd.closing_balance ) over(partition by sfd.account_id order by sdd."day"  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_closing,
sum(sfd.average_balance) over(partition by sfd.account_id order by sdd."day"  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_average
from {{var('silver_schema')}}.stg_fact_dailybalance sfd 
join {{var('silver_schema')}}.stg_dim_account sda on sfd.account_id = sda.account_id  
join {{var('silver_schema')}}.stg_dim_customer sdc  on sda.customer_id  = sdc.customer_id 
join {{var('silver_schema')}}.stg_dim_dates sdd  on sfd.date_id = sdd.date_id