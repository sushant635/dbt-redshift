{{
    config(
        materialized='incremental',
        alias='fact_transaction',
        schema=var('gold_schema'),
        unique_key = 'account_number',
        incremental_stragey='delete+insert',
        primary_key='account_number',
        sort_key='account_number',
        distribution='even'
    )
}}

select  a.full_name,a.phone_number,sda.account_number ,sda.account_type ,sdt.description,sum(sda.account_balance) as total_account_balance,
sum(sft.txn_amount )  as total_transaction_amount
from  {{var('silver_schema')}}.stg_fact_transaction sft  
join {{var('silver_schema')}}.stg_dim_customer a on sft.customer_id=a.customer_id
join {{var('silver_schema')}}.stg_dim_account sda  on sft.account_id=sda.account_id
join {{var('silver_schema')}}.stg_dim_transactiontypes sdt on sft.transaction_type_id = sdt.transaction_type_id 
where txn_status='Success'
group by 1,2,3,4,5