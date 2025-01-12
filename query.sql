create external schema dev_bronze
from data catalog 
database "data-learning-redshift-dev"
iam_role 'arn:aws:iam::354918402553:role/service-role/AmazonRedshift-CommandsAccessRole-20250109T195824'
region 'us-east-1';

select 
customer_id,
first_name,
last_name,
concat(concat(first_name,' '), last_name) as full_name,
email,
address,
city,
state,
postal_code,
phone_number,
getdate() as created_at
from 
dev_bronze.ext_customers

select  a.full_name,a.phone_number,sda.account_number ,sda.account_type ,sdt.description,sum(sda.account_balance) as total_account_balance,
sum(sft.txn_amount )  as total_transaction_amount
from  dev_bronze_dev_silver.stg_fact_transaction sft  
join dev_bronze_dev_silver.stg_dim_customer a on sft.customer_id=a.customer_id
join dev_bronze_dev_silver.stg_dim_account sda  on sft.account_id=sda.account_id
join dev_bronze_dev_silver.stg_dim_transactiontypes sdt on sft.transaction_type_id = sdt.transaction_type_id 
where txn_status='Success'
group by 1,2,3,4,5


select sdc.full_name ,sda.account_number,sda.account_type ,sda.account_balance,
sum(sfd.opening_balance ) over(partition by sfd.account_id order by sdd."day"  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_opening,
sum(sfd.closing_balance ) over(partition by sfd.account_id order by sdd."day"  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_closing,
sum(sfd.average_balance) over(partition by sfd.account_id order by sdd."day"  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_average
from dev_bronze_dev_silver.stg_fact_dailybalance sfd 
join dev_bronze_dev_silver.stg_dim_account sda on sfd.account_id = sda.account_id  
join dev_bronze_dev_silver.stg_dim_customer sdc  on sda.customer_id  = sdc.customer_id 
join dev_bronze_dev_silver.stg_dim_dates sdd  on sfd.date_id = sdd.date_id

select sdc.full_name , sda.account_number ,sda.account_type ,sda.account_balance,sdit.investment_type_name,
sfi.amount ,sfi.investment_return , (sfi.amount  + sfi.investment_return ) as total_investment_amount,(sfi.amount + sda.account_balance) as total_amount
from dev_bronze_dev_silver.stg_fact_investment sfi 
join dev_bronze_dev_silver.stg_dim_account sda  on sfi.account_id = sda.account_id 
join dev_bronze_dev_silver.stg_dim_customer sdc on sda.customer_id  = sdc.customer_id 
join dev_bronze_dev_silver.stg_dim_investment_types sdit  on sfi.investment_type_id = sdit.investment_type_id 


select sdc.full_name,sdl.loan_id ,sdc.phone_number,sdl.loan_type ,sdl.loan_amount ,sdl.interest_rate,sum(sfl.payment_amount ) as total_amount_paind   from dev_bronze_dev_silver.stg_fact_loanpayment sfl 
join dev_bronze_dev_silver.stg_dim_customer sdc  on sfl.customer_id = sdc.customer_id 
join dev_bronze_dev_silver.stg_dim_loans sdl on sfl.loan_id = sdl.loan_id 
where sfl.payment_status ='Completed'
group by 1,2,3,4,5,6


select * from dev_bronze_dev_gold.fact_loanpayments fl 





