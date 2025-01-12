{{
    config(
        materialized='incremental',
        alias='stg_fact_customer_interactions',
        schema=var('silver_schema'),
        unique_key = 'interaction_id',
        incremental_stragey='delete+insert',
        primary_key='interaction_id',
        distribution='even'
    )
}}


select 
interaction_id,
date_id,
account_id,
channel_id,
interaction_type,
interaction_rating,
getdate() as created_at
from {{var('bronze_schema')}}.ext_fact_customerinteractions