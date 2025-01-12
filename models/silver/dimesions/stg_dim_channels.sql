{{
    config(
        materialized='incremental',
        alias='stg_dim_channels',
        schema=var('silver_schema'),
        unique_key = 'channel_id',
        incremental_stragey='delete+insert'
    )
}}

select 
    channel_id,
    channel_name,
    getdate() as created_at
    from 
    {{var('bronze_schema')}}.ext_channels
