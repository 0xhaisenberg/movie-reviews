with 

source as (

    select * from {{ source('staging', 'movie_critics') }}

),

critics as (

    select
        name,
        status,
        bio

    from source

)

select * from critics
