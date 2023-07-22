with reviewers as (

    select
    name,
    bio

    from {{ ref('stg_critics')}}
)

select * from reviewers