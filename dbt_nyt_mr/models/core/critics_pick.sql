with picks as (

    select title,
    description,
    mpaa_rating,
    opening_date

    from {{ ref('stg_movie_data')}}

    where recommendation = 1

)

select * from picks