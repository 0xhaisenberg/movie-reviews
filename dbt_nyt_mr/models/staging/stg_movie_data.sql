with 

source as (

    select * from {{ source('staging', 'movie_data') }}

),

movie_details as (

    select
        title,
        critic,
        description,
        recommendation,
        opening_date,
        publication_date,
        mpaa_rating,
        review_url

    from source

)

select * from movie_details
