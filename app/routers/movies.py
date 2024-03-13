from datetime import date, datetime
from typing import Literal

from asyncpg import Connection
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from .genres import router as genres_router
from ..dependencies import get_db_connection

router = APIRouter(
    prefix="/movies",
    tags=["movies"],
    responses={404: {"description": "Not found"}},
)
router.include_router(
    genres_router,
    tags=["genres"],
    prefix="/genres",
)


class Movie(BaseModel):
    id: int
    title: str
    imdb_id: int
    tmdb_id: int
    release_date: date
    runtime: int
    tagline: str
    overview: str
    poster_path: str | None
    backdrop_path: str | None
    budget: int
    revenue: int
    status: str
    created_at: datetime
    updated_at: datetime
    average_rating: float
    predicted_rating: float | None = None
    avg_user_rating: float | None = None
    num_reviews: int
    genres: list[str] = []
    correlated_genres: list[str] = []
    actors: list[dict[str, str | int | None]] = []
    directors: list[dict[str, str | None]] = []


@router.get("/")
async def get_movies(
        limit: int = 100,
        offset: int = 0,
        genres: str = None,
        release_year: str = None,
        rating: str = None,
        query: str = None,
        sort: Literal[
            "release_date",
            "title",
            "average_rating",
            "runtime",
            "num_reviews",
            "popularity"
        ] = "release_date",
        sort_order: Literal["desc", "asc"] = "desc",
        conn: Connection = Depends(get_db_connection)
) -> dict[str, int | list[Movie | str]]:
    where = []
    params = []

    if genres:
        split_genres = [genre.strip().lower() for genre in genres.split(",") if genre]
        if split_genres:
            where.append(f'LOWER(genres::TEXT)::TEXT[] @> ${len(params) + 1}')
            params.append(split_genres)

    if release_year:
        split_years = [year.strip() for year in release_year.split(",") if year]
        if split_years:
            try:
                split_years = [int(year) for year in split_years]
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid release_year format")

            where.append(f"EXTRACT(YEAR FROM release_date) BETWEEN ${len(params) + 1} AND ${len(params) + 2}")
            params.append(min(split_years))
            params.append(max(split_years))

    if rating:
        split_rating = [r.strip() for r in rating.split(",") if r]
        if split_rating:
            try:
                split_rating = [int(r) for r in split_rating]
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid rating format")

            where.append(f"average_rating BETWEEN ${len(params) + 1} AND ${len(params) + 2}")
            params.append(min(split_rating))
            params.append(max(split_rating))

    if query:
        where.append(f"LOWER(title) LIKE ${len(params) + 1} OR "
                     f"EXISTS (SELECT 1 FROM movie_keywords mk INNER JOIN keywords k ON mk.keyword_id = k.id "
                     f"WHERE mk.movie_id = m.id AND k.name LIKE ${len(params) + 1})")
        params.append(f"%{query.lower()}%")

    if where:
        where_clause = f'WHERE {" AND ".join(where)}'
        total_movies = await conn.fetchval(f"SELECT COUNT(*) FROM movies_view m {where_clause}", *params)
    else:
        where_clause = ""
        total_movies = None

    metadata = await conn.fetchrow(
        "SELECT COUNT(*) AS total_movies, MIN(EXTRACT(YEAR FROM release_date)) AS min_year, "
        "MAX(EXTRACT(YEAR FROM release_date)) AS max_year FROM movies"
    )
    metadata = dict(metadata)
    if total_movies is not None:
        metadata['total_movies'] = total_movies

    genres = await conn.fetch("SELECT name FROM genres")
    metadata['genres'] = [genre['name'] for genre in genres]

    params.append(limit)
    params.append(offset)

    movies = await conn.fetch(
        f"""
        SELECT *
        FROM movies_view m
        {where_clause}
        ORDER BY {sort} {sort_order}, num_reviews DESC, release_date DESC, title ASC
        LIMIT ${len(params) - 1} OFFSET ${len(params)};
        """, *params
    )

    return {**metadata, 'movies': [dict(movie) for movie in movies]}  # type: ignore


@router.get("/ids")
async def get_ids(conn: Connection = Depends(get_db_connection)) -> list[int]:
    movies = await conn.fetch("SELECT id FROM movies")
    return [movie['id'] for movie in movies]


@router.get("/{movie_id}")
async def get_movie(movie_id: int, conn: Connection = Depends(get_db_connection)) -> Movie:
    movie = await conn.fetchrow("SELECT * FROM movies_view WHERE id = $1", movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")

    predicted_rating = await conn.fetchval(
        f"""
        WITH relevant_genres AS (
            SELECT genre_id, genres.name AS genre_name
            FROM movie_genres
            INNER JOIN genres ON movie_genres.genre_id = genres.id
            WHERE movie_id = $1
        ),
        sampled_user_ratings AS (
            SELECT
                ur.user_id,
                ur.movie_id,
                ur.rating,
                ROW_NUMBER() OVER (PARTITION BY ur.movie_id ORDER BY RANDOM()) AS row_num,
                COUNT(*) OVER (PARTITION BY ur.movie_id) AS total_count
            FROM
                user_ratings ur
            JOIN
                movie_genres mg ON ur.movie_id = mg.movie_id
            WHERE
                ur.movie_id = $1
                AND mg.genre_id IN (SELECT genre_id FROM relevant_genres)
        ),
        ten_percent_sample AS (
            SELECT
                sur.user_id,
                sur.movie_id,
                sur.rating
            FROM
                sampled_user_ratings sur
            WHERE
                sur.row_num <= CEIL(sur.total_count * 0.1)
        ),
        avg_genre_deviation AS (
            SELECT
                roe.user_id,
                AVG(roe.avg_rating_above_expected) AS avg_deviation
            FROM
                rating_over_expected_by_genre roe
            WHERE
                roe.name IN (SELECT genre_name FROM relevant_genres)
                AND roe.user_id IN (SELECT user_id FROM ten_percent_sample)
            GROUP BY
                roe.user_id
        ),
        predicted_rating AS (
            SELECT
                AVG(ten_percent_sample.rating - COALESCE(avg_genre_deviation.avg_deviation, 0)) AS predicted_avg_rating
            FROM
                ten_percent_sample
            LEFT JOIN
                avg_genre_deviation ON ten_percent_sample.user_id = avg_genre_deviation.user_id
        )
        SELECT 
            predicted_avg_rating
        FROM
            predicted_rating;
        """, movie_id
    )

    avg_user_rating = await conn.fetchval(
        f"""
        WITH user_averages AS (
            SELECT
                user_id,
                AVG(rating) as avg_rating
            FROM
                user_ratings
            GROUP BY
                user_id
        ),
        user_movie_averages AS (
            SELECT
                r.movie_id,
                r.user_id,
                u.avg_rating AS user_avg_rating
            FROM
                user_ratings r
            INNER JOIN
                user_averages u on r.user_id = u.user_id AND r.movie_id = 1
        )
        SELECT
            AVG(uma.user_avg_rating) AS avg_user_rating
        FROM
            user_ratings m 
        INNER JOIN
            user_movie_averages uma on m.movie_id = uma.movie_id
        GROUP BY
            m.movie_id;
        """
    )

    correlated_genres = await conn.fetch(
        f"""
        WITH genre_preferences AS (
            SELECT
                ur.user_id,
                g.name AS genre_name,
                AVG(ur.rating) AS avg_rating
            FROM
                user_ratings ur
            JOIN
                movie_genres mg ON ur.movie_id = mg.movie_id
            JOIN
                genres g ON mg.genre_id = g.id
            GROUP BY
                ur.user_id, g.name
        ),
        genre_pairs AS (
            SELECT
                a.user_id,
                a.genre_name AS genre_a,
                b.genre_name AS genre_b,
                a.avg_rating AS avg_rating_a,
                b.avg_rating AS avg_rating_b
            FROM
                genre_preferences a
            JOIN
                genre_preferences b ON a.user_id = b.user_id AND a.genre_name < b.genre_name
        ),
        filtered_pairs AS (
            SELECT
                genre_a,
                genre_b,
                corr(avg_rating_a, avg_rating_b) AS correlation,
                CASE
                    WHEN genre_a = ANY($1) THEN genre_b
                    ELSE genre_a
                END AS correlated_genre
            FROM
                genre_pairs
            GROUP BY
                genre_a, genre_b
        )
        SELECT
            genre_a,
            genre_b,
            correlation,
            correlated_genre
        FROM
            filtered_pairs
        WHERE 
            (genre_a = ANY($1) AND genre_b <> ALL($1))
            OR 
            (genre_b = ANY($1) AND genre_a <> ALL($1))
        ORDER BY
            correlation DESC
        LIMIT 2;
        """, movie['genres']
    )

    return {  # type: ignore
        **movie,
        'predicted_rating': predicted_rating,
        'avg_user_rating': avg_user_rating,
        'correlated_genres': list(set(genre['correlated_genre'] for genre in correlated_genres))
    }
