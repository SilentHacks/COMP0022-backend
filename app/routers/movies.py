from datetime import date, datetime
from typing import Literal

from asyncpg import Connection
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..dependencies import get_db_connection

router = APIRouter(
    prefix="/movies",
    tags=["movies"],
    responses={404: {"description": "Not found"}},
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
    num_reviews: int
    genres: list[str] = []
    actors: list[dict[str, str | int | None]] = []
    directors: list[dict[str, str | None]] = []


@router.get("/")
async def get_movies(
        limit: int = 100,
        offset: int = 0,
        genres: str = None,
        release_year: str = None,
        rating: str = None,
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
    where = ""
    having = ""
    params = [limit, offset]

    if genres:
        split_genres = [genre.strip().lower() for genre in genres.split(",") if genre]
        if split_genres:
            having += 'array_agg(DISTINCT LOWER(g.name)) @> $3'
            params.append(split_genres)

    if release_year:
        split_years = [year.strip() for year in release_year.split(",") if year]
        if split_years:
            try:
                split_years = [int(year) for year in split_years]
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid release_year format")

            where += f"EXTRACT(YEAR FROM m.release_date) BETWEEN ${len(params) + 1} AND ${len(params) + 2}"
            params.append(min(split_years))
            params.append(max(split_years))

    if rating:
        split_rating = [r.strip() for r in rating.split(",") if r]
        if split_rating:
            try:
                split_rating = [int(r) for r in split_rating]
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid rating format")

            having += (f"{' AND ' if having else ''}COALESCE(AVG(ur.rating::FLOAT), 0) "
                       f"BETWEEN ${len(params) + 1} AND ${len(params) + 2}")
            params.append(min(split_rating))
            params.append(max(split_rating))

    total_movies = None
    if where or having:
        total_rows_query = f"""
            SELECT 
                COUNT(*) AS total_movies
            FROM (
                SELECT 
                    m.id,
                    COALESCE(AVG(ur.rating::FLOAT), 0) AS average_rating
                FROM movies m
                INNER JOIN movie_genres mg ON m.id = mg.movie_id
                INNER JOIN genres g ON mg.genre_id = g.id
                LEFT JOIN user_ratings ur ON m.id = ur.movie_id
                {"WHERE " + where if where else ""}
                GROUP BY m.id
                {"HAVING " + having if having else ""}
            ) AS sub HAVING COUNT(*) = $1 OR COUNT(*) != $1 OR COUNT(*) = $2;
                """
        total_movies = await conn.fetchval(total_rows_query, *params)

    metadata = await conn.fetchrow(
        "SELECT COUNT(*) AS total_movies, MIN(EXTRACT(YEAR FROM release_date)) AS min_year, "
        "MAX(EXTRACT(YEAR FROM release_date)) AS max_year FROM movies"
    )
    metadata = dict(metadata)
    if total_movies is not None:
        metadata['total_movies'] = total_movies

    genres = await conn.fetch("SELECT name FROM genres")
    metadata['genres'] = [genre['name'] for genre in genres]

    movies = await conn.fetch(
        f"""
        SELECT 
            m.id, m.title, m.imdb_id, m.tmdb_id, m.release_date, m.runtime, 
            m.tagline, m.overview, m.poster_path, m.backdrop_path, m.budget, m.revenue,
            m.status, m.created_at, m.updated_at,
            COALESCE(AVG(ur.rating::FLOAT), 0) AS average_rating,
            COUNT(ur.rating) AS num_reviews,
            COALESCE((AVG(ur.rating)::FLOAT * LOG(COUNT(ur.rating) + 1)), 0) AS popularity,
            array_agg(DISTINCT g.name) AS genres,
            COALESCE(
                (
                    SELECT json_agg(jsonb_build_object('name', p.name, 'role', mp.role, 'character_name', mp.character_name,
                                                        'profile_path', p.profile_path, 'order', mp."order") ORDER BY mp."order")
                    FROM movie_people mp
                    JOIN people p ON mp.person_id = p.id
                    WHERE mp.movie_id = m.id AND mp.role = 'Actor'
                ),
                '[]'
            ) AS actors,
            COALESCE(
                (
                    SELECT json_agg(jsonb_build_object('name', p.name, 'role', mp.role, 'profile_path', p.profile_path))
                    FROM movie_people mp
                    JOIN people p ON mp.person_id = p.id
                    WHERE mp.movie_id = m.id AND mp.role = 'Director'
                ),
                '[]'
            ) AS directors
        FROM 
            movies m
        LEFT JOIN movie_genres mg ON m.id = mg.movie_id
        LEFT JOIN genres g ON mg.genre_id = g.id
        LEFT JOIN user_ratings ur ON m.id = ur.movie_id
        {"WHERE " + where if where else ""}
        GROUP BY 
            m.id
        {"HAVING " + having if having else ""}
        ORDER BY {sort} {sort_order}, num_reviews DESC, m.release_date DESC, m.title ASC
        LIMIT $1 OFFSET $2;
        """, *params
    )

    return {**metadata, 'movies': [dict(movie) for movie in movies]}  # type: ignore


@router.get("/ids")
async def get_ids(conn: Connection = Depends(get_db_connection)) -> list[int]:
    movies = await conn.fetch("SELECT id FROM movies")
    return [movie['id'] for movie in movies]


@router.get("/{movie_id}")
async def get_movie(movie_id: int, conn: Connection = Depends(get_db_connection)) -> Movie:
    movie = await conn.fetchrow(
        """
        SELECT 
            m.id, m.title, m.imdb_id, m.tmdb_id, m.release_date, m.runtime, 
            m.tagline, m.overview, m.poster_path, m.backdrop_path, m.budget, m.revenue,
            m.status, m.created_at, m.updated_at,
            COALESCE(AVG(ur.rating::FLOAT), 0) AS average_rating,
            COUNT(ur.rating) AS num_reviews,
            COALESCE((AVG(ur.rating)::FLOAT * LOG(COUNT(ur.rating) + 1)), 0) AS popularity,
            array_agg(DISTINCT g.name) AS genres,
            COALESCE(
                (
                    SELECT json_agg(jsonb_build_object('name', p.name, 'role', mp.role, 'character_name', mp.character_name,
                                                        'profile_path', p.profile_path, 'order', mp."order") ORDER BY mp."order")
                    FROM movie_people mp
                    JOIN people p ON mp.person_id = p.id
                    WHERE mp.movie_id = m.id AND mp.role = 'Actor'
                ),
                '[]'
            ) AS actors,
            COALESCE(
                (
                    SELECT json_agg(jsonb_build_object('name', p.name, 'role', mp.role, 'profile_path', p.profile_path))
                    FROM movie_people mp
                    JOIN people p ON mp.person_id = p.id
                    WHERE mp.movie_id = m.id AND mp.role = 'Director'
                ),
                '[]'
            ) AS directors
        FROM 
            movies m
        LEFT JOIN movie_genres mg ON m.id = mg.movie_id
        LEFT JOIN genres g ON mg.genre_id = g.id
        LEFT JOIN user_ratings ur ON m.id = ur.movie_id
        WHERE 
            m.id = $1
        GROUP BY 
            m.id;
        """, movie_id
    )

    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")

    return dict(movie)  # type: ignore
