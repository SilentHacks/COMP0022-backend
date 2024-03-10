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

    return dict(movie)  # type: ignore
