from datetime import date, datetime

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
    genres: list[str] = []
    actors: list[dict[str, str]] = []
    directors: list[dict[str, str]] = []


@router.get("/")
async def get_movies(conn: Connection = Depends(get_db_connection)) -> list[Movie]:
    movies = await conn.fetch("SELECT * FROM movies")
    return [dict(movie) for movie in movies]  # type: ignore


@router.get("/{movie_id}")
async def get_movie(movie_id: int, conn: Connection = Depends(get_db_connection)) -> Movie:
    movie = await conn.fetchrow(
        """
        SELECT 
            m.id, m.title, m.imdb_id, m.tmdb_id, m.release_date, m.runtime, 
            m.tagline, m.overview, m.poster_path, m.backdrop_path, m.budget, m.revenue,
            m.status, m.created_at, m.updated_at,
            array_agg(DISTINCT g.name) AS genres,
            (SELECT json_agg(jsonb_build_object('name', p.name, 'role', mp.role, 'character_name', mp.character_name))
             FROM movie_people mp
             JOIN people p ON mp.person_id = p.id
             WHERE mp.movie_id = m.id AND mp.role = 'Actor'
            ) AS actors,
            (SELECT json_agg(jsonb_build_object('name', p.name, 'role', mp.role))
             FROM movie_people mp
             JOIN people p ON mp.person_id = p.id
             WHERE mp.movie_id = m.id AND mp.role = 'Director'
            ) AS directors
        FROM 
            movies m
        LEFT JOIN movie_genres mg ON m.id = mg.movie_id
        LEFT JOIN genres g ON mg.genre_id = g.id
        WHERE 
            m.id = $1
        GROUP BY 
            m.id;
        """, movie_id
    )

    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")

    return dict(movie)  # type: ignore
