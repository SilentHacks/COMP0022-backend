from asyncpg import Connection
from fastapi import APIRouter, Depends

from ..dependencies import get_db_connection

router = APIRouter()


@router.get("/popular")
async def get_popular(conn: Connection = Depends(get_db_connection)) -> list[dict[str, str | float]]:
    genres = await conn.fetch("""
        SELECT 
            g.name AS genre, 
            COALESCE(AVG(ur.rating)::FLOAT, 0) AS average_rating,
            COUNT(ur.rating) AS "count",
            COALESCE((AVG(ur.rating)::FLOAT * LOG(COUNT(ur.rating) + 1)), 0) AS popularity
        FROM 
            genres g
        INNER JOIN 
            movie_genres mg ON g.id = mg.genre_id
        LEFT JOIN 
            user_ratings ur ON mg.movie_id = ur.movie_id
        GROUP BY 
            g.name
        ORDER BY 
            popularity DESC;
    """)

    return [dict(genre) for genre in genres]


@router.get("/polarising")
async def get_polarising(conn: Connection = Depends(get_db_connection)) -> list[dict[str, str | float]]:
    genres = await conn.fetch("""
        SELECT 
            g.name AS genre,
            COUNT(*) FILTER (WHERE ur.rating >= 4) * 100.0 / NULLIF(COUNT(ur.rating), 0) AS high_rating_pct,
            COUNT(*) FILTER (WHERE ur.rating <= 2) * 100.0 / NULLIF(COUNT(ur.rating), 0) AS low_rating_pct
        FROM 
            genres g
        INNER JOIN 
            movie_genres mg ON g.id = mg.genre_id
        LEFT JOIN 
            user_ratings ur ON mg.movie_id = ur.movie_id
        GROUP BY 
            g.name
        HAVING 
            COUNT(ur.rating) > 0
        ORDER BY 
            (COUNT(*) FILTER (WHERE ur.rating >= 4) + 
            COUNT(*) FILTER (WHERE ur.rating <= 2)) * 100.0 / NULLIF(COUNT(ur.rating), 0) DESC
        LIMIT 10;
    """)

    return [dict(genre) for genre in genres]


