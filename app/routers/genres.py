from asyncpg import Connection
from fastapi import APIRouter, Depends

from ..dependencies import get_db_connection

router = APIRouter()


@router.get("/popular")
async def get_popular(conn: Connection = Depends(get_db_connection)) -> list[dict[str, str | float]]:
    genres = await conn.fetch("""
        WITH genre_stats AS (
            SELECT 
                g.name AS genre, 
                COALESCE(AVG(ur.rating), 0) AS average_rating,
                COUNT(ur.rating) AS "count"
            FROM 
                genres g
            INNER JOIN 
                movie_genres mg ON g.id = mg.genre_id
            LEFT JOIN 
                user_ratings ur ON mg.movie_id = ur.movie_id
            GROUP BY 
                g.name
        )
        SELECT 
            genre_stats.*,
            calculate_popularity(average_rating, "count") AS popularity
        FROM 
            genre_stats
        ORDER BY
            popularity DESC;
    """)

    return [dict(genre) for genre in genres]


@router.get("/polarising")
async def get_polarising(conn: Connection = Depends(get_db_connection)) -> list[dict[str, str | float]]:
    genres = await conn.fetch("""
        WITH deviations AS (
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
        )
        SELECT * FROM deviations
        ORDER BY 
            high_rating_pct - low_rating_pct ASC
        LIMIT 10;
    """)

    return [dict(genre) for genre in genres]


