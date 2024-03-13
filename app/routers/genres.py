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


@router.get("/user_correlation")
async def get_correlation(conn: Connection = Depends(get_db_connection)) -> list[dict[str, str | float | int]]:
    genres = await conn.fetch("""
        WITH avg_rating_per_genre_per_person AS (
            SELECT
                ur.user_id,
                mg.genre_id,
                AVG(ur.rating) AS avg_rating
            FROM
                user_ratings ur
            INNER JOIN
                movie_genres mg ON ur.movie_id = mg.movie_id
            GROUP BY
                ur.user_id, mg.genre_id
        ),
        personality_genre AS (
            SELECT
                gr.genre_id,
                g.name,
                ps.id,
                ps.openness,
                ps.agreeableness,
                ps.extraversion,
                ps.emotional_stability,
                ps.conscientiousness,
                gr.avg_rating
            FROM
                users ps
            INNER JOIN
                avg_rating_per_genre_per_person gr ON ps.id = gr.user_id
            INNER JOIN
                genres g ON gr.genre_id = g.id
        )
        SELECT
            genre_id,
            name,
            corr(openness, avg_rating) AS openness_corr,
            corr(agreeableness, avg_rating) AS agreeableness_corr,
            corr(extraversion, avg_rating) AS extraversion_corr,
            corr(emotional_stability, avg_rating) AS emotional_stability_corr,
            corr(conscientiousness, avg_rating) AS conscientiousness_corr
        FROM
            personality_genre
        GROUP BY
            genre_id, name;
    """)

    return [dict(genre) for genre in genres]
