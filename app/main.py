from asyncpg.exceptions import PostgresError
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import ORJSONResponse as JSONResponse

from .lib.db import db_connect
from .routers import movies

app = FastAPI()

app.include_router(movies.router)


@app.on_event("startup")
async def startup():
    app.state.pool = await db_connect()


@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)


@app.exception_handler(PostgresError)
async def postgres_exception_handler(request: Request, exc: PostgresError):
    return JSONResponse({"detail": "Internal server error"}, status_code=500)


@app.get("/")
async def root():
    return {"message": "Hello Bigger Applications!"}
