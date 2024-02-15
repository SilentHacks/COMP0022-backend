from fastapi import APIRouter


router = APIRouter(
    prefix="/example"
)


@router.get("/")
async def read_example():
    return {"hello": "world"}
