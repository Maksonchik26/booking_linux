import uvicorn
from fastapi import FastAPI

from common.funcs import import_data
from db.base import engine
from db.tables import Base
from routers import booking, booking_stats, booking_analysis


app = FastAPI()

app.include_router(booking.router)
app.include_router(booking_stats.router)
app.include_router(booking_analysis.router)


@app.get("/")
def root():
    return {"message": "Hello ROOT !!!"}


if __name__ == "__main__":
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    import_data()
    uvicorn.run("main:app", port=8000, reload=True)
