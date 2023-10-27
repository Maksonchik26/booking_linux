from typing import Dict
import re

from fastapi import APIRouter
from fastapi import Depends, status
from starlette.responses import Response

from common.funcs import import_data_to_df


router = APIRouter(prefix="/bookings/analysis", tags=["analysis"])

# @router.get("/total_revenue",
#               status_code=status.HTTP_200_OK,
#               response_model=Dict[str, Dict[str, float]],
#               description="Retrieves the total revenue grouped by booking month and hotel type"
#               )
# async def get_total_revenue(df=Depends(import_data_to_df)):
#     df["revenue"] = df["adr"] * df["length_of_stay"]
#     i = df.groupby(["hotel", "arrival_date_month"])[["revenue"]].sum().reset_index(level=1)
#     data = {k: dict(g.values) for k, g in i.groupby(level=0)}
#     return data


@router.get("/top_countries_bookings",
              status_code=status.HTTP_200_OK,
              response_model=Dict[str, int],
              description="Retrieves the top 5 countries bookings with the highest number of bookings"
              )
async def get_top_countries_bookings(df=Depends(import_data_to_df)):
    data = df["country"].value_counts().head()
    return Response(data.to_json(orient="index"), media_type="application/json")


@router.get(
    "/total_bookings_by_arrival_month",
    response_model=Dict[int, int],
    status_code=status.HTTP_200_OK,
    description="Retrieves the total number bookings by arrival month"
)
async def get_total_bookings_by_arrival_month(df=Depends(import_data_to_df)):
    data = df["arrival_date_month"].value_counts()
    return Response(data.to_json(orient="index"), media_type="application/json")


@router.get(
    "/top_popular_meal_packages",
    response_model=Dict[str, int],
    status_code=status.HTTP_200_OK,
    description="Retrieves top of the most popular meal packages among all bookings"
)
async def get_top_popular_meal_package(df=Depends(import_data_to_df)):
    data = df["meal"].value_counts().head()
    return Response(data.to_json(orient="index"), media_type="application/json")
