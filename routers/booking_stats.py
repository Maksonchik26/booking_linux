from typing import Dict

from fastapi import APIRouter, Depends, status
from starlette.responses import Response

from common.funcs import import_data_to_df


router = APIRouter(prefix="/bookings/stats", tags=["stats"])


@router.get(
    "/total_number_of_bookings",
    response_model=Dict[str, int],
    description="Retrieves total number of bookings",
    status_code=status.HTTP_200_OK,
)
async def get_total_number_of_bookings(df=Depends(import_data_to_df)):
    return {"total_number_of_bookings": len(df)}


@router.get(
    "/avg_length_of_stay",
    response_model=Dict[str, float],
    description="Retrieves the average length of stay",
    status_code=status.HTTP_200_OK,
)
async def get_avg_length_of_stay(df=Depends(import_data_to_df)):
    df["length_of_stay"] = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]
    return {"avg_length_of_stay": df["length_of_stay"].mean()}


@router.get(
    "/avg_daily_rate",
    response_model=Dict[str, float],
    status_code=status.HTTP_200_OK,
    description="Retrieves the average daily rate bookings",
)
async def get_avg_daily_rate(df=Depends(import_data_to_df)):
    data = df[["adr"]].mean()
    return Response(data.to_json(orient="index"), media_type="application/json")
