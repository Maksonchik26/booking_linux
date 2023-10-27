from typing import Optional

from pydantic import BaseModel, Field


class BookingIn(BaseModel):
    booking_date: str = Field(min_length=10)
    length_of_stay: int = Field(ge=0)
    guest_name: str = Field(min_length=3)
    daily_rate: float = Field(ge=0)

    class Config:
        json_schema_extra = {
            "example": {
                "booking_date": "2022-May-1",
                "length_of_stay": 32,
                "guest_name": "Max",
                "daily_rate": 30.2,
            }
        }


class BookingOutDB(BookingIn):
    id: Optional[int]


class BookingOutDF(BaseModel):
    hotel: str
    is_canceled: int
    lead_time: int
    adults: int
    children: int | str
    babies: int
    meal: str
    country: str
    market_segment: str
    distribution_channel: str
    is_repeated_guest: int
    previous_cancellations: int
    previous_bookings_not_canceled: int
    reserved_room_type: str
    assigned_room_type: str
    booking_changes: int
    deposit_type: str
    agent: int | str | None
    company: int | str | None
    days_in_waiting_list: int
    customer_type: str
    adr: float
    required_car_parking_spaces: int
    total_of_special_requests: int
    reservation_status: str
    reservation_status_date: str
    name: str
    email: str
    phone_number: str
    credit_card: str
    arrival_date: str
    booking_date: str
    length_of_stay: int
