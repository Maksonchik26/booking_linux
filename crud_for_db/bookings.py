from typing import List, Type

from fastapi import Depends
from sqlalchemy import and_
from sqlalchemy.orm import Session

from db.base import get_session
from db.tables import Booking


class BookingCRUD:
    def __init__(self, session: Session = Depends(get_session)):
        self.session = session

    def read_all(self, limit: int, offset: int) -> List[Type[Booking]]:
        return self.session.query(Booking).limit(limit).offset(offset).all()

    def read_one(self, booking_id) -> Booking | None:
        booking = self.session.query(Booking).filter(Booking.id == booking_id).first()
        return booking

    def create(self, data: dict):
        obj = Booking(**data)
        self.session.add(obj)
        self.session.commit()
        return obj

    def delete(self, obj: Booking):
        self.session.delete(obj)
        self.session.commit()

    def read_by_params(
        self,
        booking_date: str | None,
        length_of_stay: int | None,
        guest_name: str | None,
        daily_rate: float | None,
    ) -> List[Type[Booking]]:
        filters = []
        if booking_date:
            filters.append(Booking.booking_date.like(booking_date))
        if length_of_stay is not None:
            filters.append(Booking.length_of_stay.like(length_of_stay))
        if guest_name:
            filters.append(Booking.guest_name.like(guest_name))
        if daily_rate is not None:
            filters.append(Booking.daily_rate.like(daily_rate))

        return self.session.query(Booking).filter(and_(*filters)).all()

    def __del__(self):
        self.session.close()
