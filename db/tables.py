from sqlalchemy import Integer, String, Float
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase


class Base(DeclarativeBase):
    pass


class Booking(Base):
    __tablename__ = "bookings"

    id: Mapped[int] = mapped_column(primary_key=True)
    booking_date: Mapped[str] = mapped_column(String(64))
    length_of_stay: Mapped[int] = mapped_column(Integer())
    guest_name: Mapped[str] = mapped_column(String(64))
    daily_rate: Mapped[float] = mapped_column(Float())
