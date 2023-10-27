from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker


engine = create_engine("sqlite:///hotel.db")

SessionLocal = sessionmaker(bind=engine)


def get_session():
    session = SessionLocal()
    try:
        yield session
    except SQLAlchemyError:
        session.rollback()
    finally:
        session.close()
        engine.dispose()
