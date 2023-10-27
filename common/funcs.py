from datetime import timedelta
import pandas as pd

from db.base import engine
from settings import settings


def import_data_to_df():
    df = pd.read_csv(settings.CSV_PATH)
    df.rename(columns={"phone-number": "phone_number"}, inplace=True)
    df["arrival_date"] = df["arrival_date_year"].apply(lambda x: str(x)) + "-" + df[
        "arrival_date_month"] + "-" + df["arrival_date_day_of_month"].apply(lambda x: str(x))

    df["arrival_date"] = pd.to_datetime(df["arrival_date"], format="mixed", errors="ignore")

    df["booking_date"] = df["arrival_date"] - df["lead_time"].apply(lambda x: timedelta(days=x))
    df["arrival_date"] = df["arrival_date"].dt.strftime("%Y-%m-%d")
    df["booking_date"] = df["booking_date"].dt.strftime("%Y-%m-%d")
    df["length_of_stay"] = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]
    df.drop(columns=["arrival_date_day_of_month",
                     "arrival_date_week_number",
                     "stays_in_weekend_nights",
                     "stays_in_week_nights"
                     ],
            inplace=True)

    return df


def import_data():
    df = pd.read_csv(settings.CSV_PATH)

    df_copy = pd.DataFrame(columns=["id", "booking_date", "length_of_stay", "guest_name", "daily_rate"])
    df_copy["booking_date"] = df["arrival_date_year"].astype(str) + "-" + \
                              df["arrival_date_month"].astype(str) + "-" + \
                              df["arrival_date_day_of_month"].astype(str)

    df_copy["length_of_stay"] = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]
    df_copy["guest_name"] = df["name"]
    df_copy["daily_rate"] = df["adr"]
    df_copy["id"] = df_copy.index

    df_copy.to_sql("bookings", con=engine, if_exists="append", index=False)
