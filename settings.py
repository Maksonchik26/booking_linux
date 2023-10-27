import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    CSV_PATH: str
    ADMIN_USERNAME: str
    ADMIN_PASSWORD: str


settings = Settings(
    _env_file=".env",
    _env_file_encoding="utf-8",
)

