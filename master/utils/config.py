from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """
    Global configuration class for the GridMR Master service.
    
    This class leverages Pydantic BaseSettings to automatically load
    configuration values from environment variables or an `.env` file.
    
    Attributes:
        host (str): Host address where the FastAPI server will bind.
        port (int): Port number for the FastAPI server.
        debug (bool): Enable/disable FastAPI debug mode.
        log_level (str): Logging level for the application (e.g., info, debug, error).
    """
    host: str = Field("0.0.0.0", description="Host for the FastAPI server")
    port: int = Field(8000, description="Port for the FastAPI server")
    debug: bool = Field(True, description="Enable debug mode for FastAPI")
    log_level: str = Field("info", description="Logging level")

    class Config:
        """
        Configuration for environment variable loading.
        
        - `env_file`: Path to the environment file to load variables from.
        - `env_file_encoding`: Encoding for the environment file.
        """
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """
    Retrieve a cached instance of the application settings.
    
    This ensures configuration is only loaded once and reused throughout
    the application lifecycle, improving performance and consistency.
    
    Returns:
        Settings: The global application configuration.
    """
    return Settings()