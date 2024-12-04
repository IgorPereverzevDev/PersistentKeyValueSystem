from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")
    HOST: str = "localhost"
    PORT: int = 4324

    @property
    def cluster_ports(self) -> List[int]:
        return [4321, 4322, 4323, 4324]

    @property
    def self_addr(self) -> str:
        return f"{self.HOST}:{self.PORT}"

    @property
    def other_nodes(self) -> List[str]:
        return [f"{self.HOST}:{p}" for p in self.cluster_ports if p != self.PORT]

    MAX_RETRY_ATTEMPTS: int = 3
    RETRY_DELAY: float = 1.0
    READY_TIMEOUT: float = 15.0
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    PROMETHEUS_PORT: int = 9090
    METRICS_ENABLED: bool = True


@lru_cache()
def get_settings():
    return Settings()


settings = get_settings()
