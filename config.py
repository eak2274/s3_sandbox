# config.py
from pathlib import Path
from dotenv import load_dotenv
import os

# Определяем путь к .env файлу (в той же директории, где находится config.py)
env_path = Path(__file__).parent / ".env"

# Загружаем переменные из .env
load_dotenv(dotenv_path=env_path)

# Читаем переменные
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
REGION = os.getenv("REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
INPUT_FILE_KEY = os.getenv("INPUT_FILE_KEY")
OUTPUT_FILE_KEY = os.getenv("OUTPUT_FILE_KEY")

# Проверяем, что все обязательные переменные определены
required_vars = {
    "ACCESS_KEY": ACCESS_KEY,
    "SECRET_KEY": SECRET_KEY,
    "ENDPOINT_URL": ENDPOINT_URL,
    "REGION": REGION,
    "BUCKET_NAME": BUCKET_NAME,
    "INPUT_FILE_KEY": INPUT_FILE_KEY,
    "OUTPUT_FILE_KEY": OUTPUT_FILE_KEY
}

# Если какая-то переменная отсутствует, вызываем исключение
for var_name, var_value in required_vars.items():
    if not var_value:
        raise ValueError(f"Missing required environment variable: {var_name}")