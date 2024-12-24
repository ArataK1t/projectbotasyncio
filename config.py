# config.py

import os
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env
load_dotenv()

# Настройки для подключения к базе данных
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))  # Порт по умолчанию 3306
DB_USER = os.getenv("DB_USER", "your_database_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_database_password")
DB_NAME = os.getenv("DB_NAME", "bot_database")

# Токен бота
BOT_TOKEN = os.getenv("BOT_TOKEN")
print("BOT_TOKEN:", BOT_TOKEN)
CREATOR_ID = 1025255996