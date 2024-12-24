from database.connection import Database
from aiogram import Bot
import logging
from database.connection import safe_get_cache, set_cache, set_hash_fields, safe_get_hash_field
import json
import aioredis
# Создаем объект db для работы с базой данных
db = Database()



# Добавление пользователя в базу данных и кеш
async def add_user(redis: aioredis.Redis, user_id: int, chat_id: int, username: str = None, rank: str = "новичок", points: int = 0, chat_name: str = None):
    # Проверяем, существует ли chat_id в таблице chats
    query_check_chat = "SELECT 1 FROM chats WHERE chat_id = $1"
    existing_chat = await db.fetchone(query_check_chat, (chat_id,))

    # Если chat_id не существует, добавляем его в таблицу chats
    if not existing_chat:
        query_insert_chat = "INSERT INTO chats (chat_id, chat_name) VALUES ($1, $2)"
        chat_name = chat_name or "Неизвестное название чата"
        await db.execute(query_insert_chat, (chat_id, chat_name))
        logging.info(f"Добавлен новый chat_id {chat_id} в таблицу chats.")

    # Проверяем, существует ли уже запись пользователя в этом чате
    query_check_user = "SELECT 1 FROM users WHERE user_id = $1 AND chat_id = $2"
    existing_user = await db.fetchone(query_check_user, (user_id, chat_id))

    # Если пользователь еще не существует в базе для этого чата, добавляем его
    if not existing_user:
        # Добавляем пользователя в таблицу users для конкретного чата
        query_insert_user = """
        INSERT INTO users (user_id, chat_id, username, "rank", points)
        VALUES ($1, $2, $3, $4, $5);
        """
        await db.execute(query_insert_user, (user_id, chat_id, username, rank, points))
        logging.info(f"Пользователь {user_id} добавлен в базу данных с рангом '{rank}' и очками {points}.")
        
        # Кешируем информацию о пользователе в Redis хэш
        cache_key = f"user:{user_id}:{chat_id}"
        user_data = {  # Создаем словарь user_data
            "user_id": str(user_id), # Преобразуем в строку
            "chat_id": str(chat_id), # Преобразуем в строку
            "username": str(username),
            "rank": str(rank),
            "points": str(points) # Преобразуем в строку
        }
        try:
            logging.info(f"Тип user_data перед set_hash_fields: {type(user_data)}")
            await set_hash_fields(redis, cache_key, user_data) # Используем set_hash_fields
            logging.info(f"Информация о пользователе {user_id} сохранена в кеш Redis.")
        except Exception as e:
            logging.error(f"Ошибка при сохранении информации о пользователе в кеш Redis: {e}")
    else:
        logging.info(f"Пользователь {user_id} уже существует в базе данных для чата {chat_id}.")


# Получение информации о пользователе из базы данных или кеша
async def get_user_info(redis: aioredis.Redis, user_id: int, chat_id: int):
    cache_key = f"user:{user_id}:{chat_id}"
    cached_user = await safe_get_hash_field(redis, cache_key, "user_id")

    if cached_user:
        return await redis.hgetall(cache_key)

    query = "SELECT user_id, chat_id, username, \"rank\", points FROM users WHERE user_id = $1 AND chat_id = $2"
    result = await db.fetchone(query, (user_id, chat_id))
    if result:
        user_data = {
            "user_id": str(result["user_id"]),
            "chat_id": str(result["chat_id"]),
            "username": str(result["username"]),
            "rank": str(result["rank"]),
            "points": str(result["points"])
        }
        await set_hash_fields(redis, cache_key, user_data)
        return result
    return None



# Получить очки пользователя
async def get_user_points(redis: aioredis.Redis, user_id: int, chat_id: int) -> int:
    cache_key = f"user:{user_id}:{chat_id}"
    cached_points = await safe_get_hash_field(redis, cache_key, "points")

    if cached_points:
        return int(cached_points)

    query = "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2"
    result = await db.fetchone(query, (user_id, chat_id))
    if result:
        await set_hash_fields(redis, cache_key, {"points": result['points']})
        return result['points']
    return 0


# Обновление ранга пользователя в базе данных и кеш
async def set_user_rank(redis: aioredis.Redis, user_id: int, chat_id: int, rank: str):
    # Проверяем текущий ранг пользователя
    user_info = await get_user_info(redis, user_id, chat_id)
    if user_info and user_info['rank'] == rank:
        logging.info(f"Ранг пользователя {user_id} в чате {chat_id} уже установлен на {rank}.")
        return  # Ранг не нужно обновлять

    query = "UPDATE users SET \"rank\" = $1 WHERE user_id = $2 AND chat_id = $3"
    try:
        result = await db.execute(query, (rank, user_id, chat_id))
        if result == 0:
            logging.error(f"Не удалось обновить ранг для пользователя {user_id} в чате {chat_id}.")
        else:
            logging.info(f"Ранг для пользователя {user_id} в чате {chat_id} успешно обновлен на {rank}.")
            # Обновляем кеш
            cache_key = f"user:{user_id}:{chat_id}"
            await set_hash_fields(redis, cache_key, {"rank": rank})
    except Exception as e:
        logging.error(f"Ошибка при обновлении ранга для пользователя {user_id} в чате {chat_id}: {e}")

# Обновление очков пользователя в базе данных и кеш
async def set_user_points(redis: aioredis.Redis, user_id: int, chat_id: int, points: int):
    """Обновление очков пользователя в базе данных и в кеше Redis."""
    # Устанавливаем или обновляем данные в базе данных
    query = """
    UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3
    """
    # Передаем все параметры как кортеж
    await db.execute(query, (points, user_id, chat_id))
    # Обновляем кеш
    await set_hash_fields(redis, f"user:{user_id}:{chat_id}", {"points": points})

# Сброс данных пользователя и изменение титула администратора
async def reset_user(redis: aioredis.Redis, user_id: int, chat_id: int, bot: Bot):
    # Получаем начальный ранг
    from database.queries import get_initial_rank
    initial_rank = await get_initial_rank(redis, chat_id)
    
    if initial_rank:
        # Сбрасываем ранг и очки, устанавливаем начальный ранг
        query = "UPDATE users SET \"rank\" = $1, points = 0 WHERE user_id = $2 AND chat_id = $3"
        await db.execute(query, (initial_rank, user_id, chat_id))
        # Сбрасываем кеш
        await redis.hset(f"user:{user_id}:{chat_id}", "rank", initial_rank)
        await redis.hset(f"user:{user_id}:{chat_id}", "points", 0)
        
        # Изменяем титул администратора на начальный ранг
        try:
            await bot.set_chat_administrator_custom_title(chat_id, user_id, initial_rank)
        except Exception as e:
            # Логируем ошибку, если не удалось изменить титул
            print(f"Ошибка при изменении титула для пользователя {user_id}: {e}")


# Получение всех пользователей чата без кеширования
async def get_all_users(chat_id: int):
    """
    Получение всех пользователей чата из базы данных.
    """
    query = "SELECT user_id, chat_id, username, \"rank\", points FROM users WHERE chat_id = $1"
    try:
        # Получаем пользователей из базы данных
        users = await db.fetchall(query, (chat_id,))
        if not users:
            logging.info(f"Пользователи для чата {chat_id} не найдены.")
            return []

        logging.info(f"Пользователи чата {chat_id} успешно получены.")
        return users
    except Exception as e:
        logging.error(f"Ошибка при получении пользователей для чата {chat_id}: {e}")
        return []






