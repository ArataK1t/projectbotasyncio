from database.connection import Database
import logging
from database.models import normalize_emoji
import aioredis
import asyncio
from database.connection import safe_get_cache, set_cache, set_hash_fields, safe_get_hash_field
from database.users import set_user_rank

# Создаём объект db для работы с базой данных
db = Database()


# Установить начальный ранг для чата
async def set_initial_rank(redis: aioredis.Redis, chat_id: int, rank: str):
    query = """
    INSERT INTO settings (chat_id, initial_rank)
    VALUES ($1, $2)
    ON CONFLICT (chat_id) DO UPDATE SET initial_rank = $2
    """
    await db.execute(query, (chat_id, rank))
    logging.info(f"Начальный ранг для чата {chat_id} установлен на {rank}.")
    
    # Обновляем кеш в Redis хэш
    await set_hash_fields(redis, f"chat:{chat_id}:settings", {"initial_rank": str(rank)})




# Получить начальный ранг из кеша или базы данных
async def get_initial_rank(redis: aioredis.Redis, chat_id: int):
    cached_rank = await safe_get_hash_field(redis, f"chat:{chat_id}:settings", "initial_rank", convert=False) # convert=False чтобы не пытаться конвертировать в int
    if cached_rank:
        return cached_rank.decode() if isinstance(cached_rank, bytes) else cached_rank # Декодируем если байты

    query = "SELECT initial_rank FROM settings WHERE chat_id = $1"
    result = await db.fetchone(query, (chat_id,))
    if result:
        await set_hash_fields(redis, f"chat:{chat_id}:settings", {"initial_rank": str(result['initial_rank'])})
        return result['initial_rank']
    return None





async def set_rank_requirements(redis: aioredis.Redis, rank_name: str, chat_id: int, points_required: int):
    """Устанавливает требования для ранга в базу данных и кеш Redis."""
    
    # Устанавливаем или обновляем данные в базе данных
    query = """
    INSERT INTO ranks (rank_name, chat_id, points_required)
    VALUES ($1, $2, $3)
    ON CONFLICT (rank_name, chat_id) DO UPDATE SET points_required = $3
    """
    await db.execute(query, (rank_name, chat_id, points_required))

    # Получаем все ранги из Redis или базы данных
    ranks = await get_all_ranks(redis, chat_id)

    # Обновляем или добавляем новый ранг в список
    ranks_dict = {rank['rank_name']: str(rank['points_required']) for rank in ranks}
    ranks_dict[rank_name] = str(points_required)  # Обновляем/добавляем новый ранг

    # Кэшируем обновленные данные о всех рангах
    await set_hash_fields(redis, f"chat:{chat_id}:ranks", ranks_dict)





# Установить эмоцию и её очки
async def set_emotion_reward(redis: aioredis.Redis, chat_id: int, emotion: str, points: int):
    query = """
    INSERT INTO emotions (chat_id, emotion, points)
    VALUES ($1, $2, $3)
    ON CONFLICT (chat_id, emotion) DO UPDATE SET points = $3
    """
    await db.execute(query, (chat_id, emotion, points))

    # Обновляем кеш в Redis
    await set_hash_fields(redis, f"chat:{chat_id}:emotions", {emotion: str(points)})




# Обновить ранги пользователей
#async def update_user_rank(redis: aioredis.Redis, user, ranks, chat_id):
#    user_id = user['user_id']
#    points = user['points']
#    new_rank = None
#
#    for rank in ranks:
#        if points >= rank['points_required']:
#            new_rank = rank['rank_name']
#            break
#
#    cache_key = f"user:{user_id}:{chat_id}"
#    if new_rank:
#        await set_hash_fields(redis, cache_key, {"rank": new_rank})
#        await set_user_rank(user_id, chat_id, new_rank)
#        logging.info(f"Пользователю {user_id} в чате {chat_id} установлен новый ранг: {new_rank}.")
#    else:
#        initial_rank = await get_initial_rank(redis, chat_id)
#        if initial_rank:
#            await set_hash_fields(redis, cache_key, {"rank": initial_rank})
#            await set_user_rank(user_id, chat_id, initial_rank)
#            logging.info(f"Пользователю {user_id} в чате {chat_id} установлен начальный ранг: {initial_rank}.")







# Получить все эмоции из кеша или базы данных
async def get_all_emotions(redis: aioredis.Redis, chat_id: int):
    cached_emotions = await redis.hgetall(f"chat:{chat_id}:emotions")
    if cached_emotions:
        # Redis hgetall возвращает словарь, преобразуем его в список кортежей
        return [(key, int(value)) for key, value in cached_emotions.items()]

    # Если нет данных в кэше, запрос из базы
    query = "SELECT emotion, points FROM emotions WHERE chat_id = $1"
    result = await db.fetchall(query, (chat_id,))
    if result:
        # Преобразуем результат в список кортежей
        emotion_data = {row['emotion']: row['points'] for row in result}
        # Кэшируем данные в Redis
        await set_hash_fields(redis, f"chat:{chat_id}:emotions", emotion_data)
        return [(row['emotion'], row['points']) for row in result]

    return []



async def get_emotion_points(redis: aioredis.Redis, emotion: str, chat_id: int):
    """
    Получаем количество очков для эмоции из базы данных.
    Возвращает None, если эмоция не найдена.
    """
    # Нормализуем эмоцию перед поиском в базе
    emotion = normalize_emoji(emotion)
    
    # Проверяем кеш для получения очков
    cached_points = await safe_get_cache(redis, f"emotion_points:{chat_id}:{emotion}")
    if cached_points:
        return int(cached_points)
    
    query = "SELECT points FROM emotions WHERE chat_id = $1 AND emotion = $2"
    result = await db.fetchone(query, (chat_id, emotion))
    
    if result and 'points' in result:
        await set_cache(redis, f"emotion_points:{chat_id}:{emotion}", result['points'], ttl=86400)
        return result['points']
    
    logging.info(f"Эмоция {emotion} не найдена в базе данных для чата {chat_id}.")
    return None


# Получить все ранги из кеша или базы данных
async def get_all_ranks(redis: aioredis.Redis, chat_id: int):
    cached_ranks = await redis.hgetall(f"chat:{chat_id}:ranks")
    
    # Если есть кэшированные данные
    if cached_ranks:
        # Если Redis настроен с decode_responses=True, то ключи и значения уже будут строками, поэтому .decode() не нужно
        return [{'rank_name': key, 'points_required': int(value)} for key, value in cached_ranks.items()]

    # Если нет кэшированных данных, извлекаем из базы данных
    query = "SELECT rank_name, points_required FROM ranks WHERE chat_id = $1"
    result = await db.fetchall(query, (chat_id,))
    
    if result:
        # Преобразуем результат из базы данных в список словарей
        ranks = [{'rank_name': row['rank_name'], 'points_required': row['points_required']} for row in result]
    
        # Кэшируем полученные данные в Redis с TTL
        await set_hash_fields(redis, f"chat:{chat_id}:ranks", {rank['rank_name']: str(rank['points_required']) for rank in ranks})
        
        return ranks
    
    return []







async def get_chat_settings(chat_id: int) -> dict:
    try:
        # Получаем общие настройки чата
        query_settings = """
            SELECT initial_rank, reaction_limit_per_person, reaction_limit_total
            FROM settings
            WHERE chat_id = $1
        """
        settings = await db.fetchone(query_settings, (chat_id,))
        if not settings:
            logging.error(f"Настройки для чата {chat_id} не найдены.")
            return None

        # Логируем настройки
        logging.debug(f"Настройки для чата {chat_id}: {settings}")

        # Преобразуем asyncpg.Record в обычный словарь для изменения
        settings_dict = dict(settings)

        # Получаем ранги
        query_ranks = """
            SELECT rank_name, points_required
            FROM ranks
            WHERE chat_id = $1
            ORDER BY points_required ASC
        """
        ranks = await db.fetchall(query_ranks, (chat_id,))
        if not ranks:
            logging.error(f"Ранги для чата {chat_id} не найдены.")
            return None

        # Добавляем ранги в словарь настроек
        settings_dict["ranks"] = ranks

        # Получаем эмоции
        query_emotions = """
            SELECT emotion, points
            FROM emotions
            WHERE chat_id = $1
        """
        emotions = await db.fetchall(query_emotions, (chat_id,))
        if emotions:
            settings_dict["emotions"] = emotions
            logging.debug(f"Эмоции для чата {chat_id}: {emotions}")
        else:
            settings_dict["emotions"] = []  # Если эмоции не настроены, добавляем пустой список

        return settings_dict
    except Exception as e:
        logging.error(f"Ошибка при получении настроек чата {chat_id}: {e}")
        return None







# Удалить ранг из базы данных и кеша
async def remove_rank(redis: aioredis.Redis, rank_name: str, chat_id: int):
    query = """
    DELETE FROM ranks WHERE rank_name = $1 AND chat_id = $2
    RETURNING rank_name, points_required
    """
    deleted_ranks = await db.fetchone(query, (rank_name, chat_id))

    if deleted_ranks:
        # Удаляем ранг из кеша Redis
        await redis.hdel(f"chat:{chat_id}:ranks", rank_name)

        # Обновляем кеш всех рангов
        ranks = await get_all_ranks(redis, chat_id)

        # Если все ранги удалены, восстанавливаем начальный ранг в Redis
        if not ranks:
            initial_rank = await get_initial_rank(redis, chat_id)
            if initial_rank:
                await set_hash_fields(redis, f"chat:{chat_id}", {"initial_rank": initial_rank})

        logging.info(f"Ранг {rank_name} удален для чата {chat_id}.")
    else:
        logging.warning(f"Не удалось удалить ранг {rank_name} для чата {chat_id}.")


# Удалить эмоцию
async def remove_emotion(redis: aioredis.Redis, chat_id: int, emotion: str):
    query = "DELETE FROM emotions WHERE chat_id = $1 AND emotion = $2"
    await db.execute(query, (chat_id, emotion))

    await redis.hdel(f"chat:{chat_id}:emotions", emotion)
    logging.info(f"Эмоция {emotion} удалена для чата {chat_id}.")


# Сбросить все данные (все пользователи, эмоции, ранги и т.д.)
async def reset_all_data(redis: aioredis.Redis):
    # Сбросить всех пользователей
    await db.execute("UPDATE users SET \"rank\" = 'Новичек', points = 0")
    
    # Удалить все эмоции
    await db.execute("DELETE FROM emotions")
    
    # Удалить все ранги
    await db.execute("DELETE FROM ranks")
    
    # Сбросить настройки
    await db.execute("DELETE FROM settings")

    # Вставить начальный ранг, если он был задан ранее
    await db.execute("INSERT INTO settings (initial_rank) VALUES ('Новичек') ON CONFLICT (chat_id) DO UPDATE SET initial_rank = 'Новичек'")

    # Очистить кеш
    await redis.flushdb()
