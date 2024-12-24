import logging
import asyncpg
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
import aioredis

class Database:
    def __init__(self):
        self.pool = None  # Инициализация пула соединений (пока не создан)

    async def init(self):
        """Инициализация пула соединений с базой данных PostgreSQL."""
        if self.pool:
            logging.info("Database connection pool is already initialized")
            return

        try:
            logging.info("Initializing database connection pool...")
            self.pool = await asyncpg.create_pool(
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                host=DB_HOST,
                port=DB_PORT
            )
            logging.info("Database connection pool initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize the database connection pool: {e}")
            raise e

    async def execute(self, query, params=None):
        """Выполнение SQL-запроса без возвращаемого результата."""
        if not self.pool:
            await self.init()  # Инициализация пула, если он еще не создан

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, *params if params else ())
                logging.info(f"Executed query: {query} with params: {params}")
        except Exception as e:
            logging.error(f"Error executing query: {query}. Error: {e}")
            raise e

    async def fetchone(self, query, params=None):
        """Получение одной строки из результата SQL-запроса."""
        if not self.pool:
            await self.init()  # Инициализация пула, если он еще не создан

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, *params if params else ())
                if result:
                    logging.info(f"Executed query: {query}, Fetched result: {result}")
                else:
                    logging.info(f"Executed query: {query}, No result found.")
                return result
        except Exception as e:
            logging.error(f"Error fetching data with query: {query}. Error: {e}")
            raise e

    async def fetchall(self, query, params=None):
        """Получение всех строк из результата SQL-запроса."""
        if not self.pool:
            await self.init()  # Инициализация пула, если он еще не создан

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetch(query, *params if params else ())
                logging.info(f"Executed query: {query}, Fetched results: {len(result)} rows.")
                return result
        except Exception as e:
            logging.error(f"Error fetching data with query: {query}. Error: {e}")
            raise e

    async def close(self):
        """Закрытие пула соединений с базой данных."""
        if self.pool:
            await self.pool.close()  # Закрываем пул
            logging.info("Database connection pool closed.")
        else:
            logging.warning("Database pool is not initialized or already closed.")

    async def fetchval(self, query, params=None):
        """Получение одного значения из результата SQL-запроса."""
        try:
            result = await self.fetchone(query, params)
            if result is None:
                logging.info("No result found.")
                return None
            logging.info(f"Fetched value: {result.get('chat_id')}")
            return result.get('chat_id')  # Возвращаем нужное значение
        except Exception as e:
            logging.error(f"Error in fetchval with query: {query}. Error: {e}")
            raise e
        

# Создаем экземпляр класса Database для использования
db = Database()

        

# Инициализация Redis
redis = None

async def init_redis():
    global redis
    try:
        redis = await aioredis.from_url("redis://localhost:6379", decode_responses=True, encoding="utf-8")
        pong = await redis.ping()  # Пингуем Redis и получаем ответ
        logging.info(f"Ответ от Redis ping: {pong}") # Выводим ответ
        logging.info("Redis успешно подключен.")
        return redis
    except aioredis.exceptions.ConnectionError as e: # Ловим именно ошибки подключения
        logging.error(f"Ошибка подключения к Redis: {e}")
        return None
    except Exception as e: # Ловим другие ошибки (например, неправильный URL)
        logging.exception(f"Неожиданная ошибка при инициализации Redis: {e}")
        return None


# Функция для безопасного получения данных из Redis
async def safe_get_cache(redis: aioredis.Redis, key: str):
    try:
        cached_value = await redis.get(key)
        if cached_value is None:
            return None
        return cached_value  # Мы больше не декодируем, так как `decode_responses=True`
    except aioredis.RedisError as e:
        logging.error(f"Ошибка Redis при получении ключа {key}: {e}")
        return None

# Функция для записи данных в Redis
async def set_cache(redis: aioredis.Redis, key: str, value: str, ttl: int = 86400):
    try:
        await redis.setex(key, ttl, value)
    except aioredis.RedisError as e:
        logging.error(f"Ошибка Redis при установке ключа {key}: {e}")


# Функция для безопасного получения поля из Redis хэша с преобразованием значений в числа
async def safe_get_hash_field(redis: aioredis.Redis, key: str, field: str, convert=True):
    try:
        value = await redis.hget(key, field)
        
        # Преобразуем значение, если оно существует и если нужно
        if value and convert:
            return str(value)  # Преобразование в строку
        return value
    except aioredis.RedisError as e:
        logging.error(f"Ошибка Redis при получении поля {field} из хэша {key}: {e}")
        return None



# Функция для безопасного установки значений в Redis хэш с преобразованием значений
# Функция для безопасного установления значений в Redis хэш с преобразованием значений
# Функция для безопасного установления значений в Redis хэш с преобразованием значений
async def set_hash_fields(redis: aioredis.Redis, key: str, mapping: dict, ttl: int = 86400):
    if mapping is None:
        logging.warning(f"Попытка установить пустые поля для ключа {key}")
        return

    try:
        logging.info(f"Переданные данные для установки в Redis: ключ: {key}, данные: {mapping}")

        if isinstance(mapping, dict):
            # Используем метод hset с параметром mapping
            for field, value in mapping.items():
                await redis.hset(key, field, value)  # Передаем поля по отдельности
        else:
            logging.error(f"Недопустимый тип данных для mapping: {type(mapping)}. Ожидается словарь.")
            return

        await redis.expire(key, ttl)
        logging.info(f"Поля для хэша {key} успешно установлены.")

    except aioredis.RedisError as e:
        logging.error(f"Ошибка Redis при установке полей для хэша {key}: {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при установке полей для хэша {key}: {e}")


# Функция для безопасного инкрементирования поля в хэше Redis с использованием hincrby
async def safe_hincrby(redis: aioredis.Redis, key: str, mapping: dict, ttl: int = 86400):
    """Инкрементирует поля в хэше Redis через hincrby для каждого поля в mapping."""
    
    try:
        for field, increment in mapping.items():
            # Инкрементируем значение поля с помощью hincrby
            new_value = await redis.hincrby(key, field, increment)
            
            # Логируем новое значение поля
            logging.info(f"Инкрементировано поле '{field}' на {increment}. Новое значение: {new_value}")
        
        # Устанавливаем TTL для ключа, если требуется
        await redis.expire(key, ttl)
        return True
    except aioredis.RedisError as e:
        logging.error(f"Ошибка Redis при инкрементировании поля в хэше {key}: {e}")
        return False
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при инкрементировании поля в хэше {key}: {e}")
        return False




def prepare_redis_data(data):
    """Преобразует данные в нужный формат для Redis (строки или байты)."""
    if isinstance(data, dict):
        # Преобразуем словарь: ключи и значения в байты
        return {str(k).encode(): str(v).encode() for k, v in data.items()}
    elif isinstance(data, list):
        # Преобразуем список кортежей
        return [(str(k).encode(), str(v).encode()) for k, v in data]
    elif isinstance(data, tuple):
        # Преобразуем кортеж
        return (str(data[0]).encode(), str(data[1]).encode())
    else:
        # Для строки или других типов данных
        return str(data).encode()


# Функция для добавления элемента в Sorted Set
async def add_to_sorted_set(redis: aioredis.Redis, key: str, member, score):
    """Добавление элемента в отсортированное множество Redis (Sorted Set)."""
    try:
        # Обрабатываем данные с помощью prepare_redis_data
        member_data = prepare_redis_data(member)
        
        if isinstance(member_data, tuple):
            # Если member - это кортеж (например, (rank_name, points_required)), добавляем в zadd
            await redis.zadd(key, {member_data[0]: score})
        elif isinstance(member_data, dict):
            # Если member - это словарь, преобразуем каждый элемент
            await redis.zadd(key, *[(k.decode(), score) for k, score in member_data.items()])
        else:
            # Преобразуем строковые данные и добавляем в zadd
            await redis.zadd(key, {member_data: score})
            
        logging.info(f"Элемент {member} с баллами {score} добавлен в Sorted Set {key}.")
    except aioredis.RedisError as e:
        logging.error(f"Ошибка Redis при добавлении элемента в Sorted Set {key}: {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при добавлении элемента в Sorted Set {key}: {e}")


# Функция для получения элементов из Sorted Set
async def get_sorted_set(redis: aioredis.Redis, key: str, start=0, end=-1):
    """Получение всех элементов из отсортированного множества Redis (Sorted Set)."""
    try:
        # Получаем отсортированные элементы с помощью zrange
        ranks = await redis.zrange(key, start, end, withscores=True)
        return [(rank, score) for rank, score in ranks]  # Декодируем из байтов и возвращаем список
    except aioredis.RedisError as e:
        logging.error(f"Ошибка Redis при получении данных из Sorted Set {key}: {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при получении данных из Sorted Set {key}: {e}")
    return []


# Функция для удаления элемента из Sorted Set
async def remove_from_sorted_set(redis: aioredis.Redis, key: str, member):
    """Удаление элемента из отсортированного множества Redis (Sorted Set)."""
    try:
        # Обрабатываем данные с помощью prepare_redis_data
        member_data = prepare_redis_data(member)
        
        if isinstance(member_data, tuple):
            # Если это кортеж, удаляем его из множества
            await redis.zrem(key, member_data[0])
        elif isinstance(member_data, dict):
            # Если это словарь, удаляем все его элементы из множества
            for k in member_data.keys():
                await redis.zrem(key, k.decode())
        else:
            # Если это строка или другие данные, просто удаляем элемент
            await redis.zrem(key, member_data)
            
        logging.info(f"Элемент {member} удален из Sorted Set {key}.")
    except aioredis.RedisError as e:
        logging.error(f"Ошибка Redis при удалении элемента из Sorted Set {key}: {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при удалении элемента из Sorted Set {key}: {e}")


# Создаем экземпляр класса Database для использования
db = Database()
