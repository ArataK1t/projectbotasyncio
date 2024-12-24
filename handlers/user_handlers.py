from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.enums import ParseMode
from database.queries import get_all_ranks, get_initial_rank
from database.users import add_user, get_user_info, set_user_rank
from database.connection import Database
import logging
from aiogram import Bot
import asyncio
import aioredis
db = Database()

async def delete_message_after_delay(message: types.Message, delay: int = 5):
    """Удаляет сообщение после указанной задержки в секундах"""
    await asyncio.sleep(delay)  # Ожидаем заданное количество секунд
    try:
        await message.delete()  # Удаляем сообщение
        logging.info(f"Сообщение {message.message_id} успешно удалено.")
    except Exception as e:
        logging.error(f"Ошибка при удалении сообщения {message.message_id}: {e}")



# Получение user_id по username
async def get_user_id_by_username(username: str, chat_id: int):
    query = "SELECT user_id FROM users WHERE username = $1 AND chat_id = $2"
    result = await db.fetchone(query, (username, chat_id))
    return result['user_id'] if result else None

# Получение имени пользователя для отображения
async def get_user_display_name(bot: Bot, chat_id: int, user_id: int) -> str:
    """
    Получает имя пользователя для отображения.
    Возвращает username (с префиксом @), полное имя или user_id, если никакой информации нет.
    """
    try:
        user_info = await bot.get_chat_member(chat_id, user_id)
        user = user_info.user
        if user.username:
            return f"@{user.username}"
        if user.first_name or user.last_name:
            return f"{user.first_name or ''} {user.last_name or ''}".strip()
        return str(user_id)
    except Exception as e:
        logging.error(f"Ошибка получения имени пользователя {user_id} в чате {chat_id}: {e}")
        return str(user_id)

# Получение названия чата
async def get_chat_display_name(bot: Bot, chat_id: int) -> str:
    """
    Получает название чата из базы данных.
    Если данных нет, возвращает chat_id в строковом формате.
    """
    try:
        query = "SELECT chat_name FROM chats WHERE chat_id = $1"
        # Передаем chat_id как кортеж
        result = await db.fetchone(query, (chat_id,))
        if result and result.get("chat_name"):
            return result["chat_name"]
        return str(chat_id)
    except Exception as e:
        logging.error(f"Ошибка получения названия чата для chat_id {chat_id}: {e}")
        return str(chat_id)


# Обработчик команды для получения информации о пользователе
async def get_user_info_handler(message: types.Message, redis: aioredis.Redis, bot: Bot):
    """
    Обработчик команды для получения информации о пользователе.
    Если аргумент не указан, показывает информацию о пользователе, вызвавшем команду.
    Если указан username или user_id, показывает информацию о соответствующем пользователе.
    """
    chat_id = message.chat.id
    try:
        args = message.text.split()
        if len(args) == 1:
            # Если аргумент не указан, показываем информацию о пользователе, вызвавшем команду
            user_id = message.from_user.id
            username = message.from_user.username or f"ID {user_id}"
        else:
            # Если аргумент указан, проверяем, это user_id или username
            identifier = args[1].strip()
            if identifier.isdigit():  # Если это user_id
                user_id = int(identifier)
                username = await get_user_display_name(bot, chat_id, user_id)
            else:  # Иначе считаем, что это username
                user_id = await get_user_id_by_username(identifier, chat_id)
                if user_id:
                    username = identifier
                else:
                    del_message = await message.reply(f"Пользователь с username @{identifier} не найден.")
                    await delete_message_after_delay(del_message, delay=5)
                    return

        # Получаем информацию о пользователе из базы данных
        user_info = await get_user_info(redis, user_id, chat_id)
        
        if user_info:
            # Формируем ответ с использованием username
            rank = user_info['rank']
            points = user_info['points']
            del_message = await message.reply(f"Информация о пользователе @{username}:\nРанг: {rank}\nОчки: {points}")
            await delete_message_after_delay(del_message, delay=5)
        else:
            del_message = await message.reply(f"Пользователь @{username} не найден в базе данных.")
            await delete_message_after_delay(del_message, delay=5)
    
    except IndexError:
        # Если команда вызвана без аргументов и отладочная ошибка
        del_message = await message.reply("Используйте команду в формате:\n`/get_user_info [user_id|username]`")
        await delete_message_after_delay(del_message, delay=5)
    except Exception as e:
        logging.error(f"Ошибка при получении информации о пользователе: {e}")
        await message.reply("Произошла ошибка при обработке запроса.")



# Добавляем пользователя в базу данных с учетом chat_id и username
# Обработчик для команды /start (или при добавлении нового пользователя)
async def add_user_handler(message: types.Message,redis: aioredis.Redis, state: FSMContext):
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.username  # Получаем username пользователя
    rank = ""  # Изначально пустой ранг
    points = 0

    # Логируем информацию для отладки
    logging.info(f"Received user info: user_id={user_id}, chat_id={chat_id}, username={username}")

    # Проверка, существует ли чат в таблице
    chat_check_query = "SELECT chat_id FROM chats WHERE chat_id = %s"
    chat_exists = await db.fetchval(chat_check_query, (chat_id,))

    if not chat_exists:
        # Если чата нет в базе, проверяем, является ли это личным чатом
        if message.chat.type == "private":
            chat_name = "Личный чат"  # Название для личных чатов
        else:
            chat_name = message.chat.title if message.chat.title else "Неизвестное название чата"

        # Логируем информацию о добавлении чата
        logging.info(f"Chat does not exist, adding chat: chat_id={chat_id}, chat_name={chat_name}")

        # Добавляем чат в базу данных
        insert_chat_query = "INSERT INTO chats (chat_id, chat_name) VALUES (%s, %s)"
        await db.execute(insert_chat_query, (chat_id, chat_name))

    # Добавляем пользователя в базу данных
    try:
        # Добавляем пользователя с пустым рангом
        await add_user(redis, user_id, chat_id, username, rank, points)
        await message.reply("Вы были успешно добавлены в систему!")

        # Получаем начальный ранг для этого чата
        initial_rank = await get_initial_rank(chat_id)
        if initial_rank:
            # Устанавливаем начальный ранг
            await set_user_rank(user_id, chat_id, initial_rank)
            await message.reply(f"Ваш начальный ранг установлен как {initial_rank}.")

        logging.info(f"User {user_id} successfully added to the system in chat {chat_id}")
    except Exception as e:
        logging.error(f"Error adding user {user_id} to chat {chat_id}: {e}")
        await message.reply("Произошла ошибка при добавлении пользователя в систему.")


# Обновление ранга пользователя в зависимости от очков
async def update_user_rank_based_on_points(redis: aioredis.Redis, user_id: int, chat_id: int, points: int, bot: Bot):
    """Обновление ранга пользователя в зависимости от очков."""
    ranks = await get_all_ranks(redis, chat_id)
    if not ranks:
        logging.error(f"Не удалось найти ранги для чата {chat_id}.")
        return

    points = int(points)
    ranks = sorted(ranks, key=lambda r: r['points_required'], reverse=True)

    new_rank = None
    for rank in ranks:
        if points >= rank['points_required']:
            new_rank = rank['rank_name']
            break

    if not new_rank:
        new_rank = await get_initial_rank(redis, chat_id) or "Новичок"

    current_user_info = await get_user_info(redis, user_id, chat_id)
    current_rank = current_user_info['rank']

    if current_rank != new_rank:
        await set_user_rank(redis, user_id, chat_id, new_rank)
        await bot.set_chat_administrator_custom_title(chat_id, user_id, new_rank)
        logging.info(f"Ранг пользователя {user_id} обновлен на {new_rank}.")
    else:
        logging.info(f"Ранг пользователя {user_id} не изменился.")





