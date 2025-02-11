import logging
from aiogram import types, Bot
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Update, Message
from aiogram.enums import ReactionTypeType
from aiogram import Dispatcher
from aiogram.types import MessageReactionUpdated
from handlers.user_handlers import get_user_id_by_username, update_user_rank_based_on_points, get_chat_display_name, get_user_display_name, delete_message_after_delay
from database.queries import ( 
                              set_initial_rank, set_rank_requirements, 
                              set_emotion_reward, get_all_emotions, 
                              get_all_ranks, 
                              remove_rank, remove_emotion, get_chat_settings,
                              reset_all_data, get_emotion_points,
                              get_initial_rank)
from config import CREATOR_ID
from handlers.common_handlers import get_user_role, assign_zero_admin_role
from database.users import add_user, get_user_info, set_user_rank, set_user_points, get_user_points, get_all_users, reset_user
from database.connection import db, set_hash_fields, safe_get_hash_field, safe_hincrby
from aiogram.types import ChatMember

from database.models import normalize_emoji
import asyncio
import aioredis
from datetime import datetime, timedelta
import emoji




# Команда: Установить начальный ранг для новых пользователей
async def set_initial_rank_handler(message: types.Message, redis: aioredis.Redis, bot: Bot):
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role != "creator":
        del_message = await message.answer("Эта команда доступна только создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        rank = message.text.split(' ', 1)[1].strip()
        chat_id = message.chat.id
        await set_initial_rank(redis, chat_id, rank)
        del_message = await message.answer(f"Начальный ранг для новых пользователей в этом чате установлен на {rank}.")
        await delete_message_after_delay(del_message, delay=5)
    except IndexError:
        del_message = await message.answer("Пожалуйста, укажите имя ранга.")
        await delete_message_after_delay(del_message, delay=5)

# Функция для добавления ранга в базу данных и кеш Redis
async def add_rank_handler(message: types.Message, redis: aioredis.Redis, bot: Bot):
    """Обрабатывает команду добавления нового ранга."""
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role != "creator":
        del_message = await message.answer("Эта команда доступна только создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        args = message.text.split(' ', 2)
        rank_name = args[1]
        chat_id = message.chat.id
        points_required = int(args[2])

        # Устанавливаем требования для ранга в базу данных и кеш
        await set_rank_requirements(redis, rank_name, chat_id, points_required)

        del_message = await message.answer(f"Ранг {rank_name} с требованиями {points_required} очков добавлен.")
        await delete_message_after_delay(del_message, delay=5)
    except (IndexError, ValueError):
        del_message = await message.answer("Неверный формат. Используйте команду: /add_rank <название_ранга> <количество_очков>.")
        await delete_message_after_delay(del_message, delay=5)


# Функция для удаления ранга из базы данных и кеш Redis
async def remove_rank_handler(message: types.Message, redis: aioredis.Redis, bot: Bot):
    """Обрабатывает команду удаления ранга."""
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role != "creator":
        del_message = await message.answer("Эта команда доступна только создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        rank_name = message.text.split(' ', 1)[1].strip()
        chat_id = message.chat.id

        # Удаляем ранг из базы данных и кеша
        await remove_rank(redis, rank_name, chat_id)

        del_message = await message.answer(f"Ранг {rank_name} удален.")
        await delete_message_after_delay(del_message, delay=5)
    except IndexError:
        del_message = await message.answer("Пожалуйста, укажите название ранга для удаления.")
        await delete_message_after_delay(del_message, delay=5)


# Функция для просмотра всех рангов
async def list_ranks_handler(message: types.Message, redis: aioredis.Redis, bot: Bot):
    """Обрабатывает команду просмотра всех рангов."""
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role not in ["creator", "admin", "admin_without_rights"]:
        del_message = await message.answer("Команда недоступна для вас.")
        await delete_message_after_delay(del_message, delay=5)
        return

    chat_id = message.chat.id
    ranks = await get_all_ranks(redis, chat_id)

    if ranks:
        response = "Список всех рангов:\n"
        for rank in ranks:
            response += f"{rank['rank_name']}: {rank['points_required']} очков\n"
        del_message = await message.answer(response)
        await delete_message_after_delay(del_message, delay=5)
    else:
        del_message = await message.answer("Ранги не настроены.")
        await delete_message_after_delay(del_message, delay=5)



# Команда: Установить очки за эмоцию
async def set_emotion_reward_handler(message: types.Message, redis: aioredis.Redis, bot: Bot):
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role != "creator":
        del_message = await message.answer("Эта команда доступна только создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        args = message.text.split(' ', 2)
        emotion = args[1]
        points = int(args[2])
        chat_id = message.chat.id

        # Нормализуем эмоцию перед сохранением
        emotion = normalize_emoji(emotion)
        
        await set_emotion_reward(redis, chat_id, emotion, points)
        del_message = await message.answer(f"Эмоция {emotion} теперь дает {points} очков.")
        await delete_message_after_delay(del_message, delay=5)
    except (IndexError, ValueError):
        del_message = await message.answer("Неверный формат. Используйте команду: /set_emotion <эмоция> <очки>.")
        await delete_message_after_delay(del_message, delay=5)

# Команда: Удалить эмоцию
async def remove_emotion_handler(message: types.Message, redis: aioredis.Redis, bot: Bot):
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role != "creator":
        del_message = await message.answer("Эта команда доступна только создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        emotion = message.text.split(' ', 1)[1].strip()
        chat_id = message.chat.id

        # Нормализуем эмоцию перед удалением
        emotion = normalize_emoji(emotion)
        
        await remove_emotion(redis, chat_id, emotion)
        del_message = await message.answer(f"Эмоция {emotion} удалена.")
        await delete_message_after_delay(del_message, delay=5)
    except IndexError:
        del_message = await message.answer("Пожалуйста, укажите эмоцию для удаления.")
        await delete_message_after_delay(del_message, delay=5)

# Команда: Просмотр всех эмоций
async def list_emotions_handler(message: types.Message, redis: aioredis.Redis, bot: Bot):
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role not in ["creator", "admin", "admin_without_rights"]:
        del_message = await message.answer("Команда недоступна для вас.")
        await delete_message_after_delay(del_message, delay=5)
        return

    chat_id = message.chat.id
    emotions = await get_all_emotions(redis, chat_id)

    if emotions:
        response = "Список всех эмоций:\n"
        for emotion, points in emotions:
            # Преобразуем текстовое представление эмодзи в саму эмодзи
            emotion_symbol = emoji.emojize(emotion)  # Если в базе данные содержат такие строки, как :smile:, он преобразует их в символы
            response += f"{emotion_symbol}: {points} очков\n"
        del_message = await message.answer(response)
        await delete_message_after_delay(del_message, delay=5)
    else:
        del_message = await message.answer("Эмоции не настроены.")
        await delete_message_after_delay(del_message, delay=5)









CHAT_LIMITS = {}

async def set_limits_command(message: types.Message, redis: aioredis.Redis, bot: Bot):
    """
    Устанавливает лимиты на реакции.
    Пример: /set_limits 5 20
    """
    chat_id = message.chat.id
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role != "creator":
        del_message = await message.answer("Эта команда доступна только создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    args = message.text.split()[1:]  # Разделяем текст сообщения на аргументы
    if len(args) != 2:
        del_message = await message.answer("Пожалуйста, укажите два числа: лимит на человека и общий лимит.")
        await delete_message_after_delay(del_message, delay=5)
        return

    # Проверяем, что оба значения - это цифры
    if not args[0].isdigit() or not args[1].isdigit():
        del_message = await message.answer("Оба лимита должны быть целыми числами.")
        await delete_message_after_delay(del_message, delay=5)
        return

    # Преобразуем строки в числа
    per_person_limit = int(args[0])
    total_limit = int(args[1])

    # Проверяем, что лимиты положительные
    if per_person_limit <= 0 or total_limit <= 0:
        del_message = await message.answer("Лимиты должны быть положительными числами.")
        await delete_message_after_delay(del_message, delay=5)
        return

    # Сохраняем лимиты в базе данных
    await db.execute("""
        UPDATE settings
        SET reaction_limit_per_person = $1, reaction_limit_total = $2
        WHERE chat_id = $3
    """, (per_person_limit, total_limit, chat_id))

    # Обновляем кеш в Redis после изменения данных в базе с использованием хэша
    await set_hash_fields(redis, f"reaction_limits:{chat_id}", {
        "per_person_limit": per_person_limit,
        "total_limit": total_limit
    })

    del_message = await message.reply(f"Лимиты установлены:\nНа одного человека: {per_person_limit}\nВсего: {total_limit}")
    await delete_message_after_delay(del_message, delay=5)


async def check_reaction_limits(redis: aioredis.Redis, chat_id: int, user_id: int, author_id: int) -> bool:
    """Проверяет лимиты реакций для пользователя."""
    logging.info(f"Проверка лимитов для пользователя {user_id} на реакцию к автору {author_id} в чате {chat_id}")

    if author_id is None:
        logging.info(f"Автор {author_id} не указан. Пропускаем проверку лимитов.")
        return False

    if user_id == author_id:
        logging.info(f"Пользователь {user_id} попытался поставить реакцию самому себе. Проверка лимитов пропущена.")
        return True

    # Получаем лимиты из Redis
    limits = await redis.hgetall(f"reaction_limits:{chat_id}")
    logging.info(f"Полученные лимиты из Redis для чата {chat_id}: {limits}")

    if not limits:
        logging.info(f"Лимиты для чата {chat_id} не найдены в Redis, пытаемся загрузить из базы данных.")
        result = await db.fetchone("""
            SELECT reaction_limit_per_person, reaction_limit_total
            FROM settings
            WHERE chat_id = $1
        """, (chat_id,))

        if not result:
            logging.error(f"Лимиты для чата {chat_id} не найдены в базе данных.")
            return False

        per_person_limit = result["reaction_limit_per_person"]
        total_limit = result["reaction_limit_total"]

        async with redis.pipeline() as pipeline:
            pipeline.hset(f"reaction_limits:{chat_id}", "per_person_limit", str(per_person_limit))
            pipeline.hset(f"reaction_limits:{chat_id}", "total_limit", str(total_limit))
            await pipeline.execute()
        logging.info(f"Записаны лимиты в Redis: per_person_limit={per_person_limit}, total_limit={total_limit}")
    else:
        try:
            per_person_limit = int(limits["per_person_limit"])
            total_limit = int(limits["total_limit"])
        except (KeyError, ValueError) as e:
            logging.error(f"Ошибка при парсинге лимитов из Redis: {e}, limits: {limits}")
            return False

    # Проверка текущих значений реакций (ОПТИМИЗИРОВАНО С PIPELINE)
    total_key = f"reactions:total:{chat_id}:{user_id}"
    per_person_key = f"reactions:person:{chat_id}:{user_id}:{author_id}"

    async with redis.pipeline() as pipeline: # Оптимизация с pipeline
        pipeline.get(total_key)
        pipeline.get(per_person_key)
        total_reactions_bytes, per_person_reactions_bytes = await pipeline.execute()

    total_reactions = int(total_reactions_bytes or 0)
    per_person_reactions = int(per_person_reactions_bytes or 0)

    logging.info(f"Общие реакции пользователя {user_id}: {total_reactions}/{total_limit}. Реакции на автора {author_id}: {per_person_reactions}/{per_person_limit}.")

    # *** ПРОВЕРКА ЛИМИТОВ ПЕРЕД ИНКРЕМЕНТОМ ***
    if total_reactions >= total_limit:
        logging.info(f"Пользователь {user_id} превысил общий лимит {total_limit} реакций.")
        return True
    if per_person_reactions >= per_person_limit:
        logging.info(f"Пользователь {user_id} превысил лимит {per_person_limit} реакций для автора {author_id}.")
        return True

    # Инкрементирование реакций
    async with redis.pipeline() as pipeline:
        pipeline.incr(total_key)
        pipeline.expire(total_key, 86400)  # 24 часа
        pipeline.incr(per_person_key)
        pipeline.expire(per_person_key, 86400)  # 24 часа
        await pipeline.execute()

    # После инкрементирования снова получаем актуальные значения
    total_reactions = int(await redis.get(total_key) or 0)
    per_person_reactions = int(await redis.get(per_person_key) or 0)

    logging.info(f"После инкрементирования: Общие реакции пользователя {user_id}: {total_reactions}/{total_limit}. Реакции на автора {author_id}: {per_person_reactions}/{per_person_limit}.")

    # Если лимиты не превышены
    logging.info(f"Лимиты не превышены для пользователя {user_id}. Реакции добавлены.")
    return False













# Кэширование информации о сообщении
async def cache_message_info(redis: aioredis.Redis, chat_id: int, message_id: int, user_id: int):
    """
    Сохраняет информацию о сообщении в кэше (Redis).
    """
    key = f"message:{chat_id}:{message_id}"
    logging.info(f"Попытка записать в Redis ключ {key} с значением {user_id}")
    
    try:
        # Вместо обычной строки, используем хэш для хранения сообщения
        await redis.hset(key, "user_id", user_id)
        await redis.expire(key, 86400)  # TTL 24 часа
        logging.info(f"Сообщение {message_id} с автором {user_id} сохранено в кэш по ключу {key}.")
    except aioredis.RedisError as e:
        logging.error(f"Ошибка записи в Redis: {e}")


# Обработчик новых сообщений
async def on_new_message(message: types.Message, redis: aioredis.Redis):
    """
    Обработчик новых сообщений с использованием Redis Pipeline для кеширования.
    """
    
    chat_id = message.chat.id
    message_id = message.message_id
    user_id = message.from_user.id
    username = message.from_user.username

    logging.info(f"Получено новое сообщение {message_id} от {user_id} в чате {chat_id}.")

    try:
        user_cache_key = f"user:{user_id}:{chat_id}"
        user_info_cache = await safe_get_hash_field(redis, user_cache_key, "user_id")

        if not user_info_cache:
            user_info = await get_user_info(redis, user_id, chat_id)
            if not user_info:
                initial_rank = await get_initial_rank(redis, chat_id) or "Новичок"
                chat_name = message.chat.title or "Неизвестное название чата"
                await add_user(redis, user_id, chat_id, username=username, rank=initial_rank, points=0, chat_name=chat_name)
                user_info = await get_user_info(redis, user_id, chat_id)
            if user_info:
                user_data_to_cache = {
                    "user_id": str(user_info['user_id']),
                    "chat_id": str(user_info['chat_id']),
                    "username": str(user_info['username']),
                    "rank": str(user_info['rank']),
                    "points": str(user_info['points'])
                }
                logging.info(f"Тип user_cache_key перед set_hash_fields: {type(user_cache_key)}")
                logging.info(f"Тип user_data_to_cache перед set_hash_fields: {type(user_data_to_cache)}")
                logging.info(f"Содержимое user_data_to_cache перед set_hash_fields: {user_data_to_cache}")
                await set_hash_fields(redis, user_cache_key, user_data_to_cache)
                user_role = await get_user_role(message.bot, chat_id, user_id)
                if user_role == "member":
                    await assign_zero_admin_role(message.bot, chat_id, user_id, user_info['rank'])

        message_cache_key = f"message:{chat_id}:{message_id}"
        message_data_to_cache = {
            "user_id": str(user_id),
            "author_id": str(user_id)  # Добавляем author_id
        }
        logging.info(f"Тип message_cache_key: {type(message_cache_key)}, Тип message_data_to_cache: {type(message_data_to_cache)}")

        await set_hash_fields(redis, message_cache_key, message_data_to_cache)


    except Exception as e:
        logging.exception(f"Ошибка при обработке сообщения {message.message_id}: {e}")













async def process_reaction(update: Update, redis: aioredis.Redis, bot: Bot):
    logging.info(f"Тип update: {type(update)}")

    try:
        if not isinstance(update, MessageReactionUpdated):
            logging.warning("Обновление не является MessageReactionUpdated.")
            return

        chat_id = update.chat.id
        message_id = update.message_id
        user_id = update.user.id
        new_reaction = update.new_reaction

        logging.info(f"Получены данные: chat_id={chat_id}, message_id={message_id}, user_id={user_id}, new_reaction={new_reaction}")

        if not new_reaction:
            logging.info("Реакция убрана. Ничего не происходит.")
            return

        emotion = None
        if new_reaction:
            for reaction in new_reaction:
                if reaction.type == "emoji":
                    emotion = reaction.emoji
                elif reaction.type == "custom_emoji":
                    emotion = reaction.custom_emoji_id
                elif reaction.type == "paid":
                    emotion = "paid"

        if not emotion:
            logging.info("Эмоция не найдена.")
            return

        logging.info(f"Эмоция для реакции: {emotion}")

        emotion = emoji.emojize(normalize_emoji(emotion))

        points_task = asyncio.create_task(get_emotion_points(redis, emotion, chat_id))
        points = await points_task
        if points is None:
            logging.info(f"Эмоция '{emotion}' в чате {chat_id} не имеет очков. Начисление пропущено.")
            return

        author_task = asyncio.create_task(safe_get_hash_field(redis, f"message:{chat_id}:{message_id}", "author_id"))
        author_id_bytes = await author_task

        if not author_id_bytes:
            await send_error_message(bot, chat_id, "Автор сообщения не найден в кеше.")
            return

        try:
            author_id = int(author_id_bytes)
        except (ValueError, TypeError) as e:
            logging.error(f"Не удалось преобразовать author_id в int: {author_id_bytes}, Ошибка: {e}")
            await send_error_message(bot, chat_id, "Ошибка обработки author_id.")
            return

        logging.info(f"Автор сообщения: {author_id}")

        if user_id == author_id:
            logging.info(f"Пользователь {user_id} попытался поставить реакцию самому себе. Проверка лимитов пропущена.")
            await send_error_message(bot, chat_id, "Вы не можете поставить реакцию на собственное сообщение.")
            return

        reaction_limits_ok = await check_reaction_limits(redis, chat_id, user_id, author_id)

        if reaction_limits_ok:
            logging.info(f"Пользователь {user_id} превысил лимиты на реакции для автора {author_id}.")
            await send_error_message(bot, chat_id, "Вы превысили лимит реакций за последние 24 часа.")
            return
        else:
            logging.info(f"Пользователь {user_id} прошел проверку лимитов.")

        display_name_tasks = asyncio.gather(
            get_user_display_name(bot, chat_id, user_id),
            get_user_display_name(bot, chat_id, int(author_id))
        )
        username, author_name = await display_name_tasks

        # Обновляем очки в Redis с использованием safe_hincrby
        user_points_key = f"user:{author_id}:{chat_id}"
        success = await safe_hincrby(redis, user_points_key, {"points": points})

        if success:
            logging.info(f"Очки успешно начислены пользователю {author_id} в чате {chat_id}.")

            # Получаем актуальные очки из Redis
            updated_points = await redis.hget(user_points_key, "points")
            updated_points = int(updated_points) if updated_points else points

            # Обновляем данные в базе данных без повторного обновления кеша
            query = """
            UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3
            """
            await db.execute(query, (updated_points, author_id, chat_id))

            # Обновляем ранг пользователя на основе новых очков
            await update_user_rank_based_on_points(redis, author_id, chat_id, updated_points, bot)

            del_message = await bot.send_message(
                chat_id,
                text=f"{username} поставил реакцию '{emotion}', и {author_name} получил {points} очков!"
            )
            asyncio.create_task(delete_message_after_delay(del_message, delay=5))
        else:
            logging.error(f"Не удалось обновить очки для пользователя {author_id} в чате {chat_id}.")

    except Exception as e:
        logging.error(f"Ошибка обработки реакции: {e}")







async def send_error_message(bot: Bot, chat_id: int, message: str):
    """Отправляет сообщение об ошибке и удаляет его через 5 секунд."""
    del_message = await bot.send_message(chat_id, text=message)
    asyncio.create_task(delete_message_after_delay(del_message, delay=5))

















async def set_user_rank_handler(redis: aioredis.Redis, message: types.Message, bot: Bot):
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role != "creator":
        del_message = await message.answer("Эта команда доступна только создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        args = message.text.split(' ', 2)
        if len(args) < 3:
            del_message = await message.answer("Неверный формат. Используйте команду: /set_user_rank <user_id/username> <rank_name>.")
            await delete_message_after_delay(del_message, delay=5)
            return

        identifier = args[1].strip()  # Это может быть либо user_id, либо username
        rank = args[2]
        chat_id = message.chat.id

        logging.info(f"Попытка установить ранг {rank} для пользователя с identifier={identifier} в чате {chat_id}.")

        # Проверяем, является ли это user_id или username
        if identifier.isdigit():  # Если это число, значит это user_id
            user_id = int(identifier)
        else:  # Иначе ищем по username
            user_id = await get_user_id_by_username(identifier, chat_id)
            if not user_id:
                del_message = await message.answer(f"Пользователь с username {identifier} не найден.")
                await delete_message_after_delay(del_message, delay=5)
                return

        # Устанавливаем новый ранг
        await set_user_rank(user_id, chat_id, rank)
        
        # Получаем текущие очки пользователя
        user_info = await get_user_info(redis, user_id, chat_id)
        if user_info:
            points = user_info['points']
            # Обновляем ранг, если нужно
            await update_user_rank_based_on_points(redis, user_id, chat_id, points, bot)

            del_message = await message.answer(f"Ранг пользователя {user_id} в чате {chat_id} установлен на {rank}.")
            await delete_message_after_delay(del_message, delay=5)
        else:
            del_message = await message.answer("Не удалось найти информацию о пользователе.")
            await delete_message_after_delay(del_message, delay=5)

    except Exception as e:
        logging.error(f"Ошибка при обработке команды set_user_rank_handler: {e}")
        await message.answer("Произошла ошибка при обработке команды.")

# Команда: Установить очки для пользователя
async def set_user_points_handler(message: Message, redis: aioredis.Redis, bot: Bot):
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role not in ["creator", "admin"]:
        del_message = await message.answer("Эта команда доступна только админам и создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        args = message.text.split(' ', 2)

        # Проверка количества аргументов
        if len(args) != 3:
            del_message = await message.reply("Неверный формат. Используйте команду: /set_user_points <user_id/username> <points>.")
            await delete_message_after_delay(del_message, delay=5)
            return

        identifier = args[1].strip()  # Это может быть либо user_id, либо username
        points = int(args[2])  # Преобразуем очки в целое число
        chat_id = message.chat.id

        # Проверка, является ли это user_id или username
        if identifier.isdigit():  # Если это число, значит это user_id
            user_id = int(identifier)
        else:  # Иначе ищем по username
            user_id = await get_user_id_by_username(identifier, chat_id)
            if not user_id:
                del_message = await message.reply(f"Пользователь с username {identifier} не найден.")
                await delete_message_after_delay(del_message, delay=5)
                return

        # Параллельно получаем данные пользователя и чата
        user_display_name_task = asyncio.create_task(get_user_display_name(bot, chat_id, user_id))
        chat_display_name_task = asyncio.create_task(get_chat_display_name(bot, chat_id))

        # Получаем имена и дополнительные данные
        username, chat_name = await asyncio.gather(user_display_name_task, chat_display_name_task)

        # Параллельно обновляем очки и ранг
        points_update_task = asyncio.create_task(set_user_points(redis, user_id, chat_id, points))
        rank_update_task = asyncio.create_task(update_user_rank_based_on_points(redis, user_id, chat_id, points, bot))

        # Ожидаем завершения всех задач
        await asyncio.gather(points_update_task, rank_update_task)

        # Отправляем ответ пользователю
        del_message = await message.reply(f"Очки пользователя {username} в чате \"{chat_name}\" обновлены до {points}. Ранг пользователя был обновлен.")
        await delete_message_after_delay(del_message, delay=5)

    except (IndexError, ValueError):
        del_message = await message.reply("Неверный формат. Используйте команду: /set_user_points <user_id/username> <points>.")
        await delete_message_after_delay(del_message, delay=5)


# Команда: Сбросить данные пользователя
async def reset_user_handler(message: Message, bot: Bot, redis: aioredis.Redis):
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role not in ["creator", "admin"]:
        del_message = await message.answer("Эта команда доступна только админам и создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        identifier = message.text.split(' ', 1)[1].strip()  # Это может быть либо user_id, либо username
        chat_id = message.chat.id

        # Проверяем, является ли это user_id или username
        if identifier.isdigit():  # Если это число, значит это user_id
            user_id = int(identifier)

            # Проверяем, существует ли пользователь в базе данных
            query = "SELECT 1 FROM users WHERE user_id = $1 AND chat_id = $2"
            user_exists = await db.fetchone(query, (user_id, chat_id))

            if not user_exists:
                del_message = await message.reply(f"Пользователь с user_id {user_id} не найден в чате.")
                await delete_message_after_delay(del_message, delay=5)
                return

        else:  # Иначе ищем по username
            user_id = await get_user_id_by_username(identifier, chat_id)
            if not user_id:
                del_message = await message.reply(f"Пользователь с username {identifier} не найден.")
                await delete_message_after_delay(del_message, delay=5)
                return

        # Сброс данных пользователя
        await reset_user(redis, user_id, chat_id, bot)
        del_message = await message.reply(f"Данные пользователя {user_id} в чате {chat_id} успешно сброшены.")
        await delete_message_after_delay(del_message, delay=5)

    except IndexError:
        del_message = await message.reply("Пожалуйста, укажите user_id или username пользователя для сброса.")
        await delete_message_after_delay(del_message, delay=5)
    except ValueError:
        del_message = await message.reply("Неверный формат user_id.")
        await delete_message_after_delay(del_message, delay=5)





# Команда: Просмотр лидерборда по поинтам
async def leaderboard_handler(message: Message, bot: Bot):
    # Проверка, является ли пользователь создателем или админом
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role not in ["creator", "admin", "admin_without_rights"]:
        del_message = await message.answer("Эта команда недоступна для вас")
        await delete_message_after_delay(del_message, delay=5)
        return

    chat_id = message.chat.id  # Получаем chat_id

    # Получаем всех пользователей и сортируем их по очкам
    users = await get_all_users(chat_id)  # Получаем всех пользователей из БД
    if not users:
        response = "Пользователи не найдены."
    else:
        # Сортировка пользователей по очкам (по убыванию)
        sorted_users = sorted(users, key=lambda u: u['points'], reverse=True)

        # Формирование топ 10
        leaderboard = "Топ 10 пользователей по очкам:\n"
        for i, user in enumerate(sorted_users[:10]):
            leaderboard += f"{i+1}. @{user.get('username', 'Не указан')} - Очки: {user['points']}, Ранг: {user['rank']}\n"

        # Проверяем место пользователя
        user_id = message.from_user.id
        user_position = None
        for i, user in enumerate(sorted_users):
            if user['user_id'] == user_id:
                user_position = i + 1
                break

        # Если пользователь не в топ 10, показываем его место
        if user_position and user_position > 10:
            user_info = next((user for user in sorted_users if user['user_id'] == user_id), None)
            if user_info:
                leaderboard += f"\nВаше место: {user_position}. @{user_info['username']} - Очки: {user_info['points']}, Ранг: {user_info['rank']}"
        else:
            leaderboard += "\nВы в топ 10!"

        response = leaderboard

    # Отправляем сообщение с лидербордом
    del_message = await message.reply(response)
    await delete_message_after_delay(del_message, delay=5)




async def get_settings_handler(message: Message, bot: Bot):
    """
    Обрабатывает команду получения текущих настроек чата.
    """
    # Проверка, является ли пользователь админом или создателем
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role not in ["creator", "admin"]:
        del_message = await message.answer("Эта команда доступна только админам и создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    chat_id = message.chat.id  # ID текущего чата

    # Получаем все настройки чата
    settings = await get_chat_settings(chat_id)
    if not settings:
        del_message = await message.reply("Настройки для данного чата не найдены.")
        await delete_message_after_delay(del_message, delay=5)
        return

    # Формирование строки для начальных настроек
    initial_settings = (
        "Текущие настройки чата:\n\n"
        f"🔶 Начальный ранг: {settings['initial_rank']}\n"
    )

    # Формирование строки для лимитов
    limits_info = (
        "\n🔷 Лимиты:\n"
        f"  🔹 Лимит реакций на одного пользователя: {settings['reaction_limit_per_person']}\n"
        f"  🔹 Общий лимит реакций в чате: {settings['reaction_limit_total']}\n"
    )

    # Формирование строки для настроек рангов
    ranks_info = "🔶 Ранги:\n"
    for rank in settings['ranks']:
        ranks_info += f"  🔸 {rank['rank_name']} ({rank['points_required']} очков)\n"

    # Формирование строки для настроек эмоций
    emotions_info = "\n🔷 Эмоции:\n"
    for emotion in settings['emotions']:
        emotion_symbol = emoji.emojize(emotion['emotion'])
        emotions_info += f"  🔹 {emotion_symbol}: {emotion['points']} очков\n"

    # Собираем финальное сообщение
    response = initial_settings + limits_info + ranks_info + emotions_info

    try:
        # Отправляем сообщение с настройками без использования Markdown
        del_message = await message.reply(response)
        await delete_message_after_delay(del_message, delay=60)  # Увеличиваем время для более долгого отображения
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения: {e}")
        await message.reply("Произошла ошибка при отправке сообщения с настройками.")



async def ban_user_handler(message: Message, redis: aioredis.Redis, bot: Bot):
    # Проверка прав вызывающего (доступно только для создателя и администраторов)
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role not in ["creator", "admin"]:
        del_message = await message.answer("Эта команда доступна только для создателя и администраторов чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    # Разбираем аргументы команды: ожидается один аргумент - user_id или username
    args = message.text.split(' ', 1)
    if len(args) != 2:
        del_message = await message.reply("Неверный формат. Используйте команду: /ban <user_id/username>.")
        await delete_message_after_delay(del_message, delay=5)
        return

    identifier = args[1].strip()
    chat_id = message.chat.id

    # Определяем target_user_id по идентификатору (если число, то это user_id, иначе ищем по username)
    if identifier.isdigit():
        target_user_id = int(identifier)
    else:
        target_user_id = await get_user_id_by_username(identifier, chat_id)
        if not target_user_id:
            del_message = await message.reply(f"Пользователь с username {identifier} не найден.")
            await delete_message_after_delay(del_message, delay=5)
            return

    # Опционально: не даём забанить самого себя
    if target_user_id == message.from_user.id:
        del_message = await message.reply("Вы не можете забанить себя.")
        await delete_message_after_delay(del_message, delay=5)
        return

    try:
        # Параллельно получаем отображаемое имя пользователя и название чата
        user_display_name_task = asyncio.create_task(get_user_display_name(bot, chat_id, target_user_id))
        chat_display_name_task = asyncio.create_task(get_chat_display_name(bot, chat_id))
        target_username, chat_name = await asyncio.gather(user_display_name_task, chat_display_name_task)

        # Отзываем админские права (если они были у пользователя)
        await bot.promote_chat_member(
            chat_id, target_user_id,
            can_change_info=False,
            can_post_messages=False,
            can_edit_messages=False,
            can_delete_messages=False,
            can_invite_users=False,
            can_restrict_members=False,
            can_pin_messages=False,
            can_promote_members=False,
            is_anonymous=False
        )

        # Баним пользователя из чата
        await bot.ban_chat_member(chat_id, target_user_id)

        # Удаляем пользователя из базы данных
        query_delete_user = "DELETE FROM users WHERE user_id = $1 AND chat_id = $2"
        await db.execute(query_delete_user, (target_user_id, chat_id))
        logging.info(f"Пользователь {target_user_id} удалён из базы данных для чата {chat_id}.")

        # Удаляем информацию о пользователе из кеша Redis
        cache_key = f"user:{target_user_id}:{chat_id}"
        await redis.delete(cache_key)
        logging.info(f"Пользователь {target_user_id} удалён из кеша Redis по ключу {cache_key}.")

        del_message = await message.reply(
            f"Пользователь {target_username} был забанен в чате \"{chat_name}\" и удалён из базы данных и кеша."
        )
        await delete_message_after_delay(del_message, delay=5)

    except Exception as e:
        logging.error(f"Ошибка при бане пользователя {target_user_id}: {e}")
        del_message = await message.reply("Произошла ошибка при попытке забанить пользователя.")
        await delete_message_after_delay(del_message, delay=5)






# Команда: Сбросить все данные
async def reset_all_handler(message: Message, redis: aioredis.Redis, bot: Bot):
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)
    if user_role != "creator":
        del_message = await message.answer("Эта команда доступна только создателю чата.")
        await delete_message_after_delay(del_message, delay=5)
        return

    # Здесь можно вызвать функцию для очистки всей информации в базе данных
    await reset_all_data(redis)
    del_message = await message.reply("Все данные успешно сброшены.")
    await delete_message_after_delay(del_message, delay=5)
