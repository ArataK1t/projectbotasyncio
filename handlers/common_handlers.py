from aiogram import types
from aiogram.enums import ParseMode
from aiogram.types import ChatMemberOwner, ChatMemberAdministrator, ChatMemberMember, ChatMemberRestricted, ChatMemberLeft, ChatMemberBanned
from aiogram import Bot
import logging
from handlers.user_handlers import delete_message_after_delay
import aiofiles
from datetime import datetime, timedelta
import json
import os
import asyncio
from pytz import timezone
MOSCOW_TZ = timezone("Europe/Moscow")

async def help_handler(message: types.Message, bot: Bot):
    """
    Обработчик команды /help с дифференцированным списком команд по ролям.
    """
    # Определяем роль пользователя
    user_role = await get_user_role(bot, message.chat.id, message.from_user.id)

    # Базовый текст помощи
    base_help_text = (
        "Привет! Я — бот, который помогает управлять рангами и очками пользователей.\n\n"
    )
    
    # Списки команд для каждой роли
    creator_commands = (
        "**Для создателя:**\n\n"
        "/set_initial_rank <rank> - Установить начальный ранг для новых пользователей\n"
        "/add_rank <rank_name> <points> - Добавить новый ранг с требованиями по очкам\n"
        "/remove_rank <rank_name> - Удалить ранг\n"
        "/set_emotions <emotion> <points> - Установить очки за эмоцию\n"
        "/remove_emotions <emotion> - Удалить эмоцию\n"
        "/set_limits_emotions <per_person> <total> - Устанавить лимиты на реакции\n\n"
    )

    admin_commands = (
        "**Для администраторов:**\n\n"
        "/set_user_points <user_id/username> <points> - Установить очки пользователю\n"
        "/reset_user <user_id/username> - Сбросить данные пользователя\n"
        "/get_settings - Получить текущие настройки\n\n"
    )
    # Списки команд для каждой роли
    user_commands = (
        "**Для пользователей:**\n\n"
        "/get_user_info - Получить информацию о себе\n"
        "/list_emotions - Список всех эмоций\n"
        "/list_ranks - Список всех рангов\n"
        "/leaderboard - Топ 10\n"
    )

    # Генерация текста помощи в зависимости от роли
    if user_role == "creator":
        help_text = (
            base_help_text +
            creator_commands +
            admin_commands +
            user_commands
        )
    elif user_role == "admin":
        help_text = (
            base_help_text +
            admin_commands +
            user_commands
        )
    elif user_role == "admin_without_rights":
        help_text = (
            base_help_text +
            user_commands
        )
    else:
        # Если роль неизвестна, отправляем сообщение с ошибкой
        del_message = await message.answer("Ваша роль не определена. Обратитесь к администратору.")
        await delete_message_after_delay(del_message, delay=5)
        return

    # Экранируем символы Markdown для безопасного форматирования текста
    help_text = help_text.replace("_", r"\_").replace("*", r"\*").replace("[", r"\[").replace("]", r"\]").replace("(", r"\(").replace(")", r"\)")

    # Отправляем сообщение с помощью
    try:
        del_message = await message.answer(help_text, parse_mode=ParseMode.MARKDOWN)
        await delete_message_after_delay(del_message, delay=6)
    except Exception as e:
        print(f"Ошибка при отправке сообщения: {e}")







async def get_user_role(bot: Bot, chat_id: int, user_id: int) -> str:
    """
    Получаем роль пользователя в чате.
    """
    chat_member = await bot.get_chat_member(chat_id, user_id)

    if isinstance(chat_member, ChatMemberOwner):
        return "creator"  # Создатель чата
    elif isinstance(chat_member, ChatMemberAdministrator):
        # Проверяем наличие прав администратора
        if any([
            chat_member.can_delete_messages,
            chat_member.can_restrict_members,
            chat_member.can_promote_members,
            chat_member.can_change_info,
            chat_member.can_pin_messages
        ]):
            return "admin"  # Полноценный администратор
        elif chat_member.can_manage_chat or chat_member.can_invite_users:  # Ограниченные права
            return "admin_without_rights"
        return "admin_without_rights"
    elif isinstance(chat_member, ChatMemberMember):
        return "member"  # Обычный пользователь
    elif isinstance(chat_member, ChatMemberRestricted):
        return "restricted"  # Ограниченный пользователь
    elif isinstance(chat_member, ChatMemberLeft):
        return "left"  # Пользователь покинул чат
    elif isinstance(chat_member, ChatMemberBanned):
        return "banned"  # Забаненный пользователь
    return "unknown"  # Неизвестный статус


# Назначение минимальных прав администратора
async def assign_zero_admin_role(bot: Bot, chat_id: int, user_id: int, initial_rank: str):
    """
    Назначение пользователю минимальных прав администратора и установка должности.
    """
    try:
        await bot.promote_chat_member(
            chat_id, user_id,
            can_change_info=False,
            can_delete_messages=False,
            can_invite_users=True,  # Единственное право, чтобы роль отличалась от member
            can_restrict_members=False,
            can_pin_messages=False,
            can_promote_members=False,
            can_manage_chat=False,
            is_anonymous=False
        )
        logging.info(f"Пользователю {user_id} назначены минимальные права админа в чате {chat_id}.")
        await bot.set_chat_administrator_custom_title(chat_id, user_id, initial_rank)
        logging.info(f"Назначен титул '{initial_rank}' пользователю {user_id} в чате {chat_id}.")
    except Exception as e:
        logging.error(f"Ошибка назначения минимальных прав админа пользователю {user_id}: {e}")




async def remove_admin_rights(bot: Bot):
    """
    Читает файл `reminder_schedule.json`, проверяет даты, и удаляет права админа у пользователей.
    """
    try:
        file_path = '/root/bot_project/projectbotasyncio/reminder_schedule.json'

        # Проверяем, существует ли файл
        if not os.path.exists(file_path):
            logging.warning(f"Файл {file_path} не найден. Пропускаем выполнение.")
            return

        # Чтение данных из файла
        async with aiofiles.open(file_path, mode='r') as file:
            file_content = await file.read()
            if not file_content:
                logging.warning(f"Файл {file_path} пуст. Пропускаем выполнение.")
                return
            try:
                data = json.loads(file_content)
            except json.JSONDecodeError:
                logging.error(f"Ошибка парсинга JSON в файле {file_path}. Пропускаем выполнение.")
                return

        # Текущая дата в формате ISO (UTC+3)
        now = datetime.now(MOSCOW_TZ).date()

        # Проверяем каждый элемент в данных
        for telegram_id, info in data.items():
            chat_id = info.get("chat_id")
            reminder_datetime_str = info.get("reminder_datetime")

            if not chat_id or not reminder_datetime_str:
                logging.warning(f"Пропущена запись {telegram_id}: отсутствуют chat_id или reminder_datetime.")
                continue

            # Преобразуем дату напоминания из строки в объект datetime
            try:
                reminder_datetime = datetime.fromisoformat(reminder_datetime_str).astimezone(MOSCOW_TZ)
            except ValueError:
                logging.error(f"Ошибка преобразования даты для {telegram_id}: {reminder_datetime_str}")
                continue

            # Сравниваем дату напоминания с текущей датой
            if reminder_datetime.date() == now:
                user_id = int(telegram_id)  # telegram_id используется как user_id
                try:
                    # Убираем права администратора
                    await bot.promote_chat_member(
                        chat_id, user_id,
                        can_change_info=False,
                        can_delete_messages=False,
                        can_invite_users=False,
                        can_restrict_members=False,
                        can_pin_messages=False,
                        can_promote_members=False,
                        can_manage_chat=False,
                        is_anonymous=False
                    )
                    logging.info(f"Права администратора у пользователя {user_id} в чате {chat_id} успешно удалены.")
                except Exception as e:
                    logging.error(f"Ошибка удаления прав администратора у пользователя {user_id} в чате {chat_id}: {e}")
            else:
                logging.info(f"Дата напоминания для {telegram_id} ({reminder_datetime.date()}) не совпадает с текущей ({now}). Пропускаем.")

    except Exception as e:
        logging.exception(f"Ошибка в функции удаления прав администратора: {e}")





async def scheduler(bot: Bot):
    """
    Ежедневный планировщик для удаления админки.
    """
    while True:
        # Рассчитываем время до 23:58:00
        now = datetime.now(MOSCOW_TZ)
        target_time = now.replace(hour=23, minute=58, second=0, microsecond=0)
        if now >= target_time:
            target_time = target_time + timedelta(days=1)
        sleep_duration = (target_time - now).total_seconds()

        logging.info(f"Следующее выполнение функции удаления прав администратора запланировано на {target_time}.")
        await asyncio.sleep(sleep_duration)

        # Выполняем функцию удаления прав администратора
        await remove_admin_rights(bot)
