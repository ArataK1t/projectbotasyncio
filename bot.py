import logging
import asyncio
from aiogram import Bot, types, BaseMiddleware
from typing import Callable, Awaitable, Any
from aiogram import Dispatcher, Router
from config import BOT_TOKEN
from database.connection import Database, init_redis # Подключаем класс Database
from aiogram.filters import Command
from handlers.creator_handlers import (
    set_initial_rank_handler, add_rank_handler, remove_rank_handler, 
    list_ranks_handler, set_emotion_reward_handler, remove_emotion_handler, 
    list_emotions_handler, set_user_rank_handler, set_user_points_handler, 
    reset_user_handler, get_settings_handler, reset_all_handler,
    process_reaction, set_limits_command, on_new_message, leaderboard_handler
)
from handlers.user_handlers import get_user_info_handler, add_user_handler
from handlers.common_handlers import help_handler, scheduler
import aioredis

logging.basicConfig(level=logging.INFO)

# Инициализация бота
bot = Bot(token=BOT_TOKEN)

# Создаем объект Dispatcher
dp = Dispatcher()

# Создаем Router для обработки команд
router = Router()

# Создаем экземпляр класса Database
db = Database()

# Подключаемся к базе данных перед запуском бота
async def on_startup(dp):
    await db.init()  # Инициализация подключения к базе данных

# Закрываем соединения с базой данных при завершении работы бота
async def on_shutdown(dp):
    await db.close()  # Закрываем подключение

# Регистрация хендлеров
def register_handlers(dp: Dispatcher):
    # Хендлеры для создателя
    router.message(Command("set_initial_rank"))(set_initial_rank_handler)
    router.message(Command("add_rank"))(add_rank_handler)
    router.message(Command("remove_rank"))(remove_rank_handler)
    router.message(Command("list_ranks"))(list_ranks_handler)
    router.message(Command("set_emotions"))(set_emotion_reward_handler)
    router.message(Command("remove_emotions"))(remove_emotion_handler)
    router.message(Command("list_emotions"))(list_emotions_handler)
    router.message(Command("set_user_rank"))(set_user_rank_handler)
    router.message(Command("set_user_points"))(set_user_points_handler)
    router.message(Command("reset_user"))(reset_user_handler)
    router.message(Command("leaderboard"))(leaderboard_handler)
    router.message(Command("get_settings"))(get_settings_handler)
    router.message(Command("reset_all"))(reset_all_handler)

    # Хендлеры для пользователей
    router.message(Command("get_user_info"))(get_user_info_handler)
    router.message(Command("start"))(add_user_handler)

    # Общие хендлеры
    router.message(Command("help"))(help_handler)


    router.message(Command("set_limits_emotions"))(set_limits_command)

    #router.message_reaction()(process_reaction)
    router.message()(on_new_message)
    # Добавляем наш роутер в диспетчер
    dp.include_router(router)



class RedisMiddleware(BaseMiddleware):
    def __init__(self, redis: aioredis.Redis):
        super().__init__()
        self.redis = redis

    async def __call__(
        self,
        handler: Callable[[types.TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: types.TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        data["redis"] = self.redis
        logging.info(f"Middleware: redis добавлен в data: {data.get('redis')}") # Проверка!
        return await handler(event, data)



# Регистрация всех событий
# @dp.update()
# async def handle_unhandled_update(update: Update):
#     logging.warning(f"Необработанное обновление: {update}")

# Middleware для логирования всех обновлений
# class LogAllUpdatesMiddleware(BaseMiddleware):
#     async def __call__(self, handler: Callable[[Update, Any], Awaitable[Any]], event: Update, data: dict):
#         logging.info(f"Получено обновление: {event}")
#         return await handler(event, data)

# Подключение middleware
# dp.update.middleware(LogAllUpdatesMiddleware())   # Убираем эту строку


# Регистрация хендлера для реакций
router.message_reaction()(process_reaction) 
# Обработчики команд

async def main():
    

    redis = await init_redis()  # Получаем redis из init_redis()
    if redis is None:  # Проверяем результат init_redis()
        logging.error("Не удалось подключиться к Redis. Завершение работы.")
        return
    
    dp.message.middleware.register(RedisMiddleware(redis))
    router.message_reaction.middleware(RedisMiddleware(redis))
    # Регистрируем хендлеры
    register_handlers(dp)
    # Инициализируем базу данных
    await on_startup(dp)

# Запускаем планировщик
    asyncio.create_task(scheduler(bot))

    # Запуск бота
    try:
        # Используем polling для получения обновлений
        await dp.start_polling(
            bot, 
            allowed_updates=["message", "message_reaction", "message_reaction_count"],
            on_shutdown=on_shutdown
        )
    except KeyboardInterrupt:
        logging.info("Бот остановлен.")
    finally:
        # Завершаем подключение при завершении
        await on_shutdown(dp)

if __name__ == '__main__':
    # Запуск бота
    asyncio.run(main())

