import asyncio
import json
import os

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.utils import executor

from main import aggregate_salaries


load_dotenv()

bot = Bot(token=os.getenv('API_TOKEN'))
dp = Dispatcher(bot)
logging_middleware = LoggingMiddleware()
dp.middleware.setup(logging_middleware)


@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    await message.answer("Привет! Этот бот агрегирует статистические данные о зарплатах. "
                         "Отправьте JSON-сообщение с входными данными, и я верну агрегированные данные.")


@dp.message_handler(content_types=['text'])
async def handle_message(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data["dt_from"]
        dt_upto = data["dt_upto"]
        group_type = data["group_type"]

        dataset, labels = await aggregate_salaries(dt_from, dt_upto, group_type)

        response = {"dataset": dataset, "labels": labels}
        response_json = json.dumps(response)

        await message.answer(response_json)
    except Exception as e:
        await message.answer('Невалидный запос. Пример запроса:\n'
                             '{"dt_from": "2022-09-01T00:00:00", '
                             '"dt_upto": "2022-12-31T23:59:00", '
                             '"group_type": "month"}')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dp.skip_updates())
    executor.start_polling(dp, skip_updates=True)
