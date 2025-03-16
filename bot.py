import asyncio
import logging
from typing import Any

from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters.command import Command
from aiogram.types import InputMediaDocument
from config_reader import config

logging.basicConfig(level=logging.INFO)

router = Router()

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Hello!")

@router.message(F.document)
async def cmd_doc(message: types.Message) -> Any:
    caption = message.document.file_name.replace("_", " ")
    await message.delete()
    if message.reply_to_message.forum_topic_created:
        await message.answer_document(
            message.document.file_id,
            caption=caption
        )
    else:
        await message.bot.edit_message_media(
            chat_id=message.chat.id,
            message_id=message.reply_to_message.message_id,
            media=InputMediaDocument(media=message.document.file_id,
                                     caption=caption)
        )

@router.edited_message()
async def edited_message_handler(edited_message: types.Message) -> Any:
    await edited_message.answer("You edited it!")

@router.message(F.chat.type.in_(["group", "supergroup"]))
async def cmd_sup(message: types.Message):
    if message.reply_to_message.forum_topic_created:
        await message.answer("Sup!")
    else:
        await message.reply_to_message.edit_text("I changed it!")

async def main():
    bot = Bot(token=config.bot_token.get_secret_value())
    dp = Dispatcher()
    dp.include_routers(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())