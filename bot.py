import asyncio
import logging
import os
import shelve
from typing import Any

import fitz
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import FSInputFile, InputMediaDocument
from config_reader import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
router = Router()
BOOK_SHELF = "book.shelf"

def update_file_data(file_unique_id, pages):
    with shelve.open(BOOK_SHELF, flag='c') as db:
        db[file_unique_id] = pages

def load_data():
    with shelve.open(BOOK_SHELF, flag='r') as db:
        return dict(db)

def remove_file_data(file_unique_id):
    with shelve.open(BOOK_SHELF, flag='w') as db:
        if file_unique_id in db:
            del db[file_unique_id]
            print(f"Removed data for key: {file_unique_id}")

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Hello!")
    print(message.message_thread_id)
    print(message.chat.id)

@router.message(F.document, F.message_thread_id, ~F.message_thread_id.in_([738, 741]))
async def process_document(message: types.Message, bot: Bot) -> Any:
    file_id = message.document.file_id
    file = await bot.get_file(file_id, request_timeout=300)
    file_path = file.file_path
    document_file = fitz.open(file_path)
    metadata = document_file.metadata

    ext = message.document.file_name.rsplit(".", 1)[-1]
    full_title = metadata.get('title')
    authors, title = map(str.strip, full_title.split(":", 1))
    npage = document_file.page_count
    document_file.close()
    caption = (f"<b>Title:</b> <code>{title}</code>\n"
               f"<b>Authors:</b> <code>{authors}</code>\n"
               f"<b>Pages:</b> <code>{npage}</code>. <b>Format:</b> <code>{ext}</code>.")
    document = FSInputFile(path=file_path, filename=title + "." + ext)

    await message.delete()
    update_file_data(message.document.file_unique_id, npage)
    if message.reply_to_message.forum_topic_created:
        await message.answer_document(
            document=document,
            caption=caption
        )
    elif message.reply_to_message.from_user.is_bot:
        link = message.reply_to_message.get_url(include_thread_id=True)
        text_to_sender = (f"The <a href='{link}'>book</a> has been modified.\n"
                          f"Previously, it was:\n\n"
                          f"{message.reply_to_message.caption}")

        remove_file_data(message.reply_to_message.document.file_unique_id)
        await message.bot.send_document(
            chat_id=message.from_user.id,
            document=message.reply_to_message.document.file_id,
            caption=text_to_sender
        )
        await message.bot.edit_message_media(
            chat_id=message.chat.id,
            message_id=message.reply_to_message.message_id,
            media=InputMediaDocument(media=document, caption=caption)
        )

    book_shelve = load_data()
    total_books = len(book_shelve)
    total_pages = sum(book_shelve.values())
    stats_text = (f"Total books: {total_books}\n"
                  f"Total pages: {total_pages}")
    await bot.edit_message_text(
        chat_id=message.chat.id,
        message_id=943,
        text=stats_text
    )

    os.remove(file_path)

async def main():
    local_server = TelegramAPIServer.from_base('http://localhost:8081')
    session = AiohttpSession(api=local_server)

    bot = Bot(
        token=config.bot_token.get_secret_value(),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=session
    )
    dp = Dispatcher()
    dp.include_routers(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())