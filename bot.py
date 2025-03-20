import asyncio
import hashlib
import logging
import os
from contextlib import suppress
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import aiosqlite
import fitz
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramNetworkError
from aiogram.filters import Command
from aiogram.types import FSInputFile, InputMediaDocument
from aiogram.utils.markdown import hcode, hbold
from aiosqlite import Connection
from config_reader import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
fitz.TOOLS.mupdf_display_errors(False)
router = Router()

to_cat = {
    1052: "Algebra & Geometry",
    1054: "Algorithms",
    1056: "Book Series",
    1058: "Business",
    1060: "Calculus",
    1062: "Computer Science",
    1064: "Data Science",
    1066: "Discrete Mathematics",
    1068: "Economics",
    1070: "Linear Algebra",
    1072: "Linux",
    1074: "Literature",
    1076: "Machine Learning",
    1078: "Mathematics",
    1080: "Maths History",
    1082: "Maths Problems",
    1084: "Miscellaneous",
    1086: "Physics",
    1088: "Python",
    1090: "R",
    1092: "SQL",
    1094: "Statistics",
    1096: "Visualizations"
}

def get_file_hash(filename, algorithm="sha256", chunk_size=8192):
    hash_obj = hashlib.new(algorithm)
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

async def create_library(db: Connection):
    await db.execute("""
    CREATE TABLE IF NOT EXISTS library (
        id TEXT PRIMARY KEY,
        cat_name TEXT NOT NULL,
        pages INTEGER NOT NULL,
        title TEXT NOT NULL,
        authors TEXT NOT NULL,
        message_id UNIQUE NOT NULL,
        file_id TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    await db.commit()

async def add_book(db: Connection, book_id, cat_name, npage,
                   title, authors, message_id, file_id) -> None:
    await db.execute(
        "INSERT INTO library (id, cat_name, pages, title, authors, message_id, file_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (book_id, cat_name, npage, title, authors, message_id, file_id)
    )
    await db.commit()

async def remove_book(db: Connection, message_id) -> None:
    await db.execute("DELETE FROM library WHERE message_id = ?", (message_id,))
    await db.commit()

async def check_exists(db: Connection, book_id) -> bool:
    async with db.execute("SELECT 1 FROM library WHERE id = ?", (book_id,)) as cursor:
        exists = await cursor.fetchone()
        print(exists)
    return bool(exists)

async def get_library_stats(db: Connection):
    async with db.execute("SELECT COUNT(*) FROM library") as cursor: # Total books
        total_books = (await cursor.fetchone())[0]

    async with db.execute("SELECT COALESCE(SUM(pages), 0) FROM library") as cursor: # Total pages
        total_pages = (await cursor.fetchone())[0]

    async with db.execute("SELECT COUNT(DISTINCT cat_name) FROM library") as cursor: # Total categories
        total_categories = (await cursor.fetchone())[0]

    async with db.execute("""SELECT cat_name, COUNT(*) AS book_count, SUM(pages) AS total_pages
                             FROM library
                             GROUP BY cat_name
                             ORDER BY cat_name""") as cursor: # Books and Pages per category
        per_category = await cursor.fetchall()

    return {
        "total_books": total_books,
        "total_pages": total_pages,
        "total_categories": total_categories,
        "per_category": per_category
    }

@router.message(Command("delete"), F.from_user.id.in_({569356638}))
async def delete_book(message: types.Message, db: Connection):
    await message.delete()
    await remove_book(db, message.reply_to_message.message_id)
    notify_text = (f"The following book has been removed successfully:\n"
                   f"{hcode(message.reply_to_message.caption)}")
    await message.bot.send_document(
        chat_id=message.from_user.id,
        document=message.reply_to_message.document.file_id,
        caption=notify_text
    )
    await message.reply_to_message.delete()

@router.message(Command("update"))
async def update_stats(message: types.Message, bot: Bot, db: Connection):
    await message.delete()
    stats = await get_library_stats(db)
    total_books = stats["total_books"]
    total_pages = stats["total_pages"]
    total_categories = stats["total_categories"]
    per_category = stats["per_category"]
    now = datetime.now(ZoneInfo("UTC")).astimezone()
    formatted_datetime = now.strftime("%B %d, %Y %I:%M %p UTC%z")[:-2]

    general_stats = (f"<b>Total books:</b> {hcode(total_books)}\n"
                     f"<b>Total pages:</b> {hcode(total_pages)}\n"
                     f"<b>Total categories:</b> {hcode(total_categories)}")

    detailed_stats = "\n".join([f"{hbold(category)}: {hcode(books)} books, {hcode(pages)} pages"
                                   for category, books, pages in per_category])
    if detailed_stats: detailed_stats = "\n" + detailed_stats + "\n"
    refreshed_time = f"<b>Last refreshed:</b> {hcode(formatted_datetime)}"

    stats_text = general_stats + "\n" + detailed_stats + "\n" + refreshed_time

    with suppress(TelegramBadRequest):
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=943,
            text=stats_text
        )

@router.message(F.document, F.message_thread_id, ~F.message_thread_id.in_([738, 741]), F.from_user.id.in_({569356638}))
async def process_document(message: types.Message, bot: Bot, db: Connection) -> Any:
    file = await bot.get_file(message.document.file_id, request_timeout=300)
    file_path = file.file_path
    unique_file_id = get_file_hash(file_path)
    await message.delete()

    exists = await check_exists(db, unique_file_id)
    print(unique_file_id)
    print(exists)
    if exists:
        os.remove(file_path)
        return

    document_file = fitz.open(file_path)
    metadata = document_file.metadata
    ext = message.document.file_name.rsplit(".", 1)[-1]
    full_title = metadata.get('title')
    authors, title = map(str.strip, full_title.split(":", 1))
    npage = document_file.page_count
    cat_name = to_cat[message.message_thread_id]
    document_file.close()
    caption = (f"<b>Title:</b> {hcode(title)}\n"
               f"<b>Authors:</b> {hcode(authors)}\n"
               f"<b>Pages:</b> {hcode(npage)}. <b>Format:</b> {hcode(ext)}.")
    document = FSInputFile(path=file_path, filename=title + "." + ext)

    if message.reply_to_message.forum_topic_created:
        try:
            book = await message.answer_document(
                document=document,
                caption=caption
            )
            await add_book(db=db, book_id=unique_file_id, cat_name=cat_name, npage=npage, title=title,
                           authors=authors, message_id=book.message_id, file_id=book.document.file_id)
        except TelegramRetryAfter as e:
            logging.warning(f"Flood control triggered. Document sending. Sleeping for {e.retry_after} seconds.")
            await asyncio.sleep(e.retry_after)
        except TelegramNetworkError as e:
            logging.warning(f"Again network problems... {e}")
    elif message.reply_to_message.from_user.is_bot:
        link = message.reply_to_message.get_url(include_thread_id=True)
        text_to_sender = (f"The <a href='{link}'>book</a> has been modified.\n"
                          f"Previously, it was:\n\n"
                          f"{message.reply_to_message.caption}")

        await remove_book(db, message.reply_to_message.message_id)
        await message.bot.send_document(
            chat_id=message.from_user.id,
            document=message.reply_to_message.document.file_id,
            caption=text_to_sender
        )
        book = await message.bot.edit_message_media(
            chat_id=message.chat.id,
            message_id=message.reply_to_message.message_id,
            media=InputMediaDocument(media=document, caption=caption)
        )
        await add_book(db=db, book_id=unique_file_id, cat_name=cat_name, npage=npage, title=title,
                       authors=authors, message_id=book.message_id, file_id=book.document.file_id)
    os.remove(file_path)


async def main():
    local_server = TelegramAPIServer.from_base('http://localhost:8081')
    session = AiohttpSession(api=local_server)

    db = await aiosqlite.connect("library.db")
    await create_library(db)
    await db.execute("SELECT 1")
    print("CONNECTION SUCCESSFUL!")

    bot = Bot(
        token=config.bot_token.get_secret_value(),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=session
    )
    dp = Dispatcher()
    dp.include_routers(router)

    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot, db=db)
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())