import asyncio
import os
import sqlite3
from contextlib import closing, suppress
from datetime import datetime, timezone
from typing import Tuple, Optional
from html import escape
import re

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode, ChatType
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.utils.token import TokenValidationError, validate_token
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    User,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from pathlib import Path

# Загружаем .env из того же каталога, что и main.py
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
# Стрипуем кавычки/пробелы, если вдруг в .env добавили их
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip().strip('"').strip("'")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x}
GLOBAL_ONE_PER_USER = os.getenv("GLOBAL_ONE_PER_USER", "FALSE").upper() in ("1", "TRUE", "YES")
DB_PATH = os.getenv("DB_PATH", "promo_bot.sqlite3")
SEND_PM_ON_REPEAT = os.getenv("SEND_PM_ON_REPEAT", "TRUE").upper() in ("1", "TRUE", "YES")

# Разнести «чат загрузки» и «чат выдачи» через .env (по желанию)
ENV_INPUT_CHAT_ID = int(os.getenv("INPUT_CHAT_ID", "0") or 0)
ENV_OUTPUT_CHAT_ID = int(os.getenv("OUTPUT_CHAT_ID", "0") or 0)

if not BOT_TOKEN:
    raise SystemExit(
        "Отсутствует BOT_TOKEN. Укажите токен бота в .env (строка вида BOT_TOKEN=123456:ABCDEF)."
    )

try:
    validate_token(BOT_TOKEN)
except TokenValidationError as exc:
    raise SystemExit(
        "Некорректный BOT_TOKEN. Проверьте значение в .env (формат 123456:ABCDEF)."
    ) from exc

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
BOT_USERNAME: Optional[str] = None

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS chats (
  chat_id INTEGER PRIMARY KEY,
  pending_pool_id INTEGER
);
CREATE TABLE IF NOT EXISTS code_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chat_id INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS codes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  code TEXT NOT NULL,
  used_by INTEGER,
  used_at TEXT
);
CREATE TABLE IF NOT EXISTS drops (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS drop_sources (
  drop_id INTEGER PRIMARY KEY,
  source_chat_id INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS drop_codes (
  drop_id INTEGER NOT NULL,
  code_id INTEGER NOT NULL,
  assigned_user_id INTEGER,
  assigned_at TEXT,
  PRIMARY KEY (drop_id, code_id)
);
CREATE TABLE IF NOT EXISTS claims (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  drop_id INTEGER NOT NULL,
  code_id INTEGER NOT NULL,
  claimed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS admin_bindings (
  user_id INTEGER PRIMARY KEY,
  chat_id INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_codes_code ON codes(code);
CREATE INDEX IF NOT EXISTS idx_codes_used ON codes(used_by);
CREATE INDEX IF NOT EXISTS idx_drop_codes_drop ON drop_codes(drop_id);
CREATE INDEX IF NOT EXISTS idx_claims_user_drop ON claims(user_id, drop_id);
"""


def db():
    # check_same_thread=False позволяет использовать соединение из разных потоков
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    with closing(db()) as conn:
        conn.executescript(SCHEMA)

def migrate_unique_per_batch():
    """Делаем уникальность кодов не глобально, а внутри одной партии (batch)."""
    with closing(db()) as conn:
        try:
            conn.execute("DROP INDEX IF EXISTS idx_codes_code")
        except Exception:
            pass
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_codes_code_batch "
            "ON codes(batch_id, code)"
        )

async def is_admin(message: Message) -> bool:
    if message.from_user and message.from_user.id in ADMIN_IDS:
        return True
    if message.chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return True
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return member.is_chat_admin()
    except Exception:
        return False


def get_target_chats(conn: sqlite3.Connection, message: Message) -> Tuple[int, int]:
    """Возвращает (input_chat_id, output_chat_id)."""
    if ENV_INPUT_CHAT_ID and ENV_OUTPUT_CHAT_ID:
        return ENV_INPUT_CHAT_ID, ENV_OUTPUT_CHAT_ID
    # Fallback: старая логика bind → всё в текущем чате/привязанном
    if message.chat.type in (ChatType.SUPERGROUP, ChatType.GROUP):
        return message.chat.id, message.chat.id
    row = conn.execute("SELECT chat_id FROM admin_bindings WHERE user_id=?", (message.from_user.id,)).fetchone()
    chat_id = row[0] if row else 0
    return chat_id, chat_id


@dp.message(Command("bind"))
async def cmd_bind(message: Message):
    if ENV_INPUT_CHAT_ID and ENV_OUTPUT_CHAT_ID:
        return await message.reply("Привязка не нужна — используются INPUT_CHAT_ID/OUTPUT_CHAT_ID из .env.")
    if message.chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return await message.reply("Эту команду нужно вызвать в группе, которую хотите привязать.")
    if not await is_admin(message):
        return await message.reply("Только администраторы могут выполнять /bind.")
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO admin_bindings(user_id, chat_id) VALUES(?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET chat_id=excluded.chat_id",
            (message.from_user.id, message.chat.id),
        )
    await message.reply("Готово! Теперь можно загружать коды в ЛС: <code>/codes AAA,BBB</code> и публиковать /post.")


@dp.message(Command("codes"))
async def cmd_codes(message: Message):
    if not await is_admin(message):
        return await message.reply("Команда доступна только администраторам.")

    raw = message.text.split(maxsplit=1)
    if len(raw) < 2:
        return await message.reply("Укажи коды через запятую: <code>/codes AAA,BBB,CCC</code>")
    codes_line = raw[1]

    # Парсим «жадно»: запятые / точки с запятой / пробелы / переносы
    parts = re.split(r"[,;\s]+", codes_line)
    incoming = [p.strip() for p in parts if p and p.strip()]
    if not incoming:
        return await message.reply("Не вижу кодов в запросе.")

    with closing(db()) as conn:
        input_chat_id, output_chat_id = get_target_chats(conn, message)
        if not input_chat_id or not output_chat_id:
            return await message.reply(
                "Не настроены чаты. Укажите INPUT_CHAT_ID/OUTPUT_CHAT_ID в .env или выполните /bind в группе."
            )

        # Разрешим загрузку только из INPUT_CHAT_ID (или из ЛС)
        if message.chat.type in (ChatType.SUPERGROUP, ChatType.GROUP) and message.chat.id != input_chat_id:
            return await message.reply("Коды нужно загружать в указанном чате загрузки (INPUT_CHAT_ID).")

        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "INSERT INTO code_batches(chat_id, created_at) VALUES (?, ?)",
            (output_chat_id, now),
        )
        batch_id = cur.lastrowid

        added = 0
        for code in incoming:
            try:
                conn.execute("INSERT INTO codes(batch_id, code) VALUES (?, ?)", (batch_id, code))
                added += 1
            except sqlite3.IntegrityError:
                # дубликаты в рамках той же партии игнорируем
                pass

        conn.execute(
            "INSERT INTO chats(chat_id, pending_pool_id) VALUES(?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET pending_pool_id=excluded.pending_pool_id",
            (output_chat_id, batch_id),
        )

    # Удалим исходное сообщение с кодами, если это группа
    if message.chat.type in (ChatType.SUPERGROUP, ChatType.GROUP):
        try:
            await message.delete()
        except Exception:
            pass

    await message.answer(
        f"Добавлено кодов: <b>{added}</b>. Теперь отправь <code>/post</code> — опубликую пост с кнопкой."
    )

@dp.message(Command("code"))
async def cmd_code_alias(message: Message):
    # Алиас на случай опечатки: /code -> /codes
    message.text = message.text.replace("/code", "/codes", 1)
    return await cmd_codes(message)


def make_drop_keyboard(drop_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="🎁 Открыть промокод", callback_data=f"get:{drop_id}"),
        # URL-кнопка открывает чат с ботом (диплинк)
        InlineKeyboardButton(text="📩 В личку", url=f"https://t.me/{BOT_USERNAME}?start=claim_{drop_id}"),
    )
    return kb.as_markup()


@dp.message(Command("post"))
async def cmd_post(message: Message):
    if not await is_admin(message):
        return await message.reply("Команда доступна только администраторам.")

    raw_text = (message.text or message.caption or "").split(maxsplit=1)
    body = (
        raw_text[1]
        if len(raw_text) > 1
        else "🎉 Промо-акция! Нажми кнопку ниже, чтобы получить личный промокод."
    )
    photo_id = message.photo[-1].file_id if message.photo else None

    with closing(db()) as conn:
        input_chat_id, output_chat_id = get_target_chats(conn, message)
        if not input_chat_id or not output_chat_id:
            return await message.reply("Не настроены чаты. Укажите INPUT_CHAT_ID/OUTPUT_CHAT_ID в .env или выполните /bind в группе.")

        # Разрешим /post только из INPUT_CHAT_ID (или из ЛС)
        if message.chat.type in (ChatType.SUPERGROUP, ChatType.GROUP) and message.chat.id != input_chat_id:
            return await message.reply("Пост публикуется из чата загрузки (INPUT_CHAT_ID) или из ЛС.")

        row = conn.execute("SELECT pending_pool_id FROM chats WHERE chat_id=?", (output_chat_id,)).fetchone()
        if not row or row[0] is None:
            return await message.reply("Сначала загрузите коды: <code>/codes AAA,BBB</code>")
        pending_batch_id = int(row[0])

        if photo_id:
            sent = await bot.send_photo(
                output_chat_id,
                photo=photo_id,
                caption=body,
                reply_markup=make_drop_keyboard(0),
            )
        else:
            sent = await bot.send_message(output_chat_id, body, reply_markup=make_drop_keyboard(0))

        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "INSERT INTO drops(chat_id, message_id, created_at) VALUES (?, ?, ?)",
            (output_chat_id, sent.message_id, now),
        )
        drop_id = cur.lastrowid

        await bot.edit_message_reply_markup(
            chat_id=sent.chat.id,
            message_id=sent.message_id,
            reply_markup=make_drop_keyboard(drop_id),
        )

        conn.execute(
            "INSERT OR REPLACE INTO drop_sources(drop_id, source_chat_id) VALUES(?, ?)",
            (drop_id, message.chat.id),
        )

        code_rows = conn.execute(
            "SELECT id FROM codes WHERE batch_id=? AND used_by IS NULL",
            (pending_batch_id,),
        ).fetchall()
        if not code_rows:
            await message.reply("В загруженной партии нет доступных кодов. Добавьте новые /codes …")
            return

        conn.executemany("INSERT OR IGNORE INTO drop_codes(drop_id, code_id) VALUES(?, ?)", [(drop_id, r[0]) for r in code_rows])
        conn.execute("UPDATE chats SET pending_pool_id=NULL WHERE chat_id=?", (output_chat_id,))

    await message.reply(f"Пост опубликован в чате {output_chat_id}. Привязано кодов: <b>{len(code_rows)}</b>.")


def _get_or_assign_code(user_id: int, drop_id: int):
    with closing(db()) as conn:
        # Уже получал в этом дропе?
        got = conn.execute(
            "SELECT c.id, c.code FROM claims cl JOIN codes c ON c.id=cl.code_id WHERE cl.user_id=? AND cl.drop_id=?",
            (user_id, drop_id),
        ).fetchone()
        if got:
            return got[0], got[1], False
            return got[0], got[1]

        # Иначе пробуем выдать новый
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT c.id, c.code FROM drop_codes dc JOIN codes c ON c.id=dc.code_id "
                "WHERE dc.drop_id=? AND c.used_by IS NULL AND dc.assigned_user_id IS NULL LIMIT 1",
                (drop_id,),
            ).fetchone()
            if not row:
                conn.execute("COMMIT")
                return 0, None, False
                return 0, None
            code_id, code_val = int(row[0]), row[1]
            now = datetime.now(timezone.utc).isoformat()
            upd1 = conn.execute(
                "UPDATE codes SET used_by=?, used_at=? WHERE id=? AND used_by IS NULL",
                (user_id, now, code_id),
            )
            if upd1.rowcount != 1:
                conn.execute("ROLLBACK")
                return 0, None, False
                return 0, None
            upd2 = conn.execute(
                "UPDATE drop_codes SET assigned_user_id=?, assigned_at=? WHERE drop_id=? AND code_id=? AND assigned_user_id IS NULL",
                (user_id, now, drop_id, code_id),
            )
            if upd2.rowcount != 1:
                conn.execute("ROLLBACK")
                return 0, None, False
                return 0, None
            conn.execute(
                "INSERT INTO claims(user_id, drop_id, code_id, claimed_at) VALUES(?, ?, ?, ?)",
                (user_id, drop_id, code_id, now),
            )
            conn.execute("COMMIT")
            return code_id, code_val, True
        except Exception:
            conn.execute("ROLLBACK")
            return 0, None, False


def resolve_report_chat(drop_id: int) -> int:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT source_chat_id FROM drop_sources WHERE drop_id=?",
            (drop_id,),
        ).fetchone()
        if row and row[0]:
            return int(row[0])
    return 0


async def send_claim_report(drop_id: int, user: User, code_val: str):
    report_chat_id = resolve_report_chat(drop_id)
    if not report_chat_id or not code_val:
        return
    full_name = user.full_name or "Пользователь"
    mention = f'<a href="tg://user?id={user.id}">{escape(full_name)}</a>'
    username = f" (@{user.username})" if user.username else ""
    text = (
        f"Код <code>{escape(str(code_val))}</code> выдан {mention}{username}. "
        f"ID: <code>{user.id}</code>. Дроп #{drop_id}."
    )
    try:
        await bot.send_message(report_chat_id, text)
    except Exception:
        pass
            return code_id, code_val
        except Exception:
            conn.execute("ROLLBACK")
            return 0, None


@dp.message(Command("start"))
async def cmd_start(message: Message):
    text = message.text or ""
    parts = text.split(maxsplit=1)
    # обычный /start без параметров
    if len(parts) == 1 or not parts[1]:
        return await message.answer(
            "Привет! Я бот для раздачи промокодов.\n\n"
            "Админ: загрузите коды <code>/codes AAA,BBB,CCC</code> и отправьте <code>/post</code>.\n"
            "Можно работать через .env (INPUT/OUTPUT_CHAT_ID) или привязать чат командой <code>/bind</code>."
        )

    param = parts[1].strip()
    if param.startswith("claim_"):
        try:
            drop_id = int(param.split("_", 1)[1])
        except ValueError:
            return await message.answer("Некорректная ссылка.")
        user_id = message.from_user.id
        loop = asyncio.get_running_loop()
        code_id, code_val, assigned_now = await loop.run_in_executor(
            None, _get_or_assign_code, user_id, drop_id
        )
        if not code_val:
            return await message.answer("Промокоды закончились или недоступны.")
        if assigned_now:
            await send_claim_report(drop_id, message.from_user, code_val)
        safe_code = escape(str(code_val))
        return await message.answer(f"Ваш промокод: <code>{safe_code}</code>")

@dp.callback_query(F.data.startswith("get:"))
async def on_get_code(cb: CallbackQuery):
    drop_id = int(cb.data.split(":", 1)[1])
    user_id = cb.from_user.id

    # Проверку глобального ограничения делаем в основном потоке своим соединением
    with closing(db()) as conn:
        if GLOBAL_ONE_PER_USER:
            got_global = conn.execute("SELECT 1 FROM claims WHERE user_id=? LIMIT 1", (user_id,)).fetchone()
            if got_global:
                return await cb.answer("У вас уже есть промокод (ограничение 1 на пользователя).", show_alert=True)

    # Выдачу кода выполняем в threadpool, но уже с отдельным соединением внутри функции
    loop = asyncio.get_running_loop()
    code_id, code_val, assigned_now = await loop.run_in_executor(
        None, _get_or_assign_code, user_id, drop_id
    )
    if code_id == 0 and code_val is None:
        return await cb.answer("Промокоды закончились. Попробуйте позже.", show_alert=True)

    extra_alert_note = ""

    if SEND_PM_ON_REPEAT and code_val:
        try:
            await bot.send_message(user_id, f"Ваш промокод: <code>{escape(str(code_val))}</code>")
        except Exception:
            extra_alert_note = (
                "\n\nНажмите кнопку «📩 В личку» под постом — откроется чат с ботом и код придёт там."
            )

    if assigned_now and code_val:
        await send_claim_report(drop_id, cb.from_user, code_val)

    await cb.answer(f"Ваш промокод: {code_val}{extra_alert_note}", show_alert=True)


@dp.message(Command("left"))
async def cmd_left(message: Message):
    if not await is_admin(message):
        return await message.reply("Команда доступна только администраторам.")
    with closing(db()) as conn:
        input_chat_id, output_chat_id = get_target_chats(conn, message)
        if not output_chat_id:
            return await message.reply("Сначала настройте чаты или сделайте /bind.")
        row = conn.execute("SELECT id FROM drops WHERE chat_id=? ORDER BY id DESC LIMIT 1", (output_chat_id,)).fetchone()
        if not row:
            return await message.reply("Нет дропов в этом чате.")
        drop_id = row[0]
        total = conn.execute("SELECT COUNT(*) FROM drop_codes WHERE drop_id=?", (drop_id,)).fetchone()[0]
        left = conn.execute(
            "SELECT COUNT(*) FROM drop_codes dc JOIN codes c ON c.id=dc.code_id WHERE dc.drop_id=? AND c.used_by IS NULL",
            (drop_id,),
        ).fetchone()[0]
    await message.reply(f"В последнем дропе осталось: <b>{left}/{total}</b> кодов.")


@dp.message(Command("report"))
async def cmd_report(message: Message):
    """Отчёт по последнему дропу привязанного/настроенного чата (видно только админам)."""
    if not await is_admin(message):
        return await message.reply("Команда доступна только администраторам.")
    with closing(db()) as conn:
        input_chat_id, output_chat_id = get_target_chats(conn, message)
        if not output_chat_id:
            return await message.reply("Сначала настройте чаты или сделайте /bind.")
        row = conn.execute("SELECT id FROM drops WHERE chat_id=? ORDER BY id DESC LIMIT 1", (output_chat_id,)).fetchone()
        if not row:
            return await message.reply("Нет дропов в этом чате.")
        drop_id = row[0]
        used = conn.execute(
            "SELECT c.code, cl.user_id, cl.claimed_at FROM claims cl JOIN codes c ON c.id=cl.code_id WHERE cl.drop_id=? ORDER BY cl.claimed_at",
            (drop_id,),
        ).fetchall()
        free = conn.execute(
            "SELECT c.code FROM drop_codes dc JOIN codes c ON c.id=dc.code_id WHERE dc.drop_id=? AND c.used_by IS NULL",
            (drop_id,),
        ).fetchall()

    parts = ["<b>Отчёт по последнему дропу</b>", f"Выдано: {len(used)} | Свободно: {len(free)}"]
    if used:
        parts.append("<b>Выданные:</b>")
        parts.extend([f"• <code>{c}</code> — user <code>{u}</code> — {t}" for c, u, t in used[:200]])
        if len(used) > 200:
            parts.append(f"…и ещё {len(used)-200}")
    if free:
        parts.append("<b>Свободные:</b>")
        parts.extend([f"• <code>{r[0]}</code>" for r in free[:200]])
        if len(free) > 200:
            parts.append(f"…и ещё {len(free)-200}")

    await message.answer("".join(parts))


async def main():
    global BOT_USERNAME
    init_db()
    migrate_unique_per_batch()
    me = await bot.get_me()
    BOT_USERNAME = me.username
    print("Bot is running…")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())