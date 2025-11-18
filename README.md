<p align="center">
  <a href="#-русская-версия">
    <img src="https://img.shields.io/badge/🇷🇺 Русская%20версия-blue?style=for-the-badge">
  </a>
  <a href="#-english-version">
    <img src="https://img.shields.io/badge/🇬🇧 English%20Version-green?style=for-the-badge">
  </a>
</p>

---

# Telegram PromoCode Bot

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)
![Aiogram](https://img.shields.io/badge/Aiogram-3.x-0A84FF?logo=telegram)
![License](https://img.shields.io/badge/License-MIT-green)
![SQLite](https://img.shields.io/badge/SQLite-Automated-lightgrey?logo=sqlite)

---

# 🇷🇺 Русская версия

## ✨ Возможности
- Добавление пулов промокодов  
- Публикация “дропа” с кнопкой получения  
- Уникальный код каждому пользователю  
- Опция: один код на всех пользователей (GLOBAL_ONE_PER_USER)  
- Автоматические отчёты и логирование  
- Поддержка постов с фото  
- Отправка кода в ЛС при повторном запросе  

---

## 🧩 Основные команды

| Команда | Описание |
|--------|----------|
| `/codes AAA,BBB,CCC` | Загрузить коды в пул |
| `/post <текст>` | Опубликовать дроп |
| `/left` | Показать оставшиеся коды |
| `/report` | Отчёт по последнему дропу |
| `/bind` | Привязать группу (если нет указания в .env) |

---

## ⚙️ Пример `.env`

```env
BOT_TOKEN=123:ABC
ADMIN_IDS=12345
GLOBAL_ONE_PER_USER=False
SEND_PM_ON_REPEAT=True
DB_PATH=promo_bot.sqlite3

git clone <repo>
cd TelegramPromocodebot
pip install -r requirements.txt
python main.py

📦 Стек технологий

Python

Aiogram 3

SQLite (WAL + индексы)

dotenv

📄 Лицензия

MIT — свободное использование и модификация разрешены.



🇬🇧 English Version
✨ Features

Import batches of promo codes

Publish a “drop” with a claim button

Unique code for each user

Optional global limit (1 code per user for all drops)

Automatic logging and reporting

Photo posts supported

Send code to DM on repeated request

🧩 Main Commands
Command	Description
/codes AAA,BBB,CCC	Upload promo codes
/post <text>	Publish a drop
/left	Show remaining codes
/report	Report for the last drop
/bind	Bind the group (if not set in .env)


⚙️ .env Example
BOT_TOKEN=123:ABC
ADMIN_IDS=12345
GLOBAL_ONE_PER_USER=False
SEND_PM_ON_REPEAT=True
DB_PATH=promo_bot.sqlite3
INPUT_CHAT_ID=
OUTPUT_CHAT_ID=


git clone <repo>
cd TelegramPromocodebot
pip install -r requirements.txt
python main.py


📦 Tech Stack

Python

Aiogram 3

SQLite (WAL + indexes)

dotenv

📄 License

MIT — free to use and modify.
