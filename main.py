import os
import sqlite3
import requests
import time
import random
from datetime import datetime, timedelta
from contextlib import contextmanager
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ===================================================================
# Конфигурация
# ===================================================================
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_USERNAME = os.getenv('CHANNEL_USERNAME')
HH_NO_AUTH = os.getenv('HH_NO_AUTH', 'false').lower() == 'true'
PROXY_URL = os.getenv('PROXY_URL')  # например, socks5://user:pass@host:port

# ===================================================================
# Telegram Publisher с поддержкой прокси
# ===================================================================
class TelegramChannelPublisher:
    def __init__(self, bot_token, proxy_url=None):
        self.bot_token = bot_token
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self.proxy_url = proxy_url
        self.exit_flag = False
        self.session = requests.Session()
        if self.proxy_url:
            self.session.proxies = {
                'http': self.proxy_url,
                'https': self.proxy_url,
            }

    def send_to_channel(self, channel_username, vacancy, retry_count=2):
        message = self.format_vacancy_message(vacancy)
        url = f"{self.api_url}/sendMessage"
        payload = {
            "chat_id": channel_username,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
            "disable_notification": True
        }
        for attempt in range(retry_count):
            try:
                response = self.session.post(url, json=payload, timeout=15)
                if response.status_code == 200:
                    print(f"✓ Отправлено в канал {channel_username}: {vacancy['title'][:50]}...")
                    return True
                else:
                    error_data = response.json()
                    print(f"✗ Ошибка Telegram API (попытка {attempt+1}/{retry_count}): {error_data.get('description', response.text)}")
                    if "chat not found" in str(error_data).lower():
                        print(f"✗ Канал {channel_username} не найден или бот не администратор")
                        return False
                    if attempt < retry_count - 1:
                        time.sleep(2)
            except requests.exceptions.Timeout:
                print(f"✗ Таймаут (попытка {attempt+1}/{retry_count})")
                if attempt < retry_count - 1:
                    time.sleep(2)
            except requests.exceptions.ConnectionError as e:
                print(f"✗ Ошибка соединения (попытка {attempt+1}/{retry_count}): {e}")
                if attempt < retry_count - 1:
                    time.sleep(3)
            except Exception as e:
                print(f"✗ Неожиданная ошибка (попытка {attempt+1}/{retry_count}): {e}")
                if attempt < retry_count - 1:
                    time.sleep(2)
        return False

    def format_vacancy_message(self, vacancy):
        def escape_html(text):
            if not text:
                return ""
            return (str(text)
                    .replace('&', '&amp;')
                    .replace('<', '&lt;')
                    .replace('>', '&gt;')
                    .replace('"', '&quot;')
                    .replace("'", '&#39;'))

        title = escape_html(vacancy.get('title', 'Без названия'))[:200]
        company = escape_html(vacancy.get('company', 'Не указано'))[:100]
        salary = escape_html(vacancy.get('salary', 'Не указана'))[:100]
        city = escape_html(vacancy.get('city', 'Не указан'))[:50]
        url = vacancy.get('url', '#')

        published = vacancy.get('published_at', '')
        if published:
            try:
                published = published.split('.')[0].replace('Z', '+00:00')
                dt = datetime.strptime(published, "%Y-%m-%dT%H:%M:%S%z")
                published_str = dt.strftime("%d.%m.%Y %H:%M")
            except:
                published_str = "Недавно"
        else:
            published_str = "Недавно"

        message = f"""
<b>{title}</b>

🏢 <b>Компания:</b> {company}
💰 <b>Зарплата:</b> {salary}
📍 <b>Город:</b> {city}
📅 <b>Опубликовано:</b> {published_str}

🔗 <a href="{url}">Подробнее на сайте</a>

#вакансия #{vacancy.get('source', 'hh').replace('.ru', '')}
"""
        return message.strip()


# ===================================================================
# HHruParser – публичное API без авторизации
# ===================================================================
class HHruParser:
    def __init__(self):
        self.base_url = "https://api.hh.ru/vacancies"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        if HH_NO_AUTH:
            print("⚠️ Работаем без авторизации (публичное API)")

    def get_city_id(self, city_name="Пермь"):
        cities = {
            'Пермь': 72,
            'Москва': 1,
            'Санкт-Петербург': 2,
            'Екатеринбург': 3,
            'Новосибирск': 4,
            'Казань': 88,
            'Нижний Новгород': 66,
        }
        return cities.get(city_name, 59)

    def format_salary(self, salary_data):
        if not salary_data:
            return "Не указана"
        salary_from = salary_data.get('from')
        salary_to = salary_data.get('to')
        currency = salary_data.get('currency', '')
        symbols = {'RUR': '₽', 'RUB': '₽', 'USD': '$', 'EUR': '€', 'KZT': '₸'}
        currency_display = symbols.get(currency.upper(), currency)
        if salary_from and salary_to:
            return f"{salary_from:,} - {salary_to:,} {currency_display}".replace(',', ' ')
        elif salary_from:
            return f"от {salary_from:,} {currency_display}".replace(',', ' ')
        elif salary_to:
            return f"до {salary_to:,} {currency_display}".replace(',', ' ')
        else:
            return "Не указана"

    def fetch_vacancies(self, city="Пермь", period_days=30):
        city_id = self.get_city_id(city)
        date_from = (datetime.now() - timedelta(days=period_days)).strftime("%Y-%m-%dT%H:%M:%S")
        vacancies = []
        page = 0
        print(f"Поиск вакансий в {city} за последние {period_days} дней...")

        try:
            while True:
                params = {
                    "area": city_id,
                    "per_page": 50,
                    "page": page,
                    "date_from": date_from,
                    "order_by": "publication_time"
                }

                response = self.session.get(self.base_url, params=params, timeout=20)
                print(f"  Статус ответа: {response.status_code}")
                if response.status_code == 403:
                    print("  Тело ошибки (первые 500 символов):")
                    print(response.text[:500])
                    # В публичном API 403 может быть из-за блокировки IP, но попробуем продолжить
                    break

                response.raise_for_status()
                data = response.json()

                items = data.get("items", [])
                if not items:
                    break

                for item in items:
                    if not item.get("name"):
                        continue
                    vacancy = {
                        "id": str(item["id"]),
                        "title": item.get("name", "").strip(),
                        "company": item.get("employer", {}).get("name", "").strip(),
                        "salary": self.format_salary(item.get("salary")),
                        "url": item.get("alternate_url", f"https://hh.ru/vacancy/{item['id']}"),
                        "published_at": item.get("published_at", ""),
                        "source": "hh.ru",
                        "city": item.get("area", {}).get("name", city)
                    }
                    vacancies.append(vacancy)

                print(f"  Страница {page + 1}: найдено {len(items)} вакансий")
                pages = data.get("pages", 0)
                page += 1
                if page >= pages or page >= 5:
                    break
                time.sleep(0.5)

        except Exception as e:
            print(f"  Ошибка при парсинге HH.ru: {e}")

        print(f"Всего найдено {len(vacancies)} вакансий в {city}")
        return vacancies


# ===================================================================
# VacancyDatabase – работа с SQLite
# ===================================================================
class VacancyDatabase:
    def __init__(self, db_file="vacancies.db"):
        self.db_file = db_file
        self.init_database()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    company TEXT,
                    salary TEXT,
                    url TEXT,
                    published_at TEXT,
                    source TEXT,
                    city TEXT,
                    posted_to_channel BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_posted ON vacancies(posted_to_channel)")
            conn.commit()

    def cleanup_old_vacancies(self, days_to_keep=30):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
            cursor.execute("DELETE FROM vacancies WHERE date(published_at) < date(?)", (cutoff_date,))
            deleted = cursor.rowcount
            conn.commit()
            if deleted:
                print(f"Удалено {deleted} старых вакансий (старше {days_to_keep} дней)")

    def vacancy_exists(self, vacancy_id):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM vacancies WHERE id = ?", (vacancy_id,))
            return cursor.fetchone() is not None

    def save_vacancy(self, vacancy):
        if self.vacancy_exists(vacancy['id']):
            return False
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO vacancies 
                (id, title, company, salary, url, published_at, source, city)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                vacancy['id'],
                vacancy['title'][:500],
                vacancy['company'][:200],
                vacancy['salary'][:100],
                vacancy['url'],
                vacancy['published_at'],
                vacancy['source'],
                vacancy['city']
            ))
            conn.commit()
            return cursor.rowcount > 0

    def get_unposted_vacancies(self, limit=10):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM vacancies 
                WHERE posted_to_channel = 0 
                ORDER BY 
                    CASE WHEN published_at > datetime('now', '-1 day') THEN 1 ELSE 2 END,
                    published_at DESC 
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def mark_as_posted(self, vacancy_id):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE vacancies SET posted_to_channel = 1 WHERE id = ?", (vacancy_id,))
            conn.commit()


# ===================================================================
# Основная функция – сбор и отправка
# ===================================================================
def run_aggregator():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Запуск агрегатора...")
    if not BOT_TOKEN or not CHANNEL_USERNAME:
        print("❌ Ошибка: не заданы BOT_TOKEN или CHANNEL_USERNAME")
        return {"status": "error", "message": "Missing env variables"}

    db = VacancyDatabase()
    parser = HHruParser()
    publisher = TelegramChannelPublisher(BOT_TOKEN, proxy_url=PROXY_URL)

    # Проверка бота (опционально, но для уверенности)
    # Можно пропустить, если бот уже проверен

    # Очистка старых вакансий раз в неделю (понедельник)
    if datetime.now().weekday() == 0:
        db.cleanup_old_vacancies(30)

    print("\nПолучаем вакансии с HH.ru...")
    vacancies = parser.fetch_vacancies("Пермь", period_days=30)

    new_count = 0
    for vacancy in vacancies:
        if db.save_vacancy(vacancy):
            new_count += 1
    print(f"\nНовых вакансий сохранено в БД: {new_count}")

    limit = random.randint(11, 22)
    print(f"Будет запрошено до {limit} неопубликованных вакансий")
    unposted = db.get_unposted_vacancies(limit)
    print(f"Найдено неопубликованных вакансий: {len(unposted)}")

    posted_count = 0
    if unposted:
        print(f"\nПубликую вакансии в канал {CHANNEL_USERNAME}...")
        for i, vacancy in enumerate(unposted, 1):
            print(f"  {i}. {vacancy['title'][:50]}...")
            success = publisher.send_to_channel(CHANNEL_USERNAME, vacancy)
            if success:
                db.mark_as_posted(vacancy['id'])
                posted_count += 1
                if i < len(unposted):
                    time.sleep(2)
            else:
                print(f"    Не удалось отправить вакансию")
        print(f"\nОпубликовано в канале: {posted_count} вакансий")
    else:
        print("\nНет новых вакансий для публикации")

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Завершено!")
    return {"status": "ok", "new_saved": new_count, "posted": posted_count}


# ===================================================================
# Flask endpoints
# ===================================================================
@app.route('/')
def index():
    return "Бот-агрегатор вакансий работает. Запустите сбор через /run"

@app.route('/run')
def run():
    """Запуск сбора и отправки по HTTP-запросу."""
    result = run_aggregator()
    return jsonify(result)

# Для проверки работоспособности
@app.route('/health')
def health():
    return "OK", 200


# ===================================================================
# Запуск приложения (для Render)
# ===================================================================
if __name__ == "__main__":
    # Получаем порт из переменной окружения (Render задаёт PORT)
    port = int(os.getenv('PORT', 10000))
    # Запускаем Flask
    app.run(host='0.0.0.0', port=port)
