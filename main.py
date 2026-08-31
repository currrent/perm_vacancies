import os
import sqlite3
import requests
import time
import signal
import sys
import random
import json
from datetime import datetime, timedelta
from contextlib import contextmanager
from urllib.parse import urlencode
from dotenv import load_dotenv
from flask import Flask
import threading

load_dotenv()  # загружаем переменные из .env

# ===================================================================
# Класс для управления токенами HH.ru
# Поддерживает: статический токен (HH_ACCESS_TOKEN),
#               client_credentials (токен приложения),
#               OAuth-поток пользователя (как запасной вариант)
# ===================================================================
class HHOAuth:
    def __init__(self, client_id=None, client_secret=None, redirect_uri=None, token_file='hh_token.json'):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_file = token_file
        self.token_data = None
        self._load_token()

        # 1. Если есть статический токен из переменной окружения – используем его
        static_token = os.getenv('HH_ACCESS_TOKEN')
        if static_token:
            print("✓ Используется постоянный токен из переменной HH_ACCESS_TOKEN")
            self.token_data = {
                'access_token': static_token,
                'created_at': datetime.now().isoformat(),
                'expires_in': 86400 * 365
            }
            self._save_token(self.token_data)
            return

        # 2. Если нет сохранённого токена, но есть Client ID и Secret – пробуем получить токен приложения
        if not self.token_data and self.client_id and self.client_secret:
            print("Попытка получить токен приложения (client_credentials)...")
            try:
                self._obtain_client_credentials_token()
                print("✓ Токен приложения успешно получен и сохранён.")
                return
            except Exception as e:
                print(f"✗ Не удалось получить токен приложения: {e}")

        # 3. Если ничего не вышло – токен отсутствует, позже будет запрошен OAuth-поток
        if not self.token_data:
            print("Токен не найден. Придётся пройти OAuth-авторизацию пользователя.")

    def _obtain_client_credentials_token(self):
        """Получение токена через client_credentials (для приложений)."""
        url = 'https://api.hh.ru/token'
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code != 200:
            raise Exception(f"Ошибка получения токена: {resp.status_code} {resp.text}")
        token_data = resp.json()
        token_data['created_at'] = datetime.now().isoformat()
        self._save_token(token_data)

    def _load_token(self):
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    self.token_data = json.load(f)
            except:
                self.token_data = None

    def _save_token(self, token_data):
        with open(self.token_file, 'w') as f:
            json.dump(token_data, f, indent=2)
        self.token_data = token_data

    def get_authorization_url(self):
        params = {
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'response_type': 'code',
            'scope': 'vacancies'
        }
        return 'https://hh.ru/oauth/authorize?' + urlencode(params)

    def exchange_code(self, code):
        url = 'https://api.hh.ru/token'
        data = {
            'grant_type': 'authorization_code',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': self.redirect_uri,
            'code': code
        }
        resp = requests.post(url, data=data, timeout=10)
        resp.raise_for_status()
        token_data = resp.json()
        token_data['created_at'] = datetime.now().isoformat()
        self._save_token(token_data)
        return token_data

    def get_access_token(self):
        if not self.token_data:
            raise Exception("Токен отсутствует. Выполните авторизацию.")
        created_at = datetime.fromisoformat(self.token_data['created_at'])
        expires_in = self.token_data.get('expires_in', 3600)
        if (datetime.now() - created_at).seconds > expires_in - 300:
            if 'refresh_token' in self.token_data:
                self._refresh_token()
            else:
                # Для client_credentials токен не обновляется, перезапрашиваем
                if self.client_id and self.client_secret:
                    self._obtain_client_credentials_token()
                else:
                    raise Exception("Токен истёк, а обновить нечем.")
        return self.token_data['access_token']

    def _refresh_token(self):
        if 'refresh_token' not in self.token_data:
            raise Exception("Нет refresh_token для обновления.")
        url = 'https://api.hh.ru/token'
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.token_data['refresh_token'],
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        resp = requests.post(url, data=data, timeout=10)
        resp.raise_for_status()
        new_data = resp.json()
        new_data['created_at'] = datetime.now().isoformat()
        self._save_token(new_data)


# ===================================================================
# Остальные классы (GracefulExit, TelegramChannelPublisher,
# HHruParser, VacancyDatabase) – без изменений, но с поддержкой
# режима без авторизации (HH_NO_AUTH=true)
# ===================================================================

class GracefulExit:
    def __init__(self):
        self.exit_now = False
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        print(f"\nПолучен сигнал {signum}. Завершаю работу...")
        self.exit_now = True


class TelegramChannelPublisher:
    def __init__(self, bot_token):
        self.bot_token = bot_token
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self.exit_flag = False

    def check_bot(self):
        url = f"{self.api_url}/getMe"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if data.get("ok"):
                print(f"✓ Бот @{data['result']['username']} работает")
                return True
            else:
                print(f"✗ Ошибка бота: {data.get('description')}")
                return False
        except Exception as e:
            print(f"✗ Ошибка проверки бота: {e}")
            return False

    def send_to_channel(self, channel_username, vacancy, retry_count=2):
        if self.exit_flag:
            print("Получен запрос на выход, пропускаю отправку")
            return False

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
                response = requests.post(url, json=payload, timeout=15)
                if response.status_code == 200:
                    print(f"✓ Отправлено в канал {channel_username}: {vacancy['title'][:50]}...")
                    return True
                else:
                    error_data = response.json()
                    print(f"✗ Ошибка Telegram API (попытка {attempt+1}/{retry_count}): {error_data.get('description', response.text)}")
                    if "chat not found" in str(error_data).lower():
                        print(f"✗ Канал {channel_username} не найден или бот не является администратором")
                        return False
                    if attempt < retry_count - 1:
                        time.sleep(2)
            except requests.exceptions.Timeout:
                print(f"✗ Таймаут (попытка {attempt+1}/{retry_count})")
                if attempt < retry_count - 1:
                    time.sleep(2)
            except requests.exceptions.ConnectionError:
                print(f"✗ Ошибка соединения (попытка {attempt+1}/{retry_count})")
                if attempt < retry_count - 1:
                    time.sleep(3)
            except KeyboardInterrupt:
                print("\nПрервано пользователем")
                self.exit_flag = True
                return False
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


class HHruParser:
    def __init__(self, oauth=None):
        self.oauth = oauth
        self.base_url = "https://api.hh.ru/vacancies"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        # Проверяем, нужно ли отключить авторизацию
        self.no_auth = os.getenv('HH_NO_AUTH', 'false').lower() == 'true'
        if self.no_auth:
            print("⚠️ Работаем без авторизации (публичное API)")

    def _get_auth_headers(self):
        if self.no_auth:
            return {}
        if self.oauth:
            token = self.oauth.get_access_token()
            return {'Authorization': f'Bearer {token}'}
        return {}

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

    def fetch_vacancies(self, city="Пермь", keywords=None, period_days=30):
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

                headers = self.session.headers.copy()
                headers.update(self._get_auth_headers())

                print(f"  Запрос к HH: {self.base_url}")
                print(f"  Параметры: {params}")
                response = self.session.get(self.base_url, params=params, headers=headers, timeout=20)
                print(f"  Статус ответа: {response.status_code}")

                if response.status_code == 403:
                    print("  Тело ошибки (первые 500 символов):")
                    print(response.text[:500])
                    # Если 403, можно попытаться обновить токен (если есть OAuth)
                    if self.oauth and not self.no_auth:
                        try:
                            print("  Пытаемся обновить токен...")
                            self.oauth.get_access_token()  # обновит при необходимости
                            headers.update(self._get_auth_headers())
                            response = self.session.get(self.base_url, params=params, headers=headers, timeout=20)
                            print(f"  Повторный статус: {response.status_code}")
                        except Exception as e:
                            print(f"  Ошибка обновления токена: {e}")

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
# Функции выполнения и планирования
# ===================================================================

def run_aggregator(publisher, parser, channel_username, exit_controller):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Запуск агрегатора...")
    print(f"Используется канал: {channel_username}")

    if exit_controller.exit_now:
        print("Получен запрос на выход, завершаю работу...")
        return False

    db = VacancyDatabase()

    if not publisher.check_bot():
        print("Ошибка: бот не работает. Проверьте токен.")
        return False

    if datetime.now().weekday() == 0:
        db.cleanup_old_vacancies(30)

    print("\nПолучаем вакансии с HH.ru...")
    vacancies = parser.fetch_vacancies("Пермь", period_days=30)

    new_count = 0
    for vacancy in vacancies:
        if exit_controller.exit_now:
            break
        if db.save_vacancy(vacancy):
            new_count += 1
    print(f"\nНовых вакансий сохранено в БД: {new_count}")

    limit = random.randint(11, 22)
    print(f"Будет запрошено до {limit} неопубликованных вакансий")
    unposted = db.get_unposted_vacancies(limit)
    print(f"Найдено неопубликованных вакансий: {len(unposted)}")

    if unposted:
        print(f"\nПубликую вакансии в канал {channel_username}...")
        posted_count = 0
        for i, vacancy in enumerate(unposted, 1):
            if exit_controller.exit_now or publisher.exit_flag:
                print("Получен запрос на выход, прерываю публикацию...")
                break
            print(f"  {i}. {vacancy['title'][:50]}...")
            success = publisher.send_to_channel(channel_username, vacancy)
            if success:
                db.mark_as_posted(vacancy['id'])
                posted_count += 1
                if i < len(unposted):
                    print(f"    Пауза 2 секунды...")
                    for _ in range(20):
                        if exit_controller.exit_now:
                            break
                        time.sleep(0.1)
            else:
                print(f"    Не удалось отправить вакансию")
        print(f"\nОпубликовано в канале: {posted_count} вакансий")
    else:
        print("\nНет новых вакансий для публикации")

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Завершено!")
    return True


def job(publisher, parser, channel_username, exit_controller):
    try:
        return run_aggregator(publisher, parser, channel_username, exit_controller)
    except KeyboardInterrupt:
        print("Задача прервана пользователем")
        exit_controller.exit_now = True
        return False
    except Exception as e:
        print(f"Ошибка в задаче: {e}")
        import traceback
        traceback.print_exc()
        return False


# Создать Flask-приложение
app = Flask(__name__)

@app.route('/')
def health():
    return "Bot is running", 200

def run_web():
    port = int(os.getenv('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

# Запустить веб-сервер в отдельном потоке, чтобы не мешать основному циклу
if __name__ == "__main__":
    # ... ваш существующий код ...
    # После того как настроили и запустили основной цикл (в конце файла)
    # перед while not exit_controller.exit_now: добавьте запуск потока
    web_thread = threading.Thread(target=run_web, daemon=True)
    web_thread.start()
    # Остальной код (while not exit_controller.exit_now и т.д.)

# ===================================================================
# Основной блок
# ===================================================================

if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Запуск агрегатора вакансий...")
    print("=" * 60)

    BOT_TOKEN = os.getenv('BOT_TOKEN')
    CHANNEL_USERNAME = os.getenv('CHANNEL_USERNAME')

    if not BOT_TOKEN:
        print("❌ Ошибка: не задана переменная окружения BOT_TOKEN")
        sys.exit(1)
    if not CHANNEL_USERNAME:
        print("❌ Ошибка: не задана переменная окружения CHANNEL_USERNAME")
        sys.exit(1)

    # Данные приложения HH.ru
    CLIENT_ID = "OHSE6CI5L53K2N3L22MTQR9K63ITDS0I4UDFLCFGT3FPJAKJMGSJF77DSE8LJ4SL"
    CLIENT_SECRET = "R6B3SLIEKJ1KGLVDN6T8UGBSJ4NE3E1H3GV0DHIJ0R6TLJBJ27M3QM50OS1PC0C0"
    REDIRECT_URI = "https://t.me/vacancies_perm"

    # Если включён режим без авторизации – создаём парсер без OAuth
    if os.getenv('HH_NO_AUTH', 'false').lower() == 'true':
        parser = HHruParser(oauth=None)
    else:
        # Инициализация OAuth
        oauth = HHOAuth(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)

        # Если токен ещё не получен, предлагаем OAuth-поток пользователя
        if not oauth.token_data:
            print("Токен не найден. Необходимо выполнить авторизацию.")
            print("1. Перейдите по ссылке:")
            print(oauth.get_authorization_url())
            print("2. Авторизуйтесь и разрешите доступ.")
            print("3. После редиректа скопируйте параметр 'code' из адресной строки.")
            code = input("Введите полученный код: ").strip()
            try:
                oauth.exchange_code(code)
                print("✓ Токен успешно получен и сохранён.")
            except Exception as e:
                print(f"✗ Ошибка обмена кода: {e}")
                sys.exit(1)

        parser = HHruParser(oauth)

    # Тест доступа к HH.ru
    try:
        test_headers = parser.session.headers.copy()
        test_headers.update(parser._get_auth_headers())
        test_resp = requests.get(
            "https://api.hh.ru/vacancies?area=59&per_page=3",
            headers=test_headers,
            timeout=10
        )
        print(f"Тест доступа к HH.ru: {test_resp.status_code}")
        if test_resp.status_code == 200:
            data = test_resp.json()
            found = data.get('found', 0)
            print(f"✓ HH.ru доступен, найдено вакансий в Перми (всего): {found}")
        else:
            print(f"✗ HH.ru вернул статус {test_resp.status_code}")
            if test_resp.status_code == 403:
                print("  Тело ошибки (первые 500 символов):")
                print(test_resp.text[:500])
    except Exception as e:
        print(f"✗ Не удалось подключиться к HH.ru: {e}")

    exit_controller = GracefulExit()
    publisher = TelegramChannelPublisher(BOT_TOKEN)

    print("Конфигурация:")
    print(f"  Бот токен: {'✓ задан' if BOT_TOKEN else '✗ отсутствует'}")
    print(f"  Канал: {CHANNEL_USERNAME}")
    print("=" * 60)

    if not publisher.check_bot():
        print("❌ Ошибка: Бот не работает. Проверьте токен и интернет-соединение.")
        print("Для выхода нажмите Ctrl+C")
        try:
            while not exit_controller.exit_now:
                time.sleep(1)
        except KeyboardInterrupt:
            sys.exit(1)

    print("\nПервый запуск...")
    try:
        job_success = job(publisher, parser, CHANNEL_USERNAME, exit_controller)
    except Exception as e:
        print(f"Ошибка при первом запуске: {e}")
        job_success = False

    # Случайный интервал 1–4 часа
    if not exit_controller.exit_now:
        interval_seconds = random.randint(3600, 14400)
        next_run = datetime.now() + timedelta(seconds=interval_seconds)
        print(f"\nСледующий запуск через {interval_seconds // 3600} ч {interval_seconds % 3600 // 60} мин")
        print(f"Ожидание до {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        next_run = None

    print("\n" + "=" * 60)
    print("Агрегатор запущен. Интервал между запусками: случайный 1–4 часа")
    print("Для остановки нажмите Ctrl+C\n")

    last_status_print = datetime.now()

    try:
        while not exit_controller.exit_now:
            if next_run and datetime.now() >= next_run:
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Наступило время запуска.")
                job_success = job(publisher, parser, CHANNEL_USERNAME, exit_controller)

                if not exit_controller.exit_now:
                    interval_seconds = random.randint(3600, 14400)
                    next_run = datetime.now() + timedelta(seconds=interval_seconds)
                    print(f"\nСледующий запуск через {interval_seconds // 3600} ч {interval_seconds % 3600 // 60} мин")
                    print(f"Ожидание до {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                    last_status_print = datetime.now()

            now = datetime.now()
            if (now - last_status_print).seconds > 300:
                if next_run:
                    remaining = (next_run - now).total_seconds()
                    if remaining > 0:
                        hours = int(remaining // 3600)
                        minutes = int((remaining % 3600) // 60)
                        print(f"[{now.strftime('%H:%M:%S')}] До следующего запуска: {hours} ч {minutes} мин")
                last_status_print = now

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nПолучен сигнал прерывания...")
    finally:
        print("\n" + "=" * 60)
        print("Агрегатор завершает работу...")
        print("Спасибо за использование!")
        print("=" * 60)
