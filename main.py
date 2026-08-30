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
from urllib.parse import urlencode, urlparse, parse_qs

# ===================== OAuth для HH.ru =====================
class HHOAuth:
    """Управление OAuth-токенами для HH.ru (Authorization Code Flow)."""
    def __init__(self, client_id, client_secret, redirect_uri, token_file='hh_token.json'):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_file = token_file
        self.token_data = None
        self._load_token()

    def _load_token(self):
        """Загружает токен из файла."""
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    self.token_data = json.load(f)
            except:
                self.token_data = None

    def _save_token(self, token_data):
        """Сохраняет токен в файл."""
        with open(self.token_file, 'w') as f:
            json.dump(token_data, f, indent=2)
        self.token_data = token_data

    def get_authorization_url(self):
        """Возвращает URL для получения кода авторизации."""
        params = {
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'response_type': 'code'
        }
        return 'https://hh.ru/oauth/authorize?' + urlencode(params)

    def exchange_code(self, code):
        """Обменивает код авторизации на токены."""
        url = 'https://hh.ru/oauth/token'
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
        # HH возвращает access_token, refresh_token, expires_in
        token_data['created_at'] = datetime.now().isoformat()
        self._save_token(token_data)
        return token_data

    def get_access_token(self):
        """Возвращает действующий access_token, при необходимости обновляет."""
        if not self.token_data:
            raise Exception("Токен отсутствует. Сначала выполните авторизацию.")
        # Проверяем, истёк ли токен (с запасом 5 минут)
        created_at = datetime.fromisoformat(self.token_data['created_at'])
        expires_in = self.token_data.get('expires_in', 3600)
        if (datetime.now() - created_at).seconds > expires_in - 300:
            self._refresh_token()
        return self.token_data['access_token']

    def _refresh_token(self):
        """Обновляет токен с помощью refresh_token."""
        if 'refresh_token' not in self.token_data:
            raise Exception("Нет refresh_token для обновления.")
        url = 'https://hh.ru/oauth/token'
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.token_data['refresh_token'],
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        resp = requests.post(url, data=data, timeout=10)
        resp.raise_for_status()
        new_data = resp.json()
        # сохраняем новые токены, обновляем created_at
        new_data['created_at'] = datetime.now().isoformat()
        self._save_token(new_data)

# ===================== Основные классы =====================
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
    def __init__(self, oauth):
        self.oauth = oauth  # объект HHOAuth
        self.base_url = "https://api.hh.ru/vacancies"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def _get_auth_headers(self):
        """Возвращает заголовки с актуальным access_token."""
        token = self.oauth.get_access_token()
        return {'Authorization': f'Bearer {token}'}

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

                # Добавляем авторизационный заголовок
                headers = self.session.headers.copy()
                headers.update(self._get_auth_headers())

                print(f"  Запрос к HH: {self.base_url}")
                print(f"  Параметры: {params}")
                response = self.session.get(self.base_url, params=params, headers=headers, timeout=20)
                print(f"  Статус ответа: {response.status_code}")

                if response.status_code == 401:
                    # Токен истёк, пытаемся обновить
                    print("  Токен истёк, обновляем...")
                    self.oauth._refresh_token()
                    headers.update(self._get_auth_headers())
                    response = self.session.get(self.base_url, params=params, headers=headers, timeout=20)

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

# ===================== Основной блок =====================
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

    # OAuth данные
    CLIENT_ID = "PAQBVPK3OPLB7TGUNM5GPM6U0A06QCAB25FLE5P2UC9UA2R968KFN9PPET33CTHI"
    CLIENT_SECRET = "JTTPBU6CMP3RCTSEMRSNI9L2AMBVONR5J5C065BIKA77O18EO0KFSVIS7BQTT4VP"
    REDIRECT_URI = "https://t.me/vacancies_perm"

    # Инициализация OAuth
    oauth = HHOAuth(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)

    # Проверка наличия токена и авторизация при необходимости
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

    # Создаём парсер с OAuth
    parser = HHruParser(oauth)

    # Тест доступа к HH.ru с авторизацией
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
            print(f"✓ HH.ru доступен (авторизован), найдено вакансий в Перми (всего): {found}")
        else:
            print(f"✗ HH.ru вернул статус {test_resp.status_code}")
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
