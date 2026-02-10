import os
import sqlite3
import requests
import time
import schedule

from datetime import datetime, timedelta
from contextlib import contextmanager


class TelegramChannelPublisher:
    """Публикация вакансий в Telegram-канал"""

    def __init__(self, bot_token):
        self.bot_token = bot_token
        self.api_url = f"https://api.telegram.org/bot{bot_token}"

    def send_to_channel(self, channel_username, vacancy):
        """
        Отправляет вакансию в канал

        Важно: Бот должен быть администратором канала!
        channel_username: @название_канала (например, @it_vacancies_perm)
        """

        # Форматируем сообщение
        message = self.format_vacancy_message(vacancy)

        # Отправляем в канал
        url = f"{self.api_url}/sendMessage"
        payload = {
            "chat_id": channel_username,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                print(f"✓ Отправлено в канал {channel_username}")
                return True
            else:
                print(f"✗ Ошибка: {response.text}")
                return False
        except Exception as e:
            print(f"✗ Ошибка сети: {e}")
            return False

    def format_vacancy_message(self, vacancy):
        """Форматирует вакансию для Telegram"""

        # Экранируем специальные символы для HTML
        def escape_html(text):
            if not text:
                return ""
            return (str(text)
                    .replace('&', '&amp;')
                    .replace('<', '&lt;')
                    .replace('>', '&gt;'))

        title = escape_html(vacancy.get('title', 'Без названия'))
        company = escape_html(vacancy.get('company', 'Не указано'))
        salary = escape_html(vacancy.get('salary', 'Не указана'))
        city = escape_html(vacancy.get('city', 'Не указан'))
        url = vacancy.get('url', '#')

        # Форматируем дату
        published = vacancy.get('published_at', '')
        if published:
            try:
                # Пробуем разные форматы даты
                for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%SZ"):
                    try:
                        dt = datetime.strptime(published, fmt)
                        break
                    except:
                        continue
                published_str = dt.strftime("%d.%m.%Y %H:%M")
            except:
                published_str = published
        else:
            published_str = "Недавно"

        # Создаем сообщение с HTML разметкой
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
    """Парсер HH.ru с правильным регионом для Перми"""

    def __init__(self):
        self.base_url = "https://api.hh.ru/vacancies"

    def get_city_id(self, city_name):
        """ID городов на HH.ru"""
        cities = {
            'Пермь': 59,  # Правильный ID!
            'Москва': 1,
            'Санкт-Петербург': 2,
            'Екатеринбург': 3,
            'Новосибирск': 4,
            'Казань': 88,
            'Нижний Новгород': 66,
            'Челябинск': 104,
            'Самара': 78,
            'Омск': 68,
            'Ростов-на-Дону': 76,
            'Уфа': 99,
            'Красноярск': 54,
            'Воронеж': 26
        }
        return cities.get(city_name, 59)

    def format_salary(self, salary_data):
        """Форматирование зарплаты"""
        if not salary_data:
            return "Не указана"

        salary_from = salary_data.get('from')
        salary_to = salary_data.get('to')
        currency = salary_data.get('currency', '')

        # Конвертируем валюту в символ
        currency_symbols = {
            'RUR': '₽',
            'RUB': '₽',
            'USD': '$',
            'EUR': '€',
            'KZT': '₸'
        }
        currency_display = currency_symbols.get(currency.upper(), currency)

        if salary_from and salary_to:
            return f"{salary_from:,} - {salary_to:,} {currency_display}".replace(',', ' ')
        elif salary_from:
            return f"от {salary_from:,} {currency_display}".replace(',', ' ')
        elif salary_to:
            return f"до {salary_to:,} {currency_display}".replace(',', ' ')
        else:
            return "Не указана"

    def fetch_vacancies(self, city="Пермь", keywords=None, period_days=7):
        """Получает вакансии за последние N дней"""
        city_id = self.get_city_id(city)

        # Дата для поиска
        date_from = datetime.now() - timedelta(days=period_days)
        date_from_str = date_from.strftime("%Y-%m-%d")

        vacancies = []
        # Пробуем несколько страниц
        for page in range(0, 2):  # Первые 2 страницы
            params = {
                "area": city_id,
                "per_page": 100,  # Максимум 100
                "page": page,
                "date_from": date_from_str,
                "order_by": "publication_time",
                "search_field": "name"  # Искать в названии
            }

            if keywords:
                params["text"] = keywords
            else:
                # Если нет ключевых слов, ищем любые IT-вакансии
                params["text"] = "программист разработчик it"

            try:
                response = requests.get(self.base_url, params=params, timeout=15)
                data = response.json()

                items = data.get("items", [])
                if not items:
                    break

                for item in items:
                    # Пропускаем вакансии без названия
                    if not item.get("name"):
                        continue

                    vacancy = {
                        "id": str(item["id"]),
                        "title": item.get("name", ""),
                        "company": item.get("employer", {}).get("name", ""),
                        "salary": self.format_salary(item.get("salary")),
                        "url": item.get("alternate_url", ""),
                        "published_at": item.get("published_at", ""),
                        "source": "hh.ru",
                        "city": item.get("area", {}).get("name", "")
                    }
                    vacancies.append(vacancy)

                print(f"Страница {page + 1}: найдено {len(items)} вакансий")

            except Exception as e:
                print(f"Ошибка при парсинге HH.ru (страница {page}): {e}")
                break

        print(f"Всего найдено {len(vacancies)} вакансий в {city}")
        return vacancies


class VacancyDatabase:
    """Работа с базой данных вакансий"""

    def __init__(self, db_file="vacancies.db"):
        self.db_file = db_file
        self.init_database()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row  # Чтобы возвращать словари
        try:
            yield conn
        finally:
            conn.close()

    def init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Проверяем, существует ли таблица
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='vacancies'
            """)

            if not cursor.fetchone():
                # Таблицы нет, создаем с правильной структурой
                cursor.execute("""
                    CREATE TABLE vacancies (
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
                print("Таблица vacancies создана")
            else:
                # Таблица существует, проверяем наличие колонки posted_to_channel
                cursor.execute("PRAGMA table_info(vacancies)")
                columns = [column[1] for column in cursor.fetchall()]

                if 'posted_to_channel' not in columns:
                    cursor.execute("ALTER TABLE vacancies ADD COLUMN posted_to_channel BOOLEAN DEFAULT 0")
                    print("Добавлена колонка posted_to_channel")

            conn.commit()

    def vacancy_exists(self, vacancy_id):
        """Проверяет, есть ли вакансия в БД"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM vacancies WHERE id = ?", (vacancy_id,))
            return cursor.fetchone() is not None

    def save_vacancy(self, vacancy):
        """Сохраняет вакансию в БД"""
        if self.vacancy_exists(vacancy['id']):
            return False

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO vacancies 
                (id, title, company, salary, url, published_at, source, city)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                vacancy['id'],
                vacancy['title'],
                vacancy['company'],
                vacancy['salary'],
                vacancy['url'],
                vacancy['published_at'],
                vacancy['source'],
                vacancy['city']
            ))
            conn.commit()
            return True

    def get_unposted_vacancies(self, limit=20):
        """Получает неопубликованные вакансии"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM vacancies 
                WHERE posted_to_channel = 0 
                ORDER BY published_at DESC 
                LIMIT ?
            """, (limit,))

            # Преобразуем результат в список словарей
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def mark_as_posted(self, vacancy_id):
        """Отмечает вакансию как опубликованную"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE vacancies SET posted_to_channel = 1 WHERE id = ?",
                (vacancy_id,)
            )
            conn.commit()


def run_aggregator():
    """Основная функция агрегатора"""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Запуск агрегатора...")

    # Инициализация
    db = VacancyDatabase()
    parser = HHruParser()

    # ВАЖНО: Укажите ваш токен бота и username канала
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
    print("BOT_TOKEN:", "OK" if BOT_TOKEN else "MISSING", flush=True)
    print("CHANNEL_USERNAME:", CHANNEL_USERNAME, flush=True)
    
    publisher = TelegramChannelPublisher(BOT_TOKEN)

    # Получаем новые вакансии (ищем за последние 7 дней)
    print("Получаем вакансии с HH.ru...")
    vacancies = parser.fetch_vacancies("Пермь", period_days=7)

    # Сохраняем новые
    new_count = 0
    for vacancy in vacancies:
        if db.save_vacancy(vacancy):
            new_count += 1

    print(f"Новых вакансий сохранено в БД: {new_count}")

    # Получаем неопубликованные
    unposted = db.get_unposted_vacancies(10)
    print(f"Найдено неопубликованных вакансий: {len(unposted)}")

    # Публикуем в канал
    if unposted:
        print(f"Публикую вакансии в канал {CHANNEL_USERNAME}...")
        for i, vacancy in enumerate(unposted, 1):
            print(f"  {i}. {vacancy['title'][:50]}...")
            success = publisher.send_to_channel(CHANNEL_USERNAME, vacancy)
            if success:
                db.mark_as_posted(vacancy['id'])
                time.sleep(1)  # Пауза между сообщениями
        print(f"Опубликовано в канале: {len(unposted)}")
    else:
        print("Нет новых вакансий для публикации")

    print("Готово!")


def job():
    """Задача для расписания"""
    run_aggregator()


# Основной запуск
if __name__ == "__main__":
    # Запустить сразу при старте
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Начальный запуск...")

    # Удалите старую базу данных, если есть проблемы
    # import os
    # if os.path.exists("vacancies.db"):
    #     os.remove("vacancies.db")
    #     print("Старая БД удалена")

    job()

    # Затем запускать каждый час
    schedule.every(1).hours.do(job)

    print("\nАгрегатор запущен. Ожидание расписания...")
    print("Для остановки нажмите Ctrl+C\n")

    # Бесконечный цикл для расписания
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Проверяем каждую минуту
    except KeyboardInterrupt:

        print("\nАгрегатор остановлен пользователем")




