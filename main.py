import os
import sqlite3
import requests
import time
import signal
import sys
from datetime import datetime, timedelta
from contextlib import contextmanager
import schedule


class GracefulExit:
    """Класс для graceful shutdown"""
    def __init__(self):
        self.exit_now = False
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        print(f"\nПолучен сигнал {signum}. Завершаю работу...")
        self.exit_now = True


class TelegramChannelPublisher:
    """Публикация вакансий в Telegram-канал"""

    def __init__(self, bot_token):
        self.bot_token = bot_token
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self.exit_flag = False

    def check_bot(self):
        """Проверяет, что бот работает"""
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
        """
        Отправляет вакансию в канал
        """
        if self.exit_flag:
            print("Получен запрос на выход, пропускаю отправку")
            return False

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

        for attempt in range(retry_count):
            try:
                response = requests.post(url, json=payload, timeout=15)
                
                if response.status_code == 200:
                    print(f"✓ Отправлено в канал {channel_username}: {vacancy['title'][:50]}...")
                    return True
                else:
                    error_data = response.json()
                    print(f"✗ Ошибка Telegram API (попытка {attempt+1}/{retry_count}): {error_data.get('description', response.text)}")
                    
                    # Если это ошибка чата, прерываем попытки
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
        """Форматирует вакансию для Telegram"""
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

        # Форматируем дату
        published = vacancy.get('published_at', '')
        if published:
            try:
                # Убираем миллисекунды и часовой пояс
                published = published.split('.')[0].replace('Z', '+00:00')
                dt = datetime.strptime(published, "%Y-%m-%dT%H:%M:%S%z")
                published_str = dt.strftime("%d.%m.%Y %H:%M")
            except:
                published_str = "Недавно"
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
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def get_city_id(self, city_name="Пермь"):
        """ID городов на HH.ru"""
        cities = {
            'Пермь': 59,  # Правильный ID для Перми (проверено)
            'Москва': 1,
            'Санкт-Петербург': 2,
            'Екатеринбург': 3,
            'Новосибирск': 4,
            'Казань': 88,
            'Нижний Новгород': 66,
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

    def fetch_vacancies(self, city="Пермь", keywords=None, period_days=1):
        """Получает вакансии за последние N дней"""
        city_id = self.get_city_id(city)

        # Дата для поиска
        date_from = datetime.now() - timedelta(days=period_days)
        date_from_str = date_from.strftime("%Y-%m-%dT%H:%M:%S")

        vacancies = []
        page = 0
        
        print(f"Поиск вакансий в {city} за последние {period_days} дней...")
        
        try:
            while True:
                params = {
                    "area": city_id,
                    "per_page": 50,
                    "page": page,
                    "date_from": date_from_str,
                    "order_by": "publication_time",
                    "search_field": "name",
                    "only_with_salary": False,
                    "experience": "noExperience",  # Любой опыт
                    "employment": "full",  # Полная занятость
                }

                if keywords:
                    params["text"] = keywords
                else:
                    # Ищем IT-вакансии
                    params["text"] = "программист OR разработчик OR it OR python OR java OR javascript OR frontend OR backend"

                response = self.session.get(self.base_url, params=params, timeout=20)
                response.raise_for_status()
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
                
                # Проверяем, есть ли еще страницы
                pages = data.get("pages", 0)
                page += 1
                if page >= pages or page >= 5:  # Максимум 5 страниц
                    break
                    
                # Пауза между запросами
                time.sleep(0.5)

        except Exception as e:
            print(f"  Ошибка при парсинге HH.ru: {e}")

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
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Создаем таблицу с правильной структурой
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
            
            # Создаем индекс для быстрого поиска
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_posted ON vacancies(posted_to_channel)
            """)
            
            conn.commit()

    def cleanup_old_vacancies(self, days_to_keep=30):
        """Удаляет старые вакансии"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
            cursor.execute("DELETE FROM vacancies WHERE date(published_at) < date(?)", (cutoff_date,))
            deleted = cursor.rowcount
            conn.commit()
            if deleted:
                print(f"Удалено {deleted} старых вакансий (старше {days_to_keep} дней)")

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
                INSERT OR IGNORE INTO vacancies 
                (id, title, company, salary, url, published_at, source, city)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                vacancy['id'],
                vacancy['title'][:500],  # Ограничиваем длину
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
        """Получает неопубликованные вакансии"""
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
        """Отмечает вакансию как опубликованную"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE vacancies SET posted_to_channel = 1 WHERE id = ?",
                (vacancy_id,)
            )
            conn.commit()


def run_aggregator(publisher, exit_controller):
    """Основная функция агрегатора"""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Запуск агрегатора...")

    # Проверяем флаг выхода
    if exit_controller.exit_now:
        print("Получен запрос на выход, завершаю работу...")
        return False

    # Инициализация
    db = VacancyDatabase()
    parser = HHruParser()

    # ВАЖНО: Укажите ваш токен бота и username канала
    BOT_TOKEN = os.getenv("BOT_TOKEN", "8549288451:AAEhvxBAfrSmqUkp5zAJg-AITE_rm2-ob1Y")
    CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@vacancies_perm")

    print(f"Используется канал: {CHANNEL_USERNAME}")
    
    # Проверяем бота
    if not publisher.check_bot():
        print("Ошибка: бот не работает. Проверьте токен.")
        return False

    # Очищаем старые вакансии (раз в неделю)
    if datetime.now().weekday() == 0:  # Каждый понедельник
        db.cleanup_old_vacancies(30)

    # Получаем новые вакансии (ищем за последний день)
    print("\nПолучаем вакансии с HH.ru...")
    vacancies = parser.fetch_vacancies("Пермь", period_days=1)

    # Сохраняем новые
    new_count = 0
    for vacancy in vacancies:
        if exit_controller.exit_now:
            break
        if db.save_vacancy(vacancy):
            new_count += 1

    print(f"\nНовых вакансий сохранено в БД: {new_count}")

    # Получаем неопубликованные
    unposted = db.get_unposted_vacancies(5)  # Максимум 5 за раз
    print(f"Найдено неопубликованных вакансий: {len(unposted)}")

    # Публикуем в канал
    if unposted:
        print(f"\nПубликую вакансии в канал {CHANNEL_USERNAME}...")
        posted_count = 0
        
        for i, vacancy in enumerate(unposted, 1):
            if exit_controller.exit_now or publisher.exit_flag:
                print("Получен запрос на выход, прерываю публикацию...")
                break
            
            print(f"  {i}. {vacancy['title'][:50]}...")
            success = publisher.send_to_channel(CHANNEL_USERNAME, vacancy)
            
            if success:
                db.mark_as_posted(vacancy['id'])
                posted_count += 1
                
                # Пауза между сообщениями
                if i < len(unposted):
                    print(f"    Пауза 2 секунды...")
                    for _ in range(20):  # Разбиваем на 20 проверок по 0.1 сек
                        if exit_controller.exit_now:
                            break
                        time.sleep(0.1)
            else:
                print(f"    Не удалось отправить вакансию")
            
            if exit_controller.exit_now:
                break
        
        print(f"\nОпубликовано в канале: {posted_count} вакансий")
    else:
        print("\nНет новых вакансий для публикации")

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Завершено!")
    return True


def job(publisher, exit_controller):
    """Задача для расписания"""
    try:
        return run_aggregator(publisher, exit_controller)
    except KeyboardInterrupt:
        print("Задача прервана пользователем")
        exit_controller.exit_now = True
        return False
    except Exception as e:
        print(f"Ошибка в задаче: {e}")
        import traceback
        traceback.print_exc()
        return False


# Основной запуск
if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Запуск агрегатора вакансий...")
    print("=" * 60)
    
    # Инициализируем graceful exit
    exit_controller = GracefulExit()
    
    # Получаем токен и канал из переменных окружения
    BOT_TOKEN = os.getenv("BOT_TOKEN", "8549288451:AAEhvxBAfrSmqUkp5zAJg-AITE_rm2-ob1Y")
    CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@vacancies_perm")
    
    # Создаем publisher
    publisher = TelegramChannelPublisher(BOT_TOKEN)
    
    print(f"Конфигурация:")
    print(f"  Бот токен: {'OK' if BOT_TOKEN else 'НЕТ'}")
    print(f"  Канал: {CHANNEL_USERNAME}")
    print("=" * 60)
    
    # Проверяем бота
    if not publisher.check_bot():
        print("❌ Ошибка: Бот не работает. Проверьте токен и интернет соединение.")
        print("Для выхода нажмите Ctrl+C")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            sys.exit(0)
    
    # Первый запуск
    print("\nПервый запуск...")
    try:
        job(publisher, exit_controller)
    except Exception as e:
        print(f"Ошибка при первом запуске: {e}")
    
    # Затем запускать каждые 4 часа
    schedule.every(4).hours.do(lambda: job(publisher, exit_controller))
    
    print("\n" + "=" * 60)
    print("Агрегатор запущен. Расписание: каждые 4 часа")
    print("Для остановки нажмите Ctrl+C\n")
    
    # Бесконечный цикл для расписания
    last_run = datetime.now()
    
    try:
        while not exit_controller.exit_now:
            # Запускаем задачи по расписанию
            schedule.run_pending()
            
            # Показываем статус каждые 5 минут
            current_time = datetime.now()
            if (current_time - last_run).seconds > 300:  # 5 минут
                print(f"[{current_time.strftime('%H:%M:%S')}] Ожидание следующего запуска...")
                last_run = current_time
            
            # Пауза с проверкой флага выхода
            for _ in range(60):  # Проверяем каждую секунду
                if exit_controller.exit_now:
                    break
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\nПолучен сигнал прерывания...")
    finally:
        print("\n" + "=" * 60)
        print("Агрегатор завершает работу...")
        print("Спасибо за использование!")
        print("=" * 60)
