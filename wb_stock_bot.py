"""Мониторинг WB FBO/FBS -> Telegram, ежедневно в 10:00 МСК.

Установка: pip install 'aiogram>=3,<4' 'APScheduler>=3.10,<4' requests xlsxwriter
Конфигурация: WB_TOKEN и BOT_TOKEN в переменных окружения. Доступ всем пользователям.
В Colab: import wb_stock_bot; wb_stock_bot.start_colab() (не блокирует ячейку).
На сервере: python wb_stock_bot.py
"""
import asyncio
import io
import logging
import os
import sqlite3
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import xlsxwriter
from aiogram import Bot, Dispatcher, F
from aiogram.types import User
from aiogram.exceptions import (TelegramRetryAfter, TelegramConflictError,
                                TelegramNetworkError, TelegramServerError,
                                TelegramUnauthorizedError)
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, KeyboardButton, Message, ReplyKeyboardMarkup
from apscheduler.schedulers.asyncio import AsyncIOScheduler

MOSCOW = ZoneInfo("Europe/Moscow")
WB_ANALYTICS = "https://seller-analytics-api.wildberries.ru"
WB_MARKET = "https://marketplace-api.wildberries.ru"
WB_CONTENT = "https://content-api.wildberries.ru"
WB_STATS = "https://statistics-api.wildberries.ru"
LOG = logging.getLogger("wb_monitor")


def config(name, default=""):
    val = os.getenv(name, default)
    if not val:
        try:
            from google.colab import userdata
            val = userdata.get(name) or default
        except Exception:  # Colab: секрет может отсутствовать или доступ может быть запрещён
            pass
    return val


class WBError(RuntimeError):
    pass


class WBClient:
    def __init__(self, token):
        if not token:
            raise WBError("Отсутствует WB_TOKEN")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": token, "Accept": "application/json"})
        self.last_request = {}

    def request(self, method, url, *, json=None, params=None, interval=0.3):
        # Ограничитель по каждому методу (особенно важен для двух суточных выгрузок продаж).
        for attempt in range(4):
            prev = self.last_request.get(url, 0)
            pause = interval - (time.monotonic() - prev)
            if pause > 0:
                time.sleep(pause)
            self.last_request[url] = time.monotonic()
            try:
                r = self.session.request(method, url, json=json, params=params, timeout=(15, 90))
            except requests.RequestException as e:
                if attempt == 3:
                    raise WBError(f"Ошибка соединения с WB: {type(e).__name__}") from e
                time.sleep(2 ** attempt * 5)
                continue
            if r.status_code == 429:
                raw = r.headers.get("X-Ratelimit-Retry") or r.headers.get("Retry-After")
                try:
                    wait = min(max(float(raw), interval), 7200) if raw else max(interval, 60)
                except ValueError:
                    wait = max(interval, 60)
                LOG.warning("WB 429: ожидание %.0f сек (%s)", wait, url.split("/")[2])
                if attempt == 3:
                    raise WBError("WB API: 429, превышен лимит после повторов")
                time.sleep(wait + 1)
                continue
            if r.status_code in (401, 402, 403):
                raise WBError(f"WB API: HTTP {r.status_code}. Проверьте токен, права категорий и тариф")
            if r.status_code >= 400:
                raise WBError(f"WB API: HTTP {r.status_code}: {r.text[:250]}")
            if r.status_code == 204:
                return None
            try:
                return r.json()
            except ValueError as e:
                raise WBError("WB API вернул не JSON") from e
        raise WBError("WB API: не удалось получить ответ")

    def cards(self):
        """Карточки: barcode -> сведения, chrtID -> баркоды.
        Данные берутся целиком, не только из остатков, чтобы учитывать нулевые позиции.
        """
        catalog, chrt_to_skus = {}, defaultdict(set)
        cursor = {"limit": 100}
        previous = None
        while True:
            body = {"settings": {"sort": {"ascending": True},
                                 "cursor": cursor,
                                 "filter": {"withPhoto": -1}}}
            data = self.request("POST", WB_CONTENT + "/content/v2/get/cards/list",
                                json=body, interval=0.7)
            items = data.get("cards", [])
            if not isinstance(items, list):
                raise WBError("Неожиданный формат карточек WB")
            for card in items:
                nm = str(card.get("nmID") or card.get("nmId") or "")
                title = card.get("title") or card.get("subjectName") or card.get("vendorCode") or ""
                for size in card.get("sizes", []):
                    chrt = size.get("chrtID") or size.get("chrtId")
                    for barcode in size.get("skus", []):
                        sku = str(barcode).strip()
                        if not sku:
                            continue
                        if sku in catalog and catalog[sku]["nmId"] != nm:
                            raise WBError(f"Один barcode привязан к разным nmId: {sku}")
                        catalog[sku] = {"sku": sku, "nmId": nm, "name": title,
                                        "article": str(card.get("vendorCode") or ""),
                                        "chrtId": int(chrt) if chrt else None}
                        if chrt:
                            chrt_to_skus[int(chrt)].add(sku)
            cur = data.get("cursor") or {}
            # Защита от зацикливания при неожиданном ответе.
            if len(items) < 100:
                break
            marker = (cur.get("updatedAt"), cur.get("nmID"))
            if not all(marker) or marker == previous:
                raise WBError("Не удалось продолжить постраничную выгрузку карточек")
            previous = marker
            cursor = {"limit": 100, "updatedAt": marker[0], "nmID": marker[1]}
        LOG.info("Карточки: %d barcode, %d размеров", len(catalog), len(chrt_to_skus))
        return catalog, chrt_to_skus

    def fbo(self):
        """Полный асинхронный отчёт. Итоговые и транзитные строки не суммируем."""
        url = WB_ANALYTICS + "/api/v1/warehouse_remains"
        task = self.request("GET", url, params={"locale": "ru", "groupByNm": "true",
                        "groupByBarcode": "true", "groupBySize": "true"}, interval=61)
        task_id = (task or {}).get("data", {}).get("taskId")
        if not task_id:
            raise WBError("Отчёт FBO не вернул taskId")
        base = url + f"/tasks/{task_id}"
        for _ in range(90):  # до ~15 минут ожидания
            time.sleep(10)
            obj = self.request("GET", base + "/status", interval=6)
            status = str((obj or {}).get("data", {}).get("status", "")).lower()
            if status == "done":
                break
            if status in {"new", "processing", "pending", "created"}:
                continue
            raise WBError(f"Неожиданный статус отчёта FBO: {status}")
        else:
            raise WBError("Истекло время ожидания отчёта FBO")
        rows = self.request("GET", base + "/download", interval=61)
        if rows is None:
            raise WBError("FBO вернул 204: нет данных (не приравниваем к нулю)")
        if not isinstance(rows, list):
            raise WBError("Неверный формат выгрузки FBO")
        result = Counter()
        for item in rows:
            sku = str(item.get("barcode") or "").strip()
            if not sku:
                continue
            warehouses = item.get("warehouses")
            if warehouses is None:
                # WB не включает warehouses, если у товара все остатки равны нулю.
                warehouses = []
            if not isinstance(warehouses, list):
                raise WBError("FBO: некорректный формат детализации по складам")
            totals, physical = [], []
            for wh in warehouses:
                name = str(wh.get("warehouseName") or "").strip().casefold()
                qty = int(wh.get("quantity") or 0)
                # Названия итогов и транзита в русской и английской локалях.
                if any(term in name for term in ("total in warehouses", "итого по складам", "всего на складах", "итого на складах")):
                    totals.append(qty)
                elif any(term in name for term in ("to the client", "from the client",
                                                     "к клиенту", "от клиента", "в пути", "транзит")):
                    continue
                else:
                    physical.append(qty)
            if len(totals) > 1:
                raise WBError("FBO: неоднозначное количество итоговых строк")
            result[sku] += totals[0] if totals else sum(physical)
        LOG.info("FBO: %d строк, %d barcode", len(rows), len(result))
        return result

    def fbs(self, chrt_to_skus):
        warehouses = self.request("GET", WB_MARKET + "/api/v3/warehouses")
        if not isinstance(warehouses, list):
            raise WBError("Неожиданный ответ списка складов FBS")
        totals = Counter()
        ids = sorted(chrt_to_skus)
        for wh in warehouses:
            wh_id = wh.get("id")
            if wh_id is None:
                raise WBError("FBS: отсутствует ID склада")
            for i in range(0, len(ids), 1000):
                batch = ids[i:i + 1000]
                obj = self.request("POST", WB_MARKET + f"/api/v3/stocks/{wh_id}",
                                   json={"chrtIds": batch}, interval=0.35)
                stocks = (obj or {}).get("stocks")
                if not isinstance(stocks, list):
                    raise WBError("FBS: отсутствует список stocks")
                for stock in stocks:
                    chrt = int(stock["chrtId"])
                    if chrt not in chrt_to_skus:
                        continue
                    # У размера может быть несколько barcode; это общий остаток размера.
                    for sku in chrt_to_skus[chrt]:
                        totals[sku] += int(stock.get("amount") or 0)
        LOG.info("FBS: %d складов, %d barcode с ненулевыми/записанными остатками",
                 len(warehouses), len(totals))
        return totals, bool(warehouses)

    def sale_rows(self, start, end, monthly):
        url = WB_STATS + "/api/v1/supplier/sales"
        if not monthly:
            day = start.date()
            while day <= end.date():
                rows = self.request("GET", url, params={"dateFrom": day.isoformat(), "flag": 1},
                                    interval=61)
                if not isinstance(rows, list):
                    raise WBError("Неверный формат статистики продаж")
                yield from rows
                day += timedelta(days=1)
            return

        cursor = start.replace(tzinfo=None).isoformat(timespec="seconds")
        while True:
            rows = self.request("GET", url, params={"dateFrom": cursor, "flag": 0}, interval=61)
            if not isinstance(rows, list):
                raise WBError("Неверный формат статистики продаж")
            if not rows:
                return
            next_cursor = rows[-1].get("lastChangeDate")
            if not isinstance(next_cursor, str) or next_cursor <= cursor:
                raise WBError("Wildberries не вернул следующий курсор продаж")
            yield from rows
            cursor = next_cursor

    def sales(self, start, end, *, monthly=False):
        result, seen = Counter(), set()
        for row in self.sale_rows(start, end, monthly):
            sale_id = str(row.get("saleID") or "")
            if not sale_id.startswith("S") or sale_id in seen:
                continue
            stamp = row.get("date")
            if not stamp:
                continue
            try:
                sold_at = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            except ValueError:
                LOG.warning("Пропущена некорректная дата продажи")
                continue
            if sold_at.tzinfo is None:
                sold_at = sold_at.replace(tzinfo=MOSCOW)
            if start <= sold_at <= end:
                seen.add(sale_id)
                barcode = str(row.get("barcode") or "").strip()
                if barcode:
                    result[barcode] += 1
        LOG.info("Продажи за период: %d шт., %d barcode", sum(result.values()), len(result))
        return result


def collect_report(token, *, monthly=False):
    """Возвращает полный список остатков и продаж, а также отфильтрованные строки."""
    wb = WBClient(token)
    end = datetime.now(MOSCOW)
    cards, chrt_to_skus = wb.cards()
    if not cards:
        raise WBError("Не найдены карточки: нельзя достоверно сопоставить SKU и остатки")
    fbo = wb.fbo()
    fbs, fbs_enabled = wb.fbs(chrt_to_skus)
    if monthly:
        end = datetime.now(MOSCOW)
    start = end - timedelta(days=30 if monthly else 1)
    sales = wb.sales(start, end, monthly=monthly)
    full = []
    for sku, card in cards.items():
        if card["chrtId"] is None and fbs_enabled:
            raise WBError(f"Нет chrtId у SKU {sku}; остатки FBS нельзя проверить")
        record = {"sku": sku, "name": card["name"], "article": card.get("article", ""),
                  "fbo": fbo[sku], "fbs": fbs[sku] if fbs_enabled or not monthly else None,
                  "sales": sales[sku]}
        full.append(record)
    # Ноль хотя бы в одном используемом канале: FBO или (при наличии складов) FBS.
    filtered = [r for r in full if r["sales"] >= 2 and
                (r["fbo"] == 0 or r["fbs"] == 0)] if monthly else [
                    r for r in full if r["sales"] >= 2 and
                    (r["fbo"] <= 0 or (fbs_enabled and r["fbs"] <= 0))
                ]
    filtered.sort(key=lambda r: (-r["sales"], r["sku"]))
    return full, filtered, start, end


def make_csv(rows):
    # BOM: Excel правильно открывает UTF-8. Разделитель ; удобен для русской локали.
    import csv
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Артикул (SKU)", "Название", "Остаток FBO", "Остаток FBS", "Продажи за период"])
    for row in rows:
        # Предотвращаем CSV formula injection при открытии названий товаров в Excel.
        def safe(s):
            text = str(s)
            return "'" + text if text.lstrip().startswith(("=", "+", "-", "@")) else text
        writer.writerow([safe(row["sku"]), safe(row["name"]), row["fbo"], row["fbs"], row["sales"]])
    return output.getvalue().encode("utf-8-sig")


def make_monthly_excel(rows, start, end):
    output = io.BytesIO()
    with xlsxwriter.Workbook(output, {"in_memory": True, "strings_to_formulas": False,
                                       "strings_to_urls": False}) as workbook:
        sheet = workbook.add_worksheet("Нулевые остатки")
        header = workbook.add_format({"bold": True, "bg_color": "#E5DDFA"})
        sheet.write_string(0, 0, f"Остатки на {end:%d.%m.%Y %H:%M} МСК")
        sheet.write_string(1, 0, f"Выкупы с {start:%d.%m.%Y %H:%M} до {end:%d.%m.%Y %H:%M} МСК")
        sheet.write_row(3, 0, ["Артикул продавца", "Баркод", "Название",
                               "Остаток FBO", "Остаток FBS", "Выкупы за 30 дней"], header)
        for index, row in enumerate(rows, 4):
            sheet.write_row(index, 0, [row[key] for key in
                                       ("article", "sku", "name", "fbo", "fbs", "sales")])
        sheet.set_column(0, 1, 23)
        sheet.set_column(2, 2, 50)
        sheet.set_column(3, 5, 19)
        sheet.freeze_panes(4, 0)
        sheet.autofilter(3, 0, 3 + len(rows), 5)
    return output.getvalue()


def start_health_server_if_needed():
    """Только для Render Web Service: HTTP /health для внешней проверки доступности."""
    port = os.getenv("PORT")
    if not port:
        return
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ("/", "/health"):
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("0.0.0.0", int(port)), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True, name="health-http").start()
    LOG.info("HTTP /health запущен на порту %s", port)


async def prepare_polling(bot):
    """Убираем webhook только при его наличии; Telegram 429 не должен ронять процесс.

    Важно: оставлять два экземпляра polling с одним токеном нельзя.
    """
    while True:
        try:
            info = await bot.get_webhook_info()
            if not info.url:
                LOG.info("Webhook не установлен, режим polling готов")
                return
            LOG.warning("Обнаружен Telegram webhook; отключаем для polling")
            await bot.delete_webhook(drop_pending_updates=False)
            LOG.info("Webhook отключён")
            return
        except TelegramRetryAfter as e:
            delay = max(1, int(e.retry_after)) + 2
            LOG.warning("Telegram 429 при настройке polling; ждём %s сек", delay)
            await asyncio.sleep(delay)
        except (TelegramNetworkError, TelegramServerError) as e:
            LOG.warning("Telegram временно недоступен: %s; повтор через 15 сек", type(e).__name__)
            await asyncio.sleep(15)


def subscriber_db_path():
    """Сохраняем подписчиков на диске Bothost, а не в оперативной памяти."""
    folder = os.getenv("DATA_DIR") or "."
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, "wb_subscribers.sqlite3")


def subscriber_store():
    """Создать таблицу при первом запуске."""
    path = subscriber_db_path()
    with sqlite3.connect(path, timeout=15) as db:
        db.execute("CREATE TABLE IF NOT EXISTS subscribers (chat_id INTEGER PRIMARY KEY)")
    return path


def add_subscriber(chat_id):
    with sqlite3.connect(subscriber_db_path(), timeout=15) as db:
        db.execute("INSERT OR IGNORE INTO subscribers(chat_id) VALUES (?)", (int(chat_id),))


def remove_subscriber(chat_id):
    with sqlite3.connect(subscriber_db_path(), timeout=15) as db:
        db.execute("DELETE FROM subscribers WHERE chat_id = ?", (int(chat_id),))


def get_subscribers():
    with sqlite3.connect(subscriber_db_path(), timeout=15) as db:
        return [x[0] for x in db.execute("SELECT chat_id FROM subscribers").fetchall()]


def build_bot():
    # Бот доступен всем: TG_CHAT_ID больше не используется для ограничения доступа.
    bot_token = config("BOT_TOKEN")
    wb_token = config("WB_TOKEN")
    if not bot_token or not wb_token:
        raise RuntimeError("Задайте BOT_TOKEN и WB_TOKEN")
    subscriber_store()
    bot = Bot(token=bot_token)
    # aiogram перед getUpdates вызывает getMe для получения имени бота.
    # Telegram ограничил именно GetMe (429); ID бота уже есть в токене.
    # Кэшируем только минимальную техническую информацию, чтобы не
    # выполнять заблокированный GetMe при каждом запуске процесса.
    # Это обход старта, а НЕ снятие ограничений Telegram с других методов.
    bot._me = User(id=bot.id, is_bot=True, first_name="WB Monitor")
    LOG.info("GetMe пропущен: идентификатор бота взят из Telegram-токена")
    dp = Dispatcher()
    check_lock = asyncio.Lock()
    last_manual_check = 0.0
    monthly_button = "Отчёт за 30 дней"
    report_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=monthly_button)]], resize_keyboard=True
    )

    async def notify(chat_ids, text):
        for chat_id in chat_ids:
            try:
                await bot.send_message(chat_id, text)
            except Exception:
                LOG.exception("Не удалось отправить сообщение чату %s", chat_id)

    async def check(chat_ids, manual=False):
        nonlocal last_manual_check
        if manual and check_lock.locked():
            await notify(chat_ids, "Проверка уже выполняется.")
            return
        if manual:
            # Защита WB API от частых проверок любым пользователем.
            delay = 900 - (time.monotonic() - last_manual_check)
            if delay > 0:
                await notify(chat_ids, "Проверку можно повторить через %d мин." %
                             max(1, int((delay + 59) // 60)))
                return
            last_manual_check = time.monotonic()
        async with check_lock:
            try:
                full, rows, start, end = await asyncio.to_thread(collect_report, wb_token)
                LOG.info("Проверено %d SKU; найдено %d совпадений", len(full), len(rows))
                if not rows:
                    # Файл не отправляем, когда совпадений нет.
                    return
                data = make_csv(rows)
                name = "wb_zero_stock_" + end.strftime("%Y%m%d_%H%M") + ".csv"
                for chat_id in chat_ids:
                    try:
                        await bot.send_document(
                            chat_id, BufferedInputFile(data, filename=name),
                            caption="Внимание: остаток 0, но есть продажи!"
                        )
                    except Exception:
                        LOG.exception("Не удалось отправить файл чату %s", chat_id)
            except Exception as e:
                LOG.exception("Проверка WB завершилась ошибкой")
                await notify(chat_ids, f"Ошибка проверки Wildberries: {str(e)[:300]}")

    @dp.message(Command("start"))
    async def start_handler(message: Message):
        add_subscriber(message.chat.id)
        LOG.info("Подписан чат %s", message.chat.id)
        await message.answer(
            "Монитор остатков Wildberries включён для этого чата. "
            "Ежедневная проверка — в 10:00 МСК. "
            "/check_now — проверить за сутки, /month — получить Excel за 30 дней, "
            "/stop — отключить уведомления. "
            "Если нет товаров с нулевым остатком и продажами от 2 шт., файл не отправляется.",
            reply_markup=report_keyboard,
        )

    @dp.message(Command("stop"))
    async def stop_handler(message: Message):
        remove_subscriber(message.chat.id)
        await message.answer("Ежедневные уведомления отключены. /start — включить снова.")

    @dp.message(Command("check_now"))
    async def manual_handler(message: Message):
        LOG.info("Получена /check_now из чата %s", message.chat.id)
        # Не блокируем обработку команд во время выгрузки WB.
        asyncio.create_task(check([message.chat.id], manual=True))

    async def send_monthly_report(message):
        if check_lock.locked():
            await message.answer("Проверка WB уже выполняется. Повтори команду после её завершения.")
            return
        async with check_lock:
            await message.answer(
                "Запрашиваю свежие остатки и выкупы за последние 30 дней.",
                reply_markup=report_keyboard,
            )
            try:
                _, rows, start, end = await asyncio.to_thread(collect_report, wb_token, monthly=True)
                if not rows:
                    await message.answer("Позиций с нулевым остатком и двумя или более выкупами нет.")
                    return
                data = await asyncio.to_thread(make_monthly_excel, rows, start, end)
                name = f"wb_zero_stock_month_{end:%Y%m%d_%H%M}.xlsx"
                await message.answer_document(
                    BufferedInputFile(data, filename=name),
                    caption=f"Нулевые остатки FBO или FBS · выкупы за 30 дней\n"
                            f"Позиций: {len(rows)}\nОстатки проверены {end:%d.%m.%Y в %H:%M} МСК",
                )
            except Exception:
                LOG.exception("Месячный отчёт WB завершился ошибкой")
                await message.answer("Не удалось подготовить месячный отчёт WB. Попробуй ещё раз позже.")

    @dp.message(Command("month"))
    @dp.message(F.text == monthly_button)
    async def monthly_handler(message: Message):
        asyncio.create_task(send_monthly_report(message))

    async def scheduled():
        chats = get_subscribers()
        if not chats:
            LOG.info("Подписчиков нет: ежедневная выгрузка пропущена")
            return
        await check(chats)

    async def run():
        start_health_server_if_needed()
        scheduler = AsyncIOScheduler(timezone=MOSCOW)
        scheduler.add_job(scheduled, "cron", hour=10, minute=0,
                          timezone="Europe/Moscow", id="daily_wb_check",
                          replace_existing=True, coalesce=True,
                          max_instances=1, misfire_grace_time=300)
        try:
            LOG.info("Запускаем Telegram getUpdates с учётом retry_after; подписчиков: %d", len(get_subscribers()))
            scheduler.start()
            LOG.info("Следующая проверка: %s",
                     scheduler.get_job("daily_wb_check").next_run_time)
            # Встроенный _listen_updates() aiogram при TelegramRetryAfter
            # повторяет запросы каждые 1–5 секунд, игнорируя retry_after.
            # Используем официальный getUpdates напрямую и передаём
            # сообщения в aiogram через поддерживаемый feed_update().
            offset = None
            LOG.info("Получение сообщений Telegram через getUpdates запущено")
            while True:
                try:
                    updates = await bot.get_updates(
                        offset=offset,
                        timeout=30,
                        allowed_updates=["message"],
                        request_timeout=45,
                    )
                    for update in updates:
                        try:
                            await dp.feed_update(bot, update)
                        except Exception:
                            LOG.exception("Ошибка обработки Telegram update_id=%s",
                                          update.update_id)
                        # Следующий getUpdates подтвердит обработанные сообщения.
                        offset = update.update_id + 1
                except TelegramRetryAfter as e:
                    delay = max(1, int(e.retry_after)) + 2
                    LOG.warning(
                        "Telegram 429: метод=%s; строго ждём %s сек "
                        "(retry_after=%s) перед новым запросом",
                        type(getattr(e, "method", None)).__name__,
                        delay, e.retry_after,
                    )
                    await asyncio.sleep(delay)
                except TelegramConflictError as e:
                    LOG.error(
                        "Telegram getUpdates: конфликт другого polling "
                        "или действующего webhook: %s", e
                    )
                    await asyncio.sleep(60)
                except TelegramUnauthorizedError:
                    LOG.exception("Telegram-токен недействителен; polling остановлен")
                    raise
                except (TelegramNetworkError, TelegramServerError, asyncio.TimeoutError) as e:
                    LOG.warning("Telegram: %s; повтор через 15 сек",
                                type(e).__name__)
                    await asyncio.sleep(15)
        finally:
            if scheduler.running:
                scheduler.shutdown(wait=False)
            await bot.session.close()

    return run


def start_colab():
    """Вызов из ячейки Colab: запускает polling и scheduler в daemon-потоке."""
    global _COLAB_THREAD
    if _COLAB_THREAD is not None and _COLAB_THREAD.is_alive():
        raise RuntimeError("Бот уже запущен в этом процессе")
    def worker():
        asyncio.run(build_bot()())
    _COLAB_THREAD = threading.Thread(target=worker, daemon=True, name="WB-Telegram-Bot")
    _COLAB_THREAD.start()
    return _COLAB_THREAD


_COLAB_THREAD = None

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(build_bot()())
