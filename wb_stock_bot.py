"""Мониторинг WB FBO/FBS -> Telegram, ежедневно в 10:00 МСК.

Установка: pip install 'aiogram>=3,<4' 'APScheduler>=3.10,<4' requests
Конфигурация: WB_TOKEN, BOT_TOKEN, TG_CHAT_ID в переменных окружения.
В Colab: import wb_stock_bot; wb_stock_bot.start_colab() (не блокирует ячейку).
На сервере: python wb_stock_bot.py
"""
import asyncio
import io
import logging
import os
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from aiogram import Bot, Dispatcher
from aiogram.exceptions import (TelegramRetryAfter, TelegramConflictError,
                                TelegramNetworkError, TelegramServerError,
                                TelegramUnauthorizedError)
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message
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

    def sales(self, start, end):
        """Два календарных дня с flag=1, затем точный фильтр последних 24 часов.
        Так не теряются продажи из-за фильтра по lastChangeDate вместо date.
        ВАЖНО: метод Sales ограничен 1 запросом/мин для персонального токена.
        """
        result = Counter()
        seen = set()
        day = start.date()
        while day <= end.date():
            rows = self.request("GET", WB_STATS + "/api/v1/supplier/sales",
                                params={"dateFrom": day.isoformat(), "flag": 1},
                                interval=61)
            if not isinstance(rows, list):
                raise WBError("Неверный формат статистики продаж")
            for row in rows:
                sid = str(row.get("saleID") or "")
                # Только выкупы: возвраты помечаются R..., а не S...
                if not sid.startswith("S"):
                    continue
                stamp = row.get("date")
                if not stamp:
                    continue
                try:
                    sold_at = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                    sold_at = sold_at.replace(tzinfo=MOSCOW) if sold_at.tzinfo is None else sold_at.astimezone(MOSCOW)
                except ValueError:
                    LOG.warning("Пропущена некорректная дата продажи")
                    continue
                if not (start <= sold_at <= end):
                    continue
                unique = sid  # saleID идентифицирует операцию продажи
                if unique in seen:
                    continue
                seen.add(unique)
                barcode = str(row.get("barcode") or "").strip()
                if barcode:
                    result[barcode] += 1
                else:
                    LOG.warning("Продажа %s без barcode пропущена", sid)
            day += timedelta(days=1)
        LOG.info("Продажи за 24 часа: %d шт., %d barcode", sum(result.values()), len(result))
        return result


def collect_report(token):
    """Возвращает полный список остатков и продаж, а также отфильтрованные строки."""
    wb = WBClient(token)
    end = datetime.now(MOSCOW)
    start = end - timedelta(hours=24)
    cards, chrt_to_skus = wb.cards()
    if not cards:
        raise WBError("Не найдены карточки: нельзя достоверно сопоставить SKU и остатки")
    fbo = wb.fbo()
    fbs, fbs_enabled = wb.fbs(chrt_to_skus)
    sales = wb.sales(start, end)
    full = []
    for sku, card in cards.items():
        if card["chrtId"] is None and fbs_enabled:
            raise WBError(f"Нет chrtId у SKU {sku}; остатки FBS нельзя проверить")
        record = {"sku": sku, "name": card["name"], "fbo": fbo[sku],
                  "fbs": fbs[sku], "sales": sales[sku]}
        full.append(record)
    # Ноль хотя бы в одном используемом канале: FBO или (при наличии складов) FBS.
    filtered = [r for r in full if r["sales"] >= 2 and
                (r["fbo"] <= 0 or (fbs_enabled and r["fbs"] <= 0))]
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


def build_bot():
    # Название BOT_TOKEN оставляем прежним: уже задано на Bothost.
    bot_token = config("BOT_TOKEN")
    wb_token = config("WB_TOKEN")
    chat_raw = config("TG_CHAT_ID")
    if not bot_token or not wb_token:
        raise RuntimeError("Задайте BOT_TOKEN и WB_TOKEN")
    chat_id = int(chat_raw) if chat_raw else None
    bot = Bot(token=bot_token)
    dp = Dispatcher()
    check_lock = asyncio.Lock()

    async def check(chat, manual=False):
        if check_lock.locked():
            if manual:
                await bot.send_message(chat, "Проверка уже выполняется.")
            return
        async with check_lock:
            try:
                full, rows, start, end = await asyncio.to_thread(collect_report, wb_token)
                LOG.info("Проверено %d SKU; найдено %d совпадений", len(full), len(rows))
                if not rows:
                    # По требованию молчим, в том числе при ручном запуске.
                    return
                data = make_csv(rows)
                name = "wb_zero_stock_" + end.strftime("%Y%m%d_%H%M") + ".csv"
                await bot.send_document(chat, BufferedInputFile(data, filename=name),
                                        caption="Внимание: остаток 0, но есть продажи!")
            except Exception as e:
                LOG.exception("Проверка WB завершилась ошибкой")
                # Ошибку направляем только владельцу бота, не раскрывая токены.
                try:
                    await bot.send_message(chat, f"Ошибка проверки Wildberries: {str(e)[:300]}")
                except Exception:
                    LOG.exception("Не удалось уведомить Telegram об ошибке")

    @dp.message(Command("start"))
    async def start_handler(message: Message):
        if chat_id is not None and message.chat.id != chat_id:
            return
        await message.answer("Монитор остатков WB. /check_now — проверить сейчас. "
                             "Автопроверка — ежедневно в 10:00 МСК. "
                             f"ID этого чата: {message.chat.id}. "
                             "Укажите его в TG_CHAT_ID, чтобы включить мониторинг.")

    @dp.message(Command("check_now"))
    async def manual_handler(message: Message):
        if chat_id is None or message.chat.id != chat_id:
            return
        # Не блокируем обработку команд во время выгрузки WB.
        asyncio.create_task(check(chat_id, manual=True))

    async def scheduled():
        if chat_id is None:
            LOG.error("TG_CHAT_ID не задан, отправка невозможна")
            return
        await check(chat_id)

    async def run():
        start_health_server_if_needed()
        scheduler = AsyncIOScheduler(timezone=MOSCOW)
        scheduler.add_job(scheduled, "cron", hour=10, minute=0,
                          timezone="Europe/Moscow", id="daily_wb_check",
                          replace_existing=True, coalesce=True,
                          max_instances=1, misfire_grace_time=300)
        try:
            # Сначала подготовим Telegram: при 429 будем ждать внутри процесса,
            # а не падать и не провоцировать новые перезапуски от Bothost.
            await prepare_polling(bot)
            scheduler.start()
            LOG.info("Следующая проверка: %s",
                     scheduler.get_job("daily_wb_check").next_run_time)
            while True:
                try:
                    # Dispatcher сам повторяет большинство сетевых ошибок polling.
                    await dp.start_polling(bot, handle_signals=False,
                                           close_bot_session=False)
                    LOG.warning("Polling остановился; повторный запуск через 5 сек")
                    await asyncio.sleep(5)
                except TelegramRetryAfter as e:
                    delay = max(1, int(e.retry_after)) + 2
                    LOG.warning("Telegram 429 при polling; ждём %s сек", delay)
                    await asyncio.sleep(delay)
                except TelegramConflictError:
                    # Два процесса с одним BOT_TOKEN конкурируют за getUpdates.
                    LOG.error("Другой экземпляр бота использует тот же токен. "
                              "Остановите дубликат. Повтор через 30 сек")
                    await asyncio.sleep(30)
                except (TelegramNetworkError, TelegramServerError) as e:
                    LOG.warning("Telegram: %s; повтор через 15 сек", type(e).__name__)
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
