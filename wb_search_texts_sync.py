import os
import time
import json
import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values


# -----------------------------
# Настройки / константы
# -----------------------------
WB_BASE = "https://seller-analytics-api.wildberries.ru"
EP_SEARCH_TEXTS = f"{WB_BASE}/api/v2/search-report/product/search-texts"

MSK = ZoneInfo("Europe/Moscow")
UTC = ZoneInfo("UTC")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)


# -----------------------------
# Утилиты
# -----------------------------
def msk_today() -> date:
    return datetime.now(MSK).date()


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()


def env_int(name: str, default: int) -> int:
    v = env_str(name, "")
    return default if v == "" else int(v)


def parse_nm_ids_csv(s: str) -> List[int]:
    out: List[int] = []
    for part in (s or "").split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    # unique preserving order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


def wb_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


# -----------------------------
# Postgres
# -----------------------------
def pg_connect() -> psycopg2.extensions.connection:
    """
    Поддерживает 2 режима:
    1) SUPABASE_CONNINFO — готовая строка conninfo (как у тебя в логах)
    2) набор переменных SUPABASE_HOST/PORT/USER/PASSWORD/DBNAME/SSLMODE/OPTIONS
    """
    conninfo = env_str("SUPABASE_CONNINFO", "")
    if conninfo:
        logging.info("Using SUPABASE_CONNINFO")
        return psycopg2.connect(conninfo)

    host = env_str("SUPABASE_HOST")
    port = env_int("SUPABASE_PORT", 6543)
    user = env_str("SUPABASE_USER", "postgres")
    password = env_str("SUPABASE_PASSWORD")
    dbname = env_str("SUPABASE_DBNAME", "postgres")
    sslmode = env_str("SUPABASE_SSLMODE", "require")
    options = env_str("SUPABASE_OPTIONS", "")  # например: project=xxxxx

    if not host or not password:
        raise RuntimeError(
            "Не хватает переменных для подключения. "
            "Задай SUPABASE_CONNINFO ИЛИ SUPABASE_HOST + SUPABASE_PASSWORD (+ остальное по желанию)."
        )

    parts = [
        f"host={host}",
        f"port={port}",
        f"user={user}",
        f"dbname={dbname}",
        f"sslmode={sslmode}",
        f"password={password}",
    ]
    if options:
        # options могут быть вида project=... или '-c project=...'
        parts.append(f"options={options}")

    conninfo_built = " ".join(parts)
    logging.info("Using built conninfo (host/port/user/dbname/sslmode/options)")
    return psycopg2.connect(conninfo_built)


def fetch_nm_ids_from_db(conn) -> List[int]:
    """
    Берём все nm_id из твоей базы.
    Я сделал максимально совместимо: пробуем несколько таблиц.
    ТЫ МОЖЕШЬ ОСТАВИТЬ ТОЛЬКО ОДНУ, ЕСЛИ ЗНАЕШЬ ТОЧНО, ГДЕ У ТЕБЯ nm_id.
    """
    candidates = [
        ("public.wb_products_catalog", "select distinct nm_id from public.wb_products_catalog where nm_id is not null"),
        ("public.product_directory", "select distinct nm_id from public.product_directory where nm_id is not null"),
        ("public.wb_orders", "select distinct nm_id from public.wb_orders where nm_id is not null"),
    ]
    cur = conn.cursor()
    for name, sql in candidates:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            nm_ids = sorted({int(r[0]) for r in rows if r and r[0] is not None})
            if nm_ids:
                logging.info(f"Нашли nm_id из {name}: {len(nm_ids)}")
                return nm_ids
        except Exception as e:
            logging.info(f"Таблица/запрос не подошёл ({name}): {e}")
            conn.rollback()
    raise RuntimeError(
        "Не смог найти nm_id ни в одной из таблиц-кандидатов. "
        "Либо укажи NM_IDS вручную в env, либо скажи мне, где у тебя хранится список nm_id."
    )


def upsert_rows(conn, rows: List[Tuple[Any, ...]]) -> None:
    """
    ВАЖНО: rows должны быть СПИСКОМ КОРТЕЖЕЙ (НЕ dict),
    иначе снова будет can't adapt type 'dict'.
    """
    if not rows:
        logging.info("Нет строк для вставки.")
        return

    sql = """
    insert into public.wb_search_texts (
      load_dttm,
      period_start,
      period_end,
      nm_id,
      search_text,
      avg_position,
      open_card,
      add_to_cart,
      orders,
      open_to_cart,
      cart_to_order,
      open_to_order
    ) values %s
    on conflict (period_start, coalesce(period_end, period_start), nm_id, search_text)
    do update set
      load_dttm     = excluded.load_dttm,
      avg_position  = excluded.avg_position,
      open_card     = excluded.open_card,
      add_to_cart   = excluded.add_to_cart,
      orders        = excluded.orders,
      open_to_cart  = excluded.open_to_cart,
      cart_to_order = excluded.cart_to_order,
      open_to_order = excluded.open_to_order
    ;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    logging.info(f"Upsert OK: {len(rows)} строк")


# -----------------------------
# WB API
# -----------------------------
def wb_post(session: requests.Session, url: str, api_key: str, body: Dict[str, Any]) -> Dict[str, Any]:
    resp = session.post(url, headers=wb_headers(api_key), data=json.dumps(body), timeout=60)
    if resp.status_code == 429:
        raise RuntimeError(f"WB 429 Too Many Requests: {resp.text}")
    if resp.status_code != 200:
        raise RuntimeError(f"WB request failed {resp.status_code}: {resp.text}")
    return resp.json()


def fetch_search_texts_for_nmids(
    session: requests.Session,
    api_key: str,
    period_start: date,
    period_end: date,
    nm_ids: List[int],
    limit: int,
    top_order_by: str,
    pause_seconds: float
) -> List[Dict[str, Any]]:
    """
    Возвращает "сырые" items из WB.
    """
    all_items: List[Dict[str, Any]] = []
    batches = chunk(nm_ids, 100)  # безопасный батч
    for i, nm_batch in enumerate(batches, start=1):
        body = {
            "currentPeriod": {"start": period_start.isoformat(), "end": period_end.isoformat()},
            "nmIds": nm_batch,
            "topOrderBy": top_order_by,  # обычно 'orders'
            "includeSubstitutedSKUs": True,
            "includeSearchTexts": True,
            "orderBy": {"field": "avgPosition", "mode": "asc"},
            "limit": limit
        }

        logging.info(f"Запрашиваем пачку nmIds: {len(nm_batch)} (batch {i}/{len(batches)})")
        js = wb_post(session, EP_SEARCH_TEXTS, api_key, body)
        data = js.get("data") or {}
        items = data.get("items") or []
        for it in items:
            if isinstance(it, dict):
                all_items.append(it)

        # лимиты WB — бережно
        if i < len(batches):
            logging.info(f"Пауза {pause_seconds} сек (лимиты WB)…")
            time.sleep(pause_seconds)

    return all_items


def build_db_rows(period_start: date, period_end: date, items: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    """
    Превращаем items (dict) -> rows (tuple).
    НИКАКИХ dict в tuples не кладём.
    """
    load_dttm = datetime.now(UTC)

    rows: List[Tuple[Any, ...]] = []
    for it in items:
        # WB обычно отдаёт nmId в каждом item, подстрахуемся:
        nm_id = it.get("nmId") or it.get("nmID")
        search_text = (it.get("text") or "").strip()
        if not nm_id or not search_text:
            continue

        avg_position = safe_float(it.get("avgPosition"))
        open_card = safe_int(it.get("openCard"))
        add_to_cart = safe_int(it.get("addToCart"))
        orders = safe_int(it.get("orders"))

        # конверсии (если WB отдаёт — отлично, если нет — посчитаем)
        open_to_cart = safe_float(it.get("openToCart"))
        cart_to_order = safe_float(it.get("cartToOrder"))
        open_to_order = safe_float(it.get("openToOrder"))

        # если не пришло — считаем сами
        if open_to_cart is None and open_card and open_card > 0 and add_to_cart is not None:
            open_to_cart = add_to_cart / open_card
        if cart_to_order is None and add_to_cart and add_to_cart > 0 and orders is not None:
            cart_to_order = orders / add_to_cart
        if open_to_order is None and open_card and open_card > 0 and orders is not None:
            open_to_order = orders / open_card

        row = (
            load_dttm,
            period_start,
            period_end,
            int(nm_id),
            search_text,
            avg_position,
            open_card,
            add_to_cart,
            orders,
            open_to_cart,
            cart_to_order,
            open_to_order
        )
        rows.append(row)

    return rows


# -----------------------------
# main
# -----------------------------
def main():
    wb_api_key = env_str("WB_API_KEY")
    if not wb_api_key:
        raise RuntimeError("WB_API_KEY пустой")

    # Период
    period_start_s = env_str("PERIOD_START", "")
    period_end_s = env_str("PERIOD_END", "")

    if period_start_s and period_end_s:
        period_start = parse_date(period_start_s)
        period_end = parse_date(period_end_s)
    else:
        # По умолчанию грузим вчера (по МСК)
        y = msk_today() - timedelta(days=1)
        period_start = y
        period_end = y

    # Параметры WB
    limit = env_int("LIMIT", 100)  # на Jam обычно можно 100
    top_order_by = env_str("TOP_ORDER_BY", "orders")  # 'orders' самый логичный
    pause_seconds = float(env_str("WB_PAUSE_SEC", "21"))

    # nm_id
    nm_ids_env = env_str("NM_IDS", "")
    nm_ids: List[int]

    conn = pg_connect()
    try:
        if nm_ids_env:
            nm_ids = parse_nm_ids_csv(nm_ids_env)
            logging.info(f"NM_IDS из env: {len(nm_ids)}")
        else:
            nm_ids = fetch_nm_ids_from_db(conn)

        logging.info(f"Период: start={period_start.isoformat()} end={period_end.isoformat()} | TOP={top_order_by} | limit={limit}")
        logging.info(f"Всего nm_id: {len(nm_ids)}")
        logging.info(f"Первые 20 nm_id: {nm_ids[:20]}")

        with requests.Session() as s:
            items = fetch_search_texts_for_nmids(
                s, wb_api_key, period_start, period_end,
                nm_ids, limit, top_order_by, pause_seconds
            )

        logging.info(f"WB вернул items: {len(items)}")
        rows = build_db_rows(period_start, period_end, items)
        logging.info(f"Подготовили строк для БД: {len(rows)}")

        upsert_rows(conn, rows)

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
