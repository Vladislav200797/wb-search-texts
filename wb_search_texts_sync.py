import os
import time
import json
import random
import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values, Json

WB_BASE = "https://seller-analytics-api.wildberries.ru"
EP_SEARCH_TEXTS = f"{WB_BASE}/api/v2/search-report/product/search-texts"

MSK = ZoneInfo("Europe/Moscow")
UTC = ZoneInfo("UTC")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()


def env_int(name: str, default: int) -> int:
    v = env_str(name, "")
    return default if v == "" else int(v)


def msk_today() -> date:
    return datetime.now(MSK).date()


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def wb_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def get_current(v: Any) -> Any:
    """WB часто возвращает метрики как объект {current, percentile}. Берём current."""
    if isinstance(v, dict):
        if "current" in v:
            return v.get("current")
        if "value" in v:
            return v.get("value")
    return v


def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


# ---------- Postgres ----------

def pg_connect() -> psycopg2.extensions.connection:
    conninfo = env_str("SUPABASE_CONNINFO", "")
    if not conninfo:
        raise RuntimeError("SUPABASE_CONNINFO пустой")
    logging.info("Using SUPABASE_CONNINFO")
    return psycopg2.connect(conninfo)


def fetch_nm_ids_from_db(conn) -> List[int]:
    sql = "select distinct nm_id from public.wb_products_catalog where nm_id is not null"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    nm_ids = sorted({int(r[0]) for r in rows if r and r[0] is not None})
    if not nm_ids:
        raise RuntimeError("Не нашли nm_id в public.wb_products_catalog")
    logging.info(f"Нашли nm_id из public.wb_products_catalog: {len(nm_ids)}")
    return nm_ids


def upsert_rows(conn, rows: List[Tuple[Any, ...]]) -> None:
    if not rows:
        logging.info("Нет строк для вставки.")
        return

    sql = """
    insert into public.wb_search_texts (
      load_dttm,
      period_start,
      period_end,
      top_order_by,
      nm_id,
      search_text,
      avg_position,
      open_card,
      add_to_cart,
      orders,
      open_to_cart,
      cart_to_order,
      open_to_order,
      raw_item
    ) values %s
    on conflict (period_start, period_end, top_order_by, nm_id, search_text)
    do update set
      load_dttm     = excluded.load_dttm,
      avg_position  = excluded.avg_position,
      open_card     = excluded.open_card,
      add_to_cart   = excluded.add_to_cart,
      orders        = excluded.orders,
      open_to_cart  = excluded.open_to_cart,
      cart_to_order = excluded.cart_to_order,
      open_to_order = excluded.open_to_order,
      raw_item      = excluded.raw_item
    ;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    logging.info(f"Upsert OK: {len(rows)} строк")


# ---------- WB API ----------

def wb_post_json(
    session: requests.Session,
    url: str,
    api_key: str,
    body: Dict[str, Any],
    max_retries_429: int = 6
) -> Dict[str, Any]:
    headers = wb_headers(api_key)

    for attempt in range(1, max_retries_429 + 1):
        resp = session.post(url, headers=headers, data=json.dumps(body), timeout=60)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            sleep_s = min(120, 20 * attempt) + random.uniform(0, 3)
            logging.warning(f"WB 429. Sleep {sleep_s:.1f}s (attempt {attempt}/{max_retries_429})")
            time.sleep(sleep_s)
            continue

        text = resp.text or ""
        logging.error(f"WB ERROR {resp.status_code} on {url}. Response: {text[:1200]}")
        raise RuntimeError(f"WB request failed: {resp.status_code}")

    raise RuntimeError("WB 429: exceeded retries")


def fetch_search_texts(
    session: requests.Session,
    api_key: str,
    period_start: date,
    period_end: date,
    nm_ids: List[int],
    limit: int,
    top_order_by: str
) -> List[Dict[str, Any]]:
    body = {
        "currentPeriod": {"start": period_start.isoformat(), "end": period_end.isoformat()},
        "nmIds": nm_ids,
        "topOrderBy": top_order_by,
        "includeSubstitutedSKUs": True,
        "includeSearchTexts": True,
        "orderBy": {"field": "avgPosition", "mode": "asc"},
        "limit": limit
    }

    logging.info(f"WB request: nmIds={len(nm_ids)} limit={limit} topOrderBy={top_order_by} period={period_start}..{period_end}")
    js = wb_post_json(session, EP_SEARCH_TEXTS, api_key, body)

    data = js.get("data") or {}
    items = data.get("items") or []
    return [it for it in items if isinstance(it, dict)]


def build_db_rows(
    period_start: date,
    period_end: date,
    top_order_by: str,
    items: List[Dict[str, Any]]
) -> List[Tuple[Any, ...]]:
    load_dttm = datetime.now(UTC)
    rows: List[Tuple[Any, ...]] = []

    for it in items:
        nm_id = it.get("nmId") or it.get("nmID")
        text = (it.get("text") or "").strip()
        if not nm_id or not text:
            continue

        # ВАЖНО: берём current из dict
        avg_position = safe_float(get_current(it.get("avgPosition")))
        open_card = safe_int(get_current(it.get("openCard")))
        add_to_cart = safe_int(get_current(it.get("addToCart")))
        orders = safe_int(get_current(it.get("orders")))

        # WB часто отдаёт конверсии как проценты (0..100)
        open_to_cart = safe_float(get_current(it.get("openToCart")))
        cart_to_order = safe_float(get_current(it.get("cartToOrder")))
        open_to_order = safe_float(get_current(it.get("openToOrder")))

        # если WB не дал — считаем сами (тоже в процентах)
        if open_to_cart is None and open_card and open_card > 0 and add_to_cart is not None:
            open_to_cart = (add_to_cart / open_card) * 100.0
        if cart_to_order is None and add_to_cart and add_to_cart > 0 and orders is not None:
            cart_to_order = (orders / add_to_cart) * 100.0
        if open_to_order is None and open_card and open_card > 0 and orders is not None:
            open_to_order = (orders / open_card) * 100.0

        rows.append((
            load_dttm,
            period_start,
            period_end,
            top_order_by,
            int(nm_id),
            text,
            avg_position,
            open_card,
            add_to_cart,
            orders,
            open_to_cart,
            cart_to_order,
            open_to_order,
            Json(it, dumps=lambda x: json.dumps(x, ensure_ascii=False))
        ))

    return rows


def main():
    wb_api_key = env_str("WB_API_KEY")
    if not wb_api_key:
        raise RuntimeError("WB_API_KEY пустой")

    # Период: по умолчанию окно N дней, заканчиваем вчера (по МСК)
    period_days = env_int("PERIOD_DAYS", 7)          # <-- сделай 1, если хочешь строго “вчера”
    end_offset = env_int("PERIOD_END_OFFSET", 1)     # 1 = вчера
    end_day = msk_today() - timedelta(days=end_offset)
    start_day = end_day - timedelta(days=period_days - 1)

    # Можно руками задать конкретный период
    ps = env_str("PERIOD_START", "")
    pe = env_str("PERIOD_END", "")
    if ps and pe:
        start_day = parse_date(ps)
        end_day = parse_date(pe)

    limit = env_int("LIMIT", 30)
    top_order_bys = [x.strip() for x in env_str("TOP_ORDER_BYS", "orders,openCard,addToCart").split(",") if x.strip()]
    pause_sec = env_int("WB_PAUSE_SEC", 21)

    conn = pg_connect()
    try:
        nm_ids = fetch_nm_ids_from_db(conn)
        logging.info(f"Период: {start_day}..{end_day} | limit={limit} | top_order_bys={top_order_bys}")

        with requests.Session() as s:
            for i, top in enumerate(top_order_bys, start=1):
                items = fetch_search_texts(s, wb_api_key, start_day, end_day, nm_ids, limit, top)
                logging.info(f"WB items ({top}): {len(items)}")
                rows = build_db_rows(start_day, end_day, top, items)
                logging.info(f"Rows for DB ({top}): {len(rows)}")
                upsert_rows(conn, rows)

                if i < len(top_order_bys):
                    logging.info(f"Пауза {pause_sec} сек (лимиты WB)…")
                    time.sleep(pause_sec)

        logging.info("DONE")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
