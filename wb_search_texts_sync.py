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
from psycopg2.extras import execute_values


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


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


# ---------- Postgres ----------

def pg_connect() -> psycopg2.extensions.connection:
    conninfo = env_str("SUPABASE_CONNINFO", "")
    if not conninfo:
        raise RuntimeError("SUPABASE_CONNINFO пустой (нужно добавить в GitHub Secrets)")
    logging.info("Using SUPABASE_CONNINFO")
    return psycopg2.connect(conninfo)


def fetch_nm_ids_from_db(conn) -> List[int]:
    """
    У тебя в логах видно, что это работает из public.wb_products_catalog.
    Оставляем только его, чтобы не было сюрпризов.
    """
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

    # ВАЖНО: конфликт по (period_start, period_end, nm_id, search_text)
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
    on conflict (period_start, period_end, nm_id, search_text)
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


# ---------- WB API ----------

def wb_post_json(
    session: requests.Session,
    url: str,
    api_key: str,
    body: Dict[str, Any],
    max_retries_429: int = 6
) -> Dict[str, Any]:
    """
    - Логирует любые ошибки WB (код + текст ответа)
    - Ретраит 429 (лимит) с backoff
    """
    headers = wb_headers(api_key)

    for attempt in range(1, max_retries_429 + 1):
        resp = session.post(url, headers=headers, data=json.dumps(body), timeout=60)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            sleep_s = min(120, 20 * attempt) + random.uniform(0, 3)
            logging.warning(f"WB 429 Too Many Requests. Sleep {sleep_s:.1f}s (attempt {attempt}/{max_retries_429})")
            time.sleep(sleep_s)
            continue

        # Любая другая ошибка — показываем тело
        text = resp.text or ""
        text_short = text[:1200]  # чтобы лог не раздувать
        logging.error(f"WB ERROR {resp.status_code} on {url}. Response (first 1200 chars): {text_short}")
        raise RuntimeError(f"WB request failed: {resp.status_code}")

    raise RuntimeError("WB 429: exceeded retries")


def fetch_search_texts_for_nmids(
    session: requests.Session,
    api_key: str,
    period_start: date,
    period_end: date,
    nm_ids: List[int],
    limit: int,
    top_order_by: str
) -> List[Dict[str, Any]]:
    """
    Запросим в 1 батч (как у тебя уже было).
    На Jam иногда лимит по limit может быть 30. Поэтому:
    - пробуем limit как задано
    - если WB упал (400) — попробуем автоматически limit=30 один раз
    """
    nm_batch = nm_ids  # у тебя 45 — нормально

    def make_body(lim: int) -> Dict[str, Any]:
        return {
            "currentPeriod": {"start": period_start.isoformat(), "end": period_end.isoformat()},
            "nmIds": nm_batch,
            "topOrderBy": top_order_by,
            "includeSubstitutedSKUs": True,
            "includeSearchTexts": True,
            "orderBy": {"field": "avgPosition", "mode": "asc"},
            "limit": lim
        }

    logging.info(f"Запрашиваем пачку nmIds: {len(nm_batch)} (limit={limit})")

    try:
        js = wb_post_json(session, EP_SEARCH_TEXTS, api_key, make_body(limit))
    except RuntimeError:
        # fallback: часто бывает, что limit=100 недоступен, а limit=30 ок
        if limit != 30:
            logging.warning("Пробуем fallback limit=30 (если у WB ограничение на тарифе)")
            js = wb_post_json(session, EP_SEARCH_TEXTS, api_key, make_body(30))
        else:
            raise

    data = js.get("data") or {}
    items = data.get("items") or []
    clean_items = [it for it in items if isinstance(it, dict)]
    return clean_items


def build_db_rows(period_start: date, period_end: date, items: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    load_dttm = datetime.now(UTC)

    rows: List[Tuple[Any, ...]] = []
    for it in items:
        nm_id = it.get("nmId") or it.get("nmID")
        text = (it.get("text") or "").strip()
        if not nm_id or not text:
            continue

        avg_position = safe_float(it.get("avgPosition"))
        open_card = safe_int(it.get("openCard"))
        add_to_cart = safe_int(it.get("addToCart"))
        orders = safe_int(it.get("orders"))

        # конверсии: если WB не дал — считаем
        open_to_cart = safe_float(it.get("openToCart"))
        cart_to_order = safe_float(it.get("cartToOrder"))
        open_to_order = safe_float(it.get("openToOrder"))

        if open_to_cart is None and open_card and open_card > 0 and add_to_cart is not None:
            open_to_cart = add_to_cart / open_card
        if cart_to_order is None and add_to_cart and add_to_cart > 0 and orders is not None:
            cart_to_order = orders / add_to_cart
        if open_to_order is None and open_card and open_card > 0 and orders is not None:
            open_to_order = orders / open_card

        rows.append((
            load_dttm,
            period_start,
            period_end,
            int(nm_id),
            text,
            avg_position,
            open_card,
            add_to_cart,
            orders,
            open_to_cart,
            cart_to_order,
            open_to_order
        ))

    return rows


def main():
    wb_api_key = env_str("WB_API_KEY")
    if not wb_api_key:
        raise RuntimeError("WB_API_KEY пустой")

    # период: по умолчанию вчера (по МСК)
    ps = env_str("PERIOD_START", "")
    pe = env_str("PERIOD_END", "")

    if ps and pe:
        period_start = parse_date(ps)
        period_end = parse_date(pe)
    else:
        y = msk_today() - timedelta(days=1)
        period_start = y
        period_end = y

    # настройки WB
    # Ставим дефолт 30 (самый безопасный), а если хочешь — в workflow поставишь LIMIT=100
    limit = env_int("LIMIT", 30)
    top_order_by = env_str("TOP_ORDER_BY", "orders")

    conn = pg_connect()
    try:
        nm_ids = fetch_nm_ids_from_db(conn)

        logging.info(f"Период: start={period_start.isoformat()} end={period_end.isoformat()} | TOP={top_order_by} | limit={limit}")
        logging.info(f"Всего nm_id: {len(nm_ids)}")
        logging.info(f"Первые 20 nm_id: {nm_ids[:20]}")

        with requests.Session() as s:
            items = fetch_search_texts_for_nmids(
                s, wb_api_key, period_start, period_end, nm_ids, limit, top_order_by
            )

        logging.info(f"WB вернул items: {len(items)}")
        rows = build_db_rows(period_start, period_end, items)
        logging.info(f"Подготовили строк для БД: {len(rows)}")

        upsert_rows(conn, rows)

        logging.info("DONE")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
