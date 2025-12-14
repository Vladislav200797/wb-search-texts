import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone
import requests
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# === ENV ===
PG_DSN = os.environ["SUPABASE_CONN"]                # postgresql://postgres:<PWD>@...pooler.supabase.com:6543/postgres?sslmode=require
WB_API_KEY = os.environ["WB_SA_API_KEY"]            # HeaderApiKey (Seller Analytics)

TOP_ORDER_BY = os.environ.get("TOP_ORDER_BY", "orders")             # openCard|addToCart|openToCart|orders|cartToOrder
INCLUDE_SUBSTITUTED = os.environ.get("INCLUDE_SUBSTITUTED", "true").lower() == "true"
INCLUDE_SEARCH_TEXTS = os.environ.get("INCLUDE_SEARCH_TEXTS", "true").lower() == "true"
LIMIT = int(os.environ.get("TEXT_LIMIT", "30"))                     # 30 (обычный) или 100 (Продвинутый)
USE_PAST_PERIOD = os.environ.get("USE_PAST_PERIOD", "false").lower() == "true"
DAYS_SHIFT = int(os.environ.get("DAYS_SHIFT", "1"))                 # 1 = вчера по МСК

# Фильтры по каталогу (опционально; списки через запятую)
WB_FILTER_BRANDS       = [s.strip() for s in os.environ.get("WB_FILTER_BRANDS", "").split(",") if s.strip()]
WB_FILTER_SUBJECTS     = [s.strip() for s in os.environ.get("WB_FILTER_SUBJECTS", "").split(",") if s.strip()]
WB_FILTER_VENDOR_CODES = [s.strip() for s in os.environ.get("WB_FILTER_VENDOR_CODES", "").split(",") if s.strip()]

WB_URL = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
HEADERS = {"Authorization": WB_API_KEY, "Content-Type": "application/json"}

# Московское время (UTC+3 круглый год)
MSK = timezone(timedelta(hours=3))

def msk_today():
    return datetime.now(tz=MSK).date()

def get_periods():
    # Берём вчера по МСК (или другой сдвиг), чтобы день был закрыт целиком
    end = msk_today() - timedelta(days=DAYS_SHIFT)
    current = {"start": end.isoformat(), "end": end.isoformat()}
    past = None
    if USE_PAST_PERIOD:
        p = end - timedelta(days=1)
        past = {"start": p.isoformat(), "end": p.isoformat()}
    return current, past

def fetch_nm_ids(conn):
    """
    Достаём уникальные nm_id из wb_products_catalog.
    Фильтры применяем «по подстроке»: lower(trim(...)) ILIKE ANY(%patterns%).
    """
    base_sql = """
        select distinct c.nm_id
        from public.wb_products_catalog c
        where c.nm_id is not null
    """
    params = []

    def to_like_patterns(items):
        return [f"%{s.strip().lower()}%" for s in items if s.strip()]

    brands   = to_like_patterns(WB_FILTER_BRANDS)
    subjects = to_like_patterns(WB_FILTER_SUBJECTS)
    vcodes   = to_like_patterns(WB_FILTER_VENDOR_CODES)

    if brands:
        base_sql += " and lower(trim(c.brand)) ilike any(%s)"
        params.append(brands)
    if subjects:
        base_sql += " and lower(trim(c.subject)) ilike any(%s)"
        params.append(subjects)
    if vcodes:
        base_sql += " and lower(trim(c.vendor_code)) ilike any(%s)"
        params.append(vcodes)

    base_sql += " order by c.nm_id"

    with conn.cursor() as cur:
        cur.execute(base_sql, params if params else None)
        rows = cur.fetchall()
        ids = [r[0] for r in rows]
        logging.info(f"Нашли уникальных nm_id: {len(ids)}")
        logging.info(f"Первые 20 nm_id: {ids[:20]}")
        return ids

def chunk(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def call_api(nm_ids, current_period, past_period):
    body = {
        "currentPeriod": current_period,
        "nmIds": nm_ids,
        "topOrderBy": TOP_ORDER_BY,
        "includeSubstitutedSKUs": INCLUDE_SUBSTITUTED,
        "includeSearchTexts": INCLUDE_SEARCH_TEXTS,
        "orderBy": {"field": "avgPosition", "mode": "asc"},
        "limit": LIMIT
    }
    if past_period:
        body["pastPeriod"] = past_period

    resp = requests.post(WB_URL, headers=HEADERS, data=json.dumps(body), timeout=60)
    if resp.status_code == 429:
        logging.warning("429 Too Many Requests — пауза 25 секунд и повтор...")
        time.sleep(25)
        resp = requests.post(WB_URL, headers=HEADERS, data=json.dumps(body), timeout=60)
    resp.raise_for_status()

    payload = resp.json()
    return payload.get("data", {}).get("items", [])

def upsert_rows(conn, rows):
    if not rows:
        return
    sql = """
    insert into public.wb_search_texts (
      load_dttm, period_start, period_end, past_start, past_end,
      nm_id, top_order_by, include_substituted, include_search_texts,
      rank_in_top, search_text, avg_position, open_card, add_to_cart,
      open_to_cart, orders, cart_to_order, raw
    )
    values %s
    on conflict (period_start, period_end, nm_id, top_order_by, search_text, include_substituted, include_search_texts)
    do update set
      load_dttm = excluded.load_dttm,
      past_start = excluded.past_start,
      past_end = excluded.past_end,
      rank_in_top = excluded.rank_in_top,
      avg_position = excluded.avg_position,
      open_card = excluded.open_card,
      add_to_cart = excluded.add_to_cart,
      open_to_cart = excluded.open_to_cart,
      orders = excluded.orders,
      cart_to_order = excluded.cart_to_order,
      raw = excluded.raw;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()

def main():
    current_period, past_period = get_periods()
    logging.info(f"Период: {current_period} | Сравнение: {past_period}")

    conn = psycopg2.connect(PG_DSN, sslmode="require")
    try:
        nm_ids = fetch_nm_ids(conn)
        if not nm_ids:
            logging.warning("nm_id не найдено (проверь фильтры WB_FILTER_* или заполни их пустыми).")
            return

        all_rows = []
        for pack in chunk(nm_ids, 50):  # API: не более 50 nmIds за запрос
            logging.info(f"Запрашиваем пачку nmIds: {len(pack)}")
            items = call_api(pack, current_period, past_period)

            for i, it in enumerate(items, start=1):
                row = (
                    datetime.now(timezone.utc),
                    datetime.fromisoformat(current_period["start"]).date(),
                    datetime.fromisoformat(current_period["end"]).date(),
                    datetime.fromisoformat(past_period["start"]).date() if past_period else None,
                    datetime.fromisoformat(past_period["end"]).date() if past_period else None,
                    int(it.get("nmId") or it.get("nm_id") or 0),
                    TOP_ORDER_BY,
                    INCLUDE_SUBSTITUTED,
                    INCLUDE_SEARCH_TEXTS,
                    i,
                    it.get("searchText") or "",
                    it.get("avgPosition"),
                    it.get("openCard"),
                    it.get("addToCart"),
                    it.get("conversionToCart") or it.get("openToCart"),
                    it.get("orders"),
                    it.get("conversionToOrder") or it.get("cartToOrder"),
                    json.dumps(it, ensure_ascii=False),
                )
                all_rows.append(row)

            # Лимиты WB: 3 запроса/мин → выдерживаем паузу
            logging.info("Пауза 21 сек (лимиты WB)…")
            time.sleep(21)

        upsert_rows(conn, all_rows)
        logging.info(f"Готово. Записано/обновлено строк: {len(all_rows)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
