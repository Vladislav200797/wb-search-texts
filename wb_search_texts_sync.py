import os, time, json, logging
from datetime import datetime, timedelta, timezone
import requests, psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

PG_DSN = os.environ["SUPABASE_CONN"]                # postgres://...pooler.supabase.com:6543/postgres?sslmode=require
WB_API_KEY = os.environ["WB_SA_API_KEY"]            # HeaderApiKey Seller Analytics
TOP_ORDER_BY = os.environ.get("TOP_ORDER_BY","orders")
INCLUDE_SUBSTITUTED = os.environ.get("INCLUDE_SUBSTITUTED","true").lower()=="true"
INCLUDE_SEARCH_TEXTS = os.environ.get("INCLUDE_SEARCH_TEXTS","true").lower()=="true"
LIMIT = int(os.environ.get("TEXT_LIMIT","30"))
USE_PAST_PERIOD = os.environ.get("USE_PAST_PERIOD","false").lower()=="true"
DAYS_SHIFT = int(os.environ.get("DAYS_SHIFT","1"))

# опциональные фильтры (через запятую)
WB_FILTER_BRANDS   = [s.strip() for s in os.environ.get("WB_FILTER_BRANDS","").split(",") if s.strip()]
WB_FILTER_SUBJECTS = [s.strip() for s in os.environ.get("WB_FILTER_SUBJECTS","").split(",") if s.strip()]
WB_FILTER_VCODES   = [s.strip() for s in os.environ.get("WB_FILTER_VENDOR_CODES","").split(",") if s.strip()]

WB_URL = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
HEADERS = {"Authorization": WB_API_KEY, "Content-Type": "application/json"}
MSK = timezone(timedelta(hours=3))

def msk_today(): return datetime.now(tz=MSK).date()

def get_periods():
    end = msk_today() - timedelta(days=DAYS_SHIFT)
    cur = {"start": end.isoformat(), "end": end.isoformat()}
    past = None
    if USE_PAST_PERIOD:
        p = end - timedelta(days=1)
        past = {"start": p.isoformat(), "end": p.isoformat()}
    return cur, past

def fetch_nm_ids(conn):
    sql = """
      select distinct nm_id
      from public.wb_products_catalog
      where nm_id is not null
    """
    params = []
    if WB_FILTER_BRANDS:
        sql += " and brand = any(%s)"
        params.append(WB_FILTER_BRANDS)
    if WB_FILTER_SUBJECTS:
        sql += " and subject = any(%s)"
        params.append(WB_FILTER_SUBJECTS)
    if WB_FILTER_VCODES:
        sql += " and vendor_code = any(%s)"
        params.append(WB_FILTER_VCODES)
    sql += " order by nm_id"
    with conn.cursor() as cur:
        cur.execute(sql, params if params else None)
        return [r[0] for r in cur.fetchall()]

def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def call_api(nm_ids, cur_period, past_period):
    body = {
        "currentPeriod": cur_period,
        "nmIds": nm_ids,
        "topOrderBy": TOP_ORDER_BY,
        "includeSubstitutedSKUs": INCLUDE_SUBSTITUTED,
        "includeSearchTexts": INCLUDE_SEARCH_TEXTS,
        "orderBy": {"field":"avgPosition","mode":"asc"},
        "limit": LIMIT
    }
    if past_period: body["pastPeriod"] = past_period
    r = requests.post(WB_URL, headers=HEADERS, data=json.dumps(body), timeout=60)
    if r.status_code == 429:
        logging.warning("429: ждём 25с и повтор...")
        time.sleep(25)
        r = requests.post(WB_URL, headers=HEADERS, data=json.dumps(body), timeout=60)
    r.raise_for_status()
    return (r.json() or {}).get("data", {}).get("items", [])

def upsert(conn, rows):
    if not rows: return
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
      load_dttm=excluded.load_dttm, past_start=excluded.past_start, past_end=excluded.past_end,
      rank_in_top=excluded.rank_in_top, avg_position=excluded.avg_position,
      open_card=excluded.open_card, add_to_cart=excluded.add_to_cart,
      open_to_cart=excluded.open_to_cart, orders=excluded.orders,
      cart_to_order=excluded.cart_to_order, raw=excluded.raw;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()

def main():
    cur_period, past_period = get_periods()
    logging.info(f"Период: {cur_period} | Сравнение: {past_period}")
    conn = psycopg2.connect(PG_DSN, sslmode="require")
    try:
        ids = fetch_nm_ids(conn)
        if not ids:
            logging.warning("nm_id не найдено (проверь фильтры).")
            return
        out = []
        for pack in chunks(ids, 50):          # API: ≤50 nmIds за запрос
            items = call_api(pack, cur_period, past_period)
            for rank, it in enumerate(items, 1):
                out.append((
                    datetime.now(timezone.utc),
                    datetime.fromisoformat(cur_period["start"]).date(),
                    datetime.fromisoformat(cur_period["end"]).date(),
                    datetime.fromisoformat(past_period["start"]).date() if past_period else None,
                    datetime.fromisoformat(past_period["end"]).date() if past_period else None,
                    int(it.get("nmId") or 0),
                    TOP_ORDER_BY,
                    INCLUDE_SUBSTITUTED,
                    INCLUDE_SEARCH_TEXTS,
                    rank,
                    it.get("searchText") or "",
                    it.get("avgPosition"),
                    it.get("openCard"),
                    it.get("addToCart"),
                    it.get("conversionToCart") or it.get("openToCart"),
                    it.get("orders"),
                    it.get("conversionToOrder") or it.get("cartToOrder"),
                    json.dumps(it, ensure_ascii=False),
                ))
            logging.info("Пауза 21с для лимита…"); time.sleep(21)
        upsert(conn, out)
        logging.info(f"Готово: {len(out)} строк.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
