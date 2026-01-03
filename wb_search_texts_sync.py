import os
import time
import json
import random
import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Tuple

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


# ---------- Postgres ----------

def pg_connect() -> psycopg2.extensions.connection:
    conninfo = env_str("SUPABASE_CONNINFO", "")
    if not conninfo:
        raise RuntimeError("SUPABASE_CONNINFO пустой (добавь в GitHub Secrets)")
    return psycopg2.connect(conninfo)


def fetch_nm_ids(conn) -> List[int]:
    sql = "select distinct nm_id from public.wb_products_catalog where nm_id is not null"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    nm_ids = sorted({int(r[0]) for r in rows if r and r[0] is not None})
    logging.info(f"Нашли nm_id из public.wb_products_catalog: {len(nm_ids)}")
    return nm_ids


def upsert_raw(conn, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        logging.info("Нет строк для вставки.")
        return 0

    sql = """
    insert into public.wb_search_texts_raw (
      load_dttm,
      period_start, period_end, top_order_by,
      nm_id, search_text,
      raw_item
    ) values %s
    on conflict (period_start, period_end, top_order_by, nm_id, search_text)
    do update set
      load_dttm = excluded.load_dttm,
      raw_item  = excluded.raw_item
    ;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    logging.info(f"Upsert OK: {len(rows)} строк")
    return len(rows)


def delete_old(conn, retention_days: int) -> int:
    # Храним последние retention_days дней по period_end (по МСК)
    cutoff = msk_today() - timedelta(days=retention_days)
    sql = "delete from public.wb_search_texts_raw where period_end < %s;"
    with conn.cursor() as cur:
        cur.execute(sql, (cutoff,))
        deleted = cur.rowcount
    conn.commit()
    logging.info(f"Retention: удалили {deleted} строк (period_end < {cutoff})")
    return deleted


# ---------- WB API ----------

def wb_post_json(
    session: requests.Session,
    api_key: str,
    body: Dict[str, Any],
    max_retries_429: int = 6
) -> Dict[str, Any]:
    for attempt in range(1, max_retries_429 + 1):
        resp = session.post(
            EP_SEARCH_TEXTS,
            headers=wb_headers(api_key),
            data=json.dumps(body, ensure_ascii=False),
            timeout=60
        )

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            sleep_s = min(120, 20 * attempt) + random.uniform(0, 3)
            logging.warning(f"WB 429. Sleep {sleep_s:.1f}s (attempt {attempt}/{max_retries_429})")
            time.sleep(sleep_s)
            continue

        txt = resp.text or ""
        raise RuntimeError(f"WB error {resp.status_code}: {txt[:800]}")

    raise RuntimeError("WB 429: exceeded retries")


def split_batches(lst: List[int], batch_size: int) -> List[List[int]]:
    return [lst[i:i + batch_size] for i in range(0, len(lst), batch_size)]


def main():
    wb_api_key = env_str("WB_API_KEY")
    if not wb_api_key:
        raise RuntimeError("WB_API_KEY пустой (добавь в GitHub Secrets)")

    # ---- период: по умолчанию вчера (1 день)
    period_days = env_int("PERIOD_DAYS", 1)
    end_offset = env_int("PERIOD_END_OFFSET", 1)  # 1 = вчера
    period_end = msk_today() - timedelta(days=end_offset)
    period_start = period_end - timedelta(days=period_days - 1)

    # можно руками задать конкретные даты
    ps = env_str("PERIOD_START", "")
    pe = env_str("PERIOD_END", "")
    if ps and pe:
        period_start = parse_date(ps)
        period_end = parse_date(pe)

    limit_rows = env_int("LIMIT", 30)
    top_order_bys = [x.strip() for x in env_str(
        "TOP_ORDER_BYS",
        "openCard,addToCart,openToCart,orders,cartToOrder"
    ).split(",") if x.strip()]

    nmid_batch_size = env_int("NMID_BATCH_SIZE", 10)
    pause_sec = env_int("WB_PAUSE_SEC", 21)
    retention_days = env_int("RETENTION_DAYS", 92)

    conn = pg_connect()
    try:
        nm_ids = fetch_nm_ids(conn)
        if not nm_ids:
            raise RuntimeError("nm_id список пустой")

        batches = split_batches(nm_ids, nmid_batch_size)

        logging.info(f"Период: {period_start}..{period_end}")
        logging.info(f"top_order_bys={top_order_bys} limit={limit_rows} nm_ids={len(nm_ids)} batches={len(batches)}")

        total_rows = 0

        with requests.Session() as s:
            for t_i, top in enumerate(top_order_bys, start=1):
                for b_i, nm_batch in enumerate(batches, start=1):
                    body = {
                        "currentPeriod": {"start": period_start.isoformat(), "end": period_end.isoformat()},
                        "nmIds": nm_batch,
                        "topOrderBy": top,
                        "includeSubstitutedSKUs": True,
                        "includeSearchTexts": True,
                        "orderBy": {"field": "avgPosition", "mode": "asc"},
                        "limit": limit_rows
                    }

                    logging.info(f"WB call: top={top} batch={b_i}/{len(batches)} nmIds={len(nm_batch)}")
                    js = wb_post_json(s, wb_api_key, body)

                    items = ((js.get("data") or {}).get("items") or [])
                    items = [it for it in items if isinstance(it, dict)]
                    logging.info(f"WB items: {len(items)}")

                    load_dttm = datetime.now(UTC)
                    rows: List[Tuple[Any, ...]] = []

                    for it in items:
                        nm_id = it.get("nmId") or it.get("nmID")
                        text = (it.get("text") or "").strip()
                        if not nm_id or not text:
                            continue

                        rows.append((
                            load_dttm,
                            period_start, period_end, top,
                            int(nm_id), text,
                            Json(it, dumps=lambda x: json.dumps(x, ensure_ascii=False))
                        ))

                    total_rows += upsert_raw(conn, rows)

                    if (b_i < len(batches)) or (t_i < len(top_order_bys)):
                        time.sleep(pause_sec + random.uniform(0, 2))

        # чистка истории
        delete_old(conn, retention_days)

        logging.info(f"DONE. Insert/Upsert rows this run: {total_rows}")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
