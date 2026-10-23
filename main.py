import os
import re
import time
import io
import traceback

import httpx
import pandas as pd
import pymysql
from fastapi import FastAPI, HTTPException

app = FastAPI()

# --- Config (from environment variables — set these in Railway) ---
CLIENT_ID = os.environ["MS_CLIENT_ID"]
CLIENT_SECRET = os.environ["MS_CLIENT_SECRET"]
TENANT_ID = os.environ["MS_TENANT_ID"]
SITE_ID = os.environ["MS_SITE_ID"]

TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# --- MySQL connection ---
DB_HOST = os.environ.get("DB_HOST", "srv1092.hstgr.io")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USERNAME = os.environ.get("DB_USERNAME", "u525350507_root")
DB_DATABASE = os.environ.get("DB_DATABASE", "u525350507_bali_dwipa")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Ramikha81__")

FOLDER_PATH = os.environ.get(
    "STT_FOLDER_PATH", "EDP/SOURCE/CLEAN/STT WEEK/REGION/BALI"
)
FILENAME_PATTERN = re.compile(r"STT_(\d{4})_W(\d{1,2})", re.IGNORECASE)

# Excel header -> DB column
COLUMN_MAP = {
    "COMPANY": "company",
    "STOCKPOINT": "stock_point",
    "DISTRIK": "distrik",
    "RAYON": "rayon",
    "KODE SALESMAN": "kode_salesman",
    "KODE CUSTOMER": "kode_customer",
    "CUSTOMER": "customer",
    "LONGITUDE": "longitude",
    "LATITUDE": "latitude",
    "ADDRES": "addres",
    "GPS PROVINSI": "gps_provinsi",
    "GPS KABUPATEN": "gps_kabupaten",
    "GPS KECAMATAN": "gps_kecamatan",
    "GPS KELURAHAN": "gps_kelurahan",
    "PROVINSI": "provinsi",
    "KODE POS": "kode_pos",
    "KODE MATERIAL": "kode_material",
    "DESC": "desc",
    "GROSS SALES QTY IN PCS": "gross_sales_qty_pcs",
    "NET SALES QTY IN PCS": "net_sales_qty_pcs",
    "KODE MATERIAL SAP": "kode_material_sap",
    "QUARTAL": "quartal",
    "PRINCIPLE": "principle",
    "SKU ADJ": "sku_adj",
    "FKALI": "fkali",
    "BRAND": "brand",
    "GROSS SALES QTY IN CTN": "gross_sales_qty_ctn",
    "NET SALES QTY IN CTN": "net_sales_qty_ctn",
}

NUMERIC_COLUMNS = [
    "longitude", "latitude", "fkali", "quartal",
    "gross_sales_qty_pcs", "net_sales_qty_pcs",
    "gross_sales_qty_ctn", "net_sales_qty_ctn",
]

_token_cache = {"access_token": None, "expires_at": 0}


@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/version-check")
async def version_check():
    return {"marker": "sanitized-v2"}


async def get_access_token() -> str:
    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope": "https://graph.microsoft.com/.default",
            },
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Token fetch failed: {resp.text}")

    data = resp.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = time.time() + data["expires_in"]
    return data["access_token"]


async def list_stt_files() -> list[dict]:
    token = await get_access_token()
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{GRAPH_BASE}/sites/{SITE_ID}/drive/root:/{FOLDER_PATH}:/children",
            headers={"Authorization": f"Bearer {token}"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    items = resp.json().get("value", [])
    matched = []
    for item in items:
        m = FILENAME_PATTERN.search(item["name"])
        if m:
            matched.append({
                "item_id": item["id"],
                "name": item["name"],
                "report_year": int(m.group(1)),
                "report_week": int(m.group(2)),
            })
    return matched


async def fetch_and_parse(item_id: str) -> pd.DataFrame:
    token = await get_access_token()
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(
            f"{GRAPH_BASE}/sites/{SITE_ID}/drive/items/{item_id}/content",
            headers={"Authorization": f"Bearer {token}"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    df = pd.read_excel(io.BytesIO(resp.content))
    df = df.rename(columns=COLUMN_MAP)
    return df


def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        charset="utf8mb4",
    )


def upsert_rows(conn, df: pd.DataFrame, report_year: int, report_week: int):
    df = df.copy()
    df["report_year"] = report_year
    df["report_week"] = report_week

    db_columns = list(COLUMN_MAP.values()) + ["report_year", "report_week"]
    df = df[[c for c in db_columns if c in df.columns]]

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.astype(object).where(pd.notnull(df), None)

    cols = df.columns.tolist()
    col_list = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_clause = ", ".join(
        f"`{c}` = VALUES(`{c}`)"
        for c in cols
        if c not in ("report_year", "report_week", "kode_customer", "kode_material")
    )

    sql = f"""
        INSERT INTO stt_report_rows ({col_list})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """

    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    with conn.cursor() as cursor:
        cursor.executemany(sql, rows)
    conn.commit()

    return len(rows)


@app.post("/sync/stt-reports")
async def sync_stt_reports(limit: int = None):
    """Sync all STT files found in the SharePoint folder (or the first `limit` of them)."""
    files = await list_stt_files()

    if limit is not None:
        files = files[:limit]

    conn = get_db_connection()
    results = []
    try:
        for f in files:
            df = await fetch_and_parse(f["item_id"])
            inserted = upsert_rows(conn, df, f["report_year"], f["report_week"])
            results.append({
                "file": f["name"],
                "report_year": f["report_year"],
                "report_week": f["report_week"],
                "rows": inserted,
            })
        return {"files_found": len(files), "results": results}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"message": "Sync failed", "error": str(e), "trace": traceback.format_exc()},
        )
    finally:
        conn.close()


@app.get("/sharepoint/files")
async def sharepoint_files(folder_path: str):
    token = await get_access_token()
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{GRAPH_BASE}/sites/{SITE_ID}/drive/root:/{folder_path}:/children",
            headers={"Authorization": f"Bearer {token}"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@app.get("/sharepoint/excel")
async def sharepoint_excel(item_id: str):
    token = await get_access_token()
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(
            f"{GRAPH_BASE}/sites/{SITE_ID}/drive/items/{item_id}/content",
            headers={"Authorization": f"Bearer {token}"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    df = pd.read_excel(io.BytesIO(resp.content), sheet_name="Stock")
    records = df.to_dict(orient="records")

    return [
        {k: (None if pd.isna(v) else v) for k, v in record.items()}
        for record in records
    ]


@app.get("/sharepoint/excel-debug")
async def sharepoint_excel_debug(item_id: str):
    token = await get_access_token()
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(
            f"{GRAPH_BASE}/sites/{SITE_ID}/drive/items/{item_id}/content",
            headers={"Authorization": f"Bearer {token}"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    content = io.BytesIO(resp.content)
    xls = pd.ExcelFile(content)

    df_raw = pd.read_excel(xls, sheet_name="Stock", header=None, nrows=10)
    df_raw = df_raw.where(pd.notnull(df_raw), None)

    return {
        "sheet_names": xls.sheet_names,
        "first_10_rows": df_raw.values.tolist(),
    }


@app.post("/sync/stt-reports/week")
async def sync_stt_specific_week(year: int, week: int):
    if week < 1 or week > 53:
        raise HTTPException(status_code=400, detail="Week must be between 1 and 53")

    files = await list_stt_files()

    target_file = next(
        (f for f in files if f["report_year"] == year and f["report_week"] == week),
        None,
    )

    if not target_file:
        raise HTTPException(
            status_code=404,
            detail=f"STT file for {year} week {week} was not found",
        )

    conn = get_db_connection()
    try:
        df = await fetch_and_parse(target_file["item_id"])
        inserted = upsert_rows(conn, df, target_file["report_year"], target_file["report_week"])

        return {
            "status": "ok",
            "message": f"STT {year} W{week} successfully synced",
            "file": target_file["name"],
            "report_year": year,
            "report_week": week,
            "rows": inserted,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"message": f"Failed to sync STT {year} W{week}", "error": str(e)},
        )
    finally:
        conn.close()
