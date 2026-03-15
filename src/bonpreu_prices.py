import requests
import sqlite3
import time
import os
import csv
from datetime import datetime

# ──────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────

BASE_URL  = "https://www.compraonline.bonpreuesclat.cat/api"
DB_PATH   = "data/bonpreu_prices.db"
PAGE_SIZE = 24    # max productos por página (24 es seguro; no subir de 50)
SLEEP_REQ = 0.2   # segundos entre requests
RETRIES   = 3

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept":          "application/json",
    "Accept-Language": "ca-ES,ca;q=0.9,es;q=0.8",
    "Referer":         "https://www.compraonline.bonpreuesclat.cat/",
}

# Session global → gestiona cookies automáticamente entre requests
SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)

# Acumula cambios detectados en la ejecución actual
price_changes: list[tuple] = []


# ──────────────────────────────────────────────
# HELPERS DE RED
# ──────────────────────────────────────────────

def safe_get(url: str, params: dict | None = None) -> requests.Response | None:
    """GET con reintentos y timeout, usando la sesión compartida."""
    for attempt in range(1, RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r
            print(f"  ⚠️  HTTP {r.status_code} — {url}")
            print(f"       Body: {r.text[:300]}")
        except requests.RequestException as e:
            print(f"  ❌ Error red: {e} (intento {attempt}/{RETRIES})")
        time.sleep(2)
    return None


# ──────────────────────────────────────────────
# BASE DE DATOS
# ──────────────────────────────────────────────

def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    print("📦 Inicializando base de datos...")
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id            TEXT PRIMARY KEY,
            name          TEXT,
            brand         TEXT,
            pack_size     TEXT,
            last_price    REAL,
            unit_price    REAL,
            unit_label    TEXT,
            image_url     TEXT,
            category_path TEXT,
            last_update   TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id  TEXT,
            name        TEXT,
            old_price   REAL,
            new_price   REAL,
            change_date TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)
    conn.commit()
    conn.close()
    print("✅ Base de datos lista.\n")


# ──────────────────────────────────────────────
# PROCESADO DE UN PRODUCTO
# ──────────────────────────────────────────────

def process_product(p: dict) -> None:
    product_id = p.get("productId", "")
    if not product_id:
        return

    name      = p.get("name", "").strip() or "Sin nombre"
    brand     = p.get("brand", "").strip()
    pack_size = p.get("packSizeDescription", "")

    new_price  = float(p.get("price", {}).get("amount", 0))
    up_block   = p.get("unitPrice", {})
    unit_price = float(up_block.get("price", {}).get("amount", 0))
    unit_label = up_block.get("unit", "")

    # Imagen: prioritizar src del campo "image", fallback a lista "images"
    image_url = p.get("image", {}).get("src", "")
    if not image_url:
        imgs = p.get("images", [])
        if imgs:
            image_url = imgs[0].get("src", "")

    category_path = " > ".join(p.get("categoryPath", []))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    cur.execute("SELECT last_price FROM products WHERE id = ?", (product_id,))
    row = cur.fetchone()

    if row:
        old_price = row[0]
        if old_price != new_price:
            price_changes.append((product_id, name, old_price, new_price, now))
            direction = "📈" if new_price > old_price else "📉"
            print(f"  {direction} {name}: {old_price}€ → {new_price}€")
            cur.execute("""
                INSERT INTO price_history
                    (product_id, name, old_price, new_price, change_date)
                VALUES (?, ?, ?, ?, ?)
            """, (product_id, name, old_price, new_price, now))

        cur.execute("""
            UPDATE products
            SET name=?, brand=?, pack_size=?, last_price=?,
                unit_price=?, unit_label=?, image_url=?,
                category_path=?, last_update=?
            WHERE id=?
        """, (name, brand, pack_size, new_price,
              unit_price, unit_label, image_url,
              category_path, now, product_id))
    else:
        cur.execute("""
            INSERT INTO products
                (id, name, brand, pack_size, last_price,
                 unit_price, unit_label, image_url, category_path, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (product_id, name, brand, pack_size, new_price,
              unit_price, unit_label, image_url, category_path, now))

    conn.commit()
    conn.close()


# ──────────────────────────────────────────────
# OBTENCIÓN DE PRODUCTOS (paginado)
# ──────────────────────────────────────────────

def get_products_for_category(category_id: str, category_name: str) -> int:
    url        = f"{BASE_URL}/webproductpagews/v6/product-pages"
    total      = 0
    page       = 0
    next_token = None

    while True:
        page += 1
        params: dict = {
            "categoryId":                category_id,
            "includeAdditionalPageInfo": "false",
            "maxPageSize":               str(PAGE_SIZE),
            "maxProductsToDecorate":     str(PAGE_SIZE),
        }
        if next_token:
            params["pageToken"] = next_token

        response = safe_get(url, params=params)
        if response is None:
            print(f"  ❌ Saltando '{category_name}' tras {RETRIES} intentos fallidos")
            break

        data       = response.json()
        page_count = 0

        for group in data.get("productGroups", []):
            for product in group.get("decoratedProducts", []):
                process_product(product)
                page_count += 1
                total      += 1

        if page_count:
            print(f"    📄 Pág {page}: {page_count} productos  "
                  f"(total cat: {total})")

        next_token = data.get("metadata", {}).get("nextPageToken")
        if not next_token:
            break

        time.sleep(SLEEP_REQ)

    return total


# ──────────────────────────────────────────────
# ÁRBOL DE CATEGORÍAS
# ──────────────────────────────────────────────

def get_leaf_categories(categories: list[dict]) -> list[tuple[str, str]]:
    """Recorre el árbol y devuelve solo los nodos hoja (sin hijos)."""
    leaves = []
    for cat in categories:
        children = cat.get("childCategories", [])
        if children:
            leaves.extend(get_leaf_categories(children))
        else:
            leaves.append((cat["categoryId"], cat["name"]))
    return leaves


def get_all_categories() -> list[tuple[str, str]]:
    print("📌 Descargando árbol de categorías...")
    # Primera llamada: la sesión recibe cookies VISITORID, AWSALB, etc.
    r = safe_get(f"{BASE_URL}/webproductpagews/v1/categories")
    if r is None:
        print("❌ No se pudieron obtener las categorías.")
        return []

    data = r.json()
    categories = data if isinstance(data, list) else data.get("categories", [])
    leaves = get_leaf_categories(categories)
    print(f"  ✅ {len(leaves)} categorías hoja encontradas.\n")
    return leaves


# ──────────────────────────────────────────────
# EXPORTACIÓN CSV
# ──────────────────────────────────────────────

def export_to_csv() -> None:
    os.makedirs("data_public", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    with open("data_public/bonpreu_products.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "brand", "pack_size",
                    "last_price", "unit_price", "unit_label",
                    "image_url", "category_path", "last_update"])
        w.writerows(cur.execute("""
            SELECT id, name, brand, pack_size,
                   last_price, unit_price, unit_label,
                   image_url, category_path, last_update
            FROM products ORDER BY name COLLATE NOCASE
        """))

    with open("data_public/bonpreu_price_history.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "product_id", "name",
                    "old_price", "new_price", "change_date"])
        w.writerows(cur.execute("""
            SELECT id, product_id, name, old_price, new_price, change_date
            FROM price_history ORDER BY change_date DESC, id DESC
        """))

    conn.close()
    print("📤 CSV exportados en data_public/")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

if __name__ == "__main__":
    start = time.time()
    print("🚀 Iniciando actualización de precios BonPreu/Esclat...\n")

    try:
        init_db()

        leaf_categories = get_all_categories()
        if not leaf_categories:
            raise SystemExit("⚠️  Sin categorías. Abortando.")

        grand_total = 0
        for idx, (cat_id, cat_name) in enumerate(leaf_categories, 1):
            print(f"[{idx}/{len(leaf_categories)}] 📂 {cat_name}")
            n = get_products_for_category(cat_id, cat_name)
            grand_total += n
            print(f"  ✔️  {n} productos en esta categoría\n")
            time.sleep(SLEEP_REQ)

        print(f"📊 Total productos procesados: {grand_total}")

    finally:
        try:
            export_to_csv()
        except Exception as e:
            print(f"⚠️  No se pudo exportar CSV: {e}")

    elapsed = time.time() - start

    print("\n" + "═" * 55)
    print("📌 RESUMEN DE CAMBIOS DE PRECIO")
    print("═" * 55)
    if price_changes:
        for pid, name, old, new, ts in price_changes:
            d = "📈" if new > old else "📉"
            diff = round(new - old, 2)
            sign = "+" if diff > 0 else ""
            print(f"  {d} {name}: {old}€ → {new}€  ({sign}{diff}€)")
        print(f"\n  Total cambios detectados: {len(price_changes)}")
    else:
        print("  ✅ Sin cambios de precio en esta ejecución.")

    print(f"\n⏱️  Tiempo total: {elapsed:.1f}s")
    print("🏁 Proceso finalizado.")
