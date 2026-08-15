import re
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

# Regex para extraer el precio de "Abans 1,28€" o "Abans 1.28€"
_ABANS_RE = re.compile(r"[\d]+[,.]\d+")


# ──────────────────────────────────────────────
# TAXONOMÍA CANÓNICA
# Mapeo de keywords del category_path (catalán) a categoría unificada.
# Se evalúan en orden: el primer match gana.
# ──────────────────────────────────────────────

_CANONICAL_RULES: list[tuple[str, str]] = [
    # Lácteos — primero para evitar falsos positivos con derivados
    ("llet ",          "Lacteos"),
    ("llets ",         "Lacteos"),
    ("lactis",         "Lacteos"),
    ("làctics",        "Lacteos"),
    ("iogurt",         "Lacteos"),
    ("formatge",       "Lacteos"),
    ("mantega",        "Lacteos"),
    ("nata",           "Lacteos"),
    ("ous ",           "Lacteos"),
    # Carnes
    ("carn",           "Carnes"),
    ("aus ",           "Carnes"),
    ("embotit",        "Carnes"),
    ("pernil",         "Carnes"),
    ("xarcuteri",      "Carnes"),
    ("pollastre",      "Carnes"),
    ("gall dindi",     "Carnes"),
    ("hamburges",      "Carnes"),
    ("salsitx",        "Carnes"),
    ("fuet",           "Carnes"),
    ("llonganiss",     "Carnes"),
    ("xoriç",          "Carnes"),
    ("bacon",          "Carnes"),
    # Pescados y Mariscos
    ("peix",           "Pescados y Mariscos"),
    ("marisc",         "Pescados y Mariscos"),
    ("mariscos",       "Pescados y Mariscos"),
    ("salmó",          "Pescados y Mariscos"),
    ("tonyina",        "Pescados y Mariscos"),
    ("gamba",          "Pescados y Mariscos"),
    ("cloïss",         "Pescados y Mariscos"),
    ("musclo",         "Pescados y Mariscos"),
    ("cru ",           "Pescados y Mariscos"),
    ("fumats",         "Pescados y Mariscos"),
    ("surimi",         "Pescados y Mariscos"),
    # Frutas y Verduras
    ("fruita",         "Frutas y Verduras"),
    ("verdura",        "Frutas y Verduras"),
    ("hortalisses",    "Frutas y Verduras"),
    ("llegums frescos","Frutas y Verduras"),
    ("enciam",         "Frutas y Verduras"),
    ("tomàquet",       "Frutas y Verduras"),
    ("patata",         "Frutas y Verduras"),
    ("champinyó",      "Frutas y Verduras"),
    ("bolet",          "Frutas y Verduras"),
    ("ceba",           "Frutas y Verduras"),
    ("pebrot",         "Frutas y Verduras"),
    # Panadería
    ("pa i",           "Panaderia y Bolleria"),
    ("pa de ",         "Panaderia y Bolleria"),
    ("pastisseria",    "Panaderia y Bolleria"),
    ("bolleria",       "Panaderia y Bolleria"),
    ("galetes",        "Panaderia y Bolleria"),
    ("fleca",          "Panaderia y Bolleria"),
    ("brioixeria",     "Panaderia y Bolleria"),
    ("pa torrat",      "Panaderia y Bolleria"),
    ("biscot",         "Panaderia y Bolleria"),
    ("croissant",      "Panaderia y Bolleria"),
    ("magdalen",       "Panaderia y Bolleria"),
    # Congelados
    ("congelat",       "Congelados"),
    ("pizz",           "Congelados"),
    ("plats preparats","Congelados"),
    ("precuinat",      "Congelados"),
    # Bebidas
    ("begud",          "Bebidas"),
    ("cervesa",        "Bebidas"),
    (" vi ",           "Bebidas"),
    ("vins",           "Bebidas"),
    ("cava",           "Bebidas"),
    ("sucs",           "Bebidas"),
    ("aigü",           "Bebidas"),
    ("refrescos",      "Bebidas"),
    ("infusion",       "Bebidas"),
    ("café",           "Bebidas"),
    ("cafe",           "Bebidas"),
    ("licor",          "Bebidas"),
    ("sidra",          "Bebidas"),
    ("whisky",         "Bebidas"),
    ("ginebra",        "Bebidas"),
    (" ron ",           "Bebidas"),
    ("vodka",          "Bebidas"),
    ("vermut",         "Bebidas"),
    ("sangria",        "Bebidas"),
    # Conservas
    ("conserv",        "Conservas"),
    ("envasat",        "Conservas"),
    ("brou ",          "Conservas"),
    ("sopa ",          "Conservas"),
    ("crema de ",      "Conservas"),
    # Pasta, Arroz y Legumbres
    ("pasta",          "Pasta, Arroz y Legumbres"),
    ("arròs",          "Pasta, Arroz y Legumbres"),
    ("llegums",        "Pasta, Arroz y Legumbres"),
    # Cereales y Desayunos
    ("cereal",         "Cereales y Desayunos"),
    ("esmorzar",       "Cereales y Desayunos"),
    ("muesli",         "Cereales y Desayunos"),
    # Aceites y Condimentos
    ("oli ",           "Aceites y Condimentos"),
    ("olis ",          "Aceites y Condimentos"),
    ("vinagre",        "Aceites y Condimentos"),
    ("condiment",      "Aceites y Condimentos"),
    ("espècies",       "Aceites y Condimentos"),
    ("salses",         "Aceites y Condimentos"),
    # Snacks y Aperitivos
    ("snack",          "Snacks y Aperitivos"),
    ("aperitiu",       "Snacks y Aperitivos"),
    ("patates fregid", "Snacks y Aperitivos"),
    ("fruits secs",    "Snacks y Aperitivos"),
    ("olives",         "Snacks y Aperitivos"),
    # Dulces y Postres
    ("xocolata",       "Dulces y Postres"),
    ("dolços",         "Dulces y Postres"),
    ("postres",        "Dulces y Postres"),
    ("melmelad",       "Dulces y Postres"),
    ("mel ",           "Dulces y Postres"),
    ("racó dolç",      "Dulces y Postres"),
    ("xiclet",         "Dulces y Postres"),
    ("caramel",        "Dulces y Postres"),
    ("cacau",          "Dulces y Postres"),
    ("torró",          "Dulces y Postres"),
    ("bombó",          "Dulces y Postres"),
    ("llaminadur",     "Dulces y Postres"),
    # Higiene Personal
    ("higiene",        "Higiene Personal"),
    ("cura personal",  "Higiene Personal"),
    ("cosmètica",      "Higiene Personal"),
    ("perfumeria",     "Higiene Personal"),
    ("parafarmàcia",   "Higiene Personal"),
    ("dental",         "Higiene Personal"),
    ("dentífric",      "Higiene Personal"),
    ("desodorant",     "Higiene Personal"),
    ("xampú",          "Higiene Personal"),
    ("gel de bany",    "Higiene Personal"),
    ("crema ",         "Higiene Personal"),
    ("maquillatge",    "Higiene Personal"),
    ("protecció solar","Higiene Personal"),
    # Limpieza del Hogar
    ("neteja",         "Limpieza del Hogar"),
    ("detergent",      "Limpieza del Hogar"),
    ("llar",           "Limpieza del Hogar"),
    ("suavitzant",     "Limpieza del Hogar"),
    ("lleixiu",        "Limpieza del Hogar"),
    ("paper higiènic", "Limpieza del Hogar"),
    ("insecticid",     "Limpieza del Hogar"),
    ("ambientador",    "Limpieza del Hogar"),
    # Bebés y Niños
    ("bebès",          "Bebes y Ninos"),
    ("nens",           "Bebes y Ninos"),
    ("infantil",       "Bebes y Ninos"),
    ("nadons",         "Bebes y Ninos"),
    ("nadó",           "Bebes y Ninos"),
    ("bolquer",        "Bebes y Ninos"),
    # Mascotas
    ("mascot",         "Mascotas"),
    ("gossos",         "Mascotas"),
    ("gats",           "Mascotas"),
    ("pinso",          "Mascotas"),
]


def get_canonical_category(category_path: str) -> str:
    """
    Mapea el category_path jerárquico de BonPreu (en catalán) a una
    categoría canónica unificada compartida entre todos los supermercados.

    Ejemplo:
        "Alimentació > Lactis > Llet" → "Lacteos"
        "Begudes > Cerveses"          → "Bebidas"
    """
    if not category_path:
        return "Otros"
    path_lower = f" {category_path.lower()} "  # espacios para matching de palabras enteras
    for keyword, canonical in _CANONICAL_RULES:
        if keyword in path_lower:
            return canonical
    return "Otros"


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
            id                 TEXT PRIMARY KEY,
            name               TEXT,
            brand              TEXT,
            pack_size          TEXT,
            last_price         REAL,
            unit_price         REAL,
            unit_label         TEXT,
            image_url          TEXT,
            category_path      TEXT,
            canonical_category TEXT,
            offer_price        REAL,
            offer_label        TEXT,
            last_update        TIMESTAMP
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

    # Migración segura: añade columnas nuevas en DBs existentes
    _add_column_if_missing(cur, "products", "canonical_category", "TEXT")
    _add_column_if_missing(cur, "products", "offer_price",        "REAL")
    _add_column_if_missing(cur, "products", "offer_label",        "TEXT")

    conn.commit()
    conn.close()
    print("✅ Base de datos lista.\n")


def _add_column_if_missing(cur: sqlite3.Cursor, table: str, column: str, col_type: str) -> None:
    """ALTER TABLE solo si la columna no existe (SQLite no soporta IF NOT EXISTS)."""
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    if column not in existing:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        print(f"  🔧 Columna '{column}' añadida a '{table}'")


# ──────────────────────────────────────────────
# DETECCIÓN DE OFERTAS
# ──────────────────────────────────────────────

def get_offer_data(p: dict) -> tuple[float | None, float | None, str | None]:
    """
    Detecta si un producto está en promoción y separa precio regular de precio de oferta.

    Patrón real de la API de BonPreu (verificado):
      - Sin oferta: price.amount = precio regular; promotions = None
      - Con oferta: price.amount = precio rebajado (oferta);
                   promotions[0].description = "Abans X,XX€" (precio original)

    Retorna: (regular_price, offer_price, offer_label)
      - Si no hay oferta: (price.amount, None, None)
      - Si hay oferta:    (precio_original_parsed, price.amount, description)
    """
    current_amount = float(p.get("price", {}).get("amount", 0))
    promotions = p.get("promotions") or []

    if not promotions:
        return current_amount, None, None

    promo       = promotions[0]
    description = promo.get("description", "")   # ej. "Abans 1,28€"
    offer_label = description.strip() or "Oferta"

    # Extrae el precio original del texto "Abans X,XX€"
    # Soporta tanto coma como punto decimal
    match = _ABANS_RE.search(description)
    if match:
        regular_price = float(match.group().replace(",", "."))
        return regular_price, current_amount, offer_label

    # Si no se puede parsear el precio original, tratamos como oferta sin precio de referencia
    return current_amount, None, offer_label


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

    up_block   = p.get("unitPrice", {})
    unit_price = float(up_block.get("price", {}).get("amount", 0))
    unit_label = up_block.get("unit", "")

    # Imagen: prioritizar src del campo "image", fallback a lista "images"
    image_url = p.get("image", {}).get("src", "")
    if not image_url:
        imgs = p.get("images", [])
        if imgs:
            image_url = imgs[0].get("src", "")

    category_path      = " > ".join(p.get("categoryPath", []))
    canonical_category = get_canonical_category(category_path)

    # new_price = precio regular (sin oferta); offer_price = precio rebajado (si hay oferta)
    new_price, offer_price, offer_label = get_offer_data(p)

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
                category_path=?, canonical_category=?,
                offer_price=?, offer_label=?, last_update=?
            WHERE id=?
        """, (name, brand, pack_size, new_price,
              unit_price, unit_label, image_url,
              category_path, canonical_category,
              offer_price, offer_label, now,
              product_id))
    else:
        cur.execute("""
            INSERT INTO products
                (id, name, brand, pack_size, last_price,
                 unit_price, unit_label, image_url,
                 category_path, canonical_category,
                 offer_price, offer_label, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (product_id, name, brand, pack_size, new_price,
              unit_price, unit_label, image_url,
              category_path, canonical_category,
              offer_price, offer_label, now))

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
                    "image_url", "category_path", "canonical_category",
                    "offer_price", "offer_label", "last_update"])
        w.writerows(cur.execute("""
            SELECT id, name, brand, pack_size,
                   last_price, unit_price, unit_label,
                   image_url, category_path, canonical_category,
                   offer_price, offer_label, last_update
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
