"""
Скачивание снимков Sentinel-2 L2A с Copernicus Data Space Ecosystem (CDSE)
Регион: Краснодарский край
Каналы: B01, B02, B03, B04, B05, B06, B07, B08, B8A, B09, B11, B12
"""

import os
import sys
import time
import requests
import logging
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ──────────────── НАСТРОЙКИ ────────────────
CDSE_USER = os.environ["CDSE_USER"]   # задаётся в .env
CDSE_PASS = os.environ["CDSE_PASS"]   # задаётся в .env

BOUNDS = {
    "north": 47.31555190588103,
    "south": 44.046203268457674,
    "east":  44.48415756225587,
    "west":  39.00833129882813,
}

MAX_CLOUD = 30          # %
START_DATE = "2016-01-01T00:00:00.000Z"
END_DATE   = "2025-12-31T23:59:59.000Z"
OUTPUT_DIR = Path("test_krasnodar")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BANDS = ["B01", "B02", "B03", "B04", "B05", "B06",
         "B07", "B08", "B8A", "B09", "B11", "B12"]

PAGE_SIZE = 100         # максимум для CDSE OData
RETRY_LIMIT = 5
RETRY_DELAY = 10        # секунды между попытками при ошибке
REQUEST_TIMEOUT = 120   # секунды ожидания ответа
# ───────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "download.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── OData URLs ──
CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
KEYCLOAK_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
)


# ─────────────────────── Авторизация ───────────────────────

class CDSESession:
    """Сессия с автоматическим обновлением токена."""

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self._access_token: str = ""
        self._refresh_token: str = ""
        self._expires_at: float = 0.0
        self._session = requests.Session()
        self._authenticate()

    def _authenticate(self):
        log.info("Авторизация в CDSE...")
        resp = requests.post(
            KEYCLOAK_URL,
            data={
                "client_id": "cdse-public",
                "grant_type": "password",
                "username": self.username,
                "password": self.password,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._refresh_token = data.get("refresh_token", "")
        self._expires_at = time.time() + data.get("expires_in", 600) - 60
        self._session.headers.update({"Authorization": f"Bearer {self._access_token}"})
        log.info("Авторизация успешна.")

    def _refresh(self):
        try:
            resp = requests.post(
                KEYCLOAK_URL,
                data={
                    "client_id": "cdse-public",
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._refresh_token = data.get("refresh_token", self._refresh_token)
            self._expires_at = time.time() + data.get("expires_in", 600) - 60
            self._session.headers.update({"Authorization": f"Bearer {self._access_token}"})
        except Exception:
            log.warning("Refresh не удался, повторная авторизация...")
            self._authenticate()

    def get(self, url: str, **kwargs) -> requests.Response:
        if time.time() >= self._expires_at:
            self._refresh()
        return self._session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)

    def stream_get(self, url: str, **kwargs) -> requests.Response:
        if time.time() >= self._expires_at:
            self._refresh()
        return self._session.get(url, stream=True, timeout=REQUEST_TIMEOUT, **kwargs)


# ─────────────────────── Поиск продуктов ───────────────────────

def build_wkt_polygon(b: dict) -> str:
    w, s, e, n = b["west"], b["south"], b["east"], b["north"]
    return (
        f"POLYGON(({w} {s},{e} {s},{e} {n},{w} {n},{w} {s}))"
    )


def iter_products(session: CDSESession, start_skip: int = 0):
    """Генератор: отдаёт по одной странице (100 продуктов) за раз."""
    wkt = build_wkt_polygon(BOUNDS)
    base_filter = (
        "Collection/Name eq 'SENTINEL-2'"
        f" and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
        f" and ContentDate/Start gt {START_DATE}"
        f" and ContentDate/Start lt {END_DATE}"
        f" and Attributes/OData.CSC.DoubleAttribute/any("
        f"att:att/Name eq 'cloudCover'"
        f" and att/OData.CSC.DoubleAttribute/Value le {MAX_CLOUD:.2f})"
        " and Attributes/OData.CSC.StringAttribute/any("
        "att:att/Name eq 'productType'"
        " and att/OData.CSC.StringAttribute/Value eq 'S2MSI2A')"
    )

    skip = start_skip
    total_known = None

    while True:
        params = {
            "$filter": base_filter,
            "$top": PAGE_SIZE,
            "$skip": skip,
            "$orderby": "ContentDate/Start asc",
            "$count": "True",
            "$expand": "Attributes",
        }
        log.info(f"Запрос страницы: skip={skip}" +
                 (f" / {total_known}" if total_known else ""))

        for attempt in range(1, RETRY_LIMIT + 1):
            try:
                resp = session.get(CATALOG_URL, params=params)
                resp.raise_for_status()
                break
            except Exception as exc:
                log.warning(f"Попытка {attempt}/{RETRY_LIMIT}: {exc}")
                if attempt == RETRY_LIMIT:
                    raise
                time.sleep(RETRY_DELAY)

        data = resp.json()
        page = data.get("value", [])
        total_known = data.get("@odata.count", total_known)

        log.info(f"Страница получена: {len(page)} продуктов  "
                 f"(всего в каталоге: {total_known})")

        yield skip, page, total_known

        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE


# ─────────────────────── Поиск URL каналов через Nodes API ───────────────────────

NODES_BASE = "https://download.dataspace.copernicus.eu/odata/v1"
_LEAF_EXTS = {".xml", ".jpg", ".png", ".html", ".txt",
              ".gml", ".xsd", ".bin", ".hdr", ".csv", ".kml", ".json"}


def find_band_urls(session: CDSESession, product_id: str) -> dict[str, str]:
    """
    Обходит структуру продукта через Nodes API на download.dataspace.copernicus.eu.
    Ключи ответа — 'result' (не 'value'), URL детей берётся из node['Nodes']['uri'].
    """
    band_urls: dict[str, str] = {}

    def walk(nodes_url: str):
        if len(band_urls) == len(BANDS):
            return
        resp = session.get(nodes_url)
        if not resp.ok:
            log.debug(f"  Nodes {resp.status_code}: {nodes_url}")
            return
        nodes = resp.json().get("result", [])
        for node in nodes:
            name: str = node.get("Name", "")
            children_uri: str = node.get("Nodes", {}).get("uri", "")
            if not name:
                continue
            ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if name.endswith(".jp2"):
                for band in BANDS:
                    if band not in band_urls:
                        if f"_{band}_" in name or name.endswith(f"_{band}.jp2"):
                            # URL скачивания = children_uri без /Nodes + /$value
                            dl_url = children_uri.removesuffix("/Nodes") + "/$value"
                            band_urls[band] = dl_url
            elif ext not in _LEAF_EXTS and children_uri:
                walk(children_uri)

    root_url = f"{NODES_BASE}/Products({product_id})/Nodes"
    walk(root_url)

    if not band_urls:
        log.warning("  Nodes API: каналы не найдены")
    else:
        missing = [b for b in BANDS if b not in band_urls]
        if missing:
            log.warning(f"  Nodes API: не найдены каналы {missing}")

    return band_urls


# ─────────────────────── Скачивание файла ───────────────────────

def _resolve_url(session: CDSESession, url: str) -> str:
    """Следует редиректам вручную, сохраняя Authorization-заголовок."""
    for _ in range(5):
        resp = session.get(url, allow_redirects=False)
        if resp.status_code in (301, 302, 303, 307, 308):
            url = resp.headers.get("Location", url)
        else:
            break
    return url


def download_file(session: CDSESession, url: str, dest: Path):
    if dest.exists():
        log.info(f"  Уже скачан: {dest.name}")
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            # Раскрываем редиректы вручную чтобы не потерять токен
            actual_url = _resolve_url(session, url)
            with session.stream_get(actual_url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", 0))
                with tqdm(total=total or None, unit="B", unit_scale=True,
                          unit_divisor=1024, desc=dest.name, leave=False) as bar:
                    with open(tmp, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=1024 * 1024):
                            f.write(chunk)
                            bar.update(len(chunk))
            tmp.rename(dest)
            return
        except Exception as exc:
            log.warning(f"  Попытка {attempt}/{RETRY_LIMIT} ({dest.name}): {exc}")
            if tmp.exists():
                tmp.unlink()
            if attempt == RETRY_LIMIT:
                log.error(f"  Не удалось скачать: {dest.name}")
                return
            time.sleep(RETRY_DELAY * attempt)


# ─────────────────────── Сохранение прогресса ───────────────────────

def load_done(done_file: Path) -> set[str]:
    if done_file.exists():
        return set(done_file.read_text(encoding="utf-8").splitlines())
    return set()


def save_done(done_file: Path, product_name: str):
    with open(done_file, "a", encoding="utf-8") as f:
        f.write(product_name + "\n")


# ─────────────────────── Основная логика ───────────────────────

def load_skip(skip_file: Path) -> int:
    """Возвращает номер страницы (skip) с которой продолжить."""
    if skip_file.exists():
        try:
            return int(skip_file.read_text(encoding="utf-8").strip())
        except ValueError:
            pass
    return 0


def save_skip(skip_file: Path, skip: int):
    skip_file.write_text(str(skip), encoding="utf-8")


def process_product(session: CDSESession, product: dict, done_file: Path,
                    done: set, index: int, total: int):
    product_id   = product["Id"]
    product_name = product["Name"]

    if product_name in done:
        log.info(f"[{index}/{total}] Пропускаем (уже скачан): {product_name}")
        return

    date_str  = product.get("ContentDate", {}).get("Start", "")[:10]
    cloud_val = next(
        (a["Value"] for a in product.get("Attributes", [])
         if a.get("Name") == "cloudCover"), "?")

    cloud_str = f"{cloud_val:.1f}%" if isinstance(cloud_val, float) else str(cloud_val)
    log.info(f"[{index}/{total}] {product_name}  дата={date_str}  облачность={cloud_str}")

    product_dir = OUTPUT_DIR / f"{date_str}_{product_name[:40]}"
    product_dir.mkdir(parents=True, exist_ok=True)

    log.info("  Получаем структуру продукта...")
    band_urls = find_band_urls(session, product_id)

    if not band_urls:
        log.warning("  Каналы не найдены — скачиваем полный ZIP.")
        zip_url  = (f"https://download.dataspace.copernicus.eu"
                    f"/odata/v1/Products({product_id})/$value")
        zip_path = product_dir / f"{product_name}.zip"
        try:
            download_file(session, zip_url, zip_path)
        except Exception as exc:
            log.error(f"  Ошибка скачивания ZIP: {exc}")
            return  # не помечаем как готовый — пусть повторит
    else:
        missing = [b for b in BANDS if b not in band_urls]
        log.info(f"  Найдены каналы: {sorted(band_urls)}")
        if missing:
            log.warning(f"  Не найдены каналы: {missing}")
        for band, url in band_urls.items():
            download_file(session, url, product_dir / f"{date_str}_{band}.jp2")

    save_done(done_file, product_name)
    done.add(product_name)
    log.info(f"  Готово: {product_name}")


def main():
    done_file = OUTPUT_DIR / "done_products.txt"
    skip_file = OUTPUT_DIR / "last_skip.txt"
    done = load_done(done_file)
    start_skip = load_skip(skip_file)

    if start_skip:
        log.info(f"Продолжаем с позиции skip={start_skip} "
                 f"(уже скачано продуктов: {len(done)})")

    session = CDSESession(CDSE_USER, CDSE_PASS)

    global_index = start_skip + 1  # порядковый номер для лога

    for skip, page, total in iter_products(session, start_skip):
        if not page:
            break

        log.info(f"─── Обрабатываем батч skip={skip}, продуктов в батче: {len(page)} ───")

        for product in page:
            process_product(session, product, done_file, done, global_index, total)
            global_index += 1

        # Сохраняем позицию после успешной обработки батча
        save_skip(skip_file, skip + PAGE_SIZE)

    # Всё скачано — сбрасываем позицию
    save_skip(skip_file, 0)
    log.info("=== Все продукты обработаны ===")


if __name__ == "__main__":
    main()
