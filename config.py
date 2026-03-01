"""
config.py — Единая конфигурация проекта.
"""
import sys
import os

# ═══════════════════════════════════════════════
# Пути
# ═══════════════════════════════════════════════
# ═══════════════════════════════════════════════
# Определение корня проекта
# ═══════════════════════════════════════════════
def _find_project_root():
    """
    Определяет корень проекта с учётом PyInstaller.

    При запуске из исходников:
      PROJECT_ROOT = папка где лежит config.py

    При запуске как EXE (PyInstaller):
      1. Папка где лежит .exe
      2. Если data/ нет рядом — уровень выше (dist/)
      3. Если data/ нет — текущая рабочая директория
      4. Fallback — создаём data/ рядом с exe
    """
    if getattr(sys, 'frozen', False):
        # Запущен как EXE
        exe_dir = os.path.dirname(os.path.abspath(sys.executable))

        # Проверяем где data/
        candidates = [
            exe_dir,                          # рядом с exe
            os.path.dirname(exe_dir),         # уровень выше (exe в dist/)
            os.getcwd(),                      # текущая директория
        ]

        for candidate in candidates:
            data_path = os.path.join(candidate, 'data')
            metadata_path = os.path.join(candidate, 'data', 'metadata')
            if os.path.exists(data_path) or os.path.exists(metadata_path):
                return candidate

        # Не нашли data/ — используем папку с exe
        # (data/ создастся автоматически ensure_dirs())
        print(f"  ⚠ data/ не найдена рядом с EXE: {exe_dir}")
        print(f"  ⚠ Создаю data/ в: {exe_dir}")
        return exe_dir
    else:
        # Запущен из исходников
        return os.path.dirname(os.path.abspath(__file__))


PROJECT_ROOT = _find_project_root()
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
METADATA_DIR = os.path.join(DATA_DIR, "metadata")
ARCHIVES_DIR = os.path.join(DATA_DIR, "archives")
ASSETS_DIR = os.path.join(PROJECT_ROOT, "assets")

# Кэш списка коллекций
COLLECTIONS_CACHE_FILE = os.path.join(
    METADATA_DIR, '_collections_list.json'
)
COLLECTIONS_CACHE_MAX_AGE = 3600  # секунд (1 час)

# ═══════════════════════════════════════════════
# Сетевые настройки
# ═══════════════════════════════════════════════
BASE_URL = "https://collectionerus.ru"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DELAY_PAGE = 0.1
DELAY_ITEM = 0.05

IMAGE_THREADS = 10
ITEM_THREADS = 5

# ═══════════════════════════════════════════════
# Файлы
# ═══════════════════════════════════════════════
MAX_FILENAME_LEN = 120
MIN_FILE_SIZE = 500

IMAGE_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.tiff', '.tif',
)

# FIX: Добавлены TIFF magic bytes (LE и BE)
IMAGE_MAGIC_BYTES = {
    b'\xff\xd8': 'jpeg',
    b'\x89PNG': 'png',
    b'GIF8': 'gif',
    b'RIFF': 'webp',
    b'BM': 'bmp',
    b'II\x2a\x00': 'tiff',
    b'MM\x00\x2a': 'tiff',
}

# Размер пакета для сохранения прогресса
BATCH_SIZE = 50
SCAN_SAMPLE_SIZE = 10

# Минимальная длина ответа для валидации
MIN_PAGE_CONTENT_LEN = 50
MIN_ITEMS_CONTENT_LEN = 20

# Пагинация: макс. пустых страниц подряд до остановки
MAX_CONSECUTIVE_EMPTY = 3
MAX_COLLECTION_PAGES = 50
MAX_ITEMS_EMPTY_PAGES = 5

# ═══════════════════════════════════════════════
# Viewer
# ═══════════════════════════════════════════════
VIEWER_HOST = '0.0.0.0'
VIEWER_PORT = 5000
VIEWER_DEBUG = True

# ═══════════════════════════════════════════════
# ZIP
# ═══════════════════════════════════════════════
ZIP_COMPRESSION_LEVEL = 6

# ═══════════════════════════════════════════════
# Builder
# ═══════════════════════════════════════════════
SCRAPER_ICON = os.path.join(ASSETS_DIR, "scraper.ico")
VIEWER_ICON = os.path.join(ASSETS_DIR, "viewer.ico")


def ensure_dirs():
    """Создаёт все необходимые директории."""
    for d in (DATA_DIR, METADATA_DIR, ARCHIVES_DIR, ASSETS_DIR):
        os.makedirs(d, exist_ok=True)