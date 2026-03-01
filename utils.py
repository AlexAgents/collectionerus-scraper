"""
utils.py — Общие утилиты проекта.
"""

import os
import re
import json
import hashlib
import time

from config import (
    IMAGE_EXTENSIONS, IMAGE_MAGIC_BYTES, MIN_FILE_SIZE,
    COLLECTIONS_CACHE_FILE, COLLECTIONS_CACHE_MAX_AGE,
    METADATA_DIR,
)


# ═══════════════════════════════════════════════
# Пути коллекций
# ═══════════════════════════════════════════════

def get_col_dir(slug: str) -> str:
    """Возвращает путь к папке коллекции. Единственная точка."""
    return os.path.join(METADATA_DIR, slug)


# ═══════════════════════════════════════════════
# Имена файлов
# ═══════════════════════════════════════════════

def safe_filename(text: str, max_len: int = 80) -> str:
    if not text:
        return "untitled"
    text = re.sub(r'[<>:"/\\|?*\n\r\t\x00-\x1f]', '_', text)
    text = re.sub(r'[_\s]+', '_', text)
    text = text.strip('_. ')
    return text[:max_len] if text else "untitled"


def get_extension(url: str, default: str = '.jpg') -> str:
    path = url.split('?')[0].split('#')[0]
    ext = os.path.splitext(path)[1].lower()
    if ext in IMAGE_EXTENSIONS or ext == '.svg':
        return ext
    return default


# ═══════════════════════════════════════════════
# Парсинг выбора
# ═══════════════════════════════════════════════

def parse_selection(input_str: str, max_num: int) -> set:
    """
    Парсит строку выбора номеров.
    all, *, odd, even, 5, 1-10, 1,3,5, 1-10,!5, -20, 50-
    Пустая строка возвращает пустой set.
    """
    input_str = input_str.strip().lower()

    if not input_str:
        return set()

    if input_str in ('all', 'все', '*'):
        return set(range(1, max_num + 1))
    if input_str == 'odd':
        return {i for i in range(1, max_num + 1) if i % 2 == 1}
    if input_str == 'even':
        return {i for i in range(1, max_num + 1) if i % 2 == 0}

    selected = set()
    excluded = set()

    for part in re.split(r'[,;\s]+', input_str):
        part = part.strip()
        if not part:
            continue

        try:
            if part.startswith('!'):
                inner = part[1:]
                if '-' in inner:
                    a, b = inner.split('-', 1)
                    a = int(a) if a else 1
                    b = int(b) if b else max_num
                    excluded.update(range(a, b + 1))
                else:
                    excluded.add(int(inner))
            elif '-' in part:
                parts2 = part.split('-', 1)
                a = parts2[0].strip()
                b = parts2[1].strip()
                start = int(a) if a else 1
                end = int(b) if b else max_num
                selected.update(range(start, end + 1))
            else:
                selected.add(int(part))
        except ValueError:
            continue

    if not selected and excluded:
        selected = set(range(1, max_num + 1))

    result = selected - excluded
    return {n for n in result if 1 <= n <= max_num}


# ═══════════════════════════════════════════════
# Метаданные
# ═══════════════════════════════════════════════

def load_metadata(collection_dir: str) -> list:
    json_path = os.path.join(collection_dir, 'metadata.json')
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('items', [])
        except (json.JSONDecodeError, OSError):
            pass

    progress_path = os.path.join(collection_dir, '_progress.json')
    if os.path.exists(progress_path):
        try:
            with open(progress_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('metadata', [])
        except (json.JSONDecodeError, OSError):
            pass

    return []


# ═══════════════════════════════════════════════
# Кэш списка коллекций
# ═══════════════════════════════════════════════

def load_collections_cache() -> tuple:
    """
    Загружает кэш списка коллекций.
    Возвращает (collections_list, is_fresh).
    """
    if not os.path.exists(COLLECTIONS_CACHE_FILE):
        return None, False

    try:
        mtime = os.path.getmtime(COLLECTIONS_CACHE_FILE)
        age = time.time() - mtime

        with open(COLLECTIONS_CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, list) or len(data) == 0:
            return None, False

        is_fresh = age < COLLECTIONS_CACHE_MAX_AGE
        return data, is_fresh

    except (json.JSONDecodeError, OSError):
        return None, False


def save_collections_cache(collections: list):
    """Сохраняет список коллекций в кэш."""
    atomic_write_json(COLLECTIONS_CACHE_FILE, collections, indent=2)


def format_cache_age(cache_file: str) -> str:
    """Возвращает человекочитаемый возраст кэша."""
    if not os.path.exists(cache_file):
        return "нет кэша"
    age = time.time() - os.path.getmtime(cache_file)
    if age < 60:
        return f"{int(age)} сек назад"
    elif age < 3600:
        return f"{int(age / 60)} мин назад"
    elif age < 86400:
        return f"{int(age / 3600)} ч назад"
    else:
        return f"{int(age / 86400)} дн назад"


# ═══════════════════════════════════════════════
# Валидация изображений
# ═══════════════════════════════════════════════

def is_valid_image(filepath: str) -> bool:
    try:
        size = os.path.getsize(filepath)
        if size < MIN_FILE_SIZE:
            return False

        with open(filepath, 'rb') as f:
            header = f.read(12)

        if len(header) < 2:
            return False

        for magic, fmt in IMAGE_MAGIC_BYTES.items():
            if header.startswith(magic):
                if fmt == 'webp' and b'WEBP' not in header:
                    continue
                return True

        return False
    except OSError:
        return False


def is_image_file(filename: str) -> bool:
    return filename.lower().endswith(IMAGE_EXTENSIONS)


# ═══════════════════════════════════════════════
# Атомарная запись JSON
# ═══════════════════════════════════════════════

def atomic_write_json(filepath: str, data, **kwargs):
    """Атомарная запись JSON — tmp + os.replace."""
    # FIX: проверка dirname на пустую строку (Windows)
    dir_name = os.path.dirname(filepath)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    tmp = filepath + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, **kwargs)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, filepath)
    except OSError:
        # Пробуем убрать tmp
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        # Fallback: прямая запись с бэкапом
        backup = filepath + '.bak'
        try:
            if os.path.exists(filepath):
                if os.path.exists(backup):
                    os.remove(backup)
                os.rename(filepath, backup)
        except OSError:
            pass
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, **kwargs)


# ═══════════════════════════════════════════════
# Файловые утилиты
# ═══════════════════════════════════════════════

def dir_size(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def file_hash(filepath: str, block_size: int = 8192) -> str:
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        while True:
            block = f.read(block_size)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def count_images_recursive(images_dir: str) -> int:
    count = 0
    if not os.path.exists(images_dir):
        return 0
    for root, _, files in os.walk(images_dir):
        for f in files:
            if f.startswith('_'):
                continue
            if is_image_file(f):
                count += 1
    return count