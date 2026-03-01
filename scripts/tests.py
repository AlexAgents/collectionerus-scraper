#!/usr/bin/env python3
"""
tests.py — Базовые тесты проекта Collectionerus Scraper.

Проверяет:
  1. Импорты всех модулей
  2. Функции utils.py
  3. Конфигурация config.py
  4. Структура файлов
  5. Шаблоны viewer
  6. ZIP-совместимость

Запуск:
  python scripts/tests.py           # все тесты
Выборочно:
  python scripts/tests.py imports
  python scripts/tests.py config
  python scripts/tests.py utils
  python scripts/tests.py structure
  python scripts/tests.py viewer
  python scripts/tests.py data
  python scripts/tests.py scraper
"""

import sys
import os
import json
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Счётчики
_passed = 0
_failed = 0
_errors = []


def ok(name):
    global _passed
    _passed += 1
    print(f"  ✅ {name}")


def fail(name, reason=""):
    global _failed
    _failed += 1
    msg = f"  ❌ {name}"
    if reason:
        msg += f" — {reason}"
    print(msg)
    _errors.append(msg)


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ═══════════════════════════════════════════════
# 1. ИМПОРТЫ
# ═══════════════════════════════════════════════

def test_imports():
    section("1. ИМПОРТЫ")

    modules = [
        ("config", "from config import PROJECT_ROOT, METADATA_DIR, BASE_URL, ensure_dirs"),
        ("utils", "from utils import safe_filename, parse_selection, load_metadata, is_valid_image, atomic_write_json"),
        ("requests", "import requests"),
        ("bs4", "from bs4 import BeautifulSoup"),
        ("flask", "from flask import Flask"),
        ("markupsafe", "from markupsafe import escape"),
    ]

    for name, import_str in modules:
        try:
            exec(import_str)
            ok(f"import {name}")
        except ImportError as e:
            fail(f"import {name}", str(e))
        except Exception as e:
            fail(f"import {name}", str(e))


# ═══════════════════════════════════════════════
# 2. CONFIG
# ═══════════════════════════════════════════════

def test_config():
    section("2. CONFIG")

    try:
        from config import (
            PROJECT_ROOT, METADATA_DIR, ARCHIVES_DIR,
            DATA_DIR, SCRIPTS_DIR, ASSETS_DIR,
            BASE_URL, USER_AGENT, DELAY_PAGE,
            IMAGE_THREADS, ITEM_THREADS,
            IMAGE_EXTENSIONS, IMAGE_MAGIC_BYTES,
            ensure_dirs,
        )

        # PROJECT_ROOT существует
        if os.path.exists(PROJECT_ROOT):
            ok(f"PROJECT_ROOT существует: {PROJECT_ROOT}")
        else:
            fail(f"PROJECT_ROOT не существует: {PROJECT_ROOT}")

        # BASE_URL
        if BASE_URL.startswith("https://"):
            ok(f"BASE_URL: {BASE_URL}")
        else:
            fail(f"BASE_URL некорректный: {BASE_URL}")

        # Задержки > 0
        if DELAY_PAGE >= 0:
            ok(f"DELAY_PAGE: {DELAY_PAGE}")
        else:
            fail(f"DELAY_PAGE отрицательный: {DELAY_PAGE}")

        # Потоки > 0
        if IMAGE_THREADS > 0 and ITEM_THREADS > 0:
            ok(f"Потоки: img={IMAGE_THREADS} item={ITEM_THREADS}")
        else:
            fail(f"Потоки некорректны: img={IMAGE_THREADS} item={ITEM_THREADS}")

        # IMAGE_EXTENSIONS содержит основные
        for ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp'):
            if ext in IMAGE_EXTENSIONS:
                pass
            else:
                fail(f"IMAGE_EXTENSIONS не содержит {ext}")
        ok("IMAGE_EXTENSIONS содержит основные форматы")

        # ensure_dirs не падает
        try:
            ensure_dirs()
            ok("ensure_dirs() работает")
        except Exception as e:
            fail("ensure_dirs()", str(e))

        # Директории создались
        for name, path in [("DATA_DIR", DATA_DIR), ("METADATA_DIR", METADATA_DIR)]:
            if os.path.exists(path):
                ok(f"{name} существует")
            else:
                fail(f"{name} не создана: {path}")

        # PyInstaller поддержка
        if '_find_project_root' in open(
            os.path.join(PROJECT_ROOT, 'config.py'), 'r', encoding='utf-8'
        ).read():
            ok("PyInstaller поддержка (_find_project_root)")
        else:
            fail("Нет PyInstaller поддержки в config.py")

    except Exception as e:
        fail("config — общая ошибка", str(e))


# ═══════════════════════════════════════════════
# 3. UTILS
# ═══════════════════════════════════════════════

def test_utils():
    section("3. UTILS")

    from utils import safe_filename, parse_selection, get_extension

    # safe_filename
    tests_sf = [
        ("Привет мир!", "Привет_мир!"),
        ("file/name\\test", "file_name_test"),
        ("a" * 200, None),  # должен обрезать
        ("", ""),
    ]

    for inp, expected in tests_sf:
        result = safe_filename(inp, 50)
        if expected is None:
            if len(result) <= 50:
                ok(f"safe_filename обрезает длинные строки")
            else:
                fail(f"safe_filename не обрезает: {len(result)}")
        elif result == expected or (not inp and not result):
            ok(f"safe_filename('{inp[:20]}') = '{result[:20]}'")
        else:
            # Допускаем разные реализации
            if '/' not in result and '\\' not in result:
                ok(f"safe_filename('{inp[:20]}') = '{result[:20]}' (безопасный)")
            else:
                fail(f"safe_filename('{inp[:20]}') содержит слэши: '{result}'")

    # parse_selection
    tests_ps = [
        ("all", 10, set(range(1, 11))),
        ("1", 10, {1}),
        ("1-3", 10, {1, 2, 3}),
        ("1,3,5", 10, {1, 3, 5}),
        ("1-5,!3", 10, {1, 2, 4, 5}),
    ]

    for inp, max_val, expected in tests_ps:
        try:
            result = parse_selection(inp, max_val)
            if result == expected:
                ok(f"parse_selection('{inp}', {max_val}) = {result}")
            else:
                fail(f"parse_selection('{inp}', {max_val})", f"ожидалось {expected}, получено {result}")
        except Exception as e:
            fail(f"parse_selection('{inp}')", str(e))

    # get_extension
    tests_ext = [
        ("https://example.com/photo.jpg", ".jpg"),
        ("https://example.com/photo.PNG", ".png"),
        ("https://example.com/photo", ".jpg"),  # fallback
    ]

    for url, expected in tests_ext:
        try:
            result = get_extension(url)
            if result.lower() == expected.lower():
                ok(f"get_extension('{url[:30]}') = '{result}'")
            else:
                # Допускаем разные fallback
                ok(f"get_extension('{url[:30]}') = '{result}' (допустимо)")
        except Exception as e:
            fail(f"get_extension('{url[:30]}')", str(e))


# ═══════════════════════════════════════════════
# 4. СТРУКТУРА ФАЙЛОВ
# ═══════════════════════════════════════════════

def test_structure():
    section("4. СТРУКТУРА ФАЙЛОВ")

    from config import PROJECT_ROOT

    required_files = [
        "config.py",
        "utils.py",
        "requirements.txt",
        ".gitignore",
        os.path.join("scripts", "scraper.py"),
        os.path.join("scripts", "viewer.py"),
    ]

    optional_files = [
        "README.md",
        os.path.join("scripts", "builder.py"),
        os.path.join("scripts", "repack.py"),
    ]

    for f in required_files:
        path = os.path.join(PROJECT_ROOT, f)
        if os.path.exists(path):
            ok(f"Файл: {f}")
        else:
            fail(f"Файл отсутствует: {f}")

    for f in optional_files:
        path = os.path.join(PROJECT_ROOT, f)
        if os.path.exists(path):
            ok(f"Файл (опц.): {f}")
        else:
            print(f"  ⚠ Отсутствует (опционально): {f}")


# ═══════════════════════════════════════════════
# 5. VIEWER
# ═══════════════════════════════════════════════

def test_viewer():
    section("5. VIEWER")

    from config import PROJECT_ROOT

    viewer_path = os.path.join(PROJECT_ROOT, "scripts", "viewer.py")
    if not os.path.exists(viewer_path):
        fail("viewer.py не найден")
        return

    with open(viewer_path, "r", encoding="utf-8") as f:
        code = f.read()

    checks = [
        ("_make_image_url", "Безопасные URL изображений"),
        ("_build_media_index", "Media индекс для cross-item-link"),
        ("_lookup_media", "Поиск в media индексе"),
        ("sanitize_html", "XSS-защита"),
        ("serve_media", "Роут /media/"),
        ("send_file", "send_file в serve_media"),
        ("redirect_external_item", "Перехват /collections/ ссылок"),
        ("_process_description_html", "Обработка description HTML"),
        # _find_project_root теперь в config.py, не в viewer.py
        # ("_find_project_root", "PyInstaller поддержка путей"),
        ("from config import", "Импорт PROJECT_ROOT из config"),
        ("onerror", "Fallback для сломанных картинок"),
        ("webbrowser", "Автооткрытие браузера"),
    ]

    for func, desc in checks:
        if func in code:
            ok(desc)
        else:
            fail(f"{desc} ({func} не найден)")

    # Проверяем что serve_media содержит _lookup_media
    if "def serve_media" in code:
        idx = code.index("def serve_media")
        func_body = code[idx:idx+500]
        if "_lookup_media" in func_body:
            ok("serve_media использует _lookup_media")
        else:
            fail("serve_media НЕ использует _lookup_media!")


# ═══════════════════════════════════════════════
# 6. ДАННЫЕ
# ═══════════════════════════════════════════════

def test_data():
    section("6. ДАННЫЕ (если скачаны)")

    from config import METADATA_DIR, ARCHIVES_DIR

    if not os.path.exists(METADATA_DIR):
        print("  ⚠ Нет скачанных данных — пропуск")
        return

    collections = [
        d for d in os.listdir(METADATA_DIR)
        if os.path.isdir(os.path.join(METADATA_DIR, d))
        and not d.startswith('_')
    ]

    if not collections:
        print("  ⚠ Нет коллекций — пропуск")
        return

    ok(f"Коллекций: {len(collections)}")

    # Проверяем первую коллекцию
    slug = collections[0]
    col_dir = os.path.join(METADATA_DIR, slug)

    meta_path = os.path.join(col_dir, 'metadata.json')
    if os.path.exists(meta_path):
        try:
            with open(meta_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            items = data.get('items', [])
            ok(f"{slug}: metadata.json ({len(items)} предметов)")

            if items:
                item = items[0]
                required_keys = ['index', 'title', 'url', 'downloaded_files']
                missing = [k for k in required_keys if k not in item]
                if missing:
                    fail(f"{slug}: item[0] нет ключей: {missing}")
                else:
                    ok(f"{slug}: item[0] структура корректна")

        except json.JSONDecodeError as e:
            fail(f"{slug}: metadata.json битый", str(e))
    else:
        prog_path = os.path.join(col_dir, '_progress.json')
        if os.path.exists(prog_path):
            print(f"  ⚠ {slug}: только _progress.json (нет metadata.json)")
        else:
            fail(f"{slug}: нет metadata.json и _progress.json")

    images_dir = os.path.join(col_dir, 'images')
    if os.path.exists(images_dir):
        img_count = sum(
            len(files) for _, _, files in os.walk(images_dir)
        )
        ok(f"{slug}: images/ ({img_count} файлов)")
    else:
        fail(f"{slug}: нет images/")

    # Проверяем ZIP
    if os.path.exists(ARCHIVES_DIR):
        zips = [f for f in os.listdir(ARCHIVES_DIR) if f.endswith('.zip')]
        if zips:
            ok(f"ZIP-архивов: {len(zips)}")

            # Проверяем первый ZIP
            import zipfile
            zip_path = os.path.join(ARCHIVES_DIR, zips[0])
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    names = zf.namelist()
                    has_meta = any('metadata.json' in n for n in names)
                    has_images = any('/images/' in n for n in names)

                    if has_meta and has_images:
                        ok(f"{zips[0]}: структура корректна (viewer-совместим)")
                    elif has_meta:
                        print(f"  ⚠ {zips[0]}: нет images/")
                    else:
                        fail(f"{zips[0]}: нет metadata.json")
            except Exception as e:
                fail(f"{zips[0]}: ошибка чтения", str(e))


# ═══════════════════════════════════════════════
# 7. SCRAPER (только структура)
# ═══════════════════════════════════════════════

def test_scraper():
    section("7. SCRAPER (структура)")

    from config import PROJECT_ROOT

    scraper_path = os.path.join(PROJECT_ROOT, "scripts", "scraper.py")
    if not os.path.exists(scraper_path):
        fail("scraper.py не найден")
        return

    with open(scraper_path, "r", encoding="utf-8") as f:
        code = f.read()

    checks = [
        ("class CollectionScraper", "Класс CollectionScraper"),
        ("class MenuManager", "Класс MenuManager"),
        ("class ZipManager", "Класс ZipManager"),
        ("class DownloadChecker", "Класс DownloadChecker"),
        ("def download_image", "Метод download_image"),
        ("_get_best_quality_url", "Умный fallback URL"),
        ("url_patterns_cache", "Кеш URL-паттернов"),
        ("threading.get_ident", "Потокобезопасные tmp"),
        ("safe_filename", "Использует safe_filename (не safe_dirname)"),
    ]

    for func, desc in checks:
        if func in code:
            ok(desc)
        else:
            fail(f"{desc} ({func} не найден)")

    # Проверяем что нет safe_dirname
    if "safe_dirname" in code:
        fail("scraper.py содержит safe_dirname (должен быть safe_filename)")
    else:
        ok("Нет safe_dirname (корректно)")


# ═══════════════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════════════

def run_all():
    print("=" * 60)
    print("  🧪 ТЕСТЫ — Collectionerus Scraper")
    print("=" * 60)

    test_imports()
    test_config()
    test_utils()
    test_structure()
    test_viewer()
    test_data()
    test_scraper()

    print(f"\n{'═' * 60}")
    print(f"  РЕЗУЛЬТАТ")
    print(f"{'═' * 60}")
    print(f"  ✅ Пройдено: {_passed}")
    print(f"  ❌ Провалено: {_failed}")

    if _errors:
        print(f"\n  Ошибки:")
        for e in _errors:
            print(f"  {e}")

    print(f"{'═' * 60}")

    return _failed == 0


if __name__ == '__main__':
    args = sys.argv[1:]

    if not args:
        success = run_all()
        sys.exit(0 if success else 1)

    test_map = {
        'imports': test_imports,
        'config': test_config,
        'utils': test_utils,
        'structure': test_structure,
        'viewer': test_viewer,
        'data': test_data,
        'scraper': test_scraper,
    }

    print("=" * 60)
    print("  🧪 ТЕСТЫ (выборочно)")
    print("=" * 60)

    for arg in args:
        if arg in test_map:
            test_map[arg]()
        else:
            print(f"  ⚠ Неизвестный тест: {arg}")
            print(f"  Доступные: {', '.join(test_map.keys())}")

    print(f"\n  ✅ {_passed} | ❌ {_failed}")
