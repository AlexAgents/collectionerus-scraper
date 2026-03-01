"""
scraper.py — Скрапер коллекций с collectionerus.ru.

v10: Все исправления из ревью — умный fallback с кешем URL,
     потокобезопасные tmp-файлы, детализированная статистика,
     корректный подсчёт ошибок, fallback для working_pattern.

Запуск: python scripts/scraper.py
"""

import sys
import os

# Определяем корень проекта
if getattr(sys, 'frozen', False):
    # EXE: ищем data/ рядом с exe, выше или в cwd
    _exe_dir = os.path.dirname(os.path.abspath(sys.executable))
    for _candidate in [_exe_dir, os.path.dirname(_exe_dir), os.getcwd()]:
        if os.path.exists(os.path.join(_candidate, 'data')):
            sys.path.insert(0, _candidate)
            break
    else:
        sys.path.insert(0, _exe_dir)
else:
    sys.path.insert(
        0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import re
import zipfile
import shutil
from urllib.parse import urljoin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import traceback
import threading

from config import (
    BASE_URL, USER_AGENT, DELAY_PAGE, DELAY_ITEM,
    IMAGE_THREADS, ITEM_THREADS, MAX_FILENAME_LEN, MIN_FILE_SIZE,
    METADATA_DIR, ARCHIVES_DIR, DATA_DIR, ZIP_COMPRESSION_LEVEL,
    COLLECTIONS_CACHE_FILE, BATCH_SIZE, SCAN_SAMPLE_SIZE,
    MIN_PAGE_CONTENT_LEN, MIN_ITEMS_CONTENT_LEN,
    MAX_CONSECUTIVE_EMPTY, MAX_COLLECTION_PAGES,
    MAX_ITEMS_EMPTY_PAGES,
    ensure_dirs,
)
from utils import (
    safe_filename, get_extension, parse_selection,
    is_valid_image, atomic_write_json, dir_size,
    load_collections_cache, save_collections_cache,
    format_cache_age, file_hash, get_col_dir,
)


# ═══════════════════════════════════════════════
# Логирование
# ═══════════════════════════════════════════════
ensure_dirs()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(
            os.path.join(DATA_DIR, 'scraper.log'), encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# ГЛАВНОЕ МЕНЮ С НАВИГАЦИЕЙ
# ═══════════════════════════════════════════════

class MenuManager:
    """
    Управляет навигацией по меню.
    Любое действие возвращает в главное меню после завершения.
    """

    def __init__(self, scraper):
        self.scraper = scraper
        self._collections = None

    def get_collections(self, force_refresh=False):
        if self._collections is not None and not force_refresh:
            return self._collections

        cached, is_fresh = load_collections_cache()

        if cached and is_fresh:
            age_str = format_cache_age(COLLECTIONS_CACHE_FILE)
            logger.info(
                f"📋 Загружено из кэша: {len(cached)} коллекций "
                f"({age_str})"
            )
            self._collections = cached
            return cached

        if cached and not is_fresh and not force_refresh:
            age_str = format_cache_age(COLLECTIONS_CACHE_FILE)
            print(
                f"\n📋 Кэш коллекций: {len(cached)} шт. "
                f"({age_str})"
            )
            print("  1. Использовать кэш (быстро)")
            print("  2. Обновить с сайта (1-2 мин)")

            choice = input("\n▶ Выбор [1]: ").strip()
            if choice == '2':
                return self._fetch_and_cache()
            else:
                self._collections = cached
                return cached

        return self._fetch_and_cache()

    def _fetch_and_cache(self):
        collections = self.scraper.get_all_collections()
        if collections:
            save_collections_cache(collections)
            logger.info(
                f"💾 Кэш обновлён: {len(collections)} коллекций"
            )
            self._collections = collections
        else:
            logger.warning(
                "⚠ Не удалось загрузить с сайта, "
                "пробую старый кэш..."
            )
            cached, _ = load_collections_cache()
            if cached:
                logger.info(
                    f"📋 Используется старый кэш: "
                    f"{len(cached)} коллекций"
                )
                self._collections = cached
            else:
                logger.error(
                    "❌ Кэш тоже отсутствует. "
                    "Проверьте подключение."
                )
                self._collections = None
        return self._collections

    def run_main_loop(self):
        while True:
            action = self.show_main_menu()

            if action is None:
                print("\n👋 Выход. До встречи!")
                break

            try:
                self.execute_action(action)
            except KeyboardInterrupt:
                print("\n\n⚠ Прервано (Ctrl+C)")
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                logger.error(traceback.format_exc())

            input("\n⏎ Нажмите Enter для возврата в меню...")

    def show_main_menu(self):
        collections = self.get_collections()
        if not collections:
            print("❌ Не удалось загрузить список коллекций")
            return None

        done_count = 0
        in_progress = 0
        not_started = 0
        for col in collections:
            slug = col['slug']
            zip_path = os.path.join(ARCHIVES_DIR, f"{slug}.zip")
            col_dir = get_col_dir(slug)

            if os.path.exists(zip_path):
                done_count += 1
            elif os.path.exists(col_dir):
                in_progress += 1
            else:
                not_started += 1

        cache_age = format_cache_age(COLLECTIONS_CACHE_FILE)

        print(f"\n{'═' * 70}")
        print(f"  📦 COLLECTIONERUS SCRAPER v10")
        print(f"{'═' * 70}")
        print(
            f"  📋 Коллекций: {len(collections)} "
            f"(кэш: {cache_age})"
        )
        print(
            f"  ✅ Готово: {done_count}  "
            f"⏳ В процессе: {in_progress}  "
            f"⬚ Не начато: {not_started}"
        )
        print(f"{'─' * 70}")
        print(f"  1. 📥 Скачать коллекции")
        print(f"  2. 🔍 Проверить скачанное")
        print(f"  3. 🔎 Сканировать (доп. фото)")
        print(f"  4. 📦 Упаковать в ZIP")
        print(f"  5. 🔄 Перекачать с нуля")
        print(f"  6. 🔃 Обновить список коллекций")
        print(f"  7. 📋 Показать список коллекций")
        print(f"{'─' * 70}")
        print(f"  q. Выход")
        print(f"{'─' * 70}")

        choice = input("\n▶ Выбор: ").strip().lower()

        action_map = {
            '1': 'download',
            '2': 'check',
            '3': 'scan',
            '4': 'zip',
            '5': 'redownload',
            '6': 'refresh',
            '7': 'list',
            'q': None, 'quit': None,
            'exit': None, 'выход': None,
        }

        return action_map.get(choice, 'unknown')

    def execute_action(self, action):
        collections = self.get_collections()

        if action == 'download':
            self._action_download(collections)
        elif action == 'check':
            self._action_check(collections)
        elif action == 'scan':
            self._action_scan(collections)
        elif action == 'zip':
            self._action_zip(collections)
        elif action == 'redownload':
            self._action_redownload(collections)
        elif action == 'refresh':
            self._action_refresh()
        elif action == 'list':
            self._action_list(collections)
        elif action == 'unknown':
            print("⚠ Неизвестная команда")

    def _action_download(self, collections):
        self._show_collections_table(collections)

        print(f"\n{'─' * 70}")
        print("Формат: all, 5, 1-10, 1,3,5, !3, odd, even")
        print("        b/back — назад в меню")
        print(f"{'─' * 70}")

        user_input = input("\n▶ Номера коллекций: ").strip()

        if not user_input or user_input.lower() in (
            'b', 'back', 'назад', 'меню'
        ):
            return

        selected = parse_selection(user_input, len(collections))
        if not selected:
            print("⚠ Ничего не выбрано")
            return

        print(f"\n✅ Выбрано {len(selected)} коллекций:")
        for n in sorted(selected):
            col = collections[n - 1]
            print(
                f"  {n:4d}. {col['name'][:60]} "
                f"[{col['count']} шт.]"
            )

        confirm = input(
            f"\nНачать скачивание? (y/n) [y]: "
        ).strip().lower()
        if confirm in ('n', 'no', 'нет'):
            return

        to_process = [collections[i - 1] for i in sorted(selected)]
        self.scraper.process_collections(to_process)

    def _action_check(self, collections):
        checker = DownloadChecker()
        checker.check_all(collections)
        fix_slugs = checker.fix_issues(collections)

        if fix_slugs:
            confirm = input(
                f"\nПерекачать {len(fix_slugs)} коллекций? "
                f"(y/n) [n]: "
            ).strip().lower()
            if confirm in ('y', 'да', 'д'):
                to_process = [
                    c for c in collections
                    if c['slug'] in fix_slugs
                ]
                self.scraper.process_collections(to_process)

    def _action_scan(self, collections):
        scan_results = self.scraper.scan_for_related(collections)
        if not scan_results:
            return

        print(f"\nЧто делать?")
        print(f"  1. Выбрать для перекачки")
        print(f"  2. Перекачать все найденные")
        print(f"  3. Только отчёт")
        print(f"  b. Назад")

        choice = input("\n▶ Выбор [3]: ").strip()

        if choice in ('b', 'back', 'назад'):
            return

        if choice == '1':
            print(f"\nНайденные коллекции:")
            for i, r in enumerate(scan_results, 1):
                types = []
                if r['items_with_cross'] > 0:
                    types.append("cross")
                if r['items_with_multi'] > 0:
                    types.append("multi")
                print(
                    f"  {i:3d}. {r['name'][:50]} [{r['count']}] "
                    f"~{r['est_total_extra']} доп. | "
                    f"{'+'.join(types)}"
                )

            sel_input = input("\n▶ Номера (b=назад): ").strip()
            if not sel_input or sel_input.lower() in (
                'b', 'back', 'назад'
            ):
                return

            sel_nums = parse_selection(
                sel_input, len(scan_results)
            )
            if not sel_nums:
                return
            slugs = [
                scan_results[n - 1]['slug'] for n in sel_nums
            ]

        elif choice == '2':
            slugs = [r['slug'] for r in scan_results]

        else:
            print("📄 Отчёт сохранён")
            return

        self._reset_and_redownload(collections, slugs)

    def _action_zip(self, collections):
        self.scraper.zip_manager.interactive_zip(collections)

    def _action_redownload(self, collections):
        self._show_collections_table(collections, show_size=True)

        print(f"\n{'─' * 70}")
        print("Номера для полной перекачки (b=назад):")

        sel_input = input("\n▶ Номера: ").strip()
        if not sel_input or sel_input.lower() in (
            'b', 'back', 'назад'
        ):
            return

        selected = parse_selection(sel_input, len(collections))
        if not selected:
            print("Ничего не выбрано")
            return

        selected_cols = [
            collections[n - 1] for n in sorted(selected)
        ]
        total_items = sum(c['count'] for c in selected_cols)

        print(
            f"\n⚠ УДАЛИТЬ и перекачать {len(selected_cols)} "
            f"коллекций (~{total_items} предметов)?"
        )

        confirm = input("Точно? (yes/no): ").strip().lower()
        if confirm not in ('yes', 'да'):
            print("Отменено")
            return

        for col in selected_cols:
            slug = col['slug']
            col_dir = get_col_dir(slug)
            zip_path = os.path.join(ARCHIVES_DIR, f"{slug}.zip")

            if os.path.exists(col_dir):
                shutil.rmtree(col_dir)
            if os.path.exists(zip_path):
                os.remove(zip_path)
            print(f"  🗑 Сброшена: {col['name'][:50]}")

        self.scraper.process_collections(selected_cols)

    def _action_refresh(self):
        print("\n🔃 Обновление списка коллекций с сайта...")
        self._collections = None
        self._fetch_and_cache()
        if self._collections:
            print(
                f"✅ Обновлено: {len(self._collections)} коллекций"
            )

    def _action_list(self, collections):
        self._show_collections_table(collections)

    def _show_collections_table(self, collections,
                                show_size=False):
        print(f"\n{'═' * 80}")
        print(f"📋 КОЛЛЕКЦИИ ({len(collections)})")
        print(f"{'═' * 80}")

        for i, col in enumerate(collections, 1):
            slug = col['slug']
            col_dir = get_col_dir(slug)
            zip_path = os.path.join(ARCHIVES_DIR, f"{slug}.zip")

            if os.path.exists(zip_path):
                status = "✅"
            elif os.path.exists(col_dir):
                progress_path = os.path.join(
                    col_dir, '_progress.json'
                )
                if os.path.exists(progress_path):
                    try:
                        with open(
                            progress_path, 'r', encoding='utf-8'
                        ) as f:
                            p = json.load(f)
                        done = len(p.get('processed_urls', []))
                        status = f"⏳{done}"
                    except Exception:
                        status = "⏳"
                else:
                    status = "⏳"
            else:
                status = "⬚ "

            size_info = ""
            if show_size:
                parts = []
                if os.path.exists(col_dir):
                    sz = dir_size(col_dir)
                    parts.append(f"📁{sz / 1024 / 1024:.0f}M")
                if os.path.exists(zip_path):
                    sz = os.path.getsize(zip_path)
                    parts.append(f"📦{sz / 1024 / 1024:.0f}M")
                size_info = ' '.join(parts)

            line = (
                f"  {status} {i:4d}. "
                f"{col['name'][:48]:<50s} "
                f"[{col['count']:>6d}] "
                f"{col.get('owner', '')[:15]}"
            )
            if size_info:
                line += f"  {size_info}"
            print(line)

    def _reset_and_redownload(self, collections, slugs):
        print(f"\n🔄 Сброс {len(slugs)} коллекций...")
        for slug in slugs:
            col_dir = get_col_dir(slug)
            zip_path = os.path.join(ARCHIVES_DIR, f"{slug}.zip")

            for fname in (
                '_progress.json', 'metadata.json',
                'metadata.csv', 'fields_info.json'
            ):
                fp = os.path.join(col_dir, fname)
                if os.path.exists(fp):
                    os.remove(fp)

            images_dir = os.path.join(col_dir, 'images')
            if os.path.exists(images_dir):
                shutil.rmtree(images_dir)

            if os.path.exists(zip_path):
                os.remove(zip_path)

            print(f"  🔄 Сброшена: {slug}")

        to_process = [
            c for c in collections if c['slug'] in slugs
        ]
        self.scraper.process_collections(to_process)


# ═══════════════════════════════════════════════
# ZIP МЕНЕДЖЕР
# ═══════════════════════════════════════════════

class ZipManager:
    def __init__(self):
        self.archives_dir = ARCHIVES_DIR

    def interactive_zip(self, collections):
        print("\n" + "=" * 80)
        print("📦 УПАКОВКА В ZIP")
        print("=" * 80)

        packable = []
        already_zipped = []

        for i, col in enumerate(collections, 1):
            slug = col['slug']
            col_dir = get_col_dir(slug)
            zip_path = os.path.join(self.archives_dir, f"{slug}.zip")

            has_folder = os.path.exists(col_dir)
            has_zip = os.path.exists(zip_path)

            if has_folder and not has_zip:
                progress_path = os.path.join(
                    col_dir, '_progress.json'
                )
                done = 0
                if os.path.exists(progress_path):
                    try:
                        with open(
                            progress_path, 'r', encoding='utf-8'
                        ) as f:
                            p = json.load(f)
                        done = len(p.get('processed_urls', []))
                    except Exception:
                        pass

                if done > 0:
                    folder_size = dir_size(col_dir)
                    packable.append({
                        'num': i, 'col': col, 'done': done,
                        'folder_size_mb': round(
                            folder_size / 1024 / 1024, 1
                        ),
                    })

            elif has_folder and has_zip:
                folder_size = dir_size(col_dir)
                zip_size = os.path.getsize(zip_path)
                already_zipped.append({
                    'num': i, 'col': col,
                    'folder_size_mb': round(
                        folder_size / 1024 / 1024, 1
                    ),
                    'zip_size_mb': round(
                        zip_size / 1024 / 1024, 1
                    ),
                })

        if not packable and not already_zipped:
            print("\n❌ Нет коллекций для упаковки")
            return

        if packable:
            total_size = sum(p['folder_size_mb'] for p in packable)
            print(
                f"\n📁 Готовы к упаковке "
                f"({len(packable)} шт., ~{total_size:.0f} MB):"
            )
            for p in packable:
                print(
                    f"  {p['num']:4d}. "
                    f"{p['col']['name'][:50]:<52s} "
                    f"{p['done']:>5d} шт. | "
                    f"{p['folder_size_mb']:.1f} MB"
                )

        if already_zipped:
            total_dup = sum(
                p['folder_size_mb'] for p in already_zipped
            )
            print(
                f"\n📦+📁 Уже упакованы, папки остались "
                f"({len(already_zipped)} шт., "
                f"~{total_dup:.0f} MB можно освободить):"
            )
            for p in already_zipped:
                print(
                    f"  {p['num']:4d}. "
                    f"{p['col']['name'][:50]:<52s} "
                    f"📁 {p['folder_size_mb']:.1f} MB | "
                    f"📦 {p['zip_size_mb']:.1f} MB"
                )

        print(f"\n{'─' * 80}")
        print("Действия:")
        print("  1. Упаковать все готовые")
        print("  2. Упаковать выбранные")
        if already_zipped:
            print("  3. Удалить папки у упакованных")
            print("  4. Упаковать + удалить")
        print("  b. Назад")

        choice = input("\n▶ Выбор: ").strip().lower()

        if choice in ('b', 'back', 'назад', ''):
            return

        delete_after = False

        if choice == '1':
            to_pack = packable
            delete_after = self._ask_delete()
        elif choice == '2':
            sel_input = input("▶ Номера (b=назад): ").strip()
            if not sel_input or sel_input.lower() in (
                'b', 'back', 'назад'
            ):
                return
            sel_nums = parse_selection(
                sel_input, len(collections)
            )
            to_pack = [
                p for p in packable if p['num'] in sel_nums
            ]
            if not to_pack:
                print("Ничего не выбрано")
                return
            delete_after = self._ask_delete()
        elif choice == '3' and already_zipped:
            self._delete_folders(already_zipped)
            return
        elif choice == '4' and already_zipped:
            to_pack = packable
            delete_after = True
            self._delete_folders(already_zipped)
        else:
            return

        if to_pack:
            print(f"\n📦 Упаковка {len(to_pack)} коллекций...")
            for p in to_pack:
                self.zip_collection(
                    p['col']['slug'],
                    delete_after=delete_after
                )
        print(f"\n✅ Готово!")

    def zip_collection(self, slug, delete_after=False,
                       force=False):
        col_dir = get_col_dir(slug)
        zip_path = os.path.join(self.archives_dir, f"{slug}.zip")

        if not os.path.exists(col_dir):
            logger.warning(f"  ⚠ Папка не найдена: {slug}")
            return None

        if os.path.exists(zip_path) and not force:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zip_files = set(
                        n.replace('\\', '/')
                        for n in zf.namelist()
                    )
                disk_files = set()
                for root, _, files in os.walk(col_dir):
                    for f in files:
                        if f == '_progress.json':
                            continue
                        arc = os.path.relpath(
                            os.path.join(root, f), METADATA_DIR
                        ).replace('\\', '/')
                        disk_files.add(arc)
                if not (disk_files - zip_files):
                    logger.info(f"  📦 ZIP актуален: {slug}")
                    if delete_after:
                        self._delete_folder(col_dir, slug)
                    return zip_path
            except Exception:
                pass

        logger.info(f"  📦 Архивация → {slug}.zip")
        cnt = 0
        with zipfile.ZipFile(
            zip_path, 'w', zipfile.ZIP_DEFLATED,
            compresslevel=ZIP_COMPRESSION_LEVEL
        ) as zf:
            for root, _, files in os.walk(col_dir):
                for f in files:
                    if f == '_progress.json':
                        continue
                    fp = os.path.join(root, f)
                    zf.write(
                        fp, os.path.relpath(fp, METADATA_DIR)
                    )
                    cnt += 1

        size = os.path.getsize(zip_path) / 1024 / 1024
        logger.info(f"  ✅ ZIP: {cnt} файлов, {size:.1f} MB")

        if delete_after:
            self._delete_folder(col_dir, slug)

        return zip_path

    def _ask_delete(self):
        print("\n⚠ Удалить папки после ZIP?")
        ans = input("  (y/n) [n]: ").strip().lower()
        return ans in ('y', 'yes', 'да', 'д')

    def _delete_folder(self, col_dir, slug):
        if os.path.exists(col_dir):
            shutil.rmtree(col_dir)
            logger.info(f"  🗑 Удалена: {slug}")

    def _delete_folders(self, items):
        total_freed = 0
        for p in items:
            slug = p['col']['slug']
            col_dir = get_col_dir(slug)
            if os.path.exists(col_dir):
                size = dir_size(col_dir)
                shutil.rmtree(col_dir)
                total_freed += size
                print(
                    f"  🗑 {slug} — "
                    f"{size / 1024 / 1024:.1f} MB освобождено"
                )
        print(
            f"\n✅ Освобождено: "
            f"{total_freed / 1024 / 1024:.1f} MB"
        )


# ═══════════════════════════════════════════════
# ПРОВЕРКА СКАЧАННОГО
# ═══════════════════════════════════════════════

class DownloadChecker:
    def __init__(self):
        self.report = {
            'checked_at': datetime.now().isoformat(),
            'collections': [],
            'summary': {
                'total_collections': 0,
                'complete': 0,
                'incomplete': 0,
                'missing_images': 0,
                'corrupt_files': 0,
                'duplicates': 0,
            }
        }

    def check_all(self, collections):
        print("\n" + "=" * 80)
        print("🔍 ПРОВЕРКА СКАЧАННОГО")
        print("=" * 80)

        for i, col in enumerate(collections, 1):
            result = self._check_one(col, i, len(collections))
            self.report['collections'].append(result)
            s = self.report['summary']
            s['total_collections'] += 1

            if result['status'] == 'complete':
                s['complete'] += 1
            else:
                s['incomplete'] += 1

            s['missing_images'] += result['missing_images']
            s['corrupt_files'] += len(result['corrupt_files'])
            s['duplicates'] += len(result['duplicate_files'])

        report_path = os.path.join(
            METADATA_DIR, '_check_report.json'
        )
        atomic_write_json(report_path, self.report, indent=2)

        s = self.report['summary']
        print(f"\n{'═' * 80}")
        print(f"📊 ИТОГИ:")
        print(f"  Коллекций:      {s['total_collections']}")
        print(f"  ✅ Полных:      {s['complete']}")
        print(f"  ⚠ Неполных:     {s['incomplete']}")
        print(f"  🖼 Пропущено:   {s['missing_images']}")
        print(f"  💔 Битых:       {s['corrupt_files']}")
        print(f"  🔄 Дубликатов:  {s['duplicates']}")
        print(f"\n  Отчёт: {report_path}")
        print("═" * 80)

        return self.report

    def _check_one(self, col, num, total):
        slug = col['slug']
        col_dir = get_col_dir(slug)
        images_dir = os.path.join(col_dir, 'images')
        zip_path = os.path.join(ARCHIVES_DIR, f"{slug}.zip")

        result = {
            'name': col['name'], 'slug': slug,
            'expected_count': col['count'],
            'status': 'not_started',
            'downloaded_items': 0,
            'downloaded_images': 0,
            'missing_images': 0,
            'corrupt_files': [],
            'duplicate_files': [],
            'duplicate_hashes': [],
            'zero_size_files': [],
            'has_zip': os.path.exists(zip_path),
            'zip_size_mb': 0,
            'issues': [],
        }

        if os.path.exists(zip_path):
            result['zip_size_mb'] = round(
                os.path.getsize(zip_path) / 1024 / 1024, 2
            )

        if not os.path.exists(col_dir):
            result['issues'].append('Не начато')
            print(
                f"  [{num}/{total}] ⬚  "
                f"{col['name'][:50]} — не начато"
            )
            return result

        progress_path = os.path.join(col_dir, '_progress.json')
        metadata = []
        processed_urls = set()

        if os.path.exists(progress_path):
            try:
                with open(
                    progress_path, 'r', encoding='utf-8'
                ) as f:
                    progress = json.load(f)
                processed_urls = set(
                    progress.get('processed_urls', [])
                )
                metadata = progress.get('metadata', [])
            except Exception as e:
                logger.debug(
                    f"Ошибка чтения прогресса {slug}: {e}"
                )
                result['issues'].append(
                    'Ошибка чтения прогресса'
                )

        result['downloaded_items'] = len(processed_urls)

        if os.path.exists(images_dir):
            all_files = []
            file_hashes = {}
            file_sizes = {}

            for root, _, files in os.walk(images_dir):
                for f in files:
                    if f.startswith('_'):
                        continue
                    fp = os.path.join(root, f)
                    rel = os.path.relpath(fp, images_dir)

                    try:
                        size = os.path.getsize(fp)
                    except OSError:
                        continue

                    file_sizes[rel] = size
                    all_files.append(rel)

                    if not is_valid_image(fp):
                        result['corrupt_files'].append(rel)
                        if size < MIN_FILE_SIZE:
                            result['zero_size_files'].append(
                                {'file': rel, 'size': size}
                            )

                    if size > 1024:
                        try:
                            h = file_hash(fp)
                            file_hashes.setdefault(
                                h, []
                            ).append(rel)
                        except Exception:
                            pass

            result['downloaded_images'] = len(all_files)

            for h, fl in file_hashes.items():
                if len(fl) > 1:
                    result['duplicate_files'].extend(fl[1:])
                    result['duplicate_hashes'].append({
                        'hash': h, 'files': fl,
                        'size': file_sizes.get(fl[0], 0)
                    })

        missing = 0
        for item in metadata:
            for fname in item.get('downloaded_files', []):
                fp = os.path.join(images_dir, fname)
                if (not os.path.exists(fp) or
                        not is_valid_image(fp)):
                    missing += 1
            for rel in item.get('related_items', []):
                rel_file = rel.get('file', '')
                if rel_file:
                    fp = os.path.join(images_dir, rel_file)
                    if (not os.path.exists(fp) or
                            not is_valid_image(fp)):
                        missing += 1

        result['missing_images'] = missing

        expected = col['count']
        done = len(processed_urls)

        if done >= expected and missing == 0:
            result['status'] = 'complete'
            icon = "✅"
        elif done > 0:
            result['status'] = 'incomplete'
            icon = "⏳"
            pct = round(
                done / expected * 100, 1
            ) if expected > 0 else 0
            result['issues'].append(
                f'Скачано {done}/{expected} ({pct}%)'
            )
        else:
            result['status'] = 'not_started'
            icon = "⬚ "

        if missing > 0:
            result['issues'].append(f'{missing} пропущено')
        if result['corrupt_files']:
            result['issues'].append(
                f'{len(result["corrupt_files"])} битых'
            )
        if result['duplicate_files']:
            result['issues'].append(
                f'{len(result["duplicate_files"])} дублей'
            )

        issues_str = (
            ', '.join(result['issues'])
            if result['issues'] else 'OK'
        )
        print(
            f"  [{num}/{total}] {icon} "
            f"{col['name'][:40]:<42s} "
            f"{done:>5d}/{expected:<5d} "
            f"img:{result['downloaded_images']:>5d} "
            f"| {issues_str}"
        )

        return result

    def fix_issues(self, collections):
        if not self.report['collections']:
            self.check_all(collections)

        problems = [
            c for c in self.report['collections']
            if (c['status'] != 'complete' or
                c['corrupt_files'] or
                c['missing_images'] > 0)
        ]

        if not problems:
            print("\n✅ Всё в порядке!")
            return []

        print(f"\n⚠ Проблемных: {len(problems)}")
        print("-" * 80)

        to_fix = []
        for i, p in enumerate(problems, 1):
            print(f"\n  {i}. {p['name']}")
            print(
                f"     {p['downloaded_items']}/"
                f"{p['expected_count']} "
                f"пропущено: {p['missing_images']}"
            )
            if p['corrupt_files']:
                for cf in p['corrupt_files'][:3]:
                    print(f"       💔 {cf}")
            to_fix.append(p['slug'])

        print(f"\n{'─' * 80}")
        action = input(
            "  fix=исправить, dedup=убрать дубли, "
            "b=назад\n▶ "
        ).strip().lower()

        if action in ('b', 'back', 'назад', ''):
            return []

        if action == 'fix':
            for p in problems:
                col_dir = get_col_dir(p['slug'])
                images_dir = os.path.join(col_dir, 'images')

                for cf in p['corrupt_files']:
                    fp = os.path.join(images_dir, cf)
                    if os.path.exists(fp):
                        os.remove(fp)
                        print(f"  🗑 {p['slug']}/{cf}")

                if (p['missing_images'] > 0 or
                        p['corrupt_files']):
                    progress_path = os.path.join(
                        col_dir, '_progress.json'
                    )
                    if os.path.exists(progress_path):
                        try:
                            with open(
                                progress_path, 'r',
                                encoding='utf-8'
                            ) as f:
                                progress = json.load(f)

                            metadata = progress.get(
                                'metadata', []
                            )
                            urls_to_retry = set()
                            corrupt_set = set(
                                p['corrupt_files']
                            )

                            for item in metadata:
                                need = False
                                for fname in item.get(
                                    'downloaded_files', []
                                ):
                                    fp = os.path.join(
                                        images_dir, fname
                                    )
                                    if (fname in corrupt_set or
                                            not os.path.exists(
                                                fp
                                            ) or
                                            not is_valid_image(
                                                fp
                                            )):
                                        need = True
                                        break

                                if not need:
                                    for rel in item.get(
                                        'related_items', []
                                    ):
                                        rf = rel.get('file', '')
                                        if rf:
                                            fp = os.path.join(
                                                images_dir, rf
                                            )
                                            if (
                                                rf in corrupt_set
                                                or not
                                                os.path.exists(fp)
                                                or not
                                                is_valid_image(fp)
                                            ):
                                                need = True
                                                break

                                if need:
                                    urls_to_retry.add(
                                        item['url']
                                    )

                            processed = set(
                                progress.get(
                                    'processed_urls', []
                                )
                            )
                            processed -= urls_to_retry

                            new_meta = [
                                m for m in metadata
                                if m['url'] not in urls_to_retry
                            ]

                            progress['processed_urls'] = list(
                                processed
                            )
                            progress['metadata'] = new_meta

                            atomic_write_json(
                                progress_path, progress
                            )
                            print(
                                f"  🔄 {p['slug']}: "
                                f"{len(urls_to_retry)} "
                                f"на перекачку"
                            )

                        except Exception as e:
                            logger.warning(
                                f"  ⚠ Ошибка fix {p['slug']}: "
                                f"{e}"
                            )

            return to_fix

        elif action == 'dedup':
            for p in problems:
                images_dir = os.path.join(
                    get_col_dir(p['slug']), 'images'
                )
                for df in p['duplicate_files']:
                    fp = os.path.join(images_dir, df)
                    if os.path.exists(fp):
                        os.remove(fp)
                        print(
                            f"  🗑 дубль: {p['slug']}/{df}"
                        )
            return []

        return []


# ═══════════════════════════════════════════════
# АНТИБОТ
# ═══════════════════════════════════════════════

class AntiBotChecker:
    @staticmethod
    def check(session):
        results = {
            'has_rate_limit': False,
            'recommended_delay': DELAY_PAGE,
            'recommended_threads': IMAGE_THREADS,
        }
        logger.info("🔍 Проверка антибот-защиты...")
        try:
            r1 = session.get(BASE_URL, timeout=15)
            server = r1.headers.get('server', '').lower()
            if ('cloudflare' in server or
                    r1.headers.get('cf-ray', '')):
                logger.info("  ⚠ Cloudflare обнаружен")
                results['recommended_delay'] = max(
                    DELAY_PAGE, 0.5
                )
                results['recommended_threads'] = min(
                    IMAGE_THREADS, 5
                )
                return results

            if (r1.status_code == 503 or
                    'captcha' in r1.text.lower()):
                results['recommended_delay'] = max(
                    DELAY_PAGE, 1.0
                )
                results['recommended_threads'] = min(
                    IMAGE_THREADS, 3
                )
                results['has_rate_limit'] = True
                return results

            blocked = False
            # FIX: увеличена задержка между тестами
            # чтобы сам тест не спровоцировал бан
            for _ in range(3):
                r = session.get(f"{BASE_URL}/", timeout=10)
                if r.status_code in (429, 503, 403):
                    blocked = True
                    break
                time.sleep(0.3)

            if blocked:
                results['has_rate_limit'] = True
                results['recommended_delay'] = max(
                    DELAY_PAGE, 0.5
                )
                results['recommended_threads'] = min(
                    IMAGE_THREADS, 3
                )
            else:
                results['recommended_delay'] = DELAY_PAGE
                results['recommended_threads'] = IMAGE_THREADS
                logger.info("  ✅ Защита не обнаружена")

        except Exception as e:
            logger.warning(f"  ⚠ Ошибка проверки: {e}")
            results['recommended_delay'] = max(
                DELAY_PAGE, 0.3
            )
            results['recommended_threads'] = min(
                IMAGE_THREADS, 5
            )

        return results


# ═══════════════════════════════════════════════
# ОСНОВНОЙ СКРАПЕР
# ═══════════════════════════════════════════════

class CollectionScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': (
                'text/html,application/xhtml+xml,*/*;q=0.8'
            ),
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': BASE_URL,
        })
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20, pool_maxsize=20,
            max_retries=requests.adapters.Retry(
                total=5, backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        self.delay_page = DELAY_PAGE
        self.delay_item = DELAY_ITEM
        self.image_threads = IMAGE_THREADS
        self.item_threads = ITEM_THREADS
        self.zip_manager = ZipManager()

        # FIX: Кеш URL-паттернов для каждой коллекции
        self.url_patterns_cache = {}
        # Блокировка для cache (лёгкая, отдельная от stats)
        self._cache_lock = threading.Lock()

        # FIX: Расширенная статистика с детализацией
        self.stats = {
            'collections_processed': 0,
            'items_downloaded': 0,
            'images_downloaded': 0,
            'images_skipped': 0,
            'errors': 0,              # предметы, где ВСЕ URL не сработали
            'errors_404_attempts': 0,  # каждый HTTP 404 ответ
            'errors_timeout': 0,
            'errors_connection': 0,
            'errors_html_response': 0,
            'errors_too_small': 0,
            'errors_invalid_format': 0,
            'fallback_used': 0,
        }
        self._lock = threading.Lock()

    def close(self):
        """Закрывает HTTP-сессию."""
        self.session.close()

    def request_with_retry(self, url, max_retries=3, **kwargs):
        for attempt in range(max_retries):
            try:
                resp = self.session.get(
                    url, timeout=(15, 120), **kwargs
                )
                if resp.status_code == 404:
                    return None
                if resp.status_code == 429:
                    wait = int(
                        resp.headers.get('Retry-After', 5)
                    )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    # FIX: НЕ считаем ошибку здесь — вызывающий код
                    # (download_image) сам решит, считать ли ошибку
                    return None

    # ─── Коллекции ───

    def get_all_collections(self):
        collections = []
        seen_slugs = set()

        logger.info(
            "📋 Загрузка списка коллекций с сайта..."
        )
        resp = self.request_with_retry(BASE_URL + "/")
        if not resp:
            return collections

        new = self._parse_collections_html(resp.text)
        for col in new:
            if col['slug'] not in seen_slugs:
                seen_slugs.add(col['slug'])
                collections.append(col)
        logger.info(f"  Стр.1: {len(new)} коллекций")

        soup = BeautifulSoup(resp.text, 'html.parser')
        ajax_base = None
        for script in soup.select('script'):
            text = script.get_text()
            match = re.search(
                r"ajax_page_url:\s*['\"]([^'\"]+)['\"]", text
            )
            if match:
                ajax_base = match.group(1)
                break

        patterns = []
        if ajax_base:
            patterns.append(ajax_base + "?page={page}")
        patterns.extend([
            "/@collections/?page={page}",
            "/?page={page}",
        ])

        working_pattern = None
        page = 2
        consecutive_empty = 0

        while (consecutive_empty < MAX_CONSECUTIVE_EMPTY and
               page < MAX_COLLECTION_PAGES):

            # FIX: если working_pattern не сработал — пробуем все
            if working_pattern:
                try_patterns = (
                    [working_pattern] +
                    [p for p in patterns if p != working_pattern]
                )
            else:
                try_patterns = patterns

            page_found = False

            for pat in try_patterns:
                url = BASE_URL + pat.format(page=page)
                time.sleep(self.delay_page)

                try:
                    resp = self.session.get(
                        url, timeout=(10, 30)
                    )
                except requests.exceptions.RequestException:
                    continue  # FIX: пробуем следующий паттерн

                if resp.status_code != 200:
                    continue  # FIX: пробуем следующий паттерн

                content = resp.text.strip()
                if (not content or
                        len(content) < MIN_PAGE_CONTENT_LEN):
                    continue  # FIX: пробуем следующий паттерн

                html = content
                has_next = True
                try:
                    data = resp.json()
                    html = data.get(
                        'html', data.get('items', content)
                    )
                    has_next = data.get(
                        'has_next_page',
                        data.get('has_next', True)
                    )
                    if not html:
                        html = content
                except (json.JSONDecodeError, ValueError):
                    pass

                new = self._parse_collections_html(html)
                added = 0
                for col in new:
                    if col['slug'] not in seen_slugs:
                        seen_slugs.add(col['slug'])
                        collections.append(col)
                        added += 1

                if added > 0:
                    logger.info(
                        f"  Стр.{page}: +{added} "
                        f"(итого: {len(collections)})"
                    )
                    page_found = True
                    working_pattern = pat
                    consecutive_empty = 0
                    break

                if not has_next:
                    consecutive_empty = MAX_CONSECUTIVE_EMPTY
                    break
                break

            if not page_found:
                consecutive_empty += 1

            page += 1

        logger.info(
            f"✅ Всего коллекций: {len(collections)}"
        )
        collections.sort(key=lambda c: c['count'])
        return collections

    def _parse_collections_html(self, html):
        collections = []
        soup = BeautifulSoup(html, 'html.parser')
        for li in soup.select('li.collection'):
            link = li.select_one('a.collection-shelf-link')
            if not link:
                continue
            href = link.get('href', '')
            slug = (
                href.strip('/').split('/')[-1] if href else ''
            )
            if not slug or slug in ('@', 'collections'):
                continue
            owner = li.select_one('a.collection-shelf-owner')
            count = li.select_one(
                'sup.collection-shelf-link-count'
            )
            img = li.select_one('.collection-shelf-image img')
            col = {
                'name': link.get('title', '').strip(),
                'slug': slug,
                'url': urljoin(BASE_URL, href),
                'owner': (
                    owner.get_text(strip=True) if owner else ''
                ),
                'count': 0,
                'thumb': (
                    urljoin(BASE_URL, img['src'])
                    if img and img.get('src') else ''
                ),
            }
            if count:
                try:
                    col['count'] = int(
                        re.sub(r'\D', '', count.get_text())
                    )
                except ValueError:
                    pass
            collections.append(col)
        return collections

    # ─── Предметы ───

    def get_collection_items(self, collection):
        slug = collection['slug']
        items = []
        seen = set()
        expected = collection.get('count', 0)

        logger.info(
            f"  Загрузка предметов (~{expected})..."
        )
        resp = self.request_with_retry(collection['url'])
        if not resp:
            return items

        for item in self._parse_items_html(resp.text, slug):
            if item['url'] not in seen:
                seen.add(item['url'])
                items.append(item)

        soup = BeautifulSoup(resp.text, 'html.parser')
        ajax_url = f"/collections/{slug}/@items/"
        for script in soup.select('script'):
            match = re.search(
                r"ajax_page_url:\s*['\"]([^'\"]+)['\"]",
                script.get_text()
            )
            if match:
                ajax_url = match.group(1)
                break

        has_more = bool(soup.select_one('.show-more-button'))
        if not has_more and len(items) < expected:
            has_more = True

        page = 2
        empty = 0
        while has_more and empty < MAX_ITEMS_EMPTY_PAGES:
            url = BASE_URL + ajax_url
            url += (
                ('&' if '?' in url else '?') + f"page={page}"
            )
            time.sleep(self.delay_page)

            resp = self.request_with_retry(url)
            if not resp:
                empty += 1
                page += 1
                continue

            content = resp.text.strip()
            if (not content or
                    len(content) < MIN_ITEMS_CONTENT_LEN):
                break

            html = content
            server_more = True
            try:
                data = resp.json()
                html = data.get(
                    'html', data.get('items', '')
                )
                server_more = data.get(
                    'has_next_page',
                    data.get('has_next', True)
                )
                if not html:
                    html = content
            except (json.JSONDecodeError, ValueError):
                pass

            new = self._parse_items_html(html, slug)
            if not new:
                empty += 1
                page += 1
                continue

            added = 0
            for item in new:
                if item['url'] not in seen:
                    seen.add(item['url'])
                    items.append(item)
                    added += 1

            if added > 0:
                empty = 0
                if len(items) % 200 < added:
                    logger.info(
                        f"  ... {len(items)}/{expected}"
                    )
            else:
                empty += 1

            if not server_more:
                break
            if expected > 0 and len(items) >= expected:
                break
            page += 1

        logger.info(
            f"  ✅ Предметов: {len(items)} "
            f"(ожидалось: {expected})"
        )
        return items

    def _parse_items_html(self, html, slug):
        items = []
        soup = BeautifulSoup(html, 'html.parser')
        for li in soup.select('li[data-id]'):
            a = li.select_one('a[href]')
            if not a:
                continue
            href = a.get('href', '')
            if f'/collections/{slug}/' not in href:
                continue
            title = a.get('title', '')
            if not title:
                p = a.select_one('p')
                if p:
                    title = p.get_text(strip=True)
            thumb = ''
            img = a.select_one('img')
            if img:
                thumb = (
                    img.get('src', '') or
                    img.get('data-src', '')
                )
                if thumb:
                    thumb = urljoin(BASE_URL, thumb)
            items.append({
                'url': urljoin(BASE_URL, href),
                'title': title.strip(),
                'thumb_url': thumb,
                'data_id': li.get('data-id', ''),
                'data_group': li.get('data-group', ''),
            })
        return items

    # ─── Детали ───

    def get_item_details(self, item_url):
        details = {
            'url': item_url, 'properties': {},
            'description': '', 'description_html': '',
            'image_description': '', 'images': [],
            'related_items': [],
        }

        resp = self.request_with_retry(item_url)
        if not resp:
            return details

        soup = BeautifulSoup(resp.text, 'html.parser')

        t = soup.select_one('h1, .item-title')
        if t:
            details['item_title'] = t.get_text(strip=True)

        desc_div = soup.select_one('.item-description')
        if desc_div:
            details['description'] = desc_div.get_text(
                strip=True
            )
            details['description_html'] = str(desc_div)

        img_desc = soup.select_one(
            '.item-description.image-description'
        )
        if img_desc:
            details['image_description'] = img_desc.get_text(
                strip=True
            )

        props_div = soup.select_one('.item-properties')
        if props_div:
            for p in props_div.select('p'):
                text = p.get_text(strip=True)
                if ':' not in text:
                    continue
                i = text.index(':')
                key = text[:i].strip()
                a_tags = p.select('a')
                if a_tags:
                    val = ', '.join(
                        a.get_text(strip=True) for a in a_tags
                    )
                else:
                    val = text[i + 1:].strip()
                if key:
                    details['properties'][key] = val

        seen_urls = set()

        thumbnails_ul = soup.select_one('ul.item-thumbnails')
        if thumbnails_ul:
            for li in thumbnails_ul.select('li'):
                original = li.get('data-original-src', '')
                large = li.get('data-src', '')
                img_tag = li.select_one('img')
                inline = (
                    img_tag.get('src', '') if img_tag else ''
                )

                # Берём лучший доступный URL для метаданных.
                # Реальный порядок скачивания определяет
                # _get_best_quality_url с учётом кеша.
                src = original or large or inline
                if src:
                    full = urljoin(BASE_URL, src)
                    if full not in seen_urls:
                        seen_urls.add(full)
                        details['images'].append(full)

        if not details['images']:
            main_img = soup.select_one('.item-image img')
            if main_img:
                src = (
                    main_img.get('src', '') or
                    main_img.get('data-src', '')
                )
                if src:
                    full = urljoin(BASE_URL, src)
                    if full not in seen_urls:
                        seen_urls.add(full)
                        details['images'].append(full)

        if not details['images']:
            orig = soup.select_one('a.image-original-link')
            if orig and orig.get('href'):
                full = urljoin(BASE_URL, orig['href'])
                if full not in seen_urls:
                    seen_urls.add(full)
                    details['images'].append(full)

        if desc_div:
            for cross in desc_div.select(
                'a.cross-item-link'
            ):
                href = cross.get('href', '')
                if not href:
                    continue
                cross_url = urljoin(BASE_URL, href)
                cross_img = cross.select_one('img')
                cross_thumb = ''
                cross_title = ''
                if cross_img:
                    cross_thumb = cross_img.get('src', '')
                    cross_title = cross_img.get('title', '')
                    if cross_thumb:
                        cross_thumb = urljoin(
                            BASE_URL, cross_thumb
                        )

                details['related_items'].append({
                    'url': cross_url,
                    'title': cross_title,
                    'thumb_url': cross_thumb,
                })

        return details

    def get_items_details_parallel(self, items,
                                   max_workers=None):
        if max_workers is None:
            max_workers = self.item_threads
        results = {}
        total = len(items)
        with ThreadPoolExecutor(
            max_workers=max_workers
        ) as ex:
            fmap = {
                ex.submit(
                    self.get_item_details, i['url']
                ): i
                for i in items
            }
            done = 0
            for f in as_completed(fmap):
                done += 1
                try:
                    results[fmap[f]['url']] = f.result()
                except Exception:
                    results[fmap[f]['url']] = {
                        'url': fmap[f]['url'],
                        'properties': {},
                        'description': '',
                        'images': [],
                        'image_description': '',
                        'related_items': [],
                    }
                if done % 50 == 0 or done == total:
                    logger.info(
                        f"    Детали: {done}/{total}"
                    )
        return results

    # ─── Умный fallback URL ───

    def _get_best_quality_url(self, url, slug=None):
        """
        FIX: Нормализует URL к base (thumb), затем строит список
        от лучшего к худшему С УЧЁТОМ кеша для ВСЕХ типов входа.
        """
        if not url:
            return []

        # Инициализация кеша для коллекции
        if slug:
            with self._cache_lock:
                if slug not in self.url_patterns_cache:
                    self.url_patterns_cache[slug] = {
                        'original_works': None,
                        'large_works': None,
                        'original_404_count': 0,
                        'large_404_count': 0,
                    }
                cache = dict(self.url_patterns_cache[slug])
        else:
            cache = {}

        # Определяем тип и нормализуем к thumb (base)
        if '/preloaded-items/' in url:
            base_url = url.replace(
                '/preloaded-items/', '/items-thumbs/'
            )
        elif '/items-large/' in url:
            base_url = url.replace(
                '/items-large/', '/items-thumbs/'
            )
        elif '/items-thumbs/' in url:
            base_url = url
        else:
            # Неизвестный паттерн — возвращаем как есть
            return [url]

        # Строим варианты от лучшего к худшему
        original_url = base_url.replace(
            '/items-thumbs/', '/preloaded-items/'
        )
        large_url = base_url.replace(
            '/items-thumbs/', '/items-large/'
        )
        thumb_url = base_url

        urls = []

        # FIX: кеш проверяется ЕДИНООБРАЗНО для всех типов входа
        if cache.get('original_works') is not False:
            urls.append(original_url)

        if cache.get('large_works') is not False:
            urls.append(large_url)

        # thumb — всегда последний, самый надёжный
        urls.append(thumb_url)

        # Убираем дубли, сохраняя порядок
        seen = set()
        result = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                result.append(u)

        return result

    def _update_url_pattern_cache(self, slug, url, status_code):
        """
        FIX: Обучаем кеш — какие паттерны URL работают.
        Потокобезопасно через _cache_lock.
        """
        if not slug:
            return

        with self._cache_lock:
            if slug not in self.url_patterns_cache:
                return

            cache = self.url_patterns_cache[slug]

            if '/preloaded-items/' in url:
                if status_code == 404:
                    cache['original_404_count'] += 1
                    if cache['original_404_count'] >= 3:
                        if cache['original_works'] is not False:
                            cache['original_works'] = False
                            logger.info(
                                f"  📵 {slug}: /preloaded-items/ "
                                f"отключены (3× 404)"
                            )
                elif status_code == 200:
                    cache['original_works'] = True
                    cache['original_404_count'] = 0

            elif '/items-large/' in url:
                if status_code == 404:
                    cache['large_404_count'] += 1
                    if cache['large_404_count'] >= 3:
                        if cache['large_works'] is not False:
                            cache['large_works'] = False
                            logger.info(
                                f"  📵 {slug}: /items-large/ "
                                f"отключены (3× 404)"
                            )
                elif status_code == 200:
                    cache['large_works'] = True
                    cache['large_404_count'] = 0

    # ─── Скачивание ───

    def _try_download_url(self, url, filepath, slug=None):
        """
        FIX: Потокобезопасный tmp, разные коды возврата
        для разных причин отказа, обучение кеша.

        Коды возврата (success, status):
            (True,  200) — успех
            (False, 404) — не найден
            (False, -1)  — HTML вместо изображения
            (False, -2)  — файл слишком маленький
            (False, -3)  — невалидный формат изображения
            (False, код) — другой HTTP-код ошибки
            (False, 0)   — сетевая ошибка
        """
        for attempt in range(2):
            try:
                resp = self.session.get(
                    url, timeout=(10, 60), stream=True
                )

                # Обучаем кеш
                if slug:
                    self._update_url_pattern_cache(
                        slug, url, resp.status_code
                    )

                if resp.status_code == 404:
                    with self._lock:
                        self.stats['errors_404_attempts'] += 1
                    return (False, 404)

                if resp.status_code >= 400:
                    return (False, resp.status_code)

                resp.raise_for_status()

                ct = resp.headers.get(
                    'content-type', ''
                ).lower()
                if ct and 'text/html' in ct:
                    with self._lock:
                        self.stats['errors_html_response'] += 1
                    return (False, -1)

                os.makedirs(
                    os.path.dirname(filepath), exist_ok=True
                )
                # FIX: уникальный tmp для каждого потока
                tid = threading.get_ident()
                tmp = f"{filepath}.tmp.{tid}"
                size = 0
                with open(tmp, 'wb') as f:
                    for chunk in resp.iter_content(
                        chunk_size=32768
                    ):
                        f.write(chunk)
                        size += len(chunk)

                if size < MIN_FILE_SIZE:
                    with self._lock:
                        self.stats['errors_too_small'] += 1
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
                    return (False, -2)

                if is_valid_image(tmp):
                    os.replace(tmp, filepath)
                    with self._lock:
                        self.stats['images_downloaded'] += 1
                    return (True, 200)
                else:
                    with self._lock:
                        self.stats['errors_invalid_format'] += 1
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
                    return (False, -3)

            except requests.exceptions.ConnectionError:
                with self._lock:
                    self.stats['errors_connection'] += 1
                if attempt < 1:
                    time.sleep(2)
                    continue
                return (False, 0)

            except requests.exceptions.Timeout:
                with self._lock:
                    self.stats['errors_timeout'] += 1
                if attempt < 1:
                    time.sleep(3)
                    continue
                return (False, 0)

            except requests.exceptions.RequestException:
                return (False, 0)

            except Exception as e:
                logger.debug(
                    f"    Неожиданная ошибка скачивания: {e}"
                )
                return (False, 0)

        return (False, 0)

    def download_image(self, url, filepath,
                       fallback_urls=None, slug=None):
        """
        FIX: Использует _get_best_quality_url как основной
        источник порядка URL (учитывает кеш).
        Считает ошибку только если ВСЕ URL не сработали.
        """
        if os.path.exists(filepath) and is_valid_image(filepath):
            with self._lock:
                self.stats['images_skipped'] += 1
            return True

        # FIX: строим умный список URL с учётом кеша
        all_urls = self._get_best_quality_url(url, slug)
        if not all_urls:
            all_urls = [url]

        # Добавляем переданные fallback (дедупликация)
        if fallback_urls:
            seen = set(all_urls)
            for fu in fallback_urls:
                if fu not in seen:
                    seen.add(fu)
                    all_urls.append(fu)

        used_fallback = False
        for url_attempt in all_urls:
            success, status = self._try_download_url(
                url_attempt, filepath, slug
            )

            if success:
                if used_fallback:
                    with self._lock:
                        self.stats['fallback_used'] += 1
                return True

            used_fallback = True
            # 404 и другие ошибки — пробуем следующий URL

        # FIX: ошибка только когда ВСЕ URL исчерпаны
        with self._lock:
            self.stats['errors'] += 1

        return False

    def download_images_batch(self, tasks):
        """
        tasks: list of (url, filepath[, fallback_urls[, slug]])
        """
        if not tasks:
            return {}

        total = len(tasks)
        results = {}

        with ThreadPoolExecutor(
            max_workers=self.image_threads
        ) as ex:
            futures = {}
            for task in tasks:
                # FIX: единообразная распаковка кортежей
                url = task[0]
                fp = task[1]
                fallbacks = task[2] if len(task) > 2 else None
                slug = task[3] if len(task) > 3 else None

                future = ex.submit(
                    self.download_image, url, fp,
                    fallbacks, slug
                )
                futures[future] = (url, fp)

            done = 0
            for future in as_completed(futures):
                done += 1
                url, fp = futures[future]
                try:
                    results[url] = future.result()
                except Exception:
                    results[url] = False
                if done % 50 == 0 or done == total:
                    ok = sum(
                        1 for v in results.values() if v
                    )
                    logger.info(
                        f"    Скачано: {done}/{total} "
                        f"(✅ {ok})"
                    )

        ok = sum(1 for v in results.values() if v)
        fail = total - ok
        if fail > 0:
            logger.warning(
                f"    ⚠ Не удалось: {fail}/{total}"
            )
        return results

    def _build_download_tasks(self, item, det, idx, fname,
                              base_name, use_folder,
                              item_folder, images_dir, slug):
        """
        Формирует задачи скачивания. Имена файлов НЕ МЕНЯЮТСЯ.
        FIX: упрощена передача URL — download_image сам
        вызывает _get_best_quality_url с кешем.
        """
        files = []
        related_files = []
        dl_tasks = []

        # Основные изображения
        for img_idx, img_url in enumerate(
            det.get('images', [])
        ):
            ext = get_extension(img_url)
            if use_folder:
                if len(det['images']) == 1:
                    img_fname = f"main{ext}"
                else:
                    img_fname = f"{img_idx + 1:02d}{ext}"
                fpath = os.path.join(item_folder, img_fname)
                rel_path = os.path.join(base_name, img_fname)
            else:
                fpath = os.path.join(images_dir, fname)
                rel_path = fname

            files.append(rel_path)
            if not (os.path.exists(fpath) and
                    is_valid_image(fpath)):
                # FIX: download_image сам построит умный список
                dl_tasks.append(
                    (img_url, fpath, None, slug)
                )

        # Связанные предметы
        for ri, related in enumerate(
            det.get('related_items', [])
        ):
            r_title = safe_filename(
                related.get('title', f'related_{ri + 1}'), 60
            )
            thumb_url = related.get('thumb_url', '')
            if thumb_url:
                # quality_urls только для метаданных
                quality_urls = self._get_best_quality_url(
                    thumb_url, slug
                )
                ext = get_extension(thumb_url)
                r_fname = (
                    f"related_{ri + 1:02d}_{r_title}{ext}"
                )
                fpath = os.path.join(item_folder, r_fname)
                rel_path = os.path.join(base_name, r_fname)

                related_files.append({
                    'file': rel_path,
                    'title': related.get('title', ''),
                    'url': related.get('url', ''),
                    'quality_urls': quality_urls,
                })

                if not (os.path.exists(fpath) and
                        is_valid_image(fpath)):
                    # FIX: download_image сам построит умный список
                    dl_tasks.append(
                        (thumb_url, fpath, None, slug)
                    )

        # Миниатюра как fallback
        if (not det.get('images') and
                not det.get('related_items') and
                item.get('thumb_url')):
            fpath = os.path.join(images_dir, fname)
            files.append(fname)
            if not (os.path.exists(fpath) and
                    is_valid_image(fpath)):
                # FIX: download_image сам построит умный список
                dl_tasks.append(
                    (item['thumb_url'], fpath, None, slug)
                )

        return files, related_files, dl_tasks

    # ─── Имя файла ───
    # НЕ МЕНЯТЬ — уже скачанные файлы зависят от этой логики

    def make_filename(self, index, item, details):
        props = details.get('properties', {})
        title = (
            item.get('title', '') or
            details.get('item_title', '')
        )
        parts = [f"{index:05d}"]

        year = ''
        for k in props:
            if k.lower() in (
                'год', 'year', 'дата', 'date'
            ):
                year = props[k]
                break

        if not year and item.get('data_group', ''):
            dg = item['data_group'].strip()
            if re.match(r'^\d{2,4}$', dg):
                year = dg

        if year:
            parts.append(safe_filename(str(year), 10))
        if title:
            parts.append(safe_filename(title, 50))

        name = '_'.join(parts)
        ext = '.jpg'
        if details.get('images'):
            ext = get_extension(details['images'][0])
        return name[:MAX_FILENAME_LEN] + ext

    # ─── Метаданные ───

    def save_metadata(self, col_dir, metadata, col_info):
        atomic_write_json(
            os.path.join(col_dir, 'metadata.json'),
            {
                'collection': col_info,
                'scraped_at': datetime.now().isoformat(),
                'total_items': len(metadata),
                'items': metadata,
            },
            indent=2,
        )

        if not metadata:
            return

        all_keys = []
        seen = set()
        for m in metadata:
            for k in m.get('properties', {}).keys():
                if k not in seen:
                    seen.add(k)
                    all_keys.append(k)

        fixed = [
            'index', 'filename', 'title', 'url',
            'data_id', 'data_group', 'description',
        ]
        tail = [
            'images', 'image_description',
            'related_count'
        ]

        csv_path = os.path.join(col_dir, 'metadata.csv')
        with open(
            csv_path, 'w', encoding='utf-8-sig', newline=''
        ) as f:
            w = csv.DictWriter(
                f, fieldnames=fixed + all_keys + tail,
                extrasaction='ignore', restval=''
            )
            w.writeheader()
            for m in metadata:
                row = {k: m.get(k, '') for k in fixed}
                row['images'] = ' | '.join(
                    m.get('images', [])
                )
                row['image_description'] = m.get(
                    'image_description', ''
                )
                row['related_count'] = len(
                    m.get('related_items', [])
                )
                for k in all_keys:
                    row[k] = m.get(
                        'properties', {}
                    ).get(k, '')
                w.writerow(row)

        coverage = {}
        for k in all_keys:
            cnt = sum(
                1 for m in metadata
                if m.get('properties', {}).get(k, '') != ''
            )
            coverage[k] = {
                'count': cnt,
                'pct': round(cnt / len(metadata) * 100, 1),
            }

        atomic_write_json(
            os.path.join(col_dir, 'fields_info.json'),
            {
                'tags': all_keys,
                'coverage': coverage,
                'total': len(metadata),
            },
            indent=2,
        )

    # ─── Обработка ───

    def process_collections(self, to_process):
        total = sum(c['count'] for c in to_process)
        logger.info(
            f"\n📋 К обработке: {len(to_process)} коллекций, "
            f"~{total} предметов"
        )

        for i, col in enumerate(to_process, 1):
            logger.info(f"\n[{i}/{len(to_process)}] →")
            try:
                self._process_one(col)
            except KeyboardInterrupt:
                logger.info(
                    "\n⚠ Ctrl+C — прогресс сохранён"
                )
                break
            except Exception as e:
                logger.error(
                    f"Ошибка: {col['name']}: {e}"
                )
                logger.error(traceback.format_exc())

        self._print_stats()

    def _process_one(self, collection):
        slug = collection['slug']
        name = collection['name']
        col_dir = get_col_dir(slug)
        images_dir = os.path.join(col_dir, 'images')
        os.makedirs(images_dir, exist_ok=True)

        logger.info(f"\n{'═' * 70}")
        logger.info(
            f"📁 {name} ({collection['count']} шт.)"
        )
        logger.info(f"{'═' * 70}")

        progress_path = os.path.join(
            col_dir, '_progress.json'
        )
        all_meta = []
        processed = set()

        if os.path.exists(progress_path):
            try:
                with open(
                    progress_path, 'r', encoding='utf-8'
                ) as f:
                    p = json.load(f)
                processed = set(
                    p.get('processed_urls', [])
                )
                all_meta = p.get('metadata', [])
                logger.info(
                    f"  ▶ Продолжение: "
                    f"{len(processed)} готово"
                )
            except Exception as e:
                logger.warning(
                    f"  ⚠ Ошибка чтения прогресса: {e}"
                )

        atomic_write_json(
            os.path.join(col_dir, 'collection_info.json'),
            collection, indent=2,
        )

        items = self.get_collection_items(collection)
        if not items:
            return

        new_items = [
            i for i in items if i['url'] not in processed
        ]
        logger.info(
            f"  Всего: {len(items)}, "
            f"новых: {len(new_items)}"
        )

        if not new_items:
            logger.info(f"  ✅ Всё скачано")
            self.save_metadata(
                col_dir, all_meta, collection
            )
            self.stats['collections_processed'] += 1
            return

        # FIX: Инициализируем кеш URL-паттернов для коллекции
        with self._cache_lock:
            if slug not in self.url_patterns_cache:
                self.url_patterns_cache[slug] = {
                    'original_works': None,
                    'large_works': None,
                    'original_404_count': 0,
                    'large_404_count': 0,
                }

        batch_size = BATCH_SIZE
        next_idx = max(
            (m.get('index', 0) for m in all_meta),
            default=0
        ) + 1

        for bs in range(0, len(new_items), batch_size):
            batch = new_items[bs:bs + batch_size]
            logger.info(
                f"\n  📥 Пакет "
                f"{bs + 1}-{bs + len(batch)} "
                f"из {len(new_items)}"
            )

            details_map = self.get_items_details_parallel(
                batch
            )
            all_dl_tasks = []
            batch_meta = []

            for item in batch:
                det = details_map.get(item['url'], {
                    'url': item['url'],
                    'properties': {},
                    'description': '',
                    'images': [],
                    'image_description': '',
                    'related_items': [],
                })

                idx = next_idx
                next_idx += 1
                fname = self.make_filename(idx, item, det)
                base_name = os.path.splitext(fname)[0]

                has_related = len(
                    det.get('related_items', [])
                ) > 0
                has_multi = len(
                    det.get('images', [])
                ) > 1
                use_folder = has_related or has_multi

                if use_folder:
                    item_folder = os.path.join(
                        images_dir, base_name
                    )
                    os.makedirs(item_folder, exist_ok=True)
                else:
                    item_folder = images_dir

                files, related_files, dl_tasks = \
                    self._build_download_tasks(
                        item, det, idx, fname, base_name,
                        use_folder, item_folder, images_dir,
                        slug
                    )

                all_dl_tasks.extend(dl_tasks)

                # _info.json для папки
                if use_folder:
                    item_info = {
                        'index': idx,
                        'title': item.get('title', ''),
                        'url': item['url'],
                        'description': det.get(
                            'description', ''
                        ),
                        'description_html': det.get(
                            'description_html', ''
                        ),
                        'properties': det.get(
                            'properties', {}
                        ),
                        'related_items': det.get(
                            'related_items', []
                        ),
                    }
                    info_path = os.path.join(
                        item_folder, '_info.json'
                    )
                    atomic_write_json(
                        info_path, item_info, indent=2
                    )

                batch_meta.append({
                    'index': idx,
                    'filename': fname,
                    'title': item.get('title', ''),
                    'url': item['url'],
                    'data_id': item.get('data_id', ''),
                    'data_group': item.get(
                        'data_group', ''
                    ),
                    'description': det.get(
                        'description', ''
                    ),
                    'description_html': det.get(
                        'description_html', ''
                    ),
                    'image_description': det.get(
                        'image_description', ''
                    ),
                    'properties': det.get(
                        'properties', {}
                    ),
                    'images': det.get('images', []),
                    'downloaded_files': files,
                    'related_items': related_files,
                    'has_folder': use_folder,
                })

            # Скачивание
            if all_dl_tasks:
                logger.info(
                    f"    📸 Задач: {len(all_dl_tasks)}"
                )
                self.download_images_batch(all_dl_tasks)
            else:
                logger.info(f"    📸 Все скачаны")

            all_meta.extend(batch_meta)
            for m in batch_meta:
                processed.add(m['url'])
            with self._lock:
                self.stats['items_downloaded'] += len(
                    batch_meta
                )

            self._save_progress(
                progress_path, processed, all_meta
            )
            logger.info(
                f"  💾 {len(processed)}/{len(items)}"
            )

        self.save_metadata(col_dir, all_meta, collection)
        self.stats['collections_processed'] += 1
        logger.info(
            f"  🏁 '{name}': {len(all_meta)} предметов"
        )

    def _save_progress(self, path, urls, meta):
        atomic_write_json(path, {
            'processed_urls': list(urls),
            'metadata': meta,
            'saved_at': datetime.now().isoformat(),
        })

    def _print_stats(self):
        """FIX: Детализированная статистика с корректными метками."""
        s = self.stats
        total_items = s['images_downloaded'] + s['errors']
        success_rate = (
            s['images_downloaded'] / max(total_items, 1) * 100
        )

        logger.info(f"\n{'═' * 70}")
        logger.info(f"📊 СТАТИСТИКА")
        logger.info(f"{'─' * 70}")
        logger.info(
            f"  Коллекций:    {s['collections_processed']}"
        )
        logger.info(
            f"  Предметов:    {s['items_downloaded']}"
        )
        logger.info(
            f"  Скачано:      {s['images_downloaded']}"
        )
        logger.info(
            f"  Пропущено:    {s['images_skipped']} "
            f"(уже были)"
        )
        logger.info(
            f"  Fallback:     {s['fallback_used']} "
            f"(скачано не с первого URL)"
        )
        logger.info(
            f"  Успешность:   {success_rate:.1f}%"
        )
        logger.info(f"{'─' * 70}")
        logger.info(
            f"  ❌ Файлы где все URL провалились: "
            f"{s['errors']}"
        )
        logger.info(f"  Детализация попыток:")
        logger.info(
            f"    404 (нет на сервере):  "
            f"{s['errors_404_attempts']}"
        )
        logger.info(
            f"    HTML вместо картинки:  "
            f"{s['errors_html_response']}"
        )
        logger.info(
            f"    Слишком маленький:     "
            f"{s['errors_too_small']}"
        )
        logger.info(
            f"    Невалидный формат:     "
            f"{s['errors_invalid_format']}"
        )
        logger.info(
            f"    Timeout:               "
            f"{s['errors_timeout']}"
        )
        logger.info(
            f"    Connection:            "
            f"{s['errors_connection']}"
        )
        logger.info("═" * 70)

    # ─── Сканирование ───

    def scan_for_related(self, collections):
        print("\n" + "=" * 80)
        print("🔍 СКАНИРОВАНИЕ: доп. изображения")
        print("=" * 80)

        results = []

        for i, col in enumerate(collections, 1):
            slug = col['slug']
            name = col['name']

            print(
                f"  [{i}/{len(collections)}] "
                f"{name[:50]}...",
                end=" ", flush=True
            )

            resp = self.request_with_retry(col['url'])
            if not resp:
                print("❌")
                continue

            items = self._parse_items_html(resp.text, slug)
            if not items:
                print("пусто")
                continue

            sample = items[:SCAN_SAMPLE_SIZE]
            with_cross = 0
            with_multi = 0
            total_cross = 0
            total_extra = 0

            for item in sample:
                time.sleep(self.delay_item)
                r2 = self.request_with_retry(item['url'])
                if not r2:
                    continue
                soup = BeautifulSoup(
                    r2.text, 'html.parser'
                )

                cl = soup.select('a.cross-item-link')
                if cl:
                    with_cross += 1
                    total_cross += len(cl)

                th = soup.select_one('ul.item-thumbnails')
                if th:
                    pc = len(th.select('li'))
                    if pc > 1:
                        with_multi += 1
                        total_extra += pc - 1

            checked = len(sample)
            if with_cross > 0 or with_multi > 0:
                cr = (
                    with_cross / checked if checked else 0
                )
                mr = (
                    with_multi / checked if checked else 0
                )
                ac = (
                    total_cross / with_cross
                    if with_cross else 0
                )
                am = (
                    total_extra / with_multi
                    if with_multi else 0
                )

                est = int(
                    cr * col['count'] * ac +
                    mr * col['count'] * am
                )

                results.append({
                    'slug': slug, 'name': name,
                    'count': col['count'],
                    'items_with_cross': with_cross,
                    'items_with_multi': with_multi,
                    'est_total_extra': est,
                })
                print(f"✅ ~{est} доп.")
            else:
                print("—")

        if results:
            results.sort(
                key=lambda r: -r['est_total_extra']
            )
            print(f"\n{'─' * 60}")
            for i, r in enumerate(results, 1):
                print(
                    f"  {i:3d}. {r['name'][:40]:<42s} "
                    f"[{r['count']:>5d}] "
                    f"~{r['est_total_extra']} доп."
                )

            report_path = os.path.join(
                METADATA_DIR, '_related_scan.json'
            )
            atomic_write_json(report_path, {
                'scanned_at': datetime.now().isoformat(),
                'results': results,
            }, indent=2)

        return results


# ═══════════════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════════════

if __name__ == '__main__':
    logger.info("🚀 COLLECTIONERUS SCRAPER v10")
    ensure_dirs()

    scraper = CollectionScraper()

    try:
        antibot = AntiBotChecker.check(scraper.session)
        scraper.delay_page = antibot['recommended_delay']
        scraper.image_threads = antibot['recommended_threads']
        scraper.item_threads = max(
            antibot['recommended_threads'] // 2, 2
        )

        logger.info(
            f"⚙ Задержка: {scraper.delay_page}s, "
            f"потоки: img={scraper.image_threads} "
            f"item={scraper.item_threads}"
        )

        menu = MenuManager(scraper)
        menu.run_main_loop()
    finally:
        scraper.close()