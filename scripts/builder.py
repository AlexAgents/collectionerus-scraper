#!/usr/bin/env python3
"""
builder.py — Сборка EXE-файлов с иконками.

Использует PyInstaller для создания:
  - collectionerus-scraper.exe (консольное)
  - collectionerus-viewer.exe  (GUI / Flask)

Запуск: python scripts/builder.py

Требования:
  pip install pyinstaller pillow
"""

import sys
import os
import subprocess
import struct
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    PROJECT_ROOT, SCRIPTS_DIR, ASSETS_DIR,
    SCRAPER_ICON, VIEWER_ICON,
    ensure_dirs,
)


# ═══════════════════════════════════════════════
# Генерация иконок (если нет готовых)
# ═══════════════════════════════════════════════

def generate_default_icons():
    """
    Генерирует простые .ico файлы если их нет в assets/.
    Использует Pillow если доступен, иначе — минимальный ICO.
    """
    ensure_dirs()

    icons_needed = []
    if not os.path.exists(SCRAPER_ICON):
        icons_needed.append(('scraper', SCRAPER_ICON, (52, 152, 219)))
    if not os.path.exists(VIEWER_ICON):
        icons_needed.append(('viewer', VIEWER_ICON, (46, 204, 113)))

    if not icons_needed:
        print("  ✅ Иконки уже существуют")
        return True

    try:
        from PIL import Image, ImageDraw
        return _generate_with_pillow(icons_needed)
    except ImportError:
        print("  ⚠ Pillow не установлен, создаю минимальные иконки")
        return _generate_minimal_ico(icons_needed)


def _generate_with_pillow(icons_needed):
    """Генерирует красивые иконки через Pillow."""
    from PIL import Image, ImageDraw

    for name, path, color in icons_needed:
        sizes = [16, 32, 48, 64, 128, 256]
        images = []

        for size in sizes:
            img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)

            margin = max(1, size // 16)
            draw.rounded_rectangle(
                [margin, margin, size - margin - 1, size - margin - 1],
                radius=max(2, size // 6),
                fill=color + (255,),
            )

            letter = 'S' if name == 'scraper' else 'V'
            try:
                draw.text(
                    (size // 2, size // 2),
                    letter,
                    fill=(255, 255, 255, 255),
                    anchor='mm',
                )
            except Exception:
                inner = size // 4
                draw.ellipse(
                    [inner, inner, size - inner, size - inner],
                    fill=(255, 255, 255, 200),
                )

            images.append(img)

        images[0].save(
            path, format='ICO',
            sizes=[(s, s) for s in sizes],
            append_images=images[1:],
        )
        print(f"  ✅ Создана иконка: {os.path.basename(path)}")

    return True


def _generate_minimal_ico(icons_needed):
    """
    Генерирует минимальный валидный .ico БЕЗ Pillow.
    32x32 пикселя, BMP формат внутри ICO.
    """
    for name, path, color in icons_needed:
        r, g, b = color
        size = 32

        pixels = bytearray()
        for y in range(size):
            for x in range(size):
                border = 3
                if (border <= x < size - border and
                        border <= y < size - border):
                    pixels.extend([b, g, r, 255])
                else:
                    pixels.extend([0, 0, 0, 0])

        mask_row_bytes = (size + 31) // 32 * 4
        and_mask = bytearray(mask_row_bytes * size)

        bmp_header = struct.pack(
            '<IiiHHIIiiII',
            40, size, size * 2, 1, 32, 0,
            len(pixels) + len(and_mask),
            0, 0, 0, 0,
        )

        image_data = bmp_header + bytes(pixels) + bytes(and_mask)

        ico_header = struct.pack('<HHH', 0, 1, 1)
        data_offset = 6 + 16
        ico_entry = struct.pack(
            '<BBBBHHII',
            size, size, 0, 0, 1, 32,
            len(image_data), data_offset,
        )

        with open(path, 'wb') as f:
            f.write(ico_header)
            f.write(ico_entry)
            f.write(image_data)

        print(f"  ✅ Создана иконка (минимальная): {os.path.basename(path)}")

    return True


# ═══════════════════════════════════════════════
# Конфигурации сборки
# ═══════════════════════════════════════════════

def get_build_configs():
    """Возвращает конфигурации сборки."""
    return {
        'scraper': {
            'script': os.path.join(SCRIPTS_DIR, 'scraper.py'),
            'name': 'collectionerus-scraper',
            'icon': SCRAPER_ICON,
            'console': True,
            'hidden_imports': [
                'requests', 'bs4', 'urllib3', 'charset_normalizer',
                'certifi', 'idna', 'html.parser',
            ],
        },
        'viewer': {
            'script': os.path.join(SCRIPTS_DIR, 'viewer.py'),
            'name': 'collectionerus-viewer',
            'icon': VIEWER_ICON,
            'console': True,  # Flask нужна консоль для вывода адреса
            'hidden_imports': [
                'flask', 'jinja2', 'markupsafe', 'werkzeug',
                'click', 'itsdangerous', 'blinker',
                'requests', 'bs4', 'urllib3', 'charset_normalizer',
                'certifi', 'idna',
            ],
        },
    }


# ═══════════════════════════════════════════════
# Проверки
# ═══════════════════════════════════════════════

def check_pyinstaller():
    """Проверяет наличие PyInstaller."""
    try:
        import PyInstaller
        version = PyInstaller.__version__
        print(f"  ✅ PyInstaller {version}")
        return True
    except ImportError:
        print("  ❌ PyInstaller не установлен!")
        return False


def install_pyinstaller():
    """Устанавливает PyInstaller."""
    print("\n  Установка PyInstaller...")
    try:
        subprocess.run(
            [sys.executable, '-m', 'pip', 'install', 'pyinstaller'],
            check=True,
            capture_output=True,
            text=True,
        )
        print("  ✅ PyInstaller установлен")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ❌ Ошибка установки: {e}")
        return False


# ═══════════════════════════════════════════════
# Сборка EXE
# ═══════════════════════════════════════════════

def build_exe(config_name, onefile=True):
    """Собирает один EXE-файл."""
    configs = get_build_configs()

    if config_name not in configs:
        print(f"  ❌ Неизвестная конфигурация: {config_name}")
        return False

    cfg = configs[config_name]
    script_path = cfg['script']
    exe_name = cfg['name']
    icon_path = cfg['icon']

    if not os.path.exists(script_path):
        print(f"  ❌ Скрипт не найден: {script_path}")
        return False

    print(f"\n{'═' * 60}")
    print(f"  🔨 Сборка: {exe_name}")
    print(f"     Скрипт: {os.path.relpath(script_path, PROJECT_ROOT)}")
    print(f"     Режим:  {'--onefile' if onefile else '--onedir'}")
    if os.path.exists(icon_path):
        print(f"     Иконка: {os.path.relpath(icon_path, PROJECT_ROOT)}")
    print(f"{'═' * 60}")

    dist_dir = os.path.join(PROJECT_ROOT, 'dist')
    build_dir = os.path.join(PROJECT_ROOT, 'build')

    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--noconfirm',
        '--clean',
        f'--name={exe_name}',
        f'--distpath={dist_dir}',
        f'--workpath={build_dir}',
        f'--specpath={build_dir}',
    ]

    if onefile:
        cmd.append('--onefile')
    else:
        cmd.append('--onedir')

    if cfg['console']:
        cmd.append('--console')
    else:
        cmd.append('--windowed')

    if os.path.exists(icon_path):
        cmd.append(f'--icon={icon_path}')

    # Добавляем config.py и utils.py как данные
    for module_name in ('config.py', 'utils.py'):
        module_path = os.path.join(PROJECT_ROOT, module_name)
        if os.path.exists(module_path):
            cmd.append(f'--add-data={module_path}{os.pathsep}.')

    # Добавляем assets/
    if os.path.exists(ASSETS_DIR):
        cmd.append(f'--add-data={ASSETS_DIR}{os.pathsep}assets')

    # Шаблоны и статика для viewer
    if config_name == 'viewer':
        templates_dir = os.path.join(PROJECT_ROOT, 'templates')
        static_dir = os.path.join(PROJECT_ROOT, 'static')
        if os.path.exists(templates_dir):
            cmd.append(f'--add-data={templates_dir}{os.pathsep}templates')
        if os.path.exists(static_dir):
            cmd.append(f'--add-data={static_dir}{os.pathsep}static')

    # Hidden imports
    for hi in cfg.get('hidden_imports', []):
        cmd.append(f'--hidden-import={hi}')

    cmd.append(script_path)

    print(f"\n  ⏳ Сборка... (это займёт 1-3 минуты)")

    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=600,
        )

        if result.returncode == 0:
            if onefile:
                exe_path = os.path.join(dist_dir, f"{exe_name}.exe")
            else:
                exe_path = os.path.join(dist_dir, exe_name, f"{exe_name}.exe")

            if os.path.exists(exe_path):
                size_mb = os.path.getsize(exe_path) / 1024 / 1024
                print(f"\n  ✅ Готово: {exe_path}")
                print(f"     Размер: {size_mb:.1f} MB")
                return True
            else:
                print(f"\n  ⚠ PyInstaller завершился, но EXE не найден")
                print(f"     Проверьте: {dist_dir}")
                return False
        else:
            print(f"\n  ❌ Ошибка сборки (код {result.returncode})!")
            if result.stderr:
                lines = result.stderr.strip().split('\n')
                print(f"\n  Последние строки ошибки:")
                for line in lines[-25:]:
                    print(f"    {line}")
            return False

    except subprocess.TimeoutExpired:
        print(f"\n  ❌ Таймаут сборки (>10 минут)")
        return False
    except FileNotFoundError:
        print(f"\n  ❌ Python или PyInstaller не найден")
        return False
    except Exception as e:
        print(f"\n  ❌ Ошибка: {e}")
        return False


def clean_build():
    """Удаляет временные файлы сборки."""
    cleaned = False

    build_dir = os.path.join(PROJECT_ROOT, 'build')
    if os.path.exists(build_dir):
        shutil.rmtree(build_dir, ignore_errors=True)
        print("  🗑 Удалена папка build/")
        cleaned = True

    # .spec файлы в корне
    for f in os.listdir(PROJECT_ROOT):
        if f.endswith('.spec'):
            os.remove(os.path.join(PROJECT_ROOT, f))
            print(f"  🗑 Удалён {f}")
            cleaned = True

    # __pycache__
    for root, dirs, files in os.walk(PROJECT_ROOT):
        for d in dirs:
            if d == '__pycache__':
                path = os.path.join(root, d)
                shutil.rmtree(path, ignore_errors=True)
                rel = os.path.relpath(path, PROJECT_ROOT)
                print(f"  🗑 Удалён {rel}")
                cleaned = True

    if not cleaned:
        print("  ✅ Нечего удалять")


def clean_dist():
    """Удаляет собранные EXE."""
    dist_dir = os.path.join(PROJECT_ROOT, 'dist')
    if os.path.exists(dist_dir):
        shutil.rmtree(dist_dir, ignore_errors=True)
        print("  🗑 Удалена папка dist/")
    else:
        print("  ✅ Папка dist/ не существует")


# ═══════════════════════════════════════════════
# Интерактивное меню
# ═══════════════════════════════════════════════

def show_status():
    """Показывает статус сборки."""
    dist_dir = os.path.join(PROJECT_ROOT, 'dist')

    configs = get_build_configs()
    statuses = {}

    for key, cfg in configs.items():
        exe_path = os.path.join(dist_dir, f"{cfg['name']}.exe")
        if os.path.exists(exe_path):
            size_mb = os.path.getsize(exe_path) / 1024 / 1024
            mtime = os.path.getmtime(exe_path)
            from datetime import datetime
            dt = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
            statuses[key] = f"✅ {size_mb:.1f} MB ({dt})"
        else:
            statuses[key] = "⬚ не собран"

    # Иконки
    ico_scraper = "✅" if os.path.exists(SCRAPER_ICON) else "⬚"
    ico_viewer = "✅" if os.path.exists(VIEWER_ICON) else "⬚"

    return statuses, ico_scraper, ico_viewer


def interactive_menu():
    """Главное меню builder."""
    print("\n  📁 Проверка иконок...")
    generate_default_icons()

    while True:
        statuses, ico_s, ico_v = show_status()

        print(f"\n{'═' * 60}")
        print(f"  🔨 BUILDER — Сборка EXE-файлов")
        print(f"{'═' * 60}")
        print(f"  Иконки: scraper {ico_s}  viewer {ico_v}")
        print(f"{'─' * 60}")
        print(f"  {statuses.get('scraper', '?')}  1. Собрать scraper.exe")
        print(f"  {statuses.get('viewer', '?')}  2. Собрать viewer.exe")
        print(f"                      3. Собрать оба")
        print(f"{'─' * 60}")
        print(f"                      4. Перегенерировать иконки")
        print(f"                      5. Очистить build/ (временные)")
        print(f"                      6. Очистить dist/ (EXE-файлы)")
        print(f"                      7. Очистить всё")
        print(f"                      q. Выход")
        print(f"{'─' * 60}")

        choice = input("\n  ▶ Выбор: ").strip().lower()

        if choice in ('q', 'quit', 'exit', 'й'):
            break
        elif choice == '1':
            build_exe('scraper')
        elif choice == '2':
            build_exe('viewer')
        elif choice == '3':
            ok1 = build_exe('scraper')
            ok2 = build_exe('viewer')
            if ok1 and ok2:
                print(f"\n  ✅ Оба EXE собраны успешно!")
        elif choice == '4':
            for p in (SCRAPER_ICON, VIEWER_ICON):
                if os.path.exists(p):
                    os.remove(p)
            generate_default_icons()
        elif choice == '5':
            clean_build()
        elif choice == '6':
            clean_dist()
        elif choice == '7':
            clean_build()
            clean_dist()
        else:
            print("  ⚠ Неизвестная команда")

        input("\n  ⏎ Enter для продолжения...")

    dist_dir = os.path.join(PROJECT_ROOT, 'dist')
    print(f"\n  👋 Готово!")
    if os.path.exists(dist_dir) and os.listdir(dist_dir):
        print(f"     EXE-файлы: {dist_dir}")


def main():
    print("=" * 60)
    print("  🔨 BUILDER — Сборка EXE-файлов")
    print("=" * 60)

    ensure_dirs()

    if not check_pyinstaller():
        ans = input("\n  Установить PyInstaller? (y/n) [y]: ").strip().lower()
        if ans in ('y', 'yes', 'да', 'д', ''):
            if not install_pyinstaller():
                return
        else:
            print("  ❌ PyInstaller необходим для сборки")
            return

    interactive_menu()


if __name__ == '__main__':
    main()
