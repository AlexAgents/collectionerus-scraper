"""
viewer.py — Локальный визуализатор скачанных коллекций.
Запуск: python scripts/viewer.py
Открыть: http://localhost:5000
v4: Полный оффлайн, исправлены пути к изображениям, URL-кодирование
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import re
from urllib.parse import quote
from flask import (
    Flask, render_template, send_from_directory,
    request, abort, redirect,
)
from markupsafe import escape

from config import (
    METADATA_DIR, VIEWER_HOST, VIEWER_PORT, VIEWER_DEBUG,
    IMAGE_EXTENSIONS, ensure_dirs,
)
from utils import load_metadata, is_image_file


# ═══════════════════════════════════════════════
# XSS-защита
# ═══════════════════════════════════════════════

def sanitize_html(html_string):
    """Очищает HTML от потенциально опасных тегов/атрибутов."""
    if not html_string:
        return ''
    try:
        import bleach
        allowed_tags = [
            'p', 'br', 'b', 'i', 'em', 'strong', 'a', 'img',
            'ul', 'ol', 'li', 'span', 'div', 'table', 'tr', 'td',
            'th', 'thead', 'tbody', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
            'blockquote', 'pre', 'code', 'hr', 'sub', 'sup',
        ]
        allowed_attrs = {
            'a': ['href', 'title', 'target', 'rel'],
            'img': ['src', 'alt', 'title', 'width', 'height'],
            'span': ['class', 'style'],
            'div': ['class'],
            'td': ['colspan', 'rowspan'],
            'th': ['colspan', 'rowspan'],
        }
        return bleach.clean(
            html_string, tags=allowed_tags,
            attributes=allowed_attrs, strip=True,
        )
    except ImportError:
        import re as _re
        cleaned = html_string
        for tag in ['script', 'iframe', 'style', 'object', 'embed', 'form']:
            cleaned = _re.sub(
                rf'<{tag}[^>]*>.*?</{tag}>', '', cleaned,
                flags=_re.IGNORECASE | _re.DOTALL,
            )
            cleaned = _re.sub(
                rf'<{tag}[^>]*/?>',  '', cleaned,
                flags=_re.IGNORECASE,
            )
        cleaned = _re.sub(
            r'\s+on\w+\s*=\s*["\'][^"\']*["\']', '', cleaned,
            flags=_re.IGNORECASE,
        )
        cleaned = _re.sub(
            r'\s+on\w+\s*=\s*[^\s>]+', '', cleaned,
            flags=_re.IGNORECASE,
        )
        cleaned = _re.sub(
            r'(href|src)\s*=\s*["\']\s*javascript:', r'\1="', cleaned,
            flags=_re.IGNORECASE,
        )
        return cleaned


# ═══════════════════════════════════════════════
# Безопасное построение URL для изображений
# ═══════════════════════════════════════════════

def _make_image_url(*parts):
    """
    Строит URL из частей, кодируя КАЖДЫЙ сегмент пути отдельно.
    Слэши между папкой и файлом гарантированно сохраняются.
    Кириллица и спецсимволы кодируются корректно.

    Пример:
      _make_image_url('data', 'ussr-postcards', 'images', '00002_Набор/related_01.jpg')
      → '/data/ussr-postcards/images/00002_%D0%9D%D0%B0%D0%B1%D0%BE%D1%80/related_01.jpg'
    """
    segments = []
    for part in parts:
        if not part:
            continue
        p = str(part).replace(os.sep, '/').replace('\\', '/')
        for seg in p.split('/'):
            seg = seg.strip()
            if seg:
                segments.append(quote(seg, safe=''))
    if not segments:
        return ''
    return '/' + '/'.join(segments)


# ═══════════════════════════════════════════════
# Настройка Flask
# ═══════════════════════════════════════════════

# PROJECT_ROOT берётся из config.py (поддерживает PyInstaller)
# Для шаблонов и статики используем PROJECT_ROOT из config
from config import PROJECT_ROOT as _cfg_root
PROJECT_ROOT = _cfg_root
TEMPLATES_DIR = os.path.join(PROJECT_ROOT, 'templates')
STATIC_DIR = os.path.join(PROJECT_ROOT, 'static')

app = Flask(
    __name__,
    template_folder=TEMPLATES_DIR,
    static_folder=STATIC_DIR,
)
app.config['METADATA_DIR'] = METADATA_DIR


@app.context_processor
def utility_processor():
    def build_url(slug, filters, page=None, exclude=None,
                  add_key=None, add_val=None):
        params = {}
        for key, val in filters.items():
            if key in ('page', 'per_page'):
                continue
            if exclude and key == exclude:
                continue
            params[key] = val
        if add_key and add_val is not None:
            params[add_key] = add_val
        if page and page > 1:
            params['page'] = str(page)
        base = f"/collection/{slug}/"
        if params:
            query = '&'.join(f"{k}={v}" for k, v in params.items())
            return f"{base}?{query}"
        return base
    return dict(build_url=build_url)


# ═══════════════════════════════════════════════
# Загрузка данных
# ═══════════════════════════════════════════════

def get_all_collections():
    collections = []
    if not os.path.exists(METADATA_DIR):
        return collections

    for name in sorted(os.listdir(METADATA_DIR)):
        path = os.path.join(METADATA_DIR, name)
        if not os.path.isdir(path) or name.startswith('_'):
            continue

        col_info = {}
        info_path = os.path.join(path, 'collection_info.json')
        if os.path.exists(info_path):
            try:
                with open(info_path, 'r', encoding='utf-8') as f:
                    col_info = json.load(f)
            except Exception:
                pass

        items_count = 0
        has_related = 0
        has_multi = 0
        meta_path = os.path.join(path, 'metadata.json')
        if os.path.exists(meta_path):
            try:
                with open(meta_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                items_count = data.get('total_items', 0)
                for item in data.get('items', []):
                    if item.get('related_items'):
                        has_related += 1
                    if len(item.get('downloaded_files', [])) > 1:
                        has_multi += 1
            except Exception:
                pass

        if not items_count:
            progress_path = os.path.join(path, '_progress.json')
            if os.path.exists(progress_path):
                try:
                    with open(progress_path, 'r', encoding='utf-8') as f:
                        p = json.load(f)
                    items_count = len(p.get('metadata', []))
                except Exception:
                    pass

        thumb = _find_first_image(path, name)

        collections.append({
            'slug': name,
            'name': col_info.get('name', name),
            'owner': col_info.get('owner', ''),
            'count': items_count or col_info.get('count', 0),
            'thumb': thumb,
            'url': col_info.get('url', ''),
            'has_related': has_related,
            'has_multi': has_multi,
        })

    collections.sort(key=lambda c: -c['count'])
    return collections


def _find_first_image(col_path, slug):
    """Находит первое изображение в коллекции."""
    images_dir = os.path.join(col_path, 'images')
    if not os.path.exists(images_dir):
        return ''

    for f in sorted(os.listdir(images_dir)):
        fp = os.path.join(images_dir, f)
        if os.path.isfile(fp) and is_image_file(f):
            return _make_image_url('data', slug, 'images', f)

    for d in sorted(os.listdir(images_dir)):
        dp = os.path.join(images_dir, d)
        if os.path.isdir(dp):
            for f in sorted(os.listdir(dp)):
                fp = os.path.join(dp, f)
                if os.path.isfile(fp) and is_image_file(f):
                    return _make_image_url('data', slug, 'images', d, f)

    return ''


def get_collection_items(slug):
    col_dir = os.path.join(METADATA_DIR, slug)

    meta_path = os.path.join(col_dir, 'metadata.json')
    if os.path.exists(meta_path):
        try:
            with open(meta_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            items = data.get('items', [])
            return items, data.get('collection', {})
        except Exception as e:
            print(f"  Error loading metadata.json: {e}")

    progress_path = os.path.join(col_dir, '_progress.json')
    if os.path.exists(progress_path):
        try:
            with open(progress_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            items = data.get('metadata', [])
            return items, {}
        except Exception as e:
            print(f"  Error loading _progress.json: {e}")

    return [], {}


def get_all_tags(items):
    tags = {}
    for item in items:
        for key, val in item.get('properties', {}).items():
            if key not in tags:
                tags[key] = set()
            tags[key].add(val)
    return {k: sorted(v) for k, v in tags.items()}


def filter_items(items, filters):
    if not filters:
        return items

    result = []
    for item in items:
        props = item.get('properties', {})
        match = True

        for key, value in filters.items():
            if key in ('q', 'sort', 'page', 'per_page', 'show'):
                continue
            item_val = props.get(key, '')
            if item_val != value:
                match = False
                break

        if match and 'q' in filters:
            query = filters['q'].lower()
            searchable = ' '.join([
                item.get('title', ''),
                item.get('description', ''),
                ' '.join(str(v) for v in props.values()),
                ' '.join(
                    r.get('title', '')
                    for r in item.get('related_items', [])
                ),
            ]).lower()
            if query not in searchable:
                match = False

        if match:
            result.append(item)

    sort_key = filters.get('sort', '')
    if sort_key:
        reverse = sort_key.startswith('-')
        key_name = sort_key.lstrip('-')

        def sort_func(it):
            val = it.get('properties', {}).get(key_name, '')
            if not val:
                return (1, 999999, 'zzz')
            num_match = re.search(r'\d+', str(val))
            num = int(num_match.group()) if num_match else 999999
            text = val.lower()
            return (0 if num_match else 1, num, text)

        result.sort(key=sort_func, reverse=reverse)

    return result


def _resolve_item_thumb(slug, item):
    """Определяет URL миниатюры для предмета."""
    files = item.get('downloaded_files', [])
    if files:
        return _make_image_url('data', slug, 'images', files[0])

    if item.get('has_folder'):
        base_name = os.path.splitext(item.get('filename', ''))[0]
        if base_name:
            folder = os.path.join(METADATA_DIR, slug, 'images', base_name)
            if os.path.exists(folder):
                for f in sorted(os.listdir(folder)):
                    if f.startswith('_'):
                        continue
                    if is_image_file(f):
                        return _make_image_url('data', slug, 'images', base_name, f)

    return ''


def _resolve_item_images(slug, item):
    """Собирает ВСЕ изображения предмета — только из локальных файлов."""
    col_dir = os.path.join(METADATA_DIR, slug)
    images_dir = os.path.join(col_dir, 'images')

    main_images = []
    related_images = []

    # ── Основные файлы из downloaded_files ──
    for fname in item.get('downloaded_files', []):
        fpath = os.path.join(images_dir, fname.replace('/', os.sep))
        if os.path.exists(fpath):
            size_bytes = os.path.getsize(fpath)
            if size_bytes < 1048576:
                size_str = f"{size_bytes / 1024:.1f} КБ"
            else:
                size_str = f"{size_bytes / 1048576:.1f} МБ"
            main_images.append({
                'url': _make_image_url('data', slug, 'images', fname),
                'filename': os.path.basename(fname),
                'size': size_str,
                'type': 'main',
            })

    # ── Если нет downloaded_files но есть подпапка ──
    if not main_images and item.get('has_folder'):
        base_name = os.path.splitext(item.get('filename', ''))[0]
        folder = os.path.join(images_dir, base_name)
        if os.path.exists(folder):
            for f in sorted(os.listdir(folder)):
                if f.startswith('_') or f.startswith('related_'):
                    continue
                fp = os.path.join(folder, f)
                if os.path.isfile(fp) and is_image_file(f):
                    size_bytes = os.path.getsize(fp)
                    if size_bytes < 1048576:
                        size_str = f"{size_bytes / 1024:.1f} КБ"
                    else:
                        size_str = f"{size_bytes / 1048576:.1f} МБ"
                    main_images.append({
                        'url': _make_image_url('data', slug, 'images', base_name, f),
                        'filename': f,
                        'size': size_str,
                        'type': 'main',
                    })

    # ── Если вообще ничего нет — сканируем файловую систему ──
    if not main_images:
        fname = item.get('filename', '')
        if fname:
            fpath = os.path.join(images_dir, fname.replace('/', os.sep))
            if os.path.isfile(fpath):
                size_bytes = os.path.getsize(fpath)
                if size_bytes < 1048576:
                    size_str = f"{size_bytes / 1024:.1f} КБ"
                else:
                    size_str = f"{size_bytes / 1048576:.1f} МБ"
                main_images.append({
                    'url': _make_image_url('data', slug, 'images', fname),
                    'filename': os.path.basename(fname),
                    'size': size_str,
                    'type': 'main',
                })
            else:
                base_name = os.path.splitext(fname)[0]
                folder = os.path.join(images_dir, base_name)
                if os.path.isdir(folder):
                    for f in sorted(os.listdir(folder)):
                        if f.startswith('_') or f.startswith('related_'):
                            continue
                        fp = os.path.join(folder, f)
                        if os.path.isfile(fp) and is_image_file(f):
                            size_bytes = os.path.getsize(fp)
                            if size_bytes < 1048576:
                                size_str = f"{size_bytes / 1024:.1f} КБ"
                            else:
                                size_str = f"{size_bytes / 1048576:.1f} МБ"
                            main_images.append({
                                'url': _make_image_url('data', slug, 'images', base_name, f),
                                'filename': f,
                                'size': size_str,
                                'type': 'main',
                            })

    # ── Связанные предметы — из related_items[].file ──
    for ri, rel in enumerate(item.get('related_items', [])):
        rel_file = rel.get('file', '')
        if rel_file:
            fpath = os.path.join(images_dir, rel_file.replace('/', os.sep))
            if os.path.exists(fpath):
                size_bytes = os.path.getsize(fpath)
                if size_bytes < 1048576:
                    size_str = f"{size_bytes / 1024:.1f} КБ"
                else:
                    size_str = f"{size_bytes / 1048576:.1f} МБ"
                related_images.append({
                    'url': _make_image_url('data', slug, 'images', rel_file),
                    'filename': os.path.basename(rel_file),
                    'size': size_str,
                    'type': 'related',
                    'title': rel.get('title', f'Связанный #{ri + 1}'),
                    'item_url': rel.get('url', ''),
                })

    # ── Fallback: related_*.* в подпапке ──
    if not related_images and item.get('has_folder'):
        base_name = os.path.splitext(item.get('filename', ''))[0]
        folder = os.path.join(images_dir, base_name)
        if os.path.exists(folder):
            for f in sorted(os.listdir(folder)):
                if f.startswith('related_') and is_image_file(f):
                    fp = os.path.join(folder, f)
                    size_bytes = os.path.getsize(fp)
                    if size_bytes < 1048576:
                        size_str = f"{size_bytes / 1024:.1f} КБ"
                    else:
                        size_str = f"{size_bytes / 1048576:.1f} МБ"
                    title_part = f.replace('related_', '').rsplit('.', 1)[0]
                    title_clean = re.sub(r'^\d+_', '', title_part)
                    related_images.append({
                        'url': _make_image_url('data', slug, 'images', base_name, f),
                        'filename': f,
                        'size': size_str,
                        'type': 'related',
                        'title': title_clean or 'Связанный',
                        'item_url': '',
                    })

    return main_images, related_images


def find_similar(target, all_items, slug, max_results=12):
    target_props = target.get('properties', {})
    if not target_props:
        return []

    scores = []
    for item in all_items:
        if item.get('index') == target.get('index'):
            continue
        props = item.get('properties', {})
        score = sum(
            1 for k, v in target_props.items() if props.get(k) == v
        )
        if score > 0:
            scores.append((score, item))

    scores.sort(key=lambda x: (-x[0], x[1].get('index', 0)))

    result = []
    for score, item in scores[:max_results]:
        item_copy = dict(item)
        item_copy['_thumb'] = _resolve_item_thumb(slug, item)
        item_copy['_score'] = score
        result.append(item_copy)

    return result






# ═══════════════════════════════════════════════
# Индекс media URL → локальный файл (v3)
# ═══════════════════════════════════════════════
_media_index = None
_media_index_built = False


def _normalize_media_path(url):
    """Извлекает путь после /media/ из любого формата URL."""
    if not url:
        return ''
    url = url.strip()
    if '/media/' in url:
        return url.split('/media/', 1)[1]
    if url.startswith('media/'):
        return url[6:]
    return url


def _all_quality_variants(media_path):
    """Генерирует thumbs/preloaded/large варианты пути."""
    if not media_path:
        return []
    variants = [media_path]
    swaps = [
        ('items-thumbs/', 'preloaded-items/'),
        ('items-thumbs/', 'items-large/'),
        ('preloaded-items/', 'items-thumbs/'),
        ('preloaded-items/', 'items-large/'),
        ('items-large/', 'items-thumbs/'),
        ('items-large/', 'preloaded-items/'),
    ]
    for old, new in swaps:
        if old in media_path:
            v = media_path.replace(old, new, 1)
            if v not in variants:
                variants.append(v)
    return variants


def _index_url(index, url, local_path):
    """Добавляет ВСЕ варианты ключа для одного URL → файл."""
    if not url or not local_path:
        return

    media_path = _normalize_media_path(url)
    if not media_path:
        # URL без /media/ — добавляем как есть
        index[url] = local_path
        return

    for variant in _all_quality_variants(media_path):
        index[variant] = local_path
        index['/media/' + variant] = local_path
        index['https://collectionerus.ru/media/' + variant] = local_path

    basename = os.path.basename(url.split('?')[0])
    if basename:
        index.setdefault(basename, local_path)


def _build_media_index():
    """
    Строит индекс: URL с сайта → путь к локальному файлу.

    Ключевое отличие v3:
    Для КАЖДОГО предмета маппим его thumb_url и images[]
    на его downloaded_files[]. Таким образом, когда в описании
    ДРУГОГО предмета встречается <img src="thumb_url">,
    мы находим файл через индекс.

    Индекс хранится в памяти (~2-10 MB).
    Файлы НЕ создаются, НЕ копируются, НЕ переименовываются.
    """
    global _media_index, _media_index_built

    if _media_index_built:
        return _media_index or {}

    import json as _json

    index = {}
    total_items = 0
    total_keys = 0

    if not os.path.exists(METADATA_DIR):
        _media_index = index
        _media_index_built = True
        return index

    for col_name in sorted(os.listdir(METADATA_DIR)):
        col_dir = os.path.join(METADATA_DIR, col_name)
        if not os.path.isdir(col_dir) or col_name.startswith('_'):
            continue

        # Загружаем items
        items_data = None
        for source in ('metadata.json', '_progress.json'):
            src_path = os.path.join(col_dir, source)
            if os.path.exists(src_path):
                try:
                    with open(src_path, 'r', encoding='utf-8') as f:
                        data = _json.load(f)
                    if source == '_progress.json':
                        items_data = data.get('metadata', [])
                    else:
                        items_data = data.get('items', [])
                    break
                except Exception:
                    continue

        if not items_data:
            continue

        images_dir = os.path.join(col_dir, 'images')

        for item in items_data:
            downloaded = item.get('downloaded_files', [])
            if not downloaded:
                continue

            # Путь к первому (основному) скачанному файлу
            main_file = downloaded[0]
            main_path = os.path.join(
                images_dir, main_file.replace('/', os.sep)
            )

            if not os.path.exists(main_path):
                # Пробуем найти файл
                # downloaded_files может быть "folder/main.jpg"
                alt_path = os.path.join(images_dir, main_file)
                if os.path.exists(alt_path):
                    main_path = alt_path
                else:
                    continue

            # ── ГЛАВНОЕ: thumb_url → main downloaded file ──
            thumb_url = item.get('thumb_url', '')
            if thumb_url:
                before = len(index)
                _index_url(index, thumb_url, main_path)
                total_keys += len(index) - before

            # ── images[i] → downloaded_files[i] ──
            images = item.get('images', [])
            for i, img_url in enumerate(images):
                if i < len(downloaded):
                    dl_file = downloaded[i]
                    dl_path = os.path.join(
                        images_dir, dl_file.replace('/', os.sep)
                    )
                    if os.path.exists(dl_path):
                        _index_url(index, img_url, dl_path)

            # ── related_items с file ──
            for rel in item.get('related_items', []):
                rel_file = rel.get('file', '')
                if not rel_file:
                    continue
                rel_path = os.path.join(
                    images_dir, rel_file.replace('/', os.sep)
                )
                if not os.path.exists(rel_path):
                    continue

                rel_thumb = rel.get('thumb_url', '')
                if rel_thumb:
                    _index_url(index, rel_thumb, rel_path)

                for qu in rel.get('quality_urls', []):
                    _index_url(index, qu, rel_path)

            total_items += 1

    _media_index = index
    _media_index_built = True
    print(
        f"  \U0001f4cb Media index v3: "
        f"{len(index)} ключей, {total_items} предметов"
    )
    return index


def _lookup_media(filepath):
    """
    Ищет локальный файл по пути из /media/<filepath>.
    Пробует несколько стратегий поиска.
    """
    index = _build_media_index()
    if not index:
        return None

    # 1. Точное совпадение
    if filepath in index:
        return index[filepath]

    # 2. С /media/
    key2 = '/media/' + filepath
    if key2 in index:
        return index[key2]

    # 3. Полный URL
    key3 = 'https://collectionerus.ru/media/' + filepath
    if key3 in index:
        return index[key3]

    # 4. Quality варианты
    for variant in _all_quality_variants(filepath):
        if variant in index:
            return index[variant]
        k = '/media/' + variant
        if k in index:
            return index[k]

    # 5. Basename
    basename = os.path.basename(filepath)
    if basename and basename in index:
        return index[basename]

    return None


def _process_description_html(html, slug, items=None):
    """
    Обрабатывает description_html для корректного отображения:
    1. <img src="/media/items-thumbs/..."> → ищем среди скачанных или проксируем
    2. <a href="/collections/slug/items/N/"> → локальный роут viewer
    3. Сохраняем cross-item-link структуру
    """
    if not html:
        return ''

    from bs4 import BeautifulSoup as BS
    soup = BS(html, 'html.parser')
    modified = False

    # Обрабатываем <img> внутри описания
    for img in soup.find_all('img'):
        src = img.get('src', '')
        if not src:
            continue

        # /media/ URL → пробуем найти скачанный файл через cross-item-link
        if '/media/' in src or src.startswith('media/'):
            # Оставляем src как /media/... — serve_media обработает
            # Но добавляем onerror для graceful degradation
            if not img.get('onerror'):
                title = img.get('title', '')
                # При ошибке загрузки показываем текстовую метку
                fallback_text = title if title else 'img'
                img['onerror'] = (
                    "this.style.display=\'none\'; "
                    "var s=document.createElement(\'span\'); "
                    "s.className=\'cross-item-fallback\'; "
                    "s.textContent=\'[\' + (this.title || \'img\') + \']\'; "
                    "this.parentNode.insertBefore(s, this);"
                )
                img['loading'] = 'lazy'
                modified = True

            # Нормализуем src
            if src.startswith('media/'):
                img['src'] = '/' + src
                modified = True

    # Обрабатываем <a> ссылки — превращаем в локальные
    for a in soup.find_all('a'):
        href = a.get('href', '')
        if not href:
            continue

        # /collections/slug/items/N/ → /collection/slug/item/N/
        import re as _re
        m = _re.search(r'/collections/([^/]+)/(?:items/)?(\d+)/?', href)
        if m:
            link_slug = m.group(1)
            item_ext_id = m.group(2)

            # Ищем предмет по URL
            local_url = None
            if items and link_slug == slug:
                for it in items:
                    it_url = it.get('url', '')
                    if f'/items/{item_ext_id}' in it_url or f'/{item_ext_id}/' in it_url:
                        idx = it.get('index', 0)
                        local_url = f'/collection/{link_slug}/item/{idx}/'
                        break

            if not local_url:
                # Используем redirect роут
                local_url = href  # /collections/slug/N/ → redirect сработает

            a['href'] = local_url
            modified = True

            # Добавляем класс для стилизации
            classes = a.get('class', [])
            if isinstance(classes, str):
                classes = classes.split()
            if 'cross-item-link' not in classes:
                classes.append('cross-item-link')
            a['class'] = ' '.join(classes)

    if modified:
        return str(soup)
    return html


def _make_local_related_url(url, slug, items):
    """
    Преобразует URL связанного предмета с collectionerus.ru
    в локальный роут viewer.
    """
    if not url:
        return ''

    m = re.search(r'/items/(\d+)/?', url)
    if m:
        item_ext_id = m.group(1)
        for it in items:
            item_url = it.get('url', '')
            if f'/items/{item_ext_id}' in item_url:
                idx = it.get('index', 0)
                return f'/collection/{slug}/item/{idx}/'

    return url


# ═══════════════════════════════════════════════
# Роуты
# ═══════════════════════════════════════════════

@app.route('/')
def index():
    collections = get_all_collections()
    q = request.args.get('q', '').strip().lower()
    if q:
        collections = [
            c for c in collections
            if (q in c['name'].lower() or
                q in c['owner'].lower() or
                q in c['slug'].lower())
        ]
    return render_template('index.html', collections=collections, query=q)


@app.route('/collection/<slug>/')
def collection_view(slug):
    if '..' in slug or '/' in slug or '\\' in slug:
        abort(400)

    col_dir = os.path.join(METADATA_DIR, slug)
    if not os.path.exists(col_dir):
        abort(404)

    items, col_info = get_collection_items(slug)
    all_tags = get_all_tags(items)

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    per_page = max(10, min(per_page, 500))

    filters = {}
    for key in request.args:
        if key in ('page', 'per_page'):
            continue
        val = request.args.get(key, '').strip()
        if val:
            filters[key] = val

    filtered_items = filter_items(items, filters)

    total = len(filtered_items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = start + per_page
    page_items = filtered_items[start:end]

    total_related = sum(
        len(item.get('related_items', []))
        for item in items
    )
    total_multi = sum(
        1 for item in items
        if len(item.get('downloaded_files', [])) > 1
    )

    for item in page_items:
        item['_thumb'] = _resolve_item_thumb(slug, item)
        item['_has_related'] = bool(item.get('related_items'))
        item['_related_count'] = len(item.get('related_items', []))
        item['_file_count'] = len(item.get('downloaded_files', []))

    return render_template(
        'collection.html',
        slug=slug,
        col_info=col_info,
        items=page_items,
        all_tags=all_tags,
        filters=filters,
        total=total,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        total_related=total_related,
        total_multi=total_multi,
    )


@app.route('/collection/<slug>/item/<int:item_index>/')
def item_view(slug, item_index):
    if '..' in slug or '/' in slug or '\\' in slug:
        abort(400)

    col_dir = os.path.join(METADATA_DIR, slug)
    if not os.path.exists(col_dir):
        abort(404)

    items, col_info = get_collection_items(slug)

    item = None
    item_pos = -1
    for i, it in enumerate(items):
        if it.get('index') == item_index:
            item = it
            item_pos = i
            break

    if not item:
        abort(404)

    prev_item = items[item_pos - 1] if item_pos > 0 else None
    next_item = (
        items[item_pos + 1] if item_pos < len(items) - 1 else None
    )

    main_images, related_images = _resolve_item_images(slug, item)

    description_html = sanitize_html(item.get('description_html', ''))
    # Обрабатываем img/a в описании для локального отображения
    description_html = _process_description_html(description_html, slug, items)
    description_text = item.get('description', '')

    similar = find_similar(item, items, slug, max_results=12)

    local_related = []
    for rel in item.get('related_items', []):
        rel_copy = dict(rel)
        if rel_copy.get('url'):
            rel_copy['local_url'] = _make_local_related_url(
                rel_copy['url'], slug, items
            )
        else:
            rel_copy['local_url'] = ''
        local_related.append(rel_copy)

    current_filters = {}
    for key in request.args:
        if key in ('page', 'per_page'):
            continue
        val = request.args.get(key, '').strip()
        if val:
            current_filters[key] = val

    return render_template(
        'item.html',
        slug=slug,
        col_info=col_info,
        item=item,
        main_images=main_images,
        related_images=related_images,
        all_images=main_images + related_images,
        prev_item=prev_item,
        next_item=next_item,
        similar=similar,
        filters=current_filters,
        description_html=description_html,
        description_text=description_text,
        local_related=local_related,
    )


# ── Перехват внешних ссылок ──

@app.route('/collections/<slug>/items/<int:item_id>/')
@app.route('/collections/<slug>/items/<int:item_id>')
def redirect_external_item(slug, item_id):
    """Перенаправляет /collections/slug/items/123/ → локальный роут."""
    try:
        items, _ = get_collection_items(slug)
        for it in items:
            item_url = it.get('url', '')
            if f'/items/{item_id}' in item_url:
                idx = it.get('index', 1)
                return redirect(f'/collection/{slug}/item/{idx}/')
    except Exception:
        pass
    return redirect(f'/collection/{slug}/')



@app.route('/collections/<slug>/<int:item_index>/')
@app.route('/collections/<slug>/<int:item_index>')
def redirect_collection_item_by_index(slug, item_index):
    """
    Перехватывает /collections/slug/107/
    (ссылки из 'Открытки в наборе' и подобных блоков).
    Перенаправляет на /collection/slug/item/107/
    """
    return redirect(f'/collection/{slug}/item/{item_index}/')

@app.route('/collections/<slug>/')
@app.route('/collections/<slug>')
def redirect_external_collection(slug):
    """Перенаправляет /collections/slug/ → /collection/slug/"""
    return redirect(f'/collection/{slug}/')


@app.route('/data/<path:filepath>')
def serve_data(filepath):
    """Отдаёт файлы из METADATA_DIR."""
    safe_path = os.path.normpath(filepath)
    if safe_path.startswith('..') or os.path.isabs(safe_path):
        abort(400)
    return send_from_directory(METADATA_DIR, filepath)


@app.route('/media/<path:filepath>')
def serve_media(filepath):
    """Ищет файл в индексе, отдаёт или прозрачный пиксель."""
    from flask import Response, send_file

    local_path = _lookup_media(filepath)
    if local_path and os.path.isfile(local_path):
        return send_file(local_path)

    pixel = (
        b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR'
        b'\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06'
        b'\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00'
        b'\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01'
        b'\r\n\xb4\x00\x00\x00\x00IEND\xaeB`\x82'
    )
    return Response(pixel, content_type='image/png',
                    headers={'Cache-Control': 'public, max-age=3600'})

def create_templates():
    """Создаёт шаблоны и CSS если они не существуют."""
    os.makedirs(TEMPLATES_DIR, exist_ok=True)
    os.makedirs(STATIC_DIR, exist_ok=True)

    base_html = _get_base_template()
    index_html = _get_index_template()
    collection_html = _get_collection_template()
    item_html = _get_item_template()
    style_css = _get_style_css()

    files = {
        os.path.join(TEMPLATES_DIR, 'base.html'): base_html,
        os.path.join(TEMPLATES_DIR, 'index.html'): index_html,
        os.path.join(TEMPLATES_DIR, 'collection.html'): collection_html,
        os.path.join(TEMPLATES_DIR, 'item.html'): item_html,
        os.path.join(STATIC_DIR, 'style.css'): style_css,
    }

    for path, content in files.items():
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✅ {os.path.basename(path)}")


def _get_base_template():
    return '''<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{% block title %}Коллекции{% endblock %}</title>
  <link rel="stylesheet" href="/static/style.css">
</head>
<body>
  <header>
    <div class="header-inner">
      <a href="/" class="logo">📦 Коллекции</a>
      <form class="search-form" action="/" method="get">
        <input type="text" name="q" placeholder="Поиск коллекций..."
               value="{{ request.args.get('q', '') }}">
        <button type="submit">🔍</button>
      </form>
    </div>
  </header>
  <main>{% block content %}{% endblock %}</main>
  <footer><p>Локальный просмотрщик коллекций v4</p></footer>
  {% block scripts %}{% endblock %}
</body>
</html>'''


def _get_index_template():
    return '''{% extends "base.html" %}
{% block title %}Коллекции{% endblock %}
{% block content %}
<div class="page-header">
  <h1>Коллекции <span class="count">({{ collections|length }})</span></h1>
  {% if query %}
  <p class="search-info">Поиск: «{{ query }}» <a href="/">× сбросить</a></p>
  {% endif %}
</div>
<div class="collections-grid">
  {% for col in collections %}
  <a href="/collection/{{ col.slug }}/" class="collection-card">
    <div class="collection-thumb">
      {% if col.thumb %}
      <img src="{{ col.thumb }}" alt="{{ col.name }}" loading="lazy"
           onerror="this.style.display='none'">
      {% else %}
      <div class="no-image">📦</div>
      {% endif %}
    </div>
    <div class="collection-info">
      <h3>{{ col.name }}</h3>
      <div class="collection-badges">
        <span class="collection-count">{{ col.count }} шт.</span>
        {% if col.has_related > 0 %}
        <span class="badge badge-related" title="Есть связанные предметы">🧲 {{ col.has_related }}</span>
        {% endif %}
        {% if col.has_multi > 0 %}
        <span class="badge badge-multi" title="Предметы с несколькими фото">📷 {{ col.has_multi }}</span>
        {% endif %}
      </div>
      {% if col.owner %}
      <span class="collection-owner">{{ col.owner }}</span>
      {% endif %}
    </div>
  </a>
  {% endfor %}
</div>
{% endblock %}'''


def _get_collection_template():
    return '''{% extends "base.html" %}
{% block title %}{{ col_info.get('name', slug) }}{% endblock %}
{% block content %}
<div class="breadcrumb">
  <a href="/">Коллекции</a> → <span>{{ col_info.get('name', slug) }}</span>
</div>

<div class="collection-page">
  <div class="collection-main">
    <div class="page-header">
      <h1>{{ col_info.get('name', slug) }}</h1>
      <p class="subtitle">
        {% if col_info.get('owner') %}{{ col_info.owner }} · {% endif %}
        {{ total }} предметов
        {% if filters %} (отфильтровано){% endif %}
        {% if total_related > 0 %}
        · 🧲 {{ total_related }} связанных
        {% endif %}
        {% if total_multi > 0 %}
        · 📷 {{ total_multi }} с галереей
        {% endif %}
      </p>
    </div>

    <form class="inline-search" method="get">
      <input type="text" name="q" placeholder="Поиск..." value="{{ filters.get('q', '') }}">
      {% for key, val in filters.items() if key != 'q' and key != 'page' and key != 'per_page' %}
      <input type="hidden" name="{{ key }}" value="{{ val }}">
      {% endfor %}
      <button type="submit">🔍</button>
      {% if filters %}
      <a href="/collection/{{ slug }}/" class="clear-filters">× Сбросить</a>
      {% endif %}
    </form>

    {% if filters %}
    <div class="active-filters">
      {% for key, val in filters.items() if key not in ('q','page','per_page','sort') %}
      <span class="filter-tag">{{ key }}: {{ val }}
        <a href="{{ build_url(slug, filters, exclude=key) }}">×</a>
      </span>
      {% endfor %}
    </div>
    {% endif %}

    <div class="items-grid">
      {% for item in items %}
      <a href="/collection/{{ slug }}/item/{{ item.index }}/" class="item-card" title="{{ item.title }}">
        <div class="item-thumb">
          {% if item._thumb %}
          <img src="{{ item._thumb }}" alt="{{ item.title }}" loading="lazy"
               onerror="this.style.display='none'">
          {% else %}
          <div class="no-image">🖼</div>
          {% endif %}
          <div class="item-badges">
            {% if item._file_count > 1 %}
            <span class="image-count">{{ item._file_count }} 📷</span>
            {% endif %}
            {% if item._has_related %}
            <span class="related-badge" title="Связанные: {{ item._related_count }}">🧲 {{ item._related_count }}</span>
            {% endif %}
          </div>
        </div>
        <div class="item-info">
          <p class="item-title">{{ item.title[:60] }}</p>
          {% set props = item.get('properties', {}) %}
          {% for key in props %}{% if loop.index <= 2 %}
          <span class="item-tag">{{ props[key] }}</span>
          {% endif %}{% endfor %}
        </div>
      </a>
      {% endfor %}
    </div>

    {% if total_pages > 1 %}
    <div class="pagination">
      {% if page > 1 %}
      <a href="{{ build_url(slug, filters, page=page-1) }}">&laquo; Назад</a>
      {% endif %}
      {% for p in range(1, total_pages + 1) %}
        {% if p == page %}
        <span class="current">{{ p }}</span>
        {% elif p <= 3 or p >= total_pages - 2 or (p >= page - 2 and p <= page + 2) %}
        <a href="{{ build_url(slug, filters, page=p) }}">{{ p }}</a>
        {% elif p == 4 or p == total_pages - 3 %}
        <span class="dots">...</span>
        {% endif %}
      {% endfor %}
      {% if page < total_pages %}
      <a href="{{ build_url(slug, filters, page=page+1) }}">Вперёд &raquo;</a>
      {% endif %}
    </div>
    {% endif %}
  </div>

  <div class="collection-sidebar">
    <h3>Фильтры</h3>
    <div class="sidebar-controls">
      <button onclick="document.querySelectorAll('.filter-group').forEach(d=>d.open=true)">Развернуть все</button>
      <button onclick="document.querySelectorAll('.filter-group').forEach(d=>d.open=false)">Свернуть все</button>
    </div>

    {% for tag_name, tag_values in all_tags.items() %}
    <details class="filter-group" {% if tag_name in filters %}open{% endif %}>
      <summary>{{ tag_name }} <span class="tag-count">({{ tag_values|length }})</span></summary>
      <ul class="filter-values">
        {% for val in tag_values[:50] %}
        <li>
          {% if filters.get(tag_name) == val %}
          <strong>{{ val }}</strong>
          <a href="{{ build_url(slug, filters, exclude=tag_name) }}" class="remove">×</a>
          {% else %}
          <a href="{{ build_url(slug, filters, add_key=tag_name, add_val=val) }}">{{ val }}</a>
          {% endif %}
        </li>
        {% endfor %}
        {% if tag_values|length > 50 %}
        <li class="more">...ещё {{ tag_values|length - 50 }}</li>
        {% endif %}
      </ul>
    </details>
    {% endfor %}

    {% if all_tags %}
    <h3>Сортировка</h3>
    <ul class="filter-values">
      {% for tag_name in all_tags %}
      <li>
        <a href="{{ build_url(slug, filters, add_key='sort', add_val=tag_name) }}">{{ tag_name }} ↑</a>
        <a href="{{ build_url(slug, filters, add_key='sort', add_val='-' + tag_name) }}">↓</a>
      </li>
      {% endfor %}
    </ul>
    {% endif %}
  </div>
</div>
{% endblock %}'''


def _get_item_template():
    return '''{% extends "base.html" %}
{% block title %}{{ item.title }}{% endblock %}
{% block content %}
<div class="breadcrumb">
  <a href="/">Коллекции</a> →
  <a href="/collection/{{ slug }}/">{{ col_info.get('name', slug) }}</a> →
  <span>{{ item.title[:50] }}</span>
</div>

<div class="item-page">
  <div class="item-images">
    {% if all_images %}
    <div class="main-image">
      <img id="mainImg" src="{{ all_images[0].url }}" alt="{{ item.title }}"
           onclick="openLightbox(this.src)"
           onerror="this.style.opacity='0.3'">
      <div class="image-type-label" id="imageTypeLabel">
        {% if all_images[0].type == 'related' %}
        🧲 Связанный: {{ all_images[0].title }}
        {% else %}
        📷 Основное фото
        {% endif %}
      </div>
    </div>

    {% if all_images|length > 1 %}
    <div class="image-sections">
      {% if main_images %}
      <div class="image-section">
        <h4>📷 Основные фото ({{ main_images|length }})</h4>
        <div class="thumbnails">
          {% for img in main_images %}
          <div class="thumb {% if loop.first %}active{% endif %}"
               onclick="selectImage(this, {{ loop.index0 }}, 'main')"
               title="{{ img.filename }} ({{ img.size }})">
            <img src="{{ img.url }}" loading="lazy"
                 onerror="this.style.display='none'">
          </div>
          {% endfor %}
        </div>
      </div>
      {% endif %}

      {% if related_images %}
      <div class="image-section related-section">
        <h4>🧲 Связанные предметы ({{ related_images|length }})</h4>
        <div class="thumbnails">
          {% for img in related_images %}
          <div class="thumb"
               onclick="selectImage(this, {{ main_images|length + loop.index0 }}, 'related')"
               title="{{ img.title }}&#10;{{ img.filename }} ({{ img.size }})">
            <img src="{{ img.url }}" loading="lazy"
                 onerror="this.style.display='none'">
            <span class="thumb-label">{{ img.title[:20] }}</span>
          </div>
          {% endfor %}
        </div>
      </div>
      {% endif %}
    </div>
    {% endif %}

    <div class="image-meta">
      {{ main_images|length }} фото
      {% if related_images %} + {{ related_images|length }} связанных{% endif %}
    </div>
    {% else %}
    <div class="no-image large">🖼</div>
    {% endif %}
  </div>

  <div class="item-details">
    <h1>{{ item.title }}</h1>

    {% if description_html %}
    <div class="item-description">{{ description_html|safe }}</div>
    {% elif description_text %}
    <div class="item-description">{{ description_text }}</div>
    {% endif %}

    {% if item.get('image_description') %}
    <div class="image-description">
      <em>{{ item.image_description }}</em>
    </div>
    {% endif %}

    {% set props = item.get('properties', {}) %}
    {% if props %}
    <div class="item-properties">
      <h3>Свойства</h3>
      <table>
        {% for key, val in props.items() %}
        <tr>
          <td class="prop-key">{{ key }}:</td>
          <td class="prop-val">
            <a href="{{ build_url(slug, filters, add_key=key, add_val=val) }}">{{ val }}</a>
          </td>
        </tr>
        {% endfor %}
      </table>
    </div>
    {% endif %}

    {% if local_related %}
    <div class="related-info">
      <h3>🧲 Связанные предметы ({{ local_related|length }})</h3>
      <ul class="related-list">
        {% for rel in local_related %}
        <li>
          {% if rel.get('title') %}
          <span class="related-title">{{ rel.title }}</span>
          {% endif %}
          {% if rel.local_url %}
          <a href="{{ rel.local_url }}" class="related-link">→ открыть</a>
          {% elif rel.get('url') %}
          <a href="{{ rel.url }}" target="_blank" class="related-link">↗ на сайте</a>
          {% endif %}
        </li>
        {% endfor %}
      </ul>
    </div>
    {% endif %}

    <div class="item-nav">
      {% if prev_item %}
      <a href="/collection/{{ slug }}/item/{{ prev_item.index }}/" class="nav-prev">← {{ prev_item.title[:30] }}</a>
      {% endif %}
      {% if next_item %}
      <a href="/collection/{{ slug }}/item/{{ next_item.index }}/" class="nav-next">{{ next_item.title[:30] }} →</a>
      {% endif %}
    </div>

    {% if item.url %}
    <div class="original-link">
      <a href="{{ item.url }}" target="_blank">Открыть на сайте ↗</a>
    </div>
    {% endif %}
  </div>
</div>

{% if similar %}
<div class="similar-section">
  <h2>Похожие предметы</h2>
  <div class="items-grid similar-grid">
    {% for sim in similar %}
    <a href="/collection/{{ slug }}/item/{{ sim.index }}/" class="item-card">
      <div class="item-thumb">
        {% if sim._thumb %}<img src="{{ sim._thumb }}" loading="lazy"
             onerror="this.style.display='none'">{% endif %}
      </div>
      <div class="item-info">
        <p class="item-title">{{ sim.title[:40] }}</p>
        <span class="similarity">совпадений: {{ sim._score }}</span>
      </div>
    </a>
    {% endfor %}
  </div>
</div>
{% endif %}

<div id="lightbox" class="lightbox" onclick="closeLightbox(event)">
  <div class="lightbox-content">
    <img id="lightboxImg" src="" alt="">
    <button class="lightbox-close" onclick="closeLightbox()">&times;</button>
    <button class="lightbox-prev" onclick="lightboxNav(-1)">&#10094;</button>
    <button class="lightbox-next" onclick="lightboxNav(1)">&#10095;</button>
    <div class="lightbox-counter" id="lightboxCounter"></div>
  </div>
</div>
{% endblock %}

{% block scripts %}
<script>
var allImages = [
  {% for img in all_images %}
  {url: {{ img.url|tojson }}, type: {{ img.type|tojson }}, title: {{ img.get("title", img.filename)|tojson }}},
  {% endfor %}
];
var currentImageIndex = 0;

function selectImage(el, idx, type) {
  if (idx < 0 || idx >= allImages.length) return;
  currentImageIndex = idx;
  var img = allImages[idx];
  document.getElementById('mainImg').src = img.url;
  document.querySelectorAll('.thumb').forEach(function(t) { t.classList.remove('active'); });
  el.classList.add('active');
  var labelEl = document.getElementById('imageTypeLabel');
  if (labelEl) {
    labelEl.textContent = (img.type === 'related' ? '🧲 ' : '📷 ') + img.title;
  }
}

function openLightbox(url) {
  var lb = document.getElementById('lightbox');
  var img = document.getElementById('lightboxImg');
  img.src = url;
  lb.classList.add('active');
  document.body.style.overflow = 'hidden';
  for (var i = 0; i < allImages.length; i++) {
    if (allImages[i].url === url) {
      currentImageIndex = i;
      break;
    }
  }
  updateLightboxCounter();
}

function closeLightbox(event) {
  if (event && event.target !== event.currentTarget &&
      !event.target.classList.contains('lightbox-close')) return;
  document.getElementById('lightbox').classList.remove('active');
  document.body.style.overflow = '';
}

function lightboxNav(dir) {
  if (event) event.stopPropagation();
  currentImageIndex += dir;
  if (currentImageIndex < 0) currentImageIndex = allImages.length - 1;
  if (currentImageIndex >= allImages.length) currentImageIndex = 0;
  var img = allImages[currentImageIndex];
  document.getElementById('lightboxImg').src = img.url;
  updateLightboxCounter();

  var thumbs = document.querySelectorAll('.thumb');
  thumbs.forEach(function(t, i) { t.classList.toggle('active', i === currentImageIndex); });
  document.getElementById('mainImg').src = img.url;
  var labelEl = document.getElementById('imageTypeLabel');
  if (labelEl) {
    labelEl.textContent = (img.type === 'related' ? '🧲 ' : '📷 ') + img.title;
  }
}

function updateLightboxCounter() {
  var el = document.getElementById('lightboxCounter');
  if (el) el.textContent = (currentImageIndex + 1) + ' / ' + allImages.length;
}

document.addEventListener('keydown', function(e) {
  var lb = document.getElementById('lightbox');
  if (lb.classList.contains('active')) {
    if (e.key === 'Escape') closeLightbox();
    if (e.key === 'ArrowLeft') lightboxNav(-1);
    if (e.key === 'ArrowRight') lightboxNav(1);
  } else {
    if (e.key === 'ArrowLeft') {
      var a = document.querySelector('.nav-prev');
      if (a) a.click();
    }
    if (e.key === 'ArrowRight') {
      var a = document.querySelector('.nav-next');
      if (a) a.click();
    }
  }
});
</script>
{% endblock %}'''


def _get_style_css():
    return '''* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; line-height: 1.5; }
a { color: #2563eb; text-decoration: none; }
a:hover { color: #1d4ed8; text-decoration: underline; }
header { background: #1a1a2e; color: white; padding: 12px 0; position: sticky; top: 0; z-index: 100; box-shadow: 0 2px 10px rgba(0,0,0,0.3); }
.header-inner { max-width: 1400px; margin: 0 auto; padding: 0 20px; display: flex; align-items: center; gap: 20px; }
.logo { color: white; font-size: 20px; font-weight: bold; white-space: nowrap; }
.logo:hover { color: #60a5fa; text-decoration: none; }
.search-form { display: flex; flex: 1; max-width: 400px; }
.search-form input { flex: 1; padding: 8px 12px; border: none; border-radius: 6px 0 0 6px; font-size: 14px; }
.search-form button { padding: 8px 14px; border: none; background: #3b82f6; color: white; border-radius: 0 6px 6px 0; cursor: pointer; }
main { max-width: 1400px; margin: 0 auto; padding: 20px; }
.breadcrumb { padding: 10px 0; color: #666; font-size: 14px; }
.breadcrumb a { color: #666; }
.page-header { margin-bottom: 20px; }
.page-header h1 { font-size: 28px; color: #1a1a2e; }
.page-header .count { color: #999; font-weight: normal; }
.page-header .subtitle { color: #666; margin-top: 4px; }
.collections-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 20px; }
.collection-card { background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08); transition: transform 0.2s, box-shadow 0.2s; display: block; color: inherit; }
.collection-card:hover { transform: translateY(-4px); box-shadow: 0 8px 25px rgba(0,0,0,0.15); text-decoration: none; }
.collection-thumb { height: 180px; overflow: hidden; background: #e5e7eb; }
.collection-thumb img { width: 100%; height: 100%; object-fit: cover; }
.collection-info { padding: 14px; }
.collection-info h3 { font-size: 15px; margin-bottom: 6px; color: #1a1a2e; }
.collection-badges { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; margin-bottom: 4px; }
.collection-count { background: #3b82f6; color: white; padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: bold; }
.collection-owner { display: block; color: #999; font-size: 13px; margin-top: 6px; }
.badge { padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: 500; }
.badge-related { background: #fef3c7; color: #92400e; }
.badge-multi { background: #dbeafe; color: #1e40af; }
.collection-page { display: grid; grid-template-columns: 1fr 280px; gap: 30px; }
@media (max-width: 900px) { .collection-page { grid-template-columns: 1fr; } .collection-sidebar { order: -1; } }
.inline-search { display: flex; gap: 8px; margin-bottom: 16px; align-items: center; }
.inline-search input { flex: 1; padding: 8px 12px; border: 2px solid #e5e7eb; border-radius: 8px; font-size: 14px; }
.inline-search input:focus { border-color: #3b82f6; outline: none; }
.inline-search button { padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 8px; cursor: pointer; }
.clear-filters { color: #ef4444; font-size: 13px; }
.active-filters { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
.filter-tag { background: #dbeafe; color: #1e40af; padding: 4px 10px; border-radius: 6px; font-size: 13px; max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.filter-tag a { color: #ef4444; margin-left: 6px; }
.items-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px; }
.item-card { background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,0.08); transition: transform 0.15s; display: block; color: inherit; }
.item-card:hover { transform: translateY(-2px); box-shadow: 0 4px 15px rgba(0,0,0,0.12); text-decoration: none; }
.item-thumb { height: 140px; overflow: hidden; background: #f3f4f6; position: relative; }
.item-thumb img { width: 100%; height: 100%; object-fit: cover; }
.item-badges { position: absolute; bottom: 4px; right: 4px; display: flex; gap: 3px; }
.image-count { background: rgba(0,0,0,0.7); color: white; padding: 2px 6px; border-radius: 4px; font-size: 11px; }
.related-badge { background: rgba(146, 64, 14, 0.85); color: #fef3c7; padding: 2px 6px; border-radius: 4px; font-size: 11px; }
.item-info { padding: 8px 10px; }
.item-title { font-size: 12px; color: #333; line-height: 1.3; margin-bottom: 4px; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
.item-tag { display: inline-block; background: #f0fdf4; color: #166534; padding: 1px 6px; border-radius: 4px; font-size: 10px; margin: 1px 2px; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.no-image { display: flex; align-items: center; justify-content: center; height: 100%; font-size: 40px; color: #ccc; }
.no-image.large { font-size: 80px; min-height: 300px; }
.collection-sidebar { background: white; border-radius: 12px; padding: 16px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); position: sticky; top: 70px; max-height: calc(100vh - 90px); overflow-y: auto; min-width: 0; }
.collection-sidebar h3 { font-size: 14px; color: #666; margin: 16px 0 8px; text-transform: uppercase; }
.collection-sidebar h3:first-child { margin-top: 0; }
.sidebar-controls { display: flex; gap: 8px; margin-bottom: 12px; }
.sidebar-controls button { flex: 1; padding: 4px 8px; font-size: 11px; border: 1px solid #e5e7eb; border-radius: 4px; background: #f9fafb; cursor: pointer; color: #666; }
.sidebar-controls button:hover { background: #e5e7eb; }
.filter-group { margin-bottom: 8px; border-bottom: 1px solid #f3f4f6; padding-bottom: 8px; }
.filter-group summary { cursor: pointer; padding: 6px 0; font-weight: 500; font-size: 14px; list-style: none; word-wrap: break-word; overflow-wrap: break-word; }
.filter-group summary::-webkit-details-marker { display: none; }
.filter-group summary::before { content: "▸ "; color: #999; }
.filter-group[open] summary::before { content: "▾ "; }
.tag-count { color: #999; font-size: 12px; font-weight: normal; }
.filter-values { list-style: none; padding: 0 0 0 12px; max-height: 150px; overflow-y: auto; }
.filter-values li { padding: 2px 0; font-size: 13px; word-wrap: break-word; overflow-wrap: break-word; }
.filter-values a { color: #555; word-break: break-word; display: inline-block; max-width: 100%; }
.filter-values a:hover { color: #2563eb; }
.filter-values .remove { color: #ef4444; }
.filter-values .more { color: #999; font-style: italic; }
.item-page { display: grid; grid-template-columns: 1fr 380px; gap: 30px; margin-top: 10px; }
@media (max-width: 900px) { .item-page { grid-template-columns: 1fr; } }
.main-image { background: white; border-radius: 12px; padding: 10px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.08); position: relative; cursor: zoom-in; }
.main-image img { max-width: 100%; max-height: 70vh; object-fit: contain; border-radius: 8px; }
.image-type-label { position: absolute; top: 16px; left: 16px; background: rgba(0,0,0,0.6); color: white; padding: 4px 10px; border-radius: 6px; font-size: 12px; pointer-events: none; }
.image-sections { margin-top: 16px; }
.image-section { margin-bottom: 16px; }
.image-section h4 { font-size: 14px; color: #555; margin-bottom: 8px; padding-bottom: 4px; border-bottom: 1px solid #e5e7eb; }
.related-section { background: #fffbeb; border-radius: 8px; padding: 12px; border: 1px solid #fde68a; }
.related-section h4 { color: #92400e; border-bottom-color: #fde68a; }
.thumbnails { display: flex; flex-wrap: wrap; gap: 8px; }
.thumb { width: 70px; min-height: 70px; border-radius: 6px; overflow: hidden; cursor: pointer; border: 3px solid transparent; position: relative; background: #f3f4f6; }
.thumb:hover { border-color: #93c5fd; }
.thumb.active { border-color: #3b82f6; }
.thumb img { width: 100%; height: 70px; object-fit: cover; }
.thumb-label { display: block; font-size: 9px; color: #666; text-align: center; padding: 2px; line-height: 1.2; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.image-meta { text-align: center; color: #999; font-size: 13px; margin-top: 8px; }
.item-details { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
.item-details h1 { font-size: 22px; margin-bottom: 12px; word-wrap: break-word; overflow-wrap: break-word; }
.item-description { color: #555; font-size: 14px; line-height: 1.6; margin-bottom: 16px; padding-bottom: 16px; border-bottom: 1px solid #e5e7eb; word-wrap: break-word; overflow-wrap: break-word; }
.item-description img { max-width: 100%; height: auto; border-radius: 6px; margin: 4px 0; }
.item-description a { color: #2563eb; }
.image-description { color: #888; font-size: 13px; margin-bottom: 12px; font-style: italic; }
.item-properties table { width: 100%; border-collapse: collapse; table-layout: fixed; }
.item-properties td { padding: 6px 0; font-size: 14px; border-bottom: 1px solid #f3f4f6; vertical-align: top; }
.prop-key { color: #999; white-space: normal; padding-right: 12px; width: 40%; word-wrap: break-word; overflow-wrap: break-word; hyphens: auto; }
.prop-val { width: 60%; word-wrap: break-word; overflow-wrap: break-word; }
.prop-val a { color: #333; word-break: break-word; }
.prop-val a:hover { color: #2563eb; }
.related-info { margin-top: 20px; padding-top: 16px; border-top: 1px solid #e5e7eb; }
.related-info h3 { font-size: 16px; color: #92400e; margin-bottom: 10px; }
.related-list { list-style: none; padding: 0; }
.related-list li { padding: 6px 8px; margin-bottom: 4px; background: #fffbeb; border-radius: 6px; border: 1px solid #fde68a; display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.related-title { font-size: 13px; color: #92400e; font-weight: 500; }
.related-link { font-size: 11px; color: #999; white-space: nowrap; }
.item-nav { display: flex; justify-content: space-between; margin-top: 20px; padding-top: 16px; border-top: 1px solid #e5e7eb; }
.nav-prev, .nav-next { padding: 8px 14px; background: #f3f4f6; border-radius: 8px; font-size: 13px; color: #555; }
.nav-prev:hover, .nav-next:hover { background: #e5e7eb; text-decoration: none; }
.original-link { margin-top: 16px; text-align: center; }
.original-link a { color: #999; font-size: 13px; }
.similar-section { margin-top: 40px; }
.similar-section h2 { font-size: 20px; margin-bottom: 16px; }
.similar-grid { grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); }
.similarity { font-size: 10px; color: #999; }
.pagination { display: flex; justify-content: center; align-items: center; gap: 4px; margin-top: 30px; flex-wrap: wrap; }
.pagination a, .pagination .current { padding: 6px 12px; border-radius: 6px; font-size: 14px; }
.pagination a { background: white; color: #333; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.pagination a:hover { background: #3b82f6; color: white; text-decoration: none; }
.pagination .current { background: #3b82f6; color: white; font-weight: bold; }
.pagination .dots { color: #999; }
.lightbox { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.9); z-index: 1000; align-items: center; justify-content: center; }
.lightbox.active { display: flex; }
.lightbox-content { position: relative; max-width: 95vw; max-height: 95vh; }
.lightbox-content img { max-width: 95vw; max-height: 90vh; object-fit: contain; border-radius: 4px; }
.lightbox-close { position: absolute; top: -40px; right: 0; background: none; border: none; color: white; font-size: 36px; cursor: pointer; padding: 0 10px; }
.lightbox-close:hover { color: #f87171; }
.lightbox-prev, .lightbox-next { position: absolute; top: 50%; transform: translateY(-50%); background: rgba(255,255,255,0.15); border: none; color: white; font-size: 28px; padding: 16px 12px; cursor: pointer; border-radius: 4px; }
.lightbox-prev:hover, .lightbox-next:hover { background: rgba(255,255,255,0.3); }
.lightbox-prev { left: -50px; }
.lightbox-next { right: -50px; }
.lightbox-counter { position: absolute; bottom: -30px; left: 50%; transform: translateX(-50%); color: rgba(255,255,255,0.7); font-size: 14px; }
@media (max-width: 768px) { .lightbox-prev { left: 5px; } .lightbox-next { right: 5px; } }
footer { text-align: center; padding: 30px; color: #999; font-size: 13px; margin-top: 40px; }

/* Cross-item-link в описании */
.item-description .cross-item-link {
    display: inline-block;
    margin: 3px;
    vertical-align: middle;
}
.item-description .cross-item-link img {
    max-height: 60px;
    max-width: 120px;
    border-radius: 4px;
    border: 1px solid #e5e7eb;
    padding: 2px;
    background: white;
    transition: transform 0.15s, box-shadow 0.15s;
    object-fit: contain;
}
.item-description .cross-item-link:hover img {
    transform: scale(1.1);
    box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    border-color: #3b82f6;
}
.cross-item-fallback {
    display: inline-block;
    background: #f3f4f6;
    color: #666;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    margin: 2px;
    border: 1px solid #e5e7eb;
}

.search-info { color: #666; font-size: 14px; }
.search-info a { color: #ef4444; }'''


# ═══════════════════════════════════════════════
# Запуск
# ═══════════════════════════════════════════════

if __name__ == '__main__':
    # Заголовок
    if getattr(sys, 'frozen', False):
        os.system('title Collectionerus Viewer')
    print("=" * 50)
    print(" 🌍 Локальный просмотрщик коллекций v4")
    print("=" * 50)

    ensure_dirs()

    # Диагностика путей
    print(f'  📂 Корень проекта: {PROJECT_ROOT}')
    print(f'  📂 Данные: {METADATA_DIR}')
    if not os.path.exists(METADATA_DIR):
        print(f'  ⚠ Папка данных не найдена!')
        print(f'  ⚠ Создайте data/metadata/ рядом с EXE или в корне проекта')
    else:
        cols = [d for d in os.listdir(METADATA_DIR)
                if os.path.isdir(os.path.join(METADATA_DIR, d)) and not d.startswith("_")]
        print(f'  📋 Коллекций найдено: {len(cols)}')
    create_templates()

    print(f"\n 📂 Данные: {METADATA_DIR}")
    print(f" 🌐 http://localhost:{VIEWER_PORT}")
    print(f"   Ctrl+C для остановки\n")


    # Автоматически открываем браузер
    import threading
    import webbrowser

    def _open_browser():
        """Открывает браузер через 1.5 секунды после старта."""
        import time
        time.sleep(1.5)
        url = f'http://localhost:{VIEWER_PORT}'
        print(f'\n  \U0001f310 Открываю браузер: {url}')
        webbrowser.open(url)

    # Открываем только при первом запуске (не при reload)
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        t = threading.Thread(target=_open_browser, daemon=True)
        t.start()


    print(f'  \U0001f6d1 Для остановки: Ctrl+C или закройте это окно')
    print()
    app.run(
        host=VIEWER_HOST,
        port=VIEWER_PORT,
        debug=VIEWER_DEBUG,
        use_reloader=not getattr(sys, 'frozen', False),
    )