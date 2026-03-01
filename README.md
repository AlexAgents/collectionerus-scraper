<!-- Бейджи -->
<p align="center">
  <a href="https://github.com/qexela/collectionerus-scraper/releases/latest">
    <img src="https://img.shields.io/github/v/release/qexela/collectionerus-scraper?style=for-the-badge&logo=github&label=Release&color=blue" alt="GitHub Release">
  </a>
  <a href="https://github.com/qexela/collectionerus-scraper/releases">
    <img src="https://img.shields.io/github/downloads/qexela/collectionerus-scraper/total?style=for-the-badge&logo=download&label=Downloads&color=green" alt="Downloads">
  </a>
  <a href="https://github.com/qexela/collectionerus-scraper/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/qexela/collectionerus-scraper?style=for-the-badge&logo=opensourceinitiative&label=License&color=yellow" alt="License">
  </a>
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  </a>
  <a href="https://flask.palletsprojects.com/">
    <img src="https://img.shields.io/badge/Flask-2.3+-000000?style=for-the-badge&logo=flask&logoColor=white" alt="Flask">
  </a>
  <a href="https://github.com/qexela/collectionerus-scraper/stargazers">
    <img src="https://img.shields.io/github/stars/qexela/collectionerus-scraper?style=for-the-badge&logo=star&label=Stars&color=gold" alt="Stars">
  </a>
  <a href="https://github.com/qexela/collectionerus-scraper/commits/main">
    <img src="https://img.shields.io/github/last-commit/qexela/collectionerus-scraper?style=for-the-badge&logo=git&label=Last%20Commit&color=purple" alt="Last Commit">
  </a>
</p>

---

# 📦 Collectionerus Scraper

> Полнофункциональный скрапер, просмотрщик и архиватор коллекций с сайта [collectionerus.ru](https://collectionerus.ru).

<p align="center">
  <img src="https://img.shields.io/badge/Коллекций-141-blue?style=flat-square" alt="Collections">
  <img src="https://img.shields.io/badge/Предметов-~200K-green?style=flat-square" alt="Items">
  <img src="https://img.shields.io/badge/Данные-~125GB-orange?style=flat-square" alt="Data Size">
</p>

---

## 📋 Оглавление

- [Описание](#описание)
- [Возможности](#возможности)
- [Структура проекта](#структура-проекта)
- [Установка](#установка)
- [Быстрый старт](#быстрый-старт)
- [Компоненты](#компоненты)
  - [Scraper (scraper.py)](#scraper-scraperpy)
  - [Viewer (viewer.py)](#viewer-viewerpy)
  - [Builder (builder.py)](#builder-builderpy)
  - [Tests (tests.py)](#tests-testspy)
  - [Config (config.py)](#config-configpy)
  - [Utils (utils.py)](#utils-utilspy)
- [Формат данных](#формат-данных)
- [Использование](#использование)
- [Сборка EXE](#сборка-exe)
- [Безопасность EXE](#-безопасность-exe)
- [FAQ](#faq)
- [Лицензия](#лицензия)

---

## Описание

**Collectionerus Scraper** — инструмент для автоматического скачивания публичных коллекций предметов (открытки, значки, марки, монеты и т.д.) с сайта collectionerus. Проект состоит из четырёх основных компонентов:

1. **Scraper** — многопоточный скрапер с интерактивным меню, умным fallback'ом URL, кешем паттернов, возобновлением после прерывания и упаковкой в ZIP.
2. **Viewer** — локальный веб-просмотрщик на Flask с фильтрацией, поиском, lightbox-галереей и поддержкой cross-item ссылок.
3. **Builder** — скрипт сборки в автономные EXE-файлы через PyInstaller.
4. **Tests** — простое тестирование функций.

---

## Возможности

### Scraper

- 🔄 **Автоматическое скачивание** всех коллекций или выбранных по номерам
- 🧵 **Многопоточность** — параллельная загрузка изображений (до 10 потоков) и деталей предметов (до 5 потоков)
- 💾 **Возобновление** — прогресс сохраняется в `_progress.json`, прерванное скачивание продолжается с того же места
- 🔍 **Умный fallback URL** — автоматический перебор качества изображений (`preloaded-items` → `items-large` → `items-thumbs`) с обучаемым кешем паттернов
- 📦 **ZIP-архивация** — упаковка скачанных коллекций с удалением исходных папок
- ✅ **Проверка целостности** — поиск битых, пропущенных и дублирующихся файлов
- 🔎 **Сканирование** — анализ коллекций на наличие дополнительных фото и связанных предметов
- 📊 **Детализированная статистика** — подсчёт 404, таймаутов, HTML-ответов, fallback'ов
- 🛡️ **Антибот-защита** — автоматическое определение Cloudflare и rate-limit'ов

### Viewer

- 🌐 **Полный оффлайн** — работает без интернета, все данные локальные
- 🔎 **Поиск и фильтрация** по всем свойствам предметов
- 🖼️ **Lightbox-галерея** с навигацией клавиатурой
- 🔗 **Cross-item ссылки** — связанные предметы отображаются с локальными изображениями через media-индекс
- 📱 **Адаптивный дизайн** — работает на мобильных устройствах
- 🔒 **XSS-защита** — санитизация HTML-описаний (bleach или fallback regex)
- 🚀 **Автооткрытие браузера** при запуске

### Builder

- 🏗️ **Сборка в EXE** — два автономных исполняемых файла
- 🎨 **Генерация иконок** — автоматическое создание `.ico` через Pillow или минимальный BMP

---

## Структура проекта

```
collectionerus-scraper/
├── config.py                  # Единая конфигурация
├── utils.py                   # Общие утилиты
├── requirements.txt           # Зависимости Python
├── .gitignore
├── README.md
├── LICENSE
├── scripts/
│   ├── scraper.py             # Основной скрапер (v10)
│   ├── viewer.py              # Веб-просмотрщик (v4)
│   ├── builder.py             # Сборка EXE
│   └── tests.py               # Тесты проекта
├── assets/
│   ├── scraper.ico            # Иконка скрапера
│   └── viewer.ico             # Иконка вьюера
├── data/
│   ├── metadata/              # Метаданные и изображения коллекций
│   │   ├── _collections_list.json
│   │   ├── _check_report.json
│   │   └── <slug>/            # Папка коллекции
│   │       ├── collection_info.json
│   │       ├── metadata.json
│   │       ├── metadata.csv
│   │       ├── fields_info.json
│   │       ├── _progress.json
│   │       └── images/
│   │           ├── 00001_Название.jpg
│   │           └── 00002_Название/
│   │               ├── main.jpg
│   │               ├── 02.jpg
│   │               ├── related_01_Связанный.jpg
│   │               └── _info.json
│   ├── archives/              # ZIP-архивы (repack)
│   └── scraper.log            # Лог работы
├── dist/                      # Собранные EXE
│   ├── collectionerus-scraper.exe
│   └── collectionerus-viewer.exe
├── templates/                 # Jinja2-шаблоны (создаются автоматически)
│   ├── base.html
│   ├── index.html
│   ├── collection.html
│   └── item.html
└── static/
    └── style.css
```

---

## Установка

### Требования

- Python 3.8+
- pip

### Установка зависимостей

```bash
pip install -r requirements.txt
```

### requirements.txt

```
requests>=2.28
beautifulsoup4>=4.11
flask>=2.3
markupsafe>=2.1
bleach>=6.0       # опционально, для XSS-защиты
pillow>=9.0       # опционально, для генерации иконок
pyinstaller>=5.0  # опционально, для сборки EXE
```

---

## Быстрый старт

### 1. Скачать коллекции

```bash
python scripts/scraper.py
```

Откроется интерактивное меню:

```
══════════════════════════════════════════════════════════════════════
 📦 COLLECTIONERUS SCRAPER v10
══════════════════════════════════════════════════════════════════════
 📋 Коллекций: 141 (кэш: 5 мин назад)
 ✅ Готово: 120  🔄 В процессе: 3  ⬚ Не начато: 18
──────────────────────────────────────────────────────────────────────
 1. 📥 Скачать коллекции
 2. 🔍 Проверить скачанное
 3. 🔎 Сканировать (доп. фото)
 4. 📦 Упаковать в ZIP
 5. 🔄 Перекачать с нуля
 6. 🔃 Обновить список коллекций
 7. 📋 Показать список коллекций
──────────────────────────────────────────────────────────────────────
 q. Выход
──────────────────────────────────────────────────────────────────────
```

### 2. Просмотреть скачанное

```bash
python scripts/viewer.py
```

Откроется браузер на `http://localhost:5000` с каталогом всех скачанных коллекций.

### 3. Собрать EXE (опционально)

```bash
python scripts/builder.py
```

---

## Компоненты

### Scraper (`scraper.py`)

Основной скрапер коллекций. Версия 10 с полным набором исправлений.

#### Ключевые классы

| Класс | Описание |
|-------|----------|
| `CollectionScraper` | Главный скрапер: HTTP-сессия, парсинг HTML, скачивание изображений, умный fallback URL |
| `MenuManager` | Интерактивное меню с навигацией, кешем коллекций |
| `ZipManager` | Упаковка в ZIP, проверка актуальности, удаление папок |
| `DownloadChecker` | Проверка целостности: битые файлы, пропущенные изображения, дубликаты |
| `AntiBotChecker` | Определение Cloudflare, rate-limit, адаптация задержек |

#### Формат выбора коллекций

```
all       — все коллекции
5         — коллекция #5
1-10      — диапазон 1..10
1,3,5     — перечисление
1-10,!5   — диапазон с исключением
odd       — нечётные
even      — чётные
```

#### Умный fallback URL

Скрапер автоматически пробует URL в порядке убывания качества:

1. `/preloaded-items/` — оригинал
2. `/items-large/` — большой размер
3. `/items-thumbs/` — миниатюра (всегда доступна)

Кеш запоминает, какие паттерны работают для каждой коллекции. После 3 ответов 404 паттерн отключается.

#### Потокобезопасность

- Каждый поток использует уникальный `tmp`-файл (`filepath.tmp.<thread_id>`)
- Статистика защищена `threading.Lock`
- Кеш URL-паттернов защищён отдельным `_cache_lock`

---

### Viewer (`viewer.py`)

Локальный веб-просмотрщик на Flask (v4).

#### Возможности

- **Главная страница** — сетка коллекций с миниатюрами, поиском, бейджами
- **Страница коллекции** — фильтрация по всем свойствам, пагинация, сортировка, боковая панель
- **Страница предмета** — галерея изображений, lightbox, связанные предметы, похожие, навигация ← →
- **Media-индекс (v3)** — маппинг URL с сайта → локальные файлы для отображения cross-item ссылок

#### Роуты

| Роут | Описание |
|------|----------|
| `/` | Список коллекций |
| `/collection/<slug>/` | Предметы коллекции |
| `/collection/<slug>/item/<index>/` | Карточка предмета |
| `/data/<path>` | Статические файлы из metadata |
| `/media/<path>` | Проксирование media URL через индекс |
| `/collections/<slug>/items/<id>/` | Редирект внешних ссылок → локальные |

#### Media-индекс

При запуске viewer строит индекс всех скачанных изображений:

```
URL с сайта (thumb_url, images[]) → путь к локальному файлу
```

Это позволяет корректно отображать `<img>` в описаниях предметов, даже если URL указывает на другой предмет (cross-item-link).

---

### Builder (`builder.py`)

Сборка в автономные EXE через PyInstaller.

#### Сборка

```bash
python scripts/builder.py
```

Интерактивное меню:

| # | Действие |
|---|----------|
| 1 | Собрать `collectionerus-scraper.exe` (консольное) |
| 2 | Собрать `collectionerus-viewer.exe` (консольное + Flask) |
| 3 | Собрать оба |
| 4 | Перегенерировать иконки |
| 5 | Очистить build/ |
| 6 | Очистить dist/ |

#### EXE + данные

EXE ищет папку `data/` в следующем порядке:

1. Рядом с `.exe`
2. На уровень выше (если exe в `dist/`)
3. В текущей рабочей директории
4. Создаёт `data/` рядом с exe

---

### Tests (`tests.py`)

Базовые тесты проекта.

```bash
# Все тесты
python scripts/tests.py

# Выборочно
python scripts/tests.py imports
python scripts/tests.py config
python scripts/tests.py utils
python scripts/tests.py structure
python scripts/tests.py viewer
python scripts/tests.py data
python scripts/tests.py scraper
```

#### Что проверяется

| Группа | Тесты |
|--------|-------|
| imports | Импорт config, utils, requests, bs4, flask, markupsafe |
| config | PROJECT\_ROOT, BASE\_URL, задержки, потоки, расширения, ensure\_dirs, PyInstaller |
| utils | safe\_filename, parse\_selection, get\_extension |
| structure | Наличие обязательных файлов |
| viewer | Ключевые функции: \_make\_image\_url, \_build\_media\_index, sanitize\_html, serve\_media и др. |
| data | Структура metadata.json, images/, ZIP-архивы |
| scraper | Классы, методы, отсутствие safe\_dirname |

---

### Config (`config.py`)

Единая конфигурация проекта.

#### Основные параметры

```python
# Пути
PROJECT_ROOT    # Корень проекта (с поддержкой PyInstaller)
DATA_DIR        # data/
METADATA_DIR    # data/metadata/
ARCHIVES_DIR    # data/archives/

# Сеть
BASE_URL = "https://collectionerus.ru"
DELAY_PAGE = 0.1          # задержка между страницами (сек)
DELAY_ITEM = 0.05         # задержка между предметами
IMAGE_THREADS = 10        # потоки для скачивания изображений
ITEM_THREADS = 5          # потоки для загрузки деталей

# Файлы
MAX_FILENAME_LEN = 120
MIN_FILE_SIZE = 500       # минимальный размер файла (байт)
BATCH_SIZE = 50           # размер пакета для сохранения прогресса

# Viewer
VIEWER_HOST = '0.0.0.0'
VIEWER_PORT = 5000
VIEWER_DEBUG = True

# ZIP
ZIP_COMPRESSION_LEVEL = 6
```

---

### Utils (`utils.py`)

Общие утилиты, используемые всеми компонентами.

| Функция | Описание |
|---------|----------|
| `safe_filename(text, max_len)` | Безопасное имя файла (убирает спецсимволы, обрезает) |
| `get_extension(url, default)` | Извлекает расширение из URL |
| `parse_selection(input, max)` | Парсит строку выбора (`all`, `1-10`, `!5`, `odd`) |
| `load_metadata(dir)` | Загружает метаданные из metadata.json или \_progress.json |
| `is_valid_image(filepath)` | Проверяет файл по magic bytes (JPEG, PNG, GIF, WebP, BMP, TIFF) |
| `is_image_file(filename)` | Проверяет расширение файла |
| `atomic_write_json(path, data)` | Атомарная запись JSON (tmp + os.replace + fallback) |
| `dir_size(path)` | Размер директории рекурсивно |
| `file_hash(filepath)` | MD5-хеш файла |
| `load_collections_cache()` | Загрузка кеша списка коллекций |
| `save_collections_cache(list)` | Сохранение кеша |
| `format_cache_age(path)` | Человекочитаемый возраст кеша |
| `get_col_dir(slug)` | Путь к папке коллекции |

---

## Формат данных

### metadata.json

```json
{
  "collection": {
    "name": "Советские открытки",
    "slug": "ussr-postcards",
    "url": "https://collectionerus.ru/collections/ussr-postcards/",
    "owner": "Иванов",
    "count": 500
  },
  "scraped_at": "2026-02-28T12:00:00",
  "total_items": 500,
  "items": [
    {
      "index": 1,
      "filename": "00001_Открытка_1960.jpg",
      "title": "Открытка «С Новым годом!»",
      "url": "https://collectionerus.ru/collections/ussr-postcards/items/12345/",
      "data_id": "12345",
      "data_group": "1960",
      "description": "Художник: Зарубин",
      "description_html": "<div>...</div>",
      "properties": {
        "Год": "1960",
        "Художник": "Зарубин",
        "Тираж": "500000"
      },
      "images": [
        "https://collectionerus.ru/media/preloaded-items/abc.jpg"
      ],
      "downloaded_files": ["00001_Открытка_1960.jpg"],
      "related_items": [
        {
          "file": "00001_Открытка_1960/related_01_Обратная_сторона.jpg",
          "title": "Обратная сторона",
          "url": "https://collectionerus.ru/collections/ussr-postcards/items/12346/"
        }
      ],
      "has_folder": true
    }
  ]
}
```

### Структура папки коллекции

```
data/metadata/<slug>/
├── collection_info.json   # Информация о коллекции
├── metadata.json          # Все предметы с метаданными
├── metadata.csv           # CSV-экспорт
├── fields_info.json       # Покрытие полей
├── _progress.json         # Прогресс скачивания (удаляется при ZIP)
└── images/
    ├── 00001_Простой.jpg                   # Предмет с 1 фото
    ├── 00002_С_галереей/                   # Предмет с несколькими фото
    │   ├── main.jpg
    │   ├── 02.jpg
    │   ├── related_01_Связанный.jpg
    │   └── _info.json
    └── ...
```

---

## Использование

### Скачивание

```bash
# Запуск с интерактивным меню
python scripts/scraper.py

# В меню выбрать:
# 1 → скачать коллекции
# Ввести: all (все), 1-10 (диапазон), 5 (одну)
```

### Проверка

```bash
# В меню скрапера:
# 2 → проверить скачанное
# Отчёт сохраняется в data/metadata/_check_report.json
```

### Просмотр

```bash
python scripts/viewer.py
# Откроется http://localhost:5000
```

### Упаковка в ZIP

```bash
# В меню скрапера:
# 4 → упаковать в ZIP
# Выбрать коллекции, опционально удалить папки после
```

### Тестирование

```bash
python scripts/tests.py
```

---

## Сборка EXE

```bash
# Установить PyInstaller
pip install pyinstaller pillow

# Запустить сборку
python scripts/builder.py
# Выбрать 3 — собрать оба EXE

# Результат:
# dist/collectionerus-scraper.exe  (~15 MB)
# dist/collectionerus-viewer.exe   (~17 MB)
```

### Использование EXE

```bash
# Положить data/ рядом с EXE или на уровень выше
# Запустить:
collectionerus-scraper.exe   # скрапер с меню
collectionerus-viewer.exe    # откроется браузер
```

---

## 🛡️ Безопасность EXE

EXE-файлы собраны через [PyInstaller](https://pyinstaller.org/) из открытого исходного кода данного репозитория. PyInstaller упаковывает интерпретатор Python и зависимости в один файл, что **часто вызывает ложные срабатывания антивирусов** (false positive).

### Проверки на VirusTotal

Каждый релиз проверен на [VirusTotal](https://www.virustotal.com/) (70+ антивирусных движков):

| Файл | Результат | Отчёт |
|------|-----------|-------|
| `collectionerus-scraper.exe` | ✅ **5/70** — AI срабатывания | [🔗 Открыть отчёт](https://www.virustotal.com/gui/file/e7729f405f9e993a529855f2b10e7cdbc0774b2649b50a89608f82f1b45b02fb) |
| `collectionerus-viewer.exe` | ✅ **6/70** — AI срабатывания | [🔗 Открыть отчёт](https://www.virustotal.com/gui/file/0d5c254baf649ea1f9b9ab7b37e0c4b733a9255e7d1bccb1c63d581f99ac2d72) |

> 💡 **Как проверить самостоятельно:** загрузите EXE на [virustotal.com](https://www.virustotal.com/) → увидите результат за 1-2 минуты.

### Контрольные суммы (SHA-256)

```
e7729f405f9e993a529855f2b10e7cdbc0774b2649b50a89608f82f1b45b02fb  collectionerus-scraper.exe
0d5c254baf649ea1f9b9ab7b37e0c4b733a9255e7d1bccb1c63d581f99ac2d72  collectionerus-viewer.exe
ee79ab3db1915b01e640222c5d35c96b66407a9ba36aa7323fa4c20725a055b5  collectionerus-scraper.zip
604c0e975e9a29200c230da1f61d427f76f6bdc93e21df660a00c9aace16ebe5  collectionerus-viewer.zip
```

### Почему антивирус может сработать?

PyInstaller-сборки иногда детектируются эвристикой как «подозрительные», потому что:

- EXE распаковывает Python-рантайм во временную папку при запуске
- Такой же механизм используют некоторые вредоносные программы
- Это **не означает**, что файл заражён

**Решения:**
- Проверьте отчёт VirusTotal по ссылкам выше
- Сравните SHA-256 хеш скачанного файла с указанным
- Соберите EXE самостоятельно из исходников: `python scripts/builder.py`
- Добавьте файл в исключения антивируса

### Windows SmartScreen

При первом запуске Windows может показать предупреждение:

```
"Windows защитила ваш компьютер"
→ Подробнее → Выполнить в любом случае
```

Это происходит потому, что EXE не подписан цифровой подписью (code signing certificate стоит $200+/год). Предупреждение исчезнет после нескольких запусков.

---

## FAQ

### Где хранятся данные?

В папке `data/metadata/` — метаданные и изображения, `data/archives/` — ZIP-архивы.

### Можно ли прервать скачивание?

Да, `Ctrl+C`. Прогресс сохраняется в `_progress.json`. При следующем запуске скачивание продолжится с того же места.

### Как перекачать коллекцию?

В меню скрапера: `5 → Перекачать с нуля`, выбрать номера коллекций.

### Viewer не находит изображения?

Убедитесь, что `data/metadata/` содержит папки коллекций с `metadata.json` и `images/`.

### Как работает media-индекс?

При запуске viewer сканирует все `metadata.json` и строит маппинг:

- `thumb_url` предмета → путь к его первому скачанному файлу
- `images[i]` → `downloaded_files[i]`
- `related_items[].thumb_url` → `related_items[].file`

Это позволяет отображать `<img src="/media/items-thumbs/...">` в описаниях предметов из локальных файлов.

### Сколько весят данные?

~125 ГБ для всех 141 коллекций (в ZIP-архивах).

### Безопасны ли EXE-файлы?

Да. Смотрите раздел [Безопасность EXE](#-безопасность-exe) — там ссылки на отчёты VirusTotal и инструкции по самостоятельной проверке.

---

## ⚠️ Отказ от ответственности

### Лицензия

Проект распространяется под лицензией [**MIT**](LICENSE) — используйте как угодно,
но на свой страх и риск.

### Гарантии

Программное обеспечение предоставляется **«КАК ЕСТЬ» (AS IS)**,
без каких-либо явных или подразумеваемых гарантий, включая, но не
ограничиваясь, гарантиями товарной пригодности и пригодности для
конкретной цели.

### Ответственность

Автор **ни при каких обстоятельствах не несёт ответственности** за:

- прямые, косвенные, случайные, штрафные или побочные убытки
- потерю данных, прибыли или деловой репутации
- блокировку аккаунтов, IP-адресов, доменов
- нарушение условий использования сторонних сервисов
- любые претензии третьих лиц

### Авторские права на данные

- Все скачиваемые материалы являются собственностью их правообладателей
- Сайт [collectionerus.ru](https://collectionerus.ru) — владелец контента
- Пользователь обязан самостоятельно убедиться в законности
  скачивания и использования материалов в своей юрисдикции

### Назначение

Проект создан **исключительно в образовательных и исследовательских целях**
для демонстрации методов веб-скрапинга на Python.
Автор не поощряет нарушение авторских прав или условий использования сайтов.
