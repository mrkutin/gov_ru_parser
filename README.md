## Парсер с пагинацией на Playwright (Python)

Этот инструмент открывает стартовый URL и кликает по кнопке/ссылке «Следующая»/«Показать ещё» (или «Next»), пока не закончатся страницы или клик перестанет приводить к навигации/изменению контента. С каждой страницы извлекается текст из блока по селектору `.reader_article_body` с сохранением разбиения на абзацы, далее абзацы кодируются эмбеддингами моделью [`ai-forever/FRIDA`](https://huggingface.co/ai-forever/FRIDA) и сохраняются в Qdrant в коллекцию, выделенную под документ.

### Требования
- Python 3.10+
- Установленные браузеры Playwright

### Установка
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
```

### Запуск
```bash
python main.py "uk_1996" "http://government.ru/docs/all/96145/" \
  --headless --max-pages 50 \
  --content-selector ".reader_article_body" \
  --next-selector "text=Показать еще" \
  --qdrant-host localhost --qdrant-port 6333
```
Где `uk_1996` — идентификатор документа; коллекция будет называться `docs_uk_1996`.

Повторная загрузка того же документа по умолчанию удаляет старые чанки и загружает новые. Чтобы сохранить старые — добавьте `--no-recreate`.

Опции:
- `doc_id` (обязателен): уникальный ID документа; имя коллекции = `docs_{doc_id}`.
- `start_url` (обязателен): начальная страница.
- `--next-selector`: явный CSS селектор кнопки/ссылки (приоритет над текстом).
- `--next-text` (по умолчанию «Показать еще»): текст кнопки/ссылки.
- `--headless`: запуск без UI.
- `--max-pages`: ограничение на количество страниц.
- `--content-selector` (по умолчанию `.reader_article_body`): селектор контента.
- `--qdrant-url`/`--qdrant-host`/`--qdrant-port`/`--qdrant-api-key`: настройки подключения к Qdrant.
- `--embedding-model`: HF модель эмбеддингов (по умолчанию `ai-forever/FRIDA`).
- `--collection-prefix` (по умолчанию `docs_`).
- `--no-recreate`: не удалять старые чанки документа перед загрузкой.

### Поведение остановки
Парсер прекращает работу, если:
- Элемент «Следующая» не найден.
- Клик не приводит к навигации и не меняет DOM (учитываются SPA-сценарии).
- Достигнут `--max-pages`.

### Примечания
- По умолчанию используется «человеческая» конфигурация контекста (локаль ru-RU, обычный размер окна, реальный User-Agent). Чтобы видеть браузер, не добавляйте `--headless`.
- Если на сайте иной текст «кнопки дальше», задайте `--next-text` или используйте точный `--next-selector`.

### Примеры

- government.ru c кнопкой «Показать еще» (ленивая подзагрузка, без смены URL):
```bash
python main.py "uk_1996" "http://government.ru/docs/all/96145/" \
  --headless --max-pages 50 \
  --content-selector ".reader_article_body" \
  --next-selector "text=Показать еще" \
  --qdrant-host localhost --qdrant-port 6333
```
Если используется «Показать ещё» (с «ё»), укажите соответствующий текст в селекторе или используйте `--next-text "Показать ещё"`.



### Docker

Сборка образа:

```bash
docker build -t gov-ru-parser:latest .
```

Запуск контейнера (пример соответствует команде `python main.py gk_1994 http://government.ru/docs/all/95825/ --qdrant-url http://localhost:6333 --no-recreate`):

> На macOS/Windows внутри контейнера вместо `localhost` используйте `host.docker.internal` для доступа к сервисам на хосте.

```bash
docker run --rm \
  -e DOC_ID=gk_1994 \
  -e START_URL=http://government.ru/docs/all/95825/ \
  -e QDRANT_URL=http://host.docker.internal:6333 \
  -e NO_RECREATE=1 \
  gov-ru-parser:latest
```

Дополнительные переменные окружения (необязательно):

- `NEXT_SELECTOR` (по умолчанию `.show-more`)
- `NEXT_TEXT` (по умолчанию `Следующая`)
- `CONTENT_SELECTOR` (по умолчанию `.reader_article_body`)
- `HEADLESS` (`1`/`true` для включения)
- `MAX_PAGES` (число)
- `ARTICLE_REGEX` (регэксп заголовка статьи, чтобы включить группировку)
- `NO_ARTICLE_GROUPING` (`1`/`true` чтобы отключить группировку по статьям)
- `NO_RECREATE` (`1`/`true` чтобы не пересоздавать коллекцию)

Docker Compose (Qdrant + приложение):

```bash
docker compose up --build
```

В `docker-compose.yml` приложение обращается к Qdrant по `http://qdrant:6333`. Чтобы задать свои параметры, используйте секцию `environment:` или флаги `-e` при запуске.

