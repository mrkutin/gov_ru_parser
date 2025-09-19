from __future__ import annotations

import hashlib
import random
import time
from typing import Iterable, List, Optional

from loguru import logger
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, Page, sync_playwright

import hashlib
import time
from typing import Optional
import difflib

def _get_page_fingerprint(page: Page) -> str:
    content_html = page.content()
    return hashlib.sha256(content_html.encode("utf-8")).hexdigest()


def _extract_paragraphs(page: Page, content_selector: str) -> List[str]:
    paragraph_locator = page.locator(f"{content_selector} p")
    paragraphs: List[str] = []
    try:
        p_count = paragraph_locator.count()
    except Exception:
        p_count = 0

    if p_count and p_count > 0:
        for i in range(p_count):
            try:
                t = paragraph_locator.nth(i).inner_text().strip()
            except Exception:
                t = ""
            if t:
                paragraphs.append(t)
        return paragraphs

    container = page.locator(content_selector)
    try:
        text = container.inner_text().strip()
    except Exception:
        text = ""
    if not text:
        return []
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    chunks = [c.strip() for c in normalized.split("\n\n")]
    return [c for c in chunks if c]


def paginate_extract_paragraphs(
    start_url: str,
    max_pages: Optional[int] = None,
    next_selector: Optional[str] = None,
    next_text: str = "Следующая",
    headless: bool = True,
    slow_mo_ms: int = 0,
    user_agent: Optional[str] = None,
    navigation_timeout_ms: int = 15000,
    content_selector: str = ".reader_article_body",
    # Human-like behavior tuning
    humanize: bool = True,
    dwell_min_s: float = 0.8,
    dwell_max_s: float = 2.0,
    read_scroll_min_steps: int = 2,
    read_scroll_max_steps: int = 4,
    read_scroll_pause_min_s: float = 0.2,
    read_scroll_pause_max_s: float = 0.6,
) -> List[str]:
    paragraphs: List[str] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, slow_mo=slow_mo_ms or None)

        # Randomize typical desktop viewport
        viewport = {
            "width": random.randint(1280, 1920),
            "height": random.randint(720, 1080),
        }

        # Rotate common Chrome user-agents if not provided
        default_uas = [
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
            ),
            (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
        ]
        chosen_ua = user_agent or random.choice(default_uas)

        context = browser.new_context(
            user_agent=chosen_ua,
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            viewport=viewport,
            device_scale_factor=random.choice([1, 1.25, 1.5, 2]),
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",
            },
        )
        page = context.new_page()
        page.set_default_navigation_timeout(navigation_timeout_ms)
        page.set_default_timeout(navigation_timeout_ms)
        logger.info(f"Открываю стартовую страницу: {start_url}")
        page.goto(start_url, wait_until="domcontentloaded")

        if humanize:
            _human_read_page(
                page,
                read_steps=random.randint(read_scroll_min_steps, read_scroll_max_steps),
                pause_min_s=read_scroll_pause_min_s,
                pause_max_s=read_scroll_pause_max_s,
            )
            _human_pause(dwell_min_s, dwell_max_s)

        previous_url: Optional[str] = None
        previous_fingerprint: Optional[str] = None
        page_count = 0

        def find_next(page: Page):
            if next_selector:
                return page.locator(next_selector).first
            # Fallbacks
            locs = [
                page.locator("a[rel=next]").first,
                page.get_by_role("button", name=next_text).first,
                page.get_by_role("link", name=next_text).first,
                page.locator(f"text=^{next_text}$").first,
                page.locator(f"text={next_text}").first,
                page.get_by_role("button", name="Next").first,
                page.get_by_role("link", name="Next").first,
            ]
            for l in locs:
                try:
                    if l and l.is_visible():
                        return l
                except Exception:
                    continue
            return None

        while True:
            page_count += 1
            logger.info(f"Текущая страница #{page_count}: {page.url}")
            current_pars = _extract_paragraphs(page, content_selector)
            if current_pars:
                if paragraphs and current_pars:
                    # Seam handling: avoid extra blanks if first item empty
                    if not current_pars[0].strip():
                        current_pars = current_pars[1:]
                paragraphs.extend(current_pars)

            if humanize:
                _human_read_page(
                    page,
                    read_steps=random.randint(read_scroll_min_steps, read_scroll_max_steps),
                    pause_min_s=read_scroll_pause_min_s,
                    pause_max_s=read_scroll_pause_max_s,
                )
                _human_pause(dwell_min_s, dwell_max_s)

            current_fingerprint = _get_page_fingerprint(page)
            if previous_url == page.url and previous_fingerprint == current_fingerprint:
                logger.info("Содержимое страницы не изменилось — завершаю пагинацию.")
                break
            previous_url = page.url
            previous_fingerprint = current_fingerprint
            if max_pages and page_count >= max_pages:
                logger.info("Достигнут предел max_pages — завершаю.")
                break

            next_btn = find_next(page)
            if not next_btn:
                logger.info("Кнопка/ссылка 'Следующая' не найдена — завершаю.")
                break
            try:
                if humanize:
                    try:
                        next_btn.scroll_into_view_if_needed(timeout=2000)
                    except Exception:
                        pass
                    try:
                        next_btn.hover(timeout=2000)
                    except Exception:
                        pass
                    _human_pause(0.3, 0.8)
                with page.expect_navigation(wait_until="domcontentloaded", timeout=navigation_timeout_ms):
                    next_btn.click()
                logger.info("Навигация выполнена — открыта следующая страница.")
            except PlaywrightTimeoutError:
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except PlaywrightTimeoutError:
                    pass
                time.sleep(1.0)
                # If DOM unchanged, stop
                after = _get_page_fingerprint(page)
                if after == current_fingerprint:
                    logger.info("Страница не изменилась после клика — завершаю.")
                    break

        browser.close()
    return paragraphs


def iterate_page_paragraphs(
    start_url: str,
    max_pages: Optional[int] = None,
    next_selector: Optional[str] = None,
    next_text: str = "Следующая",
    headless: bool = True,
    slow_mo_ms: int = 0,
    user_agent: Optional[str] = None,
    navigation_timeout_ms: int = 15000,
    content_selector: str = ".reader_article_body",
    # Human-like behavior tuning
    humanize: bool = True,
    dwell_min_s: float = 0.8,
    dwell_max_s: float = 2.0,
    read_scroll_min_steps: int = 2,
    read_scroll_max_steps: int = 4,
    read_scroll_pause_min_s: float = 0.2,
    read_scroll_pause_max_s: float = 0.6,
):
    """Yield paragraphs for each page as they are parsed."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, slow_mo=slow_mo_ms or None)

        viewport = {
            "width": random.randint(1280, 1920),
            "height": random.randint(720, 1080),
        }
        default_uas = [
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
            ),
            (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
        ]
        chosen_ua = user_agent or random.choice(default_uas)

        context = browser.new_context(
            user_agent=chosen_ua,
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            viewport=viewport,
            device_scale_factor=random.choice([1, 1.25, 1.5, 2]),
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",
            },
        )
        page = context.new_page()
        page.set_default_navigation_timeout(navigation_timeout_ms)
        page.set_default_timeout(navigation_timeout_ms)
        logger.info(f"Открываю стартовую страницу: {start_url}")
        page.goto(start_url, wait_until="domcontentloaded")

        if humanize:
            _human_read_page(
                page,
                read_steps=random.randint(read_scroll_min_steps, read_scroll_max_steps),
                pause_min_s=read_scroll_pause_min_s,
                pause_max_s=read_scroll_pause_max_s,
            )
            _human_pause(dwell_min_s, dwell_max_s)

        previous_url: Optional[str] = None
        previous_fingerprint: Optional[str] = None
        page_count = 0

        def find_next(p: Page):
            if next_selector:
                return p.locator(next_selector).first
            locs = [
                p.locator("a[rel=next]").first,
                p.get_by_role("button", name=next_text).first,
                p.get_by_role("link", name=next_text).first,
                p.locator(f"text=^{next_text}$").first,
                p.locator(f"text={next_text}").first,
                p.get_by_role("button", name="Next").first,
                p.get_by_role("link", name="Next").first,
            ]
            for l in locs:
                try:
                    if l and l.is_visible():
                        return l
                except Exception:
                    continue
            return None

        while True:
            page_count += 1
            logger.info(f"Текущая страница #{page_count}: {page.url}")
            current_pars = _extract_paragraphs(page, content_selector)
            if current_pars:
                if previous_url is not None and current_pars and not current_pars[0].strip():
                    current_pars = current_pars[1:]
                if current_pars:
                    yield current_pars

            if humanize:
                _human_read_page(
                    page,
                    read_steps=random.randint(read_scroll_min_steps, read_scroll_max_steps),
                    pause_min_s=read_scroll_pause_min_s,
                    pause_max_s=read_scroll_pause_max_s,
                )
                _human_pause(dwell_min_s, dwell_max_s)

            current_fingerprint = _get_page_fingerprint(page)
            if previous_url == page.url and previous_fingerprint == current_fingerprint:
                logger.info("Содержимое страницы не изменилось — завершаю пагинацию.")
                break
            previous_url = page.url
            previous_fingerprint = current_fingerprint
            if max_pages and page_count >= max_pages:
                logger.info("Достигнут предел max_pages — завершаю.")
                break

            next_btn = find_next(page)
            if not next_btn:
                logger.info("Кнопка/ссылка 'Следующая' не найдена — завершаю.")
                break
            try:
                if humanize:
                    try:
                        next_btn.scroll_into_view_if_needed(timeout=2000)
                    except Exception:
                        pass
                    try:
                        next_btn.hover(timeout=2000)
                    except Exception:
                        pass
                    _human_pause(0.3, 0.8)
                with page.expect_navigation(wait_until="domcontentloaded", timeout=navigation_timeout_ms):
                    next_btn.click()
                logger.info("Навигация выполнена — открыта следующая страница.")
            except PlaywrightTimeoutError:
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except PlaywrightTimeoutError:
                    pass
                time.sleep(1.0)
                after = _get_page_fingerprint(page)
                if after == current_fingerprint:
                    logger.info("Страница не изменилась после клика — завершаю.")
                    break

        browser.close()


def _human_pause(min_seconds: float, max_seconds: float) -> None:
    try:
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    except Exception:
        pass


def _human_read_page(
    page: Page,
    read_steps: int = 3,
    pause_min_s: float = 0.2,
    pause_max_s: float = 0.6,
) -> None:
    """Simulate a human reading: small scroll steps with pauses."""
    read_steps = max(1, read_steps)
    for _ in range(read_steps):
        try:
            # Random small scroll
            delta_y = random.randint(200, 600)
            page.mouse.wheel(0, delta_y)
        except Exception:
            try:
                page.evaluate("window.scrollBy(0, Math.floor(200 + Math.random()*400))")
            except Exception:
                pass
        time.sleep(random.uniform(pause_min_s, pause_max_s))

def _get_page_fingerprint(page: Page) -> str:
    """Return a stable fingerprint of the current page content.

    Used to detect whether a click resulted in any meaningful change when
    there is no traditional navigation (common in SPAs).
    """
    # Using page.content() ensures we hash the current DOM, not just the URL
    content_html = page.content()
    return hashlib.sha256(content_html.encode("utf-8")).hexdigest()


def _find_next_button(
    page: Page,
    next_text: str = "Следующая",
    custom_selector: Optional[str] = None,
):
    """Try multiple strategies to find a visible "Next"/"Следующая" control.

    The search order is:
    1) Custom CSS selector (if provided)
    2) <a rel="next">
    3) Accessible role-based matches for button/link by given text
    4) Text-based locators for exact and partial matches
    5) English fallbacks ("Next")
    6) ARIA/data-testid heuristics
    """
    candidates = []

    # 1) Explicit selector takes precedence
    if custom_selector:
        candidates.append(page.locator(custom_selector))

    # 2) rel=next semantic link
    candidates.append(page.locator("a[rel=next]"))

    # 3) Role-based queries are resilient and prefer visible elements
    candidates.append(page.get_by_role("button", name=next_text))
    candidates.append(page.get_by_role("link", name=next_text))

    # 4) Text locators (exact, then partial)
    candidates.append(page.locator(f"text=^{next_text}$"))
    candidates.append(page.locator(f"text={next_text}"))

    # 5) English fallbacks
    candidates.append(page.get_by_role("button", name="Next"))
    candidates.append(page.get_by_role("link", name="Next"))

    # 6) ARIA and test id heuristics
    candidates.append(page.locator("[aria-label*=Next i], [aria-label*=Следующая i]"))
    candidates.append(page.locator("[data-testid*=next i]"))

    for locator in candidates:
        try:
            element = locator.first
            if element and element.is_visible():
                return element
        except Exception:
            # Some locators may throw in specific contexts; ignore and continue
            continue
    return None


def _extract_paragraphs(page: Page, content_selector: str) -> list[str]:
    """Extract paragraphs from the content area.

    Prefers block-level paragraphs inside the content container (e.g. <p> tags).
    Falls back to splitting container text by blank lines if no <p> tags found.
    """
    # Try explicit paragraphs inside the content container
    paragraph_locator = page.locator(f"{content_selector} p")
    paragraphs: list[str] = []

    try:
        p_count = paragraph_locator.count()
    except Exception:
        p_count = 0

    if p_count and p_count > 0:
        for index in range(p_count):
            try:
                text = paragraph_locator.nth(index).inner_text()
            except Exception:
                continue
            cleaned = text.strip()
            if cleaned:
                paragraphs.append(cleaned)
        return paragraphs

    # Fallback: take the container's text and split by blank lines
    container = page.locator(content_selector)
    try:
        container_text = container.inner_text().strip()
    except Exception:
        container_text = ""

    if not container_text:
        return []

    # Normalize newlines and split by blank lines
    normalized = container_text.replace("\r\n", "\n").replace("\r", "\n")
    chunks = [chunk.strip() for chunk in normalized.split("\n\n")]
    return [c for c in chunks if c]


def _should_merge_cross_page(prev_par: str, next_par: str) -> bool:
    """Heuristic to decide whether the first paragraph of the new page
    should be merged with the last paragraph of the previous page.

    Rules:
    - If previous ends with a hard sentence terminator, do not merge.
    - Otherwise, merge (handles mid-paragraph splits and hyphenation).
    """
    if not prev_par or not next_par:
        return False

    sentence_terminators = (".", "!", "?", "…", ":", ";", "\u00BB", ")", "\"")
    trimmed = prev_par.rstrip()
    return not trimmed.endswith(sentence_terminators)


def _trim_cross_page_overlap(previous_paragraph: str, next_paragraph: str) -> str:
    """Trim duplicated prefix in next_paragraph if it repeats the suffix of previous_paragraph.

    Uses two strategies:
    1) Exact suffix/prefix match (fast path)
    2) Fuzzy match via difflib for near-duplicates (e.g., minor differences)
    """
    if not previous_paragraph or not next_paragraph:
        return next_paragraph

    # Normalize working windows
    prev_tail = previous_paragraph[-400:]
    next_head = next_paragraph[:400]

    # 1) Exact match: try longest suffix of prev_tail that is a prefix of next_paragraph
    max_suffix = min(len(prev_tail), 200)
    for length in range(max_suffix, 29, -10):  # 200, 190, ..., 30
        suffix = prev_tail[-length:]
        if next_paragraph.startswith(suffix):
            return next_paragraph[length:]

    # 2) Fuzzy match at the very start of next paragraph
    matcher = difflib.SequenceMatcher(None, prev_tail, next_head)
    # Use get_matching_blocks for stable tuple shape (a, b, size)
    best_size = 0
    best_b = None
    for block in matcher.get_matching_blocks():
        a = block.a
        b = block.b
        size = block.size
        # Prefer matches that start at the very beginning of next paragraph
        if b == 0 and size >= 20:
            # And that are close to the end of previous
            if a >= len(prev_tail) - 250:
                if size > best_size:
                    best_size = size
                    best_b = b
    if best_size >= 20 and best_b == 0:
        return next_paragraph[best_size:]

    return next_paragraph


def paginate_until_end(
    start_url: str,
    max_pages: Optional[int] = None,
    next_selector: Optional[str] = None,
    next_text: str = "Следующая",
    headless: bool = False,
    slow_mo_ms: int = 0,
    user_agent: Optional[str] = None,
    navigation_timeout_ms: int = 15000,
    content_selector: str = ".reader_article_body",
    merge_cross_page: bool = True,
):
    """Open the start URL and click "Next" until no more new pages load.

    Stops when:
    - No next button/control is found
    - A click does not change URL and does not change DOM content
    - max_pages (if provided) is reached
    """
    extracted_paragraphs: list[str] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=headless,
            slow_mo=slow_mo_ms if slow_mo_ms > 0 else None,
        )

        # Reasonable defaults to resemble a real user
        context = browser.new_context(
            user_agent=(
                user_agent
                or (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            ),
            viewport={"width": 1366, "height": 768},
            locale="ru-RU",
        )

        page = context.new_page()
        page.set_default_navigation_timeout(navigation_timeout_ms)
        page.set_default_timeout(navigation_timeout_ms)

        logger.info(f"Открываю стартовую страницу: {start_url}")
        page.goto(start_url, wait_until="domcontentloaded")

        page_count = 0
        previous_url: Optional[str] = None
        previous_fingerprint: Optional[str] = None

        while True:
            page_count += 1
            logger.info(f"Текущая страница #{page_count}: {page.url}")

            # Extract content from the current page before attempting to click next
            if content_selector:
                page_paragraphs = _extract_paragraphs(page, content_selector)
                if page_paragraphs:
                    # Trim duplicated prefix on the first paragraph of the new page
                    if extracted_paragraphs:
                        page_paragraphs[0] = _trim_cross_page_overlap(
                            extracted_paragraphs[-1], page_paragraphs[0]
                        )
                        # If trimming leaves the first paragraph empty, drop it
                        if not page_paragraphs[0].strip():
                            page_paragraphs = page_paragraphs[1:]

                    if merge_cross_page and extracted_paragraphs and page_paragraphs:
                        # Merge continuation across pages when previous paragraph
                        # did not end with a sentence terminator
                        if _should_merge_cross_page(
                            extracted_paragraphs[-1], page_paragraphs[0]
                        ):
                            # Handle hyphenated line breaks at page boundary
                            if extracted_paragraphs[-1].endswith("-"):
                                extracted_paragraphs[-1] = (
                                    extracted_paragraphs[-1][:-1] + page_paragraphs[0].lstrip()
                                )
                            else:
                                extracted_paragraphs[-1] = (
                                    extracted_paragraphs[-1].rstrip() + " " + page_paragraphs[0].lstrip()
                                )
                            extracted_paragraphs.extend(page_paragraphs[1:])
                        else:
                            extracted_paragraphs.extend(page_paragraphs)
                    else:
                        extracted_paragraphs.extend(page_paragraphs)

            # Loop detection using URL and DOM fingerprint
            current_fingerprint = _get_page_fingerprint(page)
            if previous_url == page.url and previous_fingerprint == current_fingerprint:
                logger.warning("Содержимое страницы не изменилось — завершаю пагинацию.")
                break

            previous_url = page.url
            previous_fingerprint = current_fingerprint

            if max_pages and page_count >= max_pages:
                logger.info("Достигнут предел max_pages — завершаю.")
                break

            next_button = _find_next_button(
                page, next_text=next_text, custom_selector=next_selector
            )
            if not next_button:
                logger.info("Кнопка/ссылка 'Следующая' не найдена — завершаю.")
                break

            try:
                with page.expect_navigation(
                    wait_until="domcontentloaded", timeout=navigation_timeout_ms
                ) as navigation_info:
                    next_button.click()
                _ = navigation_info.value
                logger.info("Навигация выполнена — открыта следующая страница.")
            except PlaywrightTimeoutError:
                # Возможно SPA: изменения без навигации
                logger.info(
                    "Навигация не обнаружена. Проверяю изменения DOM после клика..."
                )
                before = current_fingerprint
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except PlaywrightTimeoutError:
                    pass
                time.sleep(1.0)
                after = _get_page_fingerprint(page)
                if after == before:
                    logger.info("Страница не изменилась после клика — завершаю.")
                    break
                logger.info("DOM обновился без навигации — продолжаю.")

        browser.close()

    return extracted_paragraphs





