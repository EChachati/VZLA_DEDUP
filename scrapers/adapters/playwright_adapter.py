"""
scrapers/adapters/playwright_adapter.py
========================================
Adapter para fuentes de tipo web dinámica (React, Vue, etc.) usando Playwright.

Implementa el AdapterProtocol para manejar:
- Navegación a URLs dinámicas
- Espera de elementos específicos
- Extracción de contenido HTML
- Paginación automática (botones, links)
- Reintentos con exponential backoff

El adapter mantiene una instancia de navegador reutilizable para mejorar
rendimiento durante múltiples operaciones fetch en la misma sesión.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Iterator
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, Browser, Page, BrowserContext, BrowserType

from scrapers.adapters.base import AdapterProtocol, RawContent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes por defecto
# ---------------------------------------------------------------------------

_DEFAULT_TIMEOUT = 30.0          # segundos
_MAX_RETRIES = 5
_BACKOFF_BASE = 1.0              # segundos base para backoff
_BACKOFF_MAX = 60.0              # techo del backoff
_RETRYABLE_ERRORS = (
    # Playwright-specific errors indicating potential flakiness or network issues
    "TimeoutError",
    "TargetClosedError",
    "BrowserError",
    "Error: net::ERR_CONNECTION_CLOSED",
    "Error: net::ERR_CONNECTION_RESET",
    "Error: net::ERR_CONNECTION_TIMED_OUT",
    "Error: net::ERR_NETWORK_CHANGED",
    "Error: net::ERR_INTERNET_DISCONNECTED",
)

# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _now_utc() -> str:
    """ISO-8601 UTC sin microsegundos."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(obj: Any) -> str:
    """Hash SHA-256 del contenido serializado como JSON compacto."""
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _backoff_delay(attempt: int) -> float:
    """
    Exponential backoff con jitter completo.

    ``attempt`` empieza en 1.  Fórmula:
        delay = min(base * 2^(attempt-1), max) + random(0, 1)
    """
    exp = _BACKOFF_BASE * (2 ** (attempt - 1))
    capped = min(exp, _BACKOFF_MAX)
    return capped + random.random()


# ---------------------------------------------------------------------------
# Adapter principal
# ---------------------------------------------------------------------------


class PlaywrightAdapter(AdapterProtocol):
    """
    Adapter que usa Playwright para obtener contenido de páginas web dinámicas.

    Implementa el `AdapterProtocol` para manejar la navegación, espera de
    elementos y extracción de HTML desde el navegador.

    Parameters
    ----------
    source_key:
        Identificador único de la fuente.
    timeout:
        Timeout en segundos para operaciones de Playwright (default: 30s).
    max_retries:
        Número máximo de reintentos ante errores retryables (default: 5).
    headless:
        Si True, lanza el navegador sin UI (default: True).
    browser_type:
        Tipo de navegador: 'chromium', 'firefox', 'webkit' (default: 'chromium').
    **launch_options:
        Argumentos adicionales para la función launch() de Playwright.
    """

    def __init__(
        self,
        source_key: str,
        timeout: float = _DEFAULT_TIMEOUT,
        max_retries: int = _MAX_RETRIES,
        headless: bool = True,
        browser_type: str = "chromium",
        **launch_options: Any,
    ) -> None:
        self.source_key = source_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.headless = headless
        self.browser_type = browser_type
        self.launch_options = launch_options

        self._playwright = None
        self._browser = None
        self._browser_context = None

    def _get_browser_and_page(self) -> tuple[Browser, Page]:
        """
        Inicializa Playwright y lanza un navegador si no están activos.

        Returns
        -------
        tuple[Browser, Page]
            Instancia del navegador y una nueva página.
        """
        if not self._playwright:
            self._playwright = sync_playwright().start()

        if not self._browser:
            browser_launcher: BrowserType
            if self.browser_type == "firefox":
                browser_launcher = self._playwright.firefox
            elif self.browser_type == "webkit":
                browser_launcher = self._playwright.webkit
            else:
                browser_launcher = self._playwright.chromium

            self._browser = browser_launcher.launch(headless=self.headless, **self.launch_options)
            self._browser_context = self._browser.new_context()

        page = self._browser_context.new_page()
        return self._browser, page

    def _operate_page_with_retry(
        self,
        operation_name: str,
        url: str,
        page: Page,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Realiza una operación en la página con reintentos y backoff.

        Parameters
        ----------
        operation_name:
            Nombre de la operación: 'goto', 'wait_for_selector', 'click', etc.
        url:
            URL para logging purposes.
        page:
            Instancia de Page de Playwright.
        *args:
            Argumentos posicionales para la operación.
        **kwargs:
            Argumentos con nombre para la operación.

        Raises
        ------
        RuntimeError
            Si se agotan los reintentos.
        """
        last_exc: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                # Ejecutar la operación correspondiente
                if operation_name == "goto":
                    page.goto(url, timeout=int(self.timeout * 1000), **kwargs)
                elif operation_name == "wait_for_selector":
                    page.wait_for_selector(*args, timeout=int(self.timeout * 1000), **kwargs)
                elif operation_name == "click":
                    page.locator(args[0]).click(timeout=int(self.timeout * 1000), **kwargs)
                else:
                    raise ValueError(f"Unknown operation: {operation_name}")

                return  # Indicate success

            except Exception as exc:
                last_exc = exc
                exc_message = str(exc)

                # Reintentar solo si es un error retryable o un TimeoutError
                if any(retry_err in exc_message for retry_err in _RETRYABLE_ERRORS) or isinstance(
                    exc, TimeoutError
                ):
                    if attempt < self.max_retries:
                        delay = _backoff_delay(attempt)
                        logger.warning(
                            "Error en intento %d/%d (%s) — reintento en %.1fs para %s",
                            attempt,
                            self.max_retries,
                            type(exc).__name__,
                            delay,
                            url,
                        )
                        time.sleep(delay)
                    else:
                        logger.warning(
                            "Error en intento %d/%d (%s) — sin más reintentos para %s",
                            attempt,
                            self.max_retries,
                            type(exc).__name__,
                            url,
                        )
                    continue
                else:
                    # No es un error retryable, relanzar inmediatamente
                    raise

        raise RuntimeError(
            f"Máximo de reintentos ({self.max_retries}) alcanzado para la operación '{operation_name}' en {url}"
        ) from last_exc

    def _create_raw_content(
        self,
        url: str,
        content: str | dict | list,
        page_number: int | None = None,
        total_pages: int | None = None,
        http_status: int = 200,
        content_type: str = "text/html",
    ) -> RawContent:
        """
        Helper para construir el dict RawContent.

        Parameters
        ----------
        url:
            URL de la que se extrajo el contenido.
        content:
            Contenido extraído (HTML, JSON, etc.).
        page_number:
            Número de página si es paginado.
        total_pages:
            Total de páginas estimadas.
        http_status:
            Código HTTP (default: 200 para Playwright).
        content_type:
            Tipo de contenido (default: "text/html").

        Returns
        -------
        RawContent
            Dict con todos los campos requeridos por AdapterProtocol.
        """
        content_hash = _sha256(content)

        return {
            "source_key": self.source_key,
            "source_url": url,
            "fetched_at": _now_utc(),
            "http_status": http_status,
            "content_type": content_type,
            "content_hash": content_hash,
            "raw_content": content,
            "page": page_number,
            "total_pages": total_pages,
        }

    def fetch(self, url: str, **kwargs: Any) -> RawContent:
        """
        Obtiene una sola página/recurso usando Playwright.

        Parameters
        ----------
        url:
            URL de la fuente a consultar.
        **kwargs:
            Parámetros adicionales que Playwright puede aceptar, como:
            - wait_until: 'load', 'domcontentloaded', 'networkidle' (default: 'domcontentloaded')
            - wait_for_selector: Selector CSS que debe estar presente antes de extraer contenido
            - timeout: Timeout en milisegundos

        Returns
        -------
        RawContent
            Dict con el contenido crudo de la página.
        """
        logger.info(f"Fetching single page for {self.source_key} from {url}")
        browser, page = self._get_browser_and_page()

        try:
            # Usar _operate_page_with_retry para navegar a la URL
            self._operate_page_with_retry(
                "goto", url, page, wait_until=kwargs.get("wait_until", "domcontentloaded")
            )

            # Esperar por un selector específico si se proporciona
            wait_for_selector = kwargs.get("wait_for_selector")
            if wait_for_selector:
                try:
                    self._operate_page_with_retry("wait_for_selector", url, page, wait_for_selector)
                except Exception as e:
                    logger.warning(f"Selector no encontrado: {e}")
                    # Continuar de todas formas

            # Obtener el contenido HTML de la página
            html_content = page.content()
            final_url = page.url

            return self._create_raw_content(
                url=final_url,
                content=html_content,
                http_status=200,
                content_type="text/html",
            )

        except Exception as e:
            logger.error(f"Error fetching {url} with Playwright: {e}")
            raise
        finally:
            page.close()

    def fetch_all(self, url: str, **kwargs: Any) -> Iterator[RawContent]:
        """
        Obtiene todas las páginas de una fuente paginada usando Playwright.

        Soporta dos estrategias de paginación:
        1. Button-based: usa un selector de botón "Siguiente" (next_button_selector)
        2. Link-based: usa un selector para extraer el siguiente URL (next_link_selector)

        Parameters
        ----------
        url:
            URL inicial de la fuente.
        **kwargs:
            - wait_for_selector: Selector CSS que debe estar presente antes de extraer contenido
            - next_button_selector: Selector CSS del botón "Siguiente"
            - next_link_selector: Selector CSS del link "Siguiente"
            - max_pages: Número máximo de páginas a scrapear (default: None = sin límite)
            - wait_for_load_state: Estado de carga esperado ('load', 'domcontentloaded', 'networkidle')
            - wait_until: Similar a wait_for_load_state para page.goto

        Yields
        ------
        RawContent
            Dict con el contenido crudo de cada página.
        """
        logger.info(f"Fetching all pages for {self.source_key} starting from {url}")
        browser, page = self._get_browser_and_page()

        current_page_num = 1
        max_pages = kwargs.get("max_pages")
        wait_for_load_state = kwargs.get("wait_for_load_state", kwargs.get("wait_until", "domcontentloaded"))

        try:
            # Navegar a la URL inicial
            self._operate_page_with_retry("goto", url, page, wait_until=wait_for_load_state)

            while True:
                # Limitar número de páginas si max_pages está definido
                if max_pages and current_page_num > max_pages:
                    logger.info(f"Límite de páginas ({max_pages}) alcanzado")
                    break

                logger.debug(f"Fetching page {current_page_num} of {url}")

                # Esperar a un selector específico si se proporciona
                wait_for_selector = kwargs.get("wait_for_selector")
                if wait_for_selector:
                    try:
                        self._operate_page_with_retry("wait_for_selector", url, page, wait_for_selector)
                    except Exception as e:
                        logger.warning(f"Selector no encontrado en página {current_page_num}: {e}")
                        # Continuar de todas formas, el contenido podría estar disponible

                # Extraer contenido de la página actual
                html_content = page.content()
                final_url = page.url

                yield self._create_raw_content(
                    url=final_url,
                    content=html_content,
                    page_number=current_page_num,
                    http_status=200,
                    content_type="text/html",
                )

                # --- Lógica de paginación ---
                has_next_page = False

                # Estrategia 1: Botón "Siguiente"
                next_button_selector = kwargs.get("next_button_selector")
                if next_button_selector:
                    try:
                        next_button = page.locator(next_button_selector)
                        if next_button.is_visible() and not next_button.is_disabled():
                            next_button.click()
                            page.wait_for_load_state(wait_for_load_state)
                            current_page_num += 1
                            has_next_page = True
                        else:
                            logger.info(f"Botón 'Siguiente' no es visible/habilitado en página {current_page_num}")
                    except Exception as e:
                        logger.warning(f"Error al hacer click en botón 'Siguiente': {e}")

                # Estrategia 2: Link de paginación (si el botón no funcionó)
                if not has_next_page:
                    next_link_selector = kwargs.get("next_link_selector")
                    if next_link_selector:
                        try:
                            next_link = page.locator(next_link_selector).first
                            if next_link.is_visible():
                                next_url = next_link.get_attribute("href")
                                if next_url:
                                    # Resolver URL relativa si es necesario
                                    if next_url.startswith("/") or next_url.startswith("?"):
                                        next_url = urljoin(final_url, next_url)

                                    self._operate_page_with_retry(
                                        "goto", next_url, page, wait_until=wait_for_load_state
                                    )
                                    current_page_num += 1
                                    has_next_page = True
                            else:
                                logger.info(f"Link 'Siguiente' no es visible en página {current_page_num}")
                        except Exception as e:
                            logger.warning(f"Error al navegar usando link 'Siguiente': {e}")

                # Si no hay forma de ir a la siguiente página, terminar
                if not has_next_page:
                    logger.info(f"Paginación completa: última página es {current_page_num}")
                    break

        except Exception as e:
            logger.error(f"Error fetching paginated content for {url} with Playwright: {e}")
            raise
        finally:
            page.close()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "PlaywrightAdapter":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        """Cierra el navegador y la instancia de Playwright."""
        if self._browser_context:
            self._browser_context.close()
            self._browser_context = None

        if self._browser:
            self._browser.close()
            self._browser = None

        if self._playwright:
            self._playwright.stop()
            self._playwright = None

        logger.info(f"PlaywrightAdapter para {self.source_key} cerrado")
