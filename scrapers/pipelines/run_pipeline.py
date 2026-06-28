"""
scrapers/pipelines/run_pipeline.py
====================================
Orquestador principal del pipeline VZLA_DEDUP.

Flujo por fuente habilitada
----------------------------
1. **Adapter**   — fetch del contenido raw según ``source.type``
2. **Parser**    — convierte raw → list[Person | AcopioCenter | Event]
3. **PII**       — ``tokenize_pii_fields`` sobre cada entidad (model_dump)
4. **Dedup**     — ``deduplicate_typed_entities`` para Event/AcopioCenter;
                   Person se pasa sin dedup global (sensible, requiere revisión)
5. **Score**     — ``confidence_score`` sobre cada entidad
6. **Export**    — ``write_jsonl`` → persons.jsonl / acopio.jsonl / events.jsonl

Principios de resiliencia
--------------------------
- Un error en un registro individual no tumba el pipeline.
- Un error en una fuente entera se loguea y se continúa con la siguiente.
- Todos los errores se acumulan en el summary para visibilidad.

Summary devuelto
-----------------
El dict de retorno tiene las keys que espera ``cli.py``:
  sources_processed   int  — fuentes completadas sin error fatal
  documents_exported  int  — total de entidades escritas a JSONL
  claims_exported     int  — alias de documents_exported (compat. legacy)
  claims_deduplicated int  — duplicados eliminados (solo Event/AcopioCenter)
  errors              list[str]  — mensajes de error no fatales
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from scrapers.adapters._shared import now_utc, sha256_hex
from scrapers.adapters.base import RawContent
from scrapers.dedup.deduplicator import deduplicate_typed_entities
from scrapers.models import AcopioCenter, Event, Person
from scrapers.models.source import SourceConfig
from scrapers.normalizers import normalize_date, normalize_location
from scrapers.outputs.jsonl_writer import write_jsonl
from scrapers.sanitizers.pii_tokenizer import tokenize_pii_fields
from scrapers.sources.loader import load_sources
from scrapers.validators.quality import confidence_score

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tipos internos
# ---------------------------------------------------------------------------

ParsedEntity = Person | AcopioCenter | Event
_PII_SECRET_ENV = "PII_SALT"


# ---------------------------------------------------------------------------
# Registry: adapters y parsers
# ---------------------------------------------------------------------------

def _get_adapter(source: SourceConfig) -> Any:
    """
    Devuelve la instancia del adapter adecuado para el type de la fuente.

    Tipos soportados:
      api_json    → ApiAdapter (httpx, paginación automática)
      html_static → HtmlAdapter (requests + BeautifulSoup, devuelve texto limpio)
      manual_file / text → local_file (lectura local)
      pdf         → PdfAdapter (pdfplumber, texto por página)
      rss         → RssAdapter (extrae items del feed RSS/Atom)
      webapp_js   → PlaywrightAdapter (browser headless, paginas con JS)

    Tipos sin adapter registrado devuelven None y la fuente se omite.
    """
    stype = source.type

    if stype == "api_json":
        from scrapers.adapters.api_adapter import ApiAdapter
        # base_url = esquema + host; el path se pasa en fetch_all
        import httpx
        parsed = httpx.URL(source.url)
        base_url = f"{parsed.scheme}://{parsed.host}"
        if parsed.port:
            base_url += f":{parsed.port}"
        path = parsed.path or "/"
        adapter = ApiAdapter(
            base_url=base_url,
            source_key=source.id,
            default_path=path,
        )
        return adapter

    if stype == "html_static":
        from scrapers.adapters.html_adapter import HtmlAdapter
        return HtmlAdapter.from_source_config(source)

    if stype == "rss":
        from scrapers.adapters.rss_adapter import RssAdapter
        return RssAdapter.from_source_config(source)

    if stype in ("manual_file", "text"):
        from scrapers.adapters.local_file import read_local_file
        return _LocalFileAdapter(source_key=source.id, read_fn=read_local_file)

    if stype == "pdf":
        from scrapers.adapters.pdf_adapter import PdfAdapter
        return PdfAdapter.from_source_config(source)

    if stype == "webapp_js":
        from scrapers.adapters.playwright_adapter import PlaywrightAdapter
        return PlaywrightAdapter.from_source_config(source)

    log.warning(
        "Adapter para type=%r no implementado (fuente=%s) — omitida",
        stype, source.id,
    )
    return None


def _get_parser(source: SourceConfig) -> Any:
    """
    Devuelve la instancia del parser asignado según ``parser_asignado``.

    Parsers concretos (producen entidades tipadas):
      encuentralos  → EncuentralosParser → list[Person]

    Parsers genéricos (producen entidades tipadas con texto crudo como nota):
      text / html / rss / json_generic / geojson_earthquake / reliefweb_reports
      → _TextFallbackParser → list[Person] (registro único por página con nota)

    Si ``parser_asignado`` no tiene implementación registrada, se usa el
    fallback genérico para no perder datos.
    """
    pa = (source.parser_asignado or "").lower().strip()

    if pa == "encuentralos":
        from scrapers.parsers.encuentralos_parser import EncuentralosParser
        secret = os.getenv("PII_HMAC_SECRET")
        return EncuentralosParser(secret=secret)

    # Fallback genérico para parsers aún no implementados
    return _TextFallbackParser(source=source)


# ---------------------------------------------------------------------------
# Adapters ligeros para fuentes no-API
# ---------------------------------------------------------------------------

class _LocalFileAdapter:
    """Wrapper fino sobre read_local_file para fuentes manual_file."""

    def __init__(self, source_key: str, read_fn: Any) -> None:
        self.source_key = source_key
        self._read = read_fn

    def fetch(self, url: str, **_: Any) -> RawContent:
        text = self._read(url)
        return RawContent(
            source_key=self.source_key,
            source_url=url,
            fetched_at=now_utc(),
            http_status=200,
            content_type="text/plain",
            content_hash=sha256_hex(text.encode("utf-8")),
            raw_content=text,
            page=None,
            total_pages=None,
            offset=None,
            limit=None,
            records_in_page=None,
        )

    def fetch_all(self, url: str, **kwargs: Any):  # type: ignore[return]
        yield self.fetch(url)


# ---------------------------------------------------------------------------
# Parser fallback genérico
# ---------------------------------------------------------------------------

class _TextFallbackParser:
    """
    Parser de último recurso para fuentes sin parser concreto.

    Produce un único Person por página con el contenido raw en el campo
    ``nota``.  Útil para fuentes en desarrollo o de tipo manual_file.
    El registro se crea como status=unknown, confianza mínima.
    """

    def __init__(self, source: SourceConfig) -> None:
        self.source_key = source.id
        self._trust_tier = source.trust_tier
        self._fuente = source.name

    def parse(self, raw: RawContent, **_: Any) -> list[Person]:
        content = raw.get("raw_content", "")
        if isinstance(content, (dict, list)):
            import json as _json
            # Fix: raise truncation limit from 500 to 10K. At 500 chars entire
            # pages of useful data were silently dropped by the fallback parser.
            content = _json.dumps(content, ensure_ascii=False)[:10_000]
        elif isinstance(content, str):
            content = content[:10_000]
        else:
            content = str(content)[:10_000]

        if not content.strip():
            return []

        # Un único Person-placeholder por página con el contenido como nota
        try:
            return [
                Person(
                    full_name=f"[registro sin parser] {self._fuente}",
                    nota=content.strip() or None,
                    trust_tier=self._trust_tier,
                    confidence_score=0.0,
                    fuente=self._fuente,
                )
            ]
        except Exception as exc:
            log.warning("Fallback parser error para %s: %s", self.source_key, exc)
            return []


# ---------------------------------------------------------------------------
# Etapas del pipeline
# ---------------------------------------------------------------------------

def _fetch_pages(adapter: Any, source: SourceConfig) -> list[RawContent]:
    """Llama a fetch_all del adapter y recopila todas las páginas."""
    url = source.url
    pages: list[RawContent] = []

    # ApiAdapter expone default_path separado de base_url
    if hasattr(adapter, "default_path") and adapter.default_path:
        path = adapter.default_path
        for page in adapter.fetch_all(path):
            pages.append(page)
    else:
        for page in adapter.fetch_all(url):
            pages.append(page)

    return pages


def _parse_pages(
    parser: Any,
    pages: list[RawContent],
    limit: int | None,
) -> tuple[list[ParsedEntity], list[str]]:
    """Parsea todas las páginas y devuelve (entidades, errores_por_registro)."""
    entities: list[ParsedEntity] = []
    errors: list[str] = []

    for raw in pages:
        try:
            batch = parser.parse(raw)
        except Exception as exc:
            msg = f"Error parseando página {raw.get('page')}: {exc}"
            log.warning(msg)
            errors.append(msg)
            continue

        entities.extend(batch)
        if limit is not None and len(entities) >= limit:
            entities = entities[:limit]
            break

    return entities, errors


# Campos PII crudos que nunca deben llegar a export aunque tokenize falle
_PII_FIELD_NAMES = {"cedula", "cédula", "identity_document", "documento_identidad",
                    "telefono", "teléfono", "phone", "mobile", "celular"}


def _strip_raw_pii(d: dict) -> dict:
    """Elimina campos PII crudos sin hashear (defensa en profundidad)."""
    return {k: v for k, v in d.items() if k.lower() not in _PII_FIELD_NAMES}


def _apply_pii(
    entities: list[ParsedEntity],
    errors: list[str],
) -> list[dict]:
    """
    Convierte entidades tipadas a dicts y aplica tokenize_pii_fields.

    Los campos cedula_hmac/cedula_masked ya vienen del parser concreto
    (p. ej. EncuentralosParser).  tokenize_pii_fields actúa como segunda
    capa de seguridad: detecta y hashea cualquier campo PII que haya
    escapado del parser.

    Si PII_SALT no está configurado (entorno CI/dev), el registro pasa
    igualmente — sin los campos PII crudos — en lugar de perderse.
    Esto permite que el pipeline funcione en tests offline.
    Devuelve lista de dicts listos para dedup + export.
    """
    import os
    pii_salt_available = bool(os.getenv("PII_SALT"))

    result: list[dict] = []
    for entity in entities:
        try:
            d = entity.model_dump()
            if pii_salt_available:
                d = tokenize_pii_fields(d)
            else:
                # Sin salt: eliminar campos PII crudos sin tokenizar
                d = _strip_raw_pii(d)
            # Preservar el tipo para el router de export
            d["_entity_type"] = type(entity).__name__
            result.append(d)
        except Exception as exc:
            msg = f"Error en etapa PII para {type(entity).__name__}: {exc}"
            log.warning(msg)
            errors.append(msg)
            # Intentar rescatar el registro sin PII antes que perderlo
            try:
                d = _strip_raw_pii(entity.model_dump())
                d["_entity_type"] = type(entity).__name__
                result.append(d)
            except Exception:
                pass
    return result


def _apply_normalization(records: list[dict], errors: list[str]) -> list[dict]:
    """
    Normalización post-dump: last_known_location y campos de fecha.

    Los parsers concretos (EncuentralosParser) ya normalizan al parsear,
    así que esta etapa es principalmente para parsers genéricos/fallback
    que entregan texto crudo.
    """
    normalized: list[dict] = []
    for rec in records:
        try:
            # Normalizar ubicación si viene como string crudo sin objeto
            loc = rec.get("last_known_location")
            if isinstance(loc, str) and loc:
                loc_obj = normalize_location(loc)
                # Reemplazar con string legible si la normalización aportó algo
                estado = loc_obj.get("estado")
                municipio = loc_obj.get("municipio")
                if municipio and estado:
                    rec["last_known_location"] = f"{municipio}, {estado}"
                elif estado:
                    rec["last_known_location"] = estado

            # Compute deterministic_id
            from scrapers.normalizers.phonetic import phonetic_hash as _compute_phonetic, build_deterministic_id as _build_det_id
            from scrapers.normalizers.person import normalize_person_name as _norm_name
            _name_norm = _norm_name(rec.get("full_name") or "")
            _ph = _compute_phonetic(_name_norm) if _name_norm else None
            rec["deterministic_id"] = _build_det_id(_ph, rec.get("last_known_location"))

            # Normalizar date_iso para Event
            date_raw = rec.get("date_iso")
            if isinstance(date_raw, str) and date_raw:
                normalized_date = normalize_date(date_raw)
                if normalized_date and isinstance(normalized_date, str):
                    rec["date_iso"] = normalized_date

            normalized.append(rec)
        except Exception as exc:
            msg = f"Error en normalización: {exc}"
            log.warning(msg)
            errors.append(msg)
            normalized.append(rec)  # añadir sin normalizar antes que perder
    return normalized


def _apply_dedup(
    records: list[dict],
    errors: list[str],
) -> tuple[list[dict], int]:
    """
    Deduplicación para Event y AcopioCenter.

    Person se excluye de dedup global por diseño: un falso merge puede
    costar una vida.  Los Person pasan sin filtrar.
    """
    events_raw = [r for r in records if r.get("_entity_type") == "Event"]
    acopio_raw = [r for r in records if r.get("_entity_type") == "AcopioCenter"]
    persons = [r for r in records if r.get("_entity_type") == "Person"]

    total_deduped = 0

    def _to_event(d: dict) -> Event | None:
        try:
            d2 = {k: v for k, v in d.items() if not k.startswith("_")}
            return Event(**d2)
        except Exception as exc:
            errors.append(f"Error reconstruyendo Event para dedup: {exc}")
            return None

    def _to_acopio(d: dict) -> AcopioCenter | None:
        try:
            d2 = {k: v for k, v in d.items() if not k.startswith("_")}
            return AcopioCenter(**d2)
        except Exception as exc:
            errors.append(f"Error reconstruyendo AcopioCenter para dedup: {exc}")
            return None

    # Fix: deduped_events is list[dict], not list[Event] — we convert back
    # to dicts immediately after dedup. Removes dead store `events_raw_deduped`
    # and 7 type: ignore comments that were papering over the wrong annotation.
    deduped_events: list[dict] = []
    if events_raw:
        typed_events = [e for d in events_raw if (e := _to_event(d)) is not None]
        if typed_events:
            deduped_typed, n_dup = deduplicate_typed_entities(typed_events)
            total_deduped += n_dup
            for entity in deduped_typed:
                d = entity.model_dump()
                d["_entity_type"] = "Event"
                deduped_events.append(d)

    deduped_acopio: list[dict] = []
    if acopio_raw:
        typed_acopio = [a for d in acopio_raw if (a := _to_acopio(d)) is not None]
        if typed_acopio:
            deduped_typed, n_dup = deduplicate_typed_entities(typed_acopio)
            total_deduped += n_dup
            for entity in deduped_typed:
                d = entity.model_dump()
                d["_entity_type"] = "AcopioCenter"
                deduped_acopio.append(d)

    return persons + deduped_events + deduped_acopio, total_deduped


def _apply_confidence(
    records: list[dict],
    errors: list[str],
) -> list[dict]:
    """Calcula y escribe confidence_score en cada registro."""
    _MODEL_MAP = {"Person": Person, "AcopioCenter": AcopioCenter, "Event": Event}
    result: list[dict] = []
    for rec in records:
        try:
            entity_type = rec.get("_entity_type", "Person")
            model_cls = _MODEL_MAP.get(entity_type, Person)
            d2 = {k: v for k, v in rec.items() if not k.startswith("_")}
            entity = model_cls(**d2)
            score = confidence_score(entity)
            rec["confidence_score"] = score
        except Exception as exc:
            log.warning("Error calculando confidence_score: %s", exc)
            errors.append(f"Error en confidence_score: {exc}")
        result.append(rec)
    return result


def _export(
    records: list[dict],
    output_dir: Path,
    errors: list[str],
) -> int:
    """
    Escribe los registros a persons.jsonl / acopio.jsonl / events.jsonl.

    Elimina el campo interno ``_entity_type`` antes de exportar.
    Devuelve el total de registros escritos.
    """
    persons = []
    acopio = []
    events = []

    for rec in records:
        entity_type = rec.pop("_entity_type", "Person")
        if entity_type == "AcopioCenter":
            acopio.append(rec)
        elif entity_type == "Event":
            events.append(rec)
        else:
            persons.append(rec)

    total = 0
    try:
        if persons:
            n = write_jsonl(output_dir / "persons.jsonl", persons)
            total += n
            log.info("Exportados %d Person → persons.jsonl", n)
        if acopio:
            n = write_jsonl(output_dir / "acopio.jsonl", acopio)
            total += n
            log.info("Exportados %d AcopioCenter → acopio.jsonl", n)
        if events:
            n = write_jsonl(output_dir / "events.jsonl", events)
            total += n
            log.info("Exportados %d Event → events.jsonl", n)
    except Exception as exc:
        msg = f"Error exportando JSONL: {exc}"
        log.error(msg)
        errors.append(msg)

    return total


# ---------------------------------------------------------------------------
# Orquestador por fuente
# ---------------------------------------------------------------------------

def _run_source(
    source: SourceConfig,
    output_dir: Path,
    limit: int | None,
    all_errors: list[str],
) -> tuple[int, int]:
    """
    Ejecuta el pipeline completo para una fuente.

    Devuelve (registros_exportados, duplicados_eliminados).
    Cualquier excepción no capturada sube al orquestador principal.
    """
    log.info("Iniciando fuente: %s (type=%s, parser=%s)", source.id, source.type, source.parser_asignado)
    source_errors: list[str] = []

    # 1. Adapter
    adapter = _get_adapter(source)
    if adapter is None:
        return 0, 0

    # 2. Parser
    parser = _get_parser(source)

    # 3. Fetch
    # El close() va en finally: si fetch_all() lanza (ej. PlaywrightAdapter
    # agotando retries), el adapter puede mantener recursos vivos (browser,
    # conexiones) y el error sube igual al orquestador principal.
    try:
        pages = _fetch_pages(adapter, source)
    finally:
        if hasattr(adapter, "close"):
            try:
                adapter.close()
            except Exception:
                pass

    log.info("%s: %d página(s) descargadas", source.id, len(pages))

    # 4. Parse
    entities, parse_errors = _parse_pages(parser, pages, limit)
    source_errors.extend(parse_errors)
    log.info("%s: %d entidades parseadas", source.id, len(entities))

    if not entities:
        all_errors.extend([f"[{source.id}] {e}" for e in source_errors])
        return 0, 0

    # 5. PII
    records = _apply_pii(entities, source_errors)

    # 6. Normalización adicional
    records = _apply_normalization(records, source_errors)

    # 7. Dedup (solo Event/AcopioCenter)
    records, n_deduped = _apply_dedup(records, source_errors)

    # 8. Confidence score
    records = _apply_confidence(records, source_errors)

    # 9. Export
    n_exported = _export(records, output_dir, source_errors)

    all_errors.extend([f"[{source.id}] {e}" for e in source_errors])
    log.info("%s: %d exportados, %d deduplicados, %d errores", source.id, n_exported, n_deduped, len(source_errors))
    return n_exported, n_deduped


# ---------------------------------------------------------------------------
# Punto de entrada público
# ---------------------------------------------------------------------------

def run_pipeline(
    config_path: Path,
    output_dir: Path,
    limit: int | None = None,
    keep_raw: bool = False,
) -> dict:
    """
    Orquestador principal del pipeline.

    Parameters
    ----------
    config_path:
        Ruta al YAML de configuración de fuentes.
    output_dir:
        Directorio donde se escriben persons.jsonl / acopio.jsonl / events.jsonl.
    limit:
        Número máximo de entidades por fuente (None = sin límite).
    keep_raw:
        Reservado para snapshots de depuración (no implementado aún).

    Returns
    -------
    dict con keys: sources_processed, documents_exported, claims_exported,
                   claims_deduplicated, errors.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Pipeline iniciado — config=%s output=%s limit=%s", config_path, output_dir, limit)

    # Cargar fuentes
    try:
        project, sources = load_sources(config_path)
    except Exception as exc:
        log.error("Error cargando config: %s", exc)
        return {
            "sources_processed": 0,
            "documents_exported": 0,
            "claims_exported": 0,
            "claims_deduplicated": 0,
            "errors": [f"Error cargando config: {exc}"],
        }

    enabled = [s for s in sources if s.enabled]
    log.info("%d fuentes habilitadas de %d totales", len(enabled), len(sources))

    total_exported = 0
    total_deduped = 0
    sources_processed = 0
    all_errors: list[str] = []

    for source in enabled:
        try:
            n_exp, n_dup = _run_source(source, output_dir, limit, all_errors)
            total_exported += n_exp
            total_deduped += n_dup
            sources_processed += 1
        except Exception as exc:
            msg = f"[{source.id}] Error fatal en fuente: {exc}"
            log.error(msg, exc_info=True)
            all_errors.append(msg)
            # Continuar con la siguiente fuente

    summary = {
        "sources_processed": sources_processed,
        "documents_exported": total_exported,
        "claims_exported": total_exported,       # alias legacy para cli.py
        "claims_deduplicated": total_deduped,
        "errors": all_errors,
    }

    log.info(
        "Pipeline finalizado — fuentes=%d exportados=%d deduplicados=%d errores=%d",
        sources_processed, total_exported, total_deduped, len(all_errors),
    )
    return summary
