#!/usr/bin/env python3
"""
Genera noticias.json a partir del RSS de búsqueda de Google News, filtrado a
noticias bolivianas relacionadas a tipo de cambio, reservas, FMI, subsidios,
etc.

Antes usábamos el RSS de El Deber directamente (primero /rss/economia.xml,
luego /rss/home.xml), pero los logs de GitHub Actions mostraron un 403
Forbidden contra CUALQUIER ruta de eldeber.com.bo, siempre, sin importar el
User-Agent: es un bloqueo de red/IP contra los runners de GitHub Actions, no
un tema de paywall de la sección Economía. Google News agrega muchos medios
a la vez y es mucho más difícil que bloquee IPs de nubes como la de GitHub
Actions.

Trade-offs conocidos de este enfoque (aceptados explícitamente):
  - Los links son redirecciones de Google (news.google.com/rss/articles/...),
    no la URL directa del medio. Al abrirlos, el navegador sí termina en la
    nota original.
  - Google News no entrega imagen de portada (no hay <enclosure> en su feed),
    así que las noticias se muestran sin miniatura.
  - La <description> de Google News es HTML de previsualización (el mismo
    título envuelto en un <a>, más el nombre del medio) y NO es un resumen
    real de la nota - por eso acá NO se usa como "bajada": se deja vacía en
    vez de inventar un resumen falso.

No usa ningún modelo de lenguaje: solo arma la búsqueda con las palabras
clave de abajo y deja que Google News decida qué notas coinciden.
"""

import json
import re
import sys
from datetime import datetime, timezone
from urllib.parse import quote
from xml.etree import ElementTree

import requests

# Búsqueda de Google News: términos de tipo de cambio / BCB / reservas / FMI /
# subvenciones acotada a Bolivia. "OR" y comillas son operadores de búsqueda
# de Google, igual que en news.google.com.
SEARCH_QUERY = (
    '("tipo de cambio" OR dolar OR dólar OR BCB OR "banco central" OR '
    'reservas OR FMI OR "banco mundial" OR "deuda externa" OR subvencion OR '
    'subvención OR subsidio OR divisas OR USDT OR cambiario OR devaluacion OR '
    'devaluación) Bolivia'
)
RSS_URL = (
    "https://news.google.com/rss/search?q=" + quote(SEARCH_QUERY) +
    "&hl=es-419&gl=BO&ceid=BO:es-419"
)
OUT_PATH = "noticias.json"
MAX_ITEMS = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.9",
    "Accept-Language": "es-BO,es;q=0.9,en;q=0.8",
}

# Filtro adicional sobre el título (Google ya filtró por SEARCH_QUERY, pero
# esto evita ruido de resultados solo tangencialmente relacionados).
KEYWORDS = [
    "dolar", "dólar", "tipo de cambio", "tco", "devaluacion", "devaluación",
    "apreciacion", "depreciacion", "reserva", "bcb", "banco central",
    "prestamo", "préstamo", "credito internacional", "crédito internacional",
    "fmi", "banco mundial", "bid ", "deuda externa", "subsidio", "subvencion",
    "subvención", "divisas", "usdt", "paralelo", "cambiario", "cambiaria",
    "importacion", "importación", "exportacion", "exportación",
    "balanza comercial", "inflacion", "inflación",
]


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def strip_cdata(text):
    if text is None:
        return ""
    return text.strip()


def normalize(s):
    s = s.lower()
    repl = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n"}
    for a, b in repl.items():
        s = s.replace(a, b)
    return s


def matches_keywords(title):
    text = normalize(title)
    return any(normalize(kw) in text for kw in KEYWORDS)


def parse_pubdate(s):
    # Formato RFC 822 típico de RSS: "Fri, 18 Sep 2026 00:15:02 GMT"
    formats = ["%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def clean_title(title_raw, source_name):
    """Google News pone el título como 'Titular - Medio'; se quita el sufijo
    del medio si coincide, para no repetirlo (ya se muestra aparte)."""
    title = re.sub(r"\s+", " ", title_raw).strip()
    if source_name:
        suffix = " - " + source_name
        if title.endswith(suffix):
            title = title[: -len(suffix)].strip()
    return title


def main():
    log(f"Descargando RSS de Google News: {RSS_URL}")
    try:
        r = requests.get(RSS_URL, headers=HEADERS, timeout=25)
        r.raise_for_status()
        root = ElementTree.fromstring(r.content)
    except Exception as e:
        # Un fallo acá (bloqueo, timeout, XML roto, etc.) nunca debe tumbar
        # el resto del pipeline: se deja noticias.json sin tocar y se sale
        # con código 0 para que el workflow siga hasta el commit de los
        # datos numéricos, que sí se actualizaron en el paso anterior.
        log(f"ADVERTENCIA: no se pudo obtener/leer el RSS ({e}). "
            "Se deja noticias.json sin cambios.")
        return 0

    channel = root.find("channel")
    if channel is None:
        log("ADVERTENCIA: el feed no tiene <channel>. Se deja noticias.json sin cambios.")
        return 0

    candidates = []
    seen_titles = set()
    for item in channel.findall("item"):
        title_raw = strip_cdata(item.findtext("title"))
        link = strip_cdata(item.findtext("link"))
        pub_date_raw = strip_cdata(item.findtext("pubDate"))
        pub_date = parse_pubdate(pub_date_raw)

        source_el = item.find("source")
        source_name = strip_cdata(source_el.text) if source_el is not None else None

        # Algunos items de Google News sí traen imagen vía <media:content>
        # (namespace MRSS); si no está, se deja sin miniatura como antes.
        media_el = item.find("{http://search.yahoo.com/mrss/}content")
        image = media_el.get("url") if media_el is not None else None

        if not title_raw or not link:
            continue

        title = clean_title(title_raw, source_name)
        if not matches_keywords(title):
            continue

        # Google suele repetir la misma noticia vía varios agregadores/medios
        # - se descarta el duplicado exacto de título para no mostrar la
        # misma nota dos veces en los 5 espacios disponibles.
        dedup_key = normalize(title)
        if dedup_key in seen_titles:
            continue
        seen_titles.add(dedup_key)

        candidates.append({
            "title": title,
            # Sin resumen real disponible en el feed de Google News (ver
            # docstring) - se deja vacío en vez de inventar uno.
            "summary": "",
            "source": source_name or "Google News",
            "url": link,
            "image": image,
            "_pub_date": pub_date or datetime.min,
        })

    candidates.sort(key=lambda c: c["_pub_date"], reverse=True)
    chosen = candidates[:MAX_ITEMS]
    for c in chosen:
        del c["_pub_date"]

    if not chosen:
        log("ADVERTENCIA: ningún titular del RSS coincidió con las palabras clave "
            "esta corrida - se deja noticias.json sin cambios.")
        return 0

    out = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "items": chosen,
    }

    with open(OUT_PATH, "r", encoding="utf-8") as f:
        previous = json.load(f)

    if previous.get("items") == out["items"]:
        log("Sin cambios en las noticias seleccionadas - no se reescribe noticias.json.")
        return 0

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    log(f"noticias.json actualizado con {len(chosen)} titulares.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
