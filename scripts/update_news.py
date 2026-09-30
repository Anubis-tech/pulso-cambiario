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
  - Google News casi nunca entrega imagen de portada en el propio feed (no
    hay <enclosure>, y <media:content> aparece en pocos items) - por eso,
    para los 5 titulares finalmente elegidos (no para todos los candidatos,
    para no hacer decenas de requests de más), se intenta además seguir el
    link y sacar la imagen real de la nota original desde las etiquetas
    <meta property="og:image"> / <meta name="twitter:image"> de su HTML -
    el mismo mecanismo que usan las previsualizaciones de links de
    WhatsApp/Twitter/etc. Si el link de Google no llega a resolver a la nota
    real (a veces requiere JavaScript) o la nota no publica esas etiquetas,
    simplemente se deja sin miniatura - nunca se inventa o adivina una
    imagen.
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
from urllib.parse import quote, urlparse
from xml.etree import ElementTree

import requests

# Búsqueda de Google News: términos de tipo de cambio / BCB / reservas / FMI /
# subvenciones. El "Bolivia" suelto al final NO alcanza para acotar a medios
# bolivianos (Google igual trae notas de México/Venezuela/Colombia, porque
# "dólar", "subsidio", etc. son términos genéricos de cualquier país) - por
# eso además se restringe con site: a los medios bolivianos más importantes.
# Esto es un "mejor esfuerzo" en la búsqueda; el filtro real y garantizado
# está en is_bolivian() más abajo, que corre sobre cada resultado.
SEARCH_QUERY = (
    '(site:eldeber.com.bo OR site:lostiempos.com OR site:la-razon.com OR '
    'site:paginasiete.bo OR site:eldiario.net OR site:opinion.com.bo OR '
    'site:erbol.com.bo OR site:correodelsur.com OR site:abi.bo) '
    '("tipo de cambio" OR dolar OR dólar OR BCB OR "banco central" OR '
    'reservas OR FMI OR "banco mundial" OR "deuda externa" OR subvencion OR '
    'subvención OR subsidio OR divisas OR USDT OR cambiario OR devaluacion OR '
    'devaluación)'
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

# El "Bolivia" al final de SEARCH_QUERY NO garantiza que Google News solo
# devuelva notas bolivianas - en la práctica trajo notas de México, Venezuela
# y Colombia porque varias de las palabras clave (dólar, tipo de cambio,
# subsidio, FMI) son genéricas y aparecen en economía de cualquier país. Por
# eso acá se exige ADEMÁS que la nota sea de un medio boliviano conocido, o
# que el propio título mencione "Bolivia" (para el caso de que un medio
# internacional cubra específicamente algo boliviano).
BOLIVIAN_OUTLETS = [
    "el deber", "los tiempos", "la razon", "pagina siete", "opinion",
    "el diario", "correo del sur", "erbol", "agencia de noticias fides",
    " anf", "unitel", "red uno", "abi", "el potosi", "bolivia.com",
    "urgente.bo", "brujula digital", "eldeber", "reduno", "bolivision",
    "gigavision", "notibol",
]


def is_bolivian(title, source_name):
    text = normalize(title)
    if "bolivia" in text:
        return True
    if source_name:
        source_norm = normalize(source_name)
        if any(outlet in source_norm for outlet in BOLIVIAN_OUTLETS):
            return True
    return False


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


# Busca <meta property="og:image" content="..."> o su equivalente de Twitter,
# aceptando los dos órdenes posibles de atributos dentro del tag.
_OG_IMAGE_PATTERNS = [
    re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]*\scontent=["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*\sproperty=["\']og:image["\']', re.IGNORECASE),
    re.compile(r'<meta[^>]+name=["\']twitter:image["\'][^>]*\scontent=["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*\sname=["\']twitter:image["\']', re.IGNORECASE),
]


def _is_google_host(url):
    host = urlparse(url).netloc.lower()
    return "google.com" in host or "gstatic.com" in host or "googleusercontent.com" in host


def fetch_og_image(url):
    """Sigue el link (puede ser una redirección de Google News) e intenta
    sacar la imagen de portada real de la nota desde sus meta tags og:image /
    twitter:image. Devuelve None ante cualquier problema (nota bloqueada,
    sin esas etiquetas, o el link de Google que no llegó a resolver a la
    nota real) - nunca inventa una imagen. Pensado para correr solo sobre
    los 5 titulares ya elegidos, no sobre todos los candidatos del feed."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        r.raise_for_status()
    except Exception:
        return None

    # Si el link de Google no terminó de redirigir a la nota real (algunas
    # rutas de Google News necesitan JavaScript para el salto final), lo que
    # tenemos es HTML de Google, no del medio - ahí no hay imagen real que
    # sacar.
    if _is_google_host(r.url):
        return None

    html = r.text
    for pattern in _OG_IMAGE_PATTERNS:
        m = pattern.search(html)
        if not m:
            continue
        image_url = m.group(1).strip()
        if not image_url.startswith("http"):
            continue
        if _is_google_host(image_url):
            continue
        return image_url
    return None


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
        if not is_bolivian(title, source_name):
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

    # Para los elegidos que no trajeron imagen del feed (la gran mayoría),
    # se intenta sacar la imagen real de la nota siguiendo el link - esto
    # agrega hasta MAX_ITEMS requests extra, por eso se hace solo acá y no
    # sobre todos los candidatos.
    found_images = 0
    for item in chosen:
        if item.get("image"):
            found_images += 1
            continue
        item["image"] = fetch_og_image(item["url"])
        if item["image"]:
            found_images += 1
    log(f"  Miniaturas encontradas: {found_images}/{len(chosen)}.")

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
