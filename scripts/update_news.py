#!/usr/bin/env python3
"""
Genera noticias.json a partir del RSS de Economía de El Deber, filtrando por
palabras clave relacionadas a tipo de cambio, reservas, préstamos
internacionales, subsidios, etc.

No usa ningún modelo de lenguaje: el "resumen" de cada nota es la propia
descripción (bajada) que El Deber publica en su feed RSS, pensada por el
sitio para mostrarse en previsualizaciones - no es un resumen original.
Siempre se enlaza directamente a la nota completa en eldeber.com.bo.
"""

import json
import re
import sys
from datetime import datetime, timezone
from xml.etree import ElementTree

import requests

RSS_URL = "https://eldeber.com.bo/rss/economia.xml"
OUT_PATH = "noticias.json"
MAX_ITEMS = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.9",
    "Accept-Language": "es-BO,es;q=0.9,en;q=0.8",
    "Referer": "https://eldeber.com.bo/",
}

KEYWORDS = [
    "dolar", "dólar", "tipo de cambio", "tco", "devaluacion", "devaluación",
    "apreciacion", "depreciacion", "reserva", "bcb", "banco central",
    "prestamo", "préstamo", "credito internacional", "crédito internacional",
    "fmi", "banco mundial", "bid ", "deuda externa", "subsidio",
    "divisas", "usdt", "paralelo", "cambiario", "importacion", "importación",
    "exportacion", "exportación", "balanza comercial", "inflacion", "inflación",
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


def matches_keywords(title, summary):
    text = normalize(title + " " + summary)
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


def main():
    log(f"Descargando RSS: {RSS_URL}")
    try:
        r = requests.get(RSS_URL, headers=HEADERS, timeout=25)
        r.raise_for_status()
        root = ElementTree.fromstring(r.content)
    except Exception as e:
        # Un fallo acá (403 del sitio, timeout, XML roto, etc.) nunca debe
        # tumbar el resto del pipeline: se deja noticias.json sin tocar y
        # se sale con código 0 para que el workflow siga hasta el commit
        # de los datos numéricos, que sí se actualizaron en el paso anterior.
        log(f"ADVERTENCIA: no se pudo obtener/leer el RSS ({e}). "
            "Se deja noticias.json sin cambios.")
        return 0

    channel = root.find("channel")
    if channel is None:
        log("ADVERTENCIA: el feed no tiene <channel>. Se deja noticias.json sin cambios.")
        return 0

    candidates = []
    for item in channel.findall("item"):
        title = strip_cdata(item.findtext("title"))
        description = strip_cdata(item.findtext("description"))
        link = strip_cdata(item.findtext("link"))
        pub_date_raw = strip_cdata(item.findtext("pubDate"))
        pub_date = parse_pubdate(pub_date_raw)

        enclosure = item.find("enclosure")
        image = enclosure.get("url") if enclosure is not None else None

        if not title or not link:
            continue
        if not matches_keywords(title, description):
            continue

        candidates.append({
            "title": re.sub(r"\s+", " ", title).strip(),
            "summary": re.sub(r"\s+", " ", description).strip(),
            "source": "El Deber",
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
