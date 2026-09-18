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
    "User-Agent": "Mozilla/5.0 (compatible; PulsoCambiarioBot/1.0; "
                  "+https://github.com/Anubis-tech/pulso-cambiario)"
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
    r = requests.get(RSS_URL, headers=HEADERS, timeout=25)
    r.raise_for_status()

    root = ElementTree.fromstring(r.content)
    channel = root.find("channel")
    if channel is None:
        log("ERROR: el feed no tiene <channel>. Se aborta sin tocar noticias.json.")
        return 1

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
