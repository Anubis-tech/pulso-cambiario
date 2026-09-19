#!/usr/bin/env python3
"""
Actualiza data.json de Pulso Cambiario con datos frescos del BCB, del mercado
paralelo (Binance P2P) y recalcula los agregados bancarios.

Diseño defensivo: cada fuente se intenta de forma independiente y con
try/except propio. Si una fuente falla o entrega un valor que no pasa un
chequeo de sensatez (sanity check), esa fuente se salta con un mensaje de
advertencia claro en el log, pero el resto del pipeline sigue. Nunca se
escribe data.json si terminó sin ninguna actualización real.

Pensado para correr en GitHub Actions (con acceso normal a internet). NO
funciona desde el sandbox de Claude porque bcb.gob.bo está bloqueado ahí.
"""

import json
import re
import sys
import time
import traceback
from datetime import datetime, timezone

import requests

DATA_PATH = "data.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PulsoCambiarioBot/1.0; "
                  "+https://github.com/Anubis-tech/pulso-cambiario)"
}

TIMEOUT = 25


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def load_data():
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))


def upsert(series, new_row, key="date"):
    """Inserta new_row en la lista series (ordenada por fecha) si su fecha
    no existe ya, o la reemplaza si existe. Devuelve True si hubo cambio real
    de valores (no solo un no-op)."""
    for i, row in enumerate(series):
        if row[key] == new_row[key]:
            if row == new_row:
                return False
            series[i] = new_row
            return True
    series.append(new_row)
    series.sort(key=lambda r: r[key])
    return True


def sanity_ok(old_value, new_value, max_relative_change):
    if old_value is None or old_value == 0:
        return True
    change = abs(new_value - old_value) / abs(old_value)
    return change <= max_relative_change


# ---------------------------------------------------------------------------
# 1. TCO oficial (BCB) vía la API pública de cucu.bo
# ---------------------------------------------------------------------------

def fetch_tco(data):
    log("TCO oficial: consultando apibcb.cucu.bo ...")
    r = requests.get("https://apibcb.cucu.bo/api/v1/tc/oficial", headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    tc = j["tc_oficial"]
    fecha = tc["fecha"]
    compra = float(tc.get("compra", tc.get("base")))
    venta = float(tc.get("venta"))

    tco = data["tco"]
    last = tco[-1] if tco else None
    if last and not sanity_ok(last["tco_compra_bcb"], compra, 0.15):
        log(f"  ADVERTENCIA: compra {compra} difiere >15% del último valor "
            f"({last['tco_compra_bcb']}) - se omite esta actualización de TCO.")
        return False

    changed = upsert(tco, {"date": fecha, "tco_compra_bcb": compra, "tco_venta_bcb": venta})
    log(f"  TCO {fecha}: compra={compra} venta={venta} (cambio={changed})")
    return changed


# ---------------------------------------------------------------------------
# 2. Tasas banco por banco (BCB, página HTML)
# ---------------------------------------------------------------------------

BANK_RATES_URL = "https://www.bcb.gob.bo/bcb_tco_publico_evolutivo.php"

MESES_ABR = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12,
}


def fetch_bank_rates(data):
    """La página del BCB muestra el tipo de cambio por banco como una tabla
    'ancha': una fila por banco, una columna por fecha (encabezados tipo
    '17-sep'), y trae de regalo semanas de historia completa en cada
    corrida - así que cada vez que se corre, se reintentan también fechas
    pasadas (upsert es idempotente, no hace daño repetir una fecha ya
    cargada) y el pipeline se autocorrige solo si se perdió una corrida."""
    from bs4 import BeautifulSoup

    log(f"Tasas bancarias: consultando {BANK_RATES_URL} ...")
    r = requests.get(BANK_RATES_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    tables = soup.find_all("table")
    if not tables:
        log("  ADVERTENCIA: la página no tiene ninguna tabla - "
            "la estructura pudo haber cambiado. Se omite.")
        return False

    rate_table = _find_wide_table(soup, "Compra") or tables[0]
    monto_table = _find_wide_table(soup, "Montos transados") or (tables[1] if len(tables) > 1 else None)
    n_table = _find_wide_table(soup, "Transacciones") or (tables[2] if len(tables) > 2 else None)

    _, rates_by_row = _parse_wide_table(rate_table)
    _, montos_by_row = _parse_wide_table(monto_table) if monto_table is not None else (None, {})
    _, n_by_row = _parse_wide_table(n_table) if n_table is not None else (None, {})
    montos_by_row = montos_by_row or {}
    n_by_row = n_by_row or {}

    known_banks = set(data["bank_names"])

    def match_bank(label):
        label_norm = _norm_bank_name(label)
        for bank in known_banks:
            if _norm_bank_name(bank) == label_norm:
                return bank
        return None

    def lookup_other_table(by_row, bank):
        for label2, by_date2 in by_row.items():
            if match_bank(label2) == bank:
                return by_date2
        return {}

    any_change = False
    matched = 0

    for label, by_date in rates_by_row.items():
        bank = match_bank(label)
        if not bank:
            continue
        matched += 1
        montos = lookup_other_table(montos_by_row, bank)
        ns = lookup_other_table(n_by_row, bank)
        series = data["banks"].setdefault(bank, [])
        for date_str, rate in by_date.items():
            row = {"date": date_str, "rate": rate}
            if date_str in montos:
                row["monto"] = montos[date_str]
            if date_str in ns:
                row["n"] = ns[date_str]
            if upsert(series, row):
                any_change = True

    if matched == 0:
        log("  ADVERTENCIA: no se reconoció ningún banco en la tabla - "
            "la estructura de la página pudo haber cambiado. Se omite.")
        return False

    log(f"  Bancos reconocidos: {matched} de {len(rates_by_row)} filas en la tabla.")

    # Filas agregadas "BANCOS (...)" - se toman directo de la página (ya
    # vienen calculadas por el BCB) en vez de recalcularlas acá.
    agg = data.setdefault("aggregates", {})

    def find_agg(by_row, keywords):
        for label, by_date in by_row.items():
            if any(kw in label.lower() for kw in keywords):
                return by_date
        return {}

    for date_str, v in find_agg(rates_by_row, ["ponderad"]).items():
        if upsert(agg.setdefault("Bancos (promedio ponderado)", []), {"date": date_str, "rate": v}):
            any_change = True
    for date_str, v in find_agg(montos_by_row, ["montos totales", "monto total"]).items():
        if upsert(agg.setdefault("Bancos (montos totales)", []), {"date": date_str, "monto": v}):
            any_change = True
    for date_str, v in find_agg(n_by_row, ["numero de transacciones", "transacciones"]).items():
        if upsert(agg.setdefault("Bancos (numero de transacciones)", []), {"date": date_str, "n": v}):
            any_change = True

    return any_change


def _find_wide_table(soup, keyword):
    """Busca la tabla precedida (en el texto cercano anterior) por 'keyword'
    - así se distingue la tabla de tasas de la de montos y la de número de
    transacciones aunque tengan la misma forma."""
    for table in soup.find_all("table"):
        node, seen = table, ""
        for _ in range(6):
            node = node.find_previous(string=True)
            if node is None:
                break
            seen += " " + node
        if keyword.lower() in seen.lower():
            return table
    return None


def _extract_range_end(caption_text):
    """De un texto tipo 'Fecha de corte 26/06/2026 -- 17/09/2026' devuelve la
    segunda fecha (la más reciente) como datetime."""
    matches = re.findall(r"(\d{1,2})/(\d{1,2})/(\d{4})", caption_text)
    if not matches:
        return None
    d, m, y = matches[-1]
    try:
        return datetime(int(y), int(m), int(d))
    except ValueError:
        return None


def _parse_col_date(header_text, end_date):
    """Convierte un encabezado de columna tipo '17-sep' en 'YYYY-MM-DD',
    usando end_date para inferir el año (retrocede un año si el resultado
    quedaría después de end_date)."""
    m = re.match(r"^\s*(\d{1,2})[-/]([a-zA-Záéíóú]{3,})\s*$", header_text.strip(), re.IGNORECASE)
    if not m:
        return None
    day = int(m.group(1))
    mon_abbr = m.group(2).strip().lower()[:3].replace("é", "e")
    month = MESES_ABR.get(mon_abbr)
    if not month:
        return None
    year = end_date.year
    try:
        d = datetime(year, month, day)
    except ValueError:
        return None
    if d > end_date:
        try:
            d = datetime(year - 1, month, day)
        except ValueError:
            return None
    return d.strftime("%Y-%m-%d")


def _parse_wide_table(table):
    """Parsea una tabla 'ancha' del BCB: primera columna = nombre de banco (o
    fila agregada 'BANCOS (...)'), columnas siguientes = fechas cortas
    ('17-sep'). Devuelve (fecha_fin_str, {etiqueta_fila: {fecha: valor}})."""
    if table is None:
        return None, {}
    rows = table.find_all("tr")
    if len(rows) < 2:
        return None, {}

    header_cells = [c.get_text(" ", strip=True) for c in rows[0].find_all(["td", "th"])]
    date_headers = header_cells[1:]

    caption_text = ""
    node = table
    for _ in range(6):
        node = node.find_previous(string=True)
        if node is None:
            break
        caption_text += " " + node
    end_date = _extract_range_end(caption_text) or datetime.now(timezone.utc)

    col_dates = [_parse_col_date(h, end_date) for h in date_headers]

    data_by_row = {}
    for tr in rows[1:]:
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) < 2:
            continue
        label = cells[0].strip()
        by_date = {}
        for date_str, raw in zip(col_dates, cells[1:]):
            if date_str is None:
                continue
            v = _to_float(raw)
            if v is not None:
                by_date[date_str] = v
        if by_date:
            data_by_row[label] = by_date

    return end_date.strftime("%Y-%m-%d"), data_by_row


def _norm_bank_name(name):
    name = name.upper()
    name = (name.replace("Á", "A").replace("É", "E").replace("Í", "I")
                .replace("Ó", "O").replace("Ú", "U").replace("Ñ", "N"))
    name = re.sub(r"[^A-Z0-9]+", " ", name).strip()
    return name


def _to_float(s):
    """Convierte un número de texto a float, aceptando tanto formato
    anglosajón (1,234.56) como boliviano/latino (1.234,56 o simplemente
    11,52 con coma decimal). Usa el ÚLTIMO separador (',' o '.') que
    aparece como el separador decimal; el resto se trata como separador
    de miles y se elimina."""
    s = s.replace("Bs", "").replace("$us", "").replace("USD", "").strip()
    if not s or s in ("-", "—", "n/a", "N/A"):
        return None

    comma_count = s.count(",")
    dot_count = s.count(".")

    if comma_count and dot_count:
        # el símbolo que aparece más a la derecha es el separador decimal
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif comma_count > 1:
        s = s.replace(",", "")  # coma repetida = separador de miles
    elif dot_count > 1:
        s = s.replace(".", "")  # punto repetido = separador de miles
    elif comma_count == 1:
        # una sola coma: es decimal si le siguen 1-2 dígitos (formato
        # boliviano típico de una tasa, "11,52"); si le siguen 3 dígitos es
        # casi seguro un separador de miles en formato inglés ("1,175").
        decimals = s.split(",")[-1]
        if len(decimals) in (1, 2):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif dot_count == 1:
        # mismo criterio con el punto solo: 1-2 dígitos detrás = decimal,
        # 3 dígitos = separador de miles en formato boliviano ("98.086").
        decimals = s.split(".")[-1]
        if len(decimals) == 3:
            s = s.replace(".", "")
    # si no hay coma ni punto, se deja tal cual
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# 3. USDT/BOB paralelo vía Binance P2P
# ---------------------------------------------------------------------------

def fetch_usdt_bob(data):
    log("USDT/BOB: consultando Binance P2P ...")
    body = {
        "asset": "USDT", "fiat": "BOB", "tradeType": "SELL",
        "page": 1, "rows": 10, "payTypes": [], "publisherType": None,
    }
    r = requests.post("https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search",
                       json=body, headers={**HEADERS, "Content-Type": "application/json"},
                       timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    ads = j.get("data") or []
    prices = [float(a["adv"]["price"]) for a in ads if a.get("adv", {}).get("price")]
    if not prices:
        log("  ADVERTENCIA: Binance no devolvió anuncios para USDT/BOB - se omite.")
        return False
    prices.sort()
    median_price = prices[len(prices) // 2]

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    usdt = data["usdt"]
    last = usdt[-1] if usdt else None
    if last and not sanity_ok(last["usdt_bob"], median_price, 0.25):
        log(f"  ADVERTENCIA: precio USDT/BOB {median_price} difiere >25% del "
            f"último ({last['usdt_bob']}) - se omite.")
        return False

    changed = upsert(usdt, {"date": today, "usdt_bob": median_price})

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    intraday = data.setdefault("usdt_intraday_recent", [])
    intraday.append({"ts": now_str, "value": median_price})
    cutoff = time.time() - 30 * 24 * 3600
    data["usdt_intraday_recent"] = [
        p for p in intraday
        if _parse_ts(p["ts"]) >= cutoff
    ][-3000:]

    log(f"  USDT/BOB mediana de {len(prices)} anuncios: {median_price}")
    return changed or True  # el intraday casi siempre cambia


def _parse_ts(ts):
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


# ---------------------------------------------------------------------------
# 4-6. Series mensuales/diarias del BCB vía Excel (reservas, balanza, ITCR)
# ---------------------------------------------------------------------------

XLSX_SOURCES = {
    "reservas": {
        "url": "https://www.bcb.gob.bo/webdocs/sector_externo/"
               "F%20Reservas%20Internacionales%20Netas/"
               "Reservas_Internacionales_Netas_del_Banco_Central_de_Bolivia_por_componentes.xlsx",
        "columns": {
            "fmi": ["fmi", "tramo de reserva", "posicion de reserva"],
            "deg": ["deg", "derechos especiales"],
            "divisas": ["divisas"],
            "oro": ["oro"],
            "neta": ["reservas internacionales netas", "neta", "total"],
            "netas_obligaciones": ["obligaciones"],
        },
        "max_relative_change": 0.20,
    },
    "balanza": {
        "url": "https://www.bcb.gob.bo/webdocs/sector_externo/"
               "G%20Otras%20variables%20del%20sector%20externo/Balanza%20Cambiaria.xlsx",
        "columns": {
            "ingreso": ["ingreso"],
            "egreso": ["egreso"],
            "flujo": ["flujo", "saldo"],
        },
        "max_relative_change": None,  # el flujo puede cambiar de signo de un mes a otro
    },
    "itcr": {
        "url": "https://www.bcb.gob.bo/webdocs/sector_externo/"
               "G%20Otras%20variables%20del%20sector%20externo/"
               "Indices%20de%20tipo%20de%20cambio%20real.xlsx",
        "columns": {
            "value": ["itcr", "indice de tipo de cambio real", "indice"],
        },
        "max_relative_change": 0.20,
    },
}


def fetch_xlsx_series(data, series_name):
    from openpyxl import load_workbook
    import io

    cfg = XLSX_SOURCES[series_name]
    log(f"{series_name}: descargando {cfg['url']} ...")
    r = requests.get(cfg["url"], headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    wb = load_workbook(io.BytesIO(r.content), data_only=True)

    best_sheet, best_rows = None, []
    for sheet in wb.worksheets:
        rows = _extract_dated_rows(sheet, cfg["columns"])
        if len(rows) > len(best_rows):
            best_sheet, best_rows = sheet.title, rows

    if not best_rows:
        log(f"  ADVERTENCIA: no se pudo interpretar la estructura del Excel de "
            f"{series_name} (hoja probada: {[s.title for s in wb.worksheets]}). "
            f"Revisar manualmente y ajustar el mapeo de columnas en update_data.py.")
        return False

    log(f"  Hoja usada: {best_sheet}. Filas de datos detectadas: {len(best_rows)}. "
        f"Última fila: {best_rows[-1]}")

    series = data[series_name]
    last = series[-1] if series else None
    max_rel = cfg["max_relative_change"]
    any_change = False

    for row in best_rows:
        if last and max_rel is not None:
            key = "value" if series_name == "itcr" else "neta" if series_name == "reservas" else None
            if key and key in row and key in (last or {}):
                if not sanity_ok(last[key], row[key], max_rel):
                    log(f"  ADVERTENCIA: {series_name} {row['date']} valor {row[key]} "
                        f"difiere demasiado del último conocido ({last[key]}) - fila omitida.")
                    continue
        if upsert(series, row):
            any_change = True
            last = row

    return any_change


def _extract_dated_rows(sheet, column_map):
    """Ubica la tabla de datos de una hoja del BCB en dos pasadas:
    1) encuentra la columna de fechas mirando TODA la hoja (la columna con
       más celdas interpretables como fecha gana) - esto es independiente de
       dónde esté el encabezado, así que no lo confunden títulos ni notas.
    2) una vez sabido dónde empiezan los datos, busca en las filas de ARRIBA
       (nunca en título o notas sueltas más arriba) cuál columna corresponde
       a cada campo de column_map, por palabras clave.
    """
    rows_all = list(sheet.iter_rows(values_only=True))
    if not rows_all:
        return []

    max_cols = max((len(r) for r in rows_all), default=0)
    date_col, date_hits = None, 0
    for col in range(min(6, max_cols)):
        hits = sum(1 for row in rows_all if col < len(row) and _coerce_date(row[col]) is not None)
        if hits > date_hits:
            date_col, date_hits = col, hits
    if date_col is None or date_hits < 3:
        return []

    first_data_row = next(
        i for i, row in enumerate(rows_all)
        if date_col < len(row) and _coerce_date(row[date_col]) is not None
    )

    # Buscar encabezados solo entre el inicio de la hoja y la primera fila de
    # datos, quedándose con la coincidencia más cercana a los datos si una
    # misma palabra clave aparece en más de una fila (p.ej. título Y encabezado).
    col_index = {}
    for i in range(first_data_row):
        cells = [str(c).strip().lower() if c is not None else "" for c in rows_all[i]]
        for field, keywords in column_map.items():
            for j, cell in enumerate(cells):
                if j != date_col and any(kw in cell for kw in keywords):
                    col_index[field] = j
                    break

    if not col_index:
        return []

    results = []
    empty_streak = 0
    for row in rows_all[first_data_row:]:
        date_val = row[date_col] if date_col < len(row) else None
        date_str = _coerce_date(date_val)
        if date_str is None:
            empty_streak += 1
            if empty_streak > 5 and results:
                break
            continue
        empty_streak = 0
        entry = {"date": date_str}
        for field, j in col_index.items():
            if j < len(row) and isinstance(row[j], (int, float)):
                entry[field] = float(row[j])
        if len(entry) > 1:
            results.append(entry)

    results.sort(key=lambda r: r["date"])
    return results


def _coerce_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if hasattr(val, "isoformat") and not isinstance(val, str):
        try:
            return val.isoformat()
        except Exception:
            pass
    if isinstance(val, str):
        s = val.strip()
        m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
        if m:
            return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
        if m:
            return f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    return None


# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only", default=None,
        help="Lista separada por comas de fuentes a correr (tco,banks,usdt,reservas,balanza,itcr). "
             "Si se omite, corren todas."
    )
    args = parser.parse_args()
    only = set(s.strip() for s in args.only.split(",")) if args.only else None

    data = load_data()
    any_change = False
    failures = []

    steps = [
        ("tco", "TCO oficial", fetch_tco),
        ("banks", "Tasas bancarias", fetch_bank_rates),
        ("usdt", "USDT/BOB", fetch_usdt_bob),
        ("reservas", "Reservas internacionales", lambda d: fetch_xlsx_series(d, "reservas")),
        ("balanza", "Balanza cambiaria", lambda d: fetch_xlsx_series(d, "balanza")),
        ("itcr", "ITCR", lambda d: fetch_xlsx_series(d, "itcr")),
    ]
    if only:
        steps = [s for s in steps if s[0] in only]
        log(f"Corriendo solo: {', '.join(s[0] for s in steps)}")

    for _key, name, fn in steps:
        try:
            changed = fn(data)
            any_change = any_change or bool(changed)
        except Exception as e:
            log(f"ERROR en '{name}': {e}")
            traceback.print_exc()
            failures.append(name)

    if any_change:
        data["meta"]["asof"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        save_data(data)
        log("data.json actualizado y guardado.")
    else:
        log("Sin cambios en ninguna fuente - no se reescribe data.json.")

    if failures:
        log(f"Fuentes con error esta corrida: {', '.join(failures)}")
        # No se sale con código de error: un fallo parcial no debe marcar todo
        # el workflow en rojo si otras fuentes sí se actualizaron. Se deja
        # constancia en el log para revisión.

    return 0


if __name__ == "__main__":
    sys.exit(main())
