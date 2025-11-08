#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor diário de fixtures + odds (API-FOOTBALL v3) com compactação e retenção.

Funcionalidades:
 - Busca jogos do dia (03:00 America/Sao_Paulo) para 5 ligas (BR A, Serie A, LaLiga, Premier, Bundesliga).
 - Faz ~11 requests: 5x /fixtures, 5x /odds?league&date (sem bet -> todos mercados), 1x /odds/bets (opcional).
 - Extrai mercados: Match Winner (1X2), Over/Under (prefere 2.5), BTTS, Handicap (Home -1 / Away +1), First Half Winner (1X2 HT).
 - Salva snapshot em GZIP: data/odds/YYYY/MM/DD.json.gz
 - Salva/atualiza cópia não compactada de conveniência: data/odds/latest.json
 - Retenção: remove .json.gz com mais de RETENTION_DAYS (padrão 90) em data/odds/

Env vars:
 - APISPORTS_KEY (obrigatória)
 - SEASON=2025 (ano de início da temporada)
 - OUT_DIR=data/odds
 - RETENTION_DAYS=90
"""

from __future__ import annotations
import os
import re
import json
import gzip
import time
import shutil
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
    _tz = lambda name: ZoneInfo(name)
except Exception:
    import pytz
    _tz = lambda name: pytz.timezone(name)

import requests

# --- Config ---------------------------------------------------------------------------

API_KEY = os.getenv("APISPORTS_KEY", "SUA_CHAVE_AQUI")
BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

LEAGUES: Dict[str, int] = {
    "BR_SERIE_A": 71,
    "ITA_SERIE_A": 135,
    "ESP_LALIGA": 140,
    "ENG_PREMIER": 39,
    "GER_BUNDESLIGA": 78,
}

SEASON = int(os.getenv("SEASON", "2025"))
OUT_DIR = os.getenv("OUT_DIR", "data/odds")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "90"))

PREFERRED_BOOKMAKERS = ["Pinnacle", "bet365", "Betfair", "Betway", "William Hill", "Bwin"]

# Mercados (regex por nome; usamos heurística para não depender de IDs fixos)
RX_MATCH_WINNER   = re.compile(r"^(match\s*winner|1x2|win\s*draw\s*win)$", re.I)
RX_OVER_UNDER     = re.compile(r"(over\s*/\s*under|totals?)", re.I)
RX_BTTS           = re.compile(r"(^both\s*teams\s*to\s*score$|^BTTS$)", re.I)
RX_HANDICAP       = re.compile(r"(^asian\s*handicap$|^handicap(?!.*corners|.*cards))", re.I)
RX_FIRST_HALF_WIN = re.compile(r"^(1(st)?|first)\s*half\s*(winner|1x2)", re.I)

# --- HTTP util com retry/backoff ------------------------------------------------------

def http_get(url: str, params: Dict[str, Any] | None = None,
             max_retries: int = 4, timeout: int = 25) -> Dict[str, Any]:
    """GET com backoff exponencial para 429/5xx; retorna JSON (dict)."""
    delay = 0.8
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception as e:
                raise RuntimeError(f"Invalid JSON from {url}: {e}") from e

        if resp.status_code in (429, 500, 502, 503, 504):
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    delay = max(delay, float(ra))
                except Exception:
                    pass
            if attempt < len(range(1, max_retries + 1)):
                time.sleep(delay)
                delay = min(delay * 2, 10.0)
                continue

        try:
            payload = resp.json()
        except Exception:
            payload = {"status_code": resp.status_code, "text": resp.text[:2000]}
        raise RuntimeError(f"GET {url} failed: {payload}")

# --- Datas ----------------------------------------------------------------------------

def today_ymd(tz_name: str = "America/Sao_Paulo") -> str:
    tz = _tz(tz_name)
    return datetime.now(tz).strftime("%Y-%m-%d")

# --- API calls ------------------------------------------------------------------------

def get_bets_catalog() -> List[Dict[str, Any]]:
    """Opcional: /odds/bets (1x) para inspeção/diagnóstico."""
    try:
        data = http_get(f"{BASE}/odds/bets")
        return data.get("response", []) or []
    except Exception:
        return []

def get_fixtures_for_league(league_id: int, date_ymd: str) -> List[Dict[str, Any]]:
    data = http_get(f"{BASE}/fixtures", {"league": league_id, "season": SEASON, "date": date_ymd})
    return data.get("response", []) or []

def get_odds_for_league_date(league_id: int, date_ymd: str) -> List[Dict[str, Any]]:
    data = http_get(f"{BASE}/odds", {"league": league_id, "season": SEASON, "date": date_ymd})
    return data.get("response", []) or []

# --- Parsing helpers ------------------------------------------------------------------

def _val_eq(v: string) -> callable:
    lv = v.lower()
    return lambda ov: str(ov.get("value", "")).lower() == lv

def _line_from(v: Optional[Dict[str, Any]]) -> str:
    if not v:
        return ""
    h = str(v.get("handicap") or "")
    if h:
        return h
    m = re.search(r"-?\d+(?:\.\d+)?", str(v.get("value", "")))
    return m.group(0) if m else ""

def _nearest_to(target: float, arr: List[Dict[str, Any]], predicate) -> Optional[Dict[str, Any]]:
    """Escolhe o value (Over/Under) cuja linha numérica é mais próxima do target."""
    best = None
    best_dist = None
    for ov in arr:
        if not predicate(ov):
            continue
        raw = str(ov.get("handicap") or ov.get("value") or "")
        m = re.search(r"-?\d+(?:\.\d+)?", raw)
        if not m:
            continue
        try:
            n = float(m.group(0))
        except Exception:
            continue
        d = abs(n - target)
        if best is None or d < best_dist:
            best, best_dist = ov, d
    return best

def _pick_bookmaker(books: List[Dict[str, Any]], bet_name_rx: re.Pattern
                   ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Escolhe (book, bet) preferindo bookmakers em PREFERRED_BOOKMAKERS e com mais valores."""
    best = None
    best_score = None
    for book in books or []:
        bets = book.get("bets") or []
        bet = next((b for b in bets if bet_name_rx.search(str(b.get("name", "")))), None)
        if not bet:
            continue
        name = str(book.get("name", ""))
        pref_rank = PREFERRED_BOOKMAKERS.index(name) if name in PREFERRED_BOOKMAKERS else len(PREFERRED_BOOKMAKERS) + 1
        richness = len(bet.get("values") or [])
        score = (len(PREFERRED_BOOKMAKERS) + 2 - pref_rank) * 100 + richness
        if best is None or score > best_score:
            best, best_score = (book, bet), score
    return best

def extract_markets(books: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    # Match Winner (1X2)
    hit = _pick_bookmaker(books, RX_MATCH_WINNER)
    if hit:
        book, bet = hit
        vals = bet.get("values") or []
        home = next((v.get("odd") for v in vals if _val_eq("Home")(v) or _val_eq("1")(v)), None)
        draw = next((v.get("odd") for v in vals if _val_eq("Draw")(v) or _val_eq("X")(v) or re.search(r"draw", str(v.get("value","")), re.I)), None)
        away = next((v.get("odd") for v in vals if _val_eq("Away")(v) or _val_eq("2")(v)), None)
        if home or draw or away:
            out["matchWinner"] = {"home": home, "draw": draw, "away": away, "bookmaker": book.get("name")}

    # Over/Under (preferir 2.5)
    hit = _pick_bookmaker(books, RX_OVER_UNDER)
    if hit:
        book, bet = hit
        vals = bet.get("values") or []
        target = 2.5
        over_exact = next((v for v in vals if re.match(r"(?i)^over", str(v.get("value",""))) and "2.5" in str(v.get("handicap") or v.get("value",""))), None)
        under_exact = next((v for v in vals if re.match(r"(?i)^under", str(v.get("value",""))) and "2.5" in str(v.get("handicap") or v.get("value",""))), None)
        over_pick = over_exact or _nearest_to(target, [v for v in vals if re.match(r"(?i)^over", str(v.get("value","")))], target)
        under_pick = under_exact or _nearest_to(target, [v for v in vals if re.match(r"(?i)^under", str(v.get("value","")))], target)
        if over_pick or under_pick:
            line = _line_from(over_pick or under_pick) or "2.5"
            out["overUnder"] = {
                "line": line,
                "over": (over_pick or {}).get("odd"),
                "under": (under_pick or {}).get("odd"),
                "bookmaker": book.get("name"),
            }

    # Both Teams To Score
    hit = _pick_bookmaker(books, RX_BTTS)
    if hit:
        book, bet = hit
        vals = bet.get("values") or []
        yes = next((v.get("odd") for v in vals if _val_eq("Yes")(v)), None)
        no  = next((v.get("odd") for v in vals if _val_eq("No")(v)), None)
        if yes or no:
            out["btts"] = {"yes": yes, "no": no, "bookmaker": book.get("name")}

    # Handicap (Home -1, Away +1)
    hit = _pick_bookmaker(books, RX_HANDICAP)
    if hit:
        book, bet = hit
        vals = bet.get("values") or []
        home_m1 = next((v.get("odd") for v in vals if (re.search(r"home", str(v.get("value","")), re.I) and re.search(r"-?1(?:\.0)?$", str(v.get("handicap") or v.get("value","")))) or re.match(r"(?i)^home\s*-1(?:\.0)?$", str(v.get("value","")))), None)
        away_p1 = next((v.get("odd") for v in vals if (re.search(r"away", str(v.get("value","")), re.I) and re.search(r"^\+?1(?:\.0)?$", str(v.get("handicap") or v.get("value","")))) or re.match(r"(?i)^away\s*\+1(?:\.0)?$", str(v.get("value","")))), None)
        if home_m1 or away_p1:
            out["handicap"] = {"homeMinus1": home_m1, "awayPlus1": away_p1, "bookmaker": book.get("name")}

    # First Half Winner (1X2 HT)
    hit = _pick_bookmaker(books, RX_FIRST_HALF_WIN)
    if hit:
        book, bet = hit
        vals = bet.get("values") or []
        home = next((v.get("odd") for v in vals if _val_eq("Home")(v) or _val_eq("1")(v)), None)
        draw = next((v.get("odd") for v in vals if _val_eq("Draw")(v) or _val_eq("X")(v)), None)
        away = next((v.get("odd") for v in vals if _val_eq("Away")(v) or _val_eq("2")(v)), None)
        if home or draw or away:
            out["firstHalfWinner"] = {"home": home, "draw": draw, "away": away, "bookmaker": book.get("name")}

    return out

# --- Persistência (snapshot + retenção) -----------------------------------------------

def save_snapshot(out: Dict[str, Any], out_dir: str, gzip_only: bool = True) -> str:
    """
    Grava:
      - snapshot diário em {out_dir}/YYYY/MM/DD.json.gz (sempre)
      - latest.json (não compactado) para consumo fácil pelo LLM
    Retorna o caminho do .json.gz.
    """
    date_str = out["date"]  # "YYYY-MM-DD"
    year = date_str[0:4]
    month = date_str[5:7]

    # paths
    dir_ym = os.path.join(out_dir, year, month)
    os.makedirs(dir_ym, exist_ok=True)

    gz_path = os.path.join(dir_ym, f"{date_str}.json.gz")
    latest_path = os.path.join(out_dir, "latest.json")
    os.makedirs(out_dir, exist_ok=True)

    # grava gzip
    with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
        json.dump(out, gz, ensure_ascii=False)

    # grava latest.json (não compactado) para fácil leitura
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    return gz_path

def prune_old_snapshots(out_dir: str, retention_days: int) -> List[str]:
    """
    Remove arquivos *.json.gz em out_dir com mtime mais antigo que retention_days.
    Não remove 'latest.json'.
    Retorna lista de caminhos removidos.
    """
    removed: List[str] = []
    if retention_days <= 0:
        return removed

    cutoff = time.time() - (retention_days * 86400)
    for root, _, files in os.walk(out_dir):
        for fn in files:
            if fn == "latest.json":
                continue
            if not fn.endswith(".json.gz"):
                # Só o arquivamento é .json.gz; ignoramos outros artefatos
                continue
            fp = os.path.join(root, fn)
            try:
                st = os.stat(fp)
                if st.st_mtime < cutoff:
                    os.remove(fp)
                    removed.append(fp)
            except FileNotFoundError:
                pass
            except Exception as e:
                print(f"warn: falha ao remover {fp}: {e}")
    return removed

# --- Pipeline principal ---------------------------------------------------------------

def main() -> None:
    if not API_KEY or API_KEY == "SUA_CHAVE_AQUI":
        raise SystemExit("Defina a variável de ambiente APISPORTS_KEY com sua chave da API-FOOTBALL.")

    date_ymd = today_ymd("America/Sao_Paulo")

    # (Opcional) 1x /odds/bets — mantém robustez contra variações de nome
    _ = get_bets_catalog()

    # 5x fixtures
    fixtures_all: List[Dict[str, Any]] = []
    for lid in LEAGUES.values():
        fixtures_all += get_fixtures_for_league(lid, date_ymd)

    # Index por fixtureId
    by_fixture: Dict[int, Dict[str, Any]] = {}
    for f in fixtures_all:
        fixture = f.get("fixture", {})
        league  = f.get("league", {})
        teams   = f.get("teams", {})
        fid     = int(fixture.get("id"))
        dt      = fixture.get("date")
        status  = (fixture.get("status") or {}).get("short", "")
        # normaliza data para o fuso de SP, se possível
        try:
            dt_parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
            dt_sp = dt_parsed.astimezone(_tz("America/Sao_Paulo"))
            dt_str = dt_sp.strftime("%Y-%m-%d %H:%M:%S %z")
        except Exception:
            dt_str = str(dt)

        by_fixture[fid] = {
            "fixtureId": fid,
            "date": dt_str,
            "status": status,
            "leagueId": int(league.get("id")),
            "league": f"{league.get('country','') or ''} {league.get('name','')}".strip(),
            "home": (teams.get("home") or {}).get("name"),
            "away": (teams.get("away") or {}).get("name"),
            "markets": {},
        }

    # 5x odds (sem filtro de bet -> todos mercados)
    for lid in LEAGUES.values():
        items = get_odds_for_league_date(lid, date_ymd)
        for it in items:
            fid = int((it.get("fixture") or {}).get("id", 0))
            if fid == 0 or fid not in by_fixture:
                continue
            books = (it.get("bookmakers") or [])
            markets = extract_markets(books)
            if markets:
                by_fixture[fid]["markets"].update({k: v for k, v in markets.items() if v})

    # Resultado final
    items_out = sorted(by_fixture.values(), key=lambda r: (r["date"], r["leagueId"], r["fixtureId"]))
    out: Dict[str, Any] = {"date": date_ymd, "count": len(items_out), "items": items_out}

    # Persistência com compactação + latest.json
    gz_path = save_snapshot(out, OUT_DIR, gzip_only=True)

    # Retenção
    removed = prune_old_snapshots(OUT_DIR, RETENTION_DAYS)

    # Logs simples e JSON no stdout
    meta = {
        "saved_gzip": gz_path,
        "removed_count": len(removed),
        "retention_days": RETENTION_DAYS,
        "out_dir": OUT_DIR,
    }
    print(json.dumps({"meta": meta, "snapshot": out}, ensure_ascii=False))

if __name__ == "__main__":
    main()
