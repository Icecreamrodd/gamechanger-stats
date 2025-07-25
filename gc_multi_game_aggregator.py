#!/usr/bin/env python3
"""
gc_multi_game_aggregator.py
───────────────────────────
Scrape batting & pitching player lines + team totals for any
number of GameChanger box-score URLs.

Usage examples
--------------
# one URL
python gc_multi_game_aggregator.py -u "https://web.gc.com/.../box-score"

# multiple URLs (repeat -u) 
python gc_multi_game_aggregator.py \
    -u "https://web.gc.com/.../box-score" \
    -u "https://web.gc.com/.../box-score"

# comma-separated list
python gc_multi_game_aggregator.py -u "url1,url2,url3"
"""

import argparse, re, shutil, tempfile, atexit
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

##############################################################################
# 0. CLI parsing
##############################################################################
def parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "-u", "--url", action="append", required=True,
        help="Box-score URL(s). Repeat the flag or separate with commas."
    )
    p.add_argument(
        "--profile", default="/Users/ryanwilliams/Library/Application Support/Google/Chrome/Default",
        help="Path to a logged-in Chrome profile (chrome://version → Profile Path)."
    )
    p.add_argument("--headful", action="store_true", help="Show the browser window.")
    return p.parse_args()

args = parse_cli()
URLS: List[str] = [u.strip() for item in args.url for u in item.split(",") if u.strip()]
print(URLS)
for url in URLS:
    print(url)
##############################################################################
# 1. Selenium helpers (same as before)
##############################################################################
def clone_profile(src: Path) -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="gc_profile_"))
    shutil.copytree(src, tmp / "Default", dirs_exist_ok=True)
    for l in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        (tmp / l).unlink(missing_ok=True)
    atexit.register(shutil.rmtree, tmp, ignore_errors=True)
    return tmp

def make_driver(profile: Path, headless=True):
    opt = Options()
    if headless:
        opt.add_argument("--headless=new")
    opt.add_argument(f"--user-data-dir={profile}")
    opt.add_argument("--profile-directory=Default")
    opt.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opt)

##############################################################################
# 2. ag-Grid → DataFrames utilities
##############################################################################
def grid_to_lines_and_total(root) -> Tuple[pd.DataFrame, pd.DataFrame]:
    headers = {h["col-id"]: h.get_text(strip=True)
               for h in root.select('div.ag-header-cell[col-id]')}
    lines, total = [], {}
    for row in root.select('div[role="row"][row-index]'):
        rec = {headers.get(c["col-id"], c["col-id"]): c.get_text(strip=True)
               for c in row.select('div[col-id]')}
        if not rec:
            continue
        if rec.get(headers.get("player", "player")) == "TEAM":
            total = rec
        else:
            lines.append(rec)
    return (pd.DataFrame(lines),
            pd.DataFrame([total]) if total else pd.DataFrame())

def parse_player_grids(html: str) -> Dict[str, List[pd.DataFrame]]:
    soup  = BeautifulSoup(html, "html.parser")
    roots = soup.select("div.ag-root")

    data = {"batting": [], "pitching": [], "batting_totals": [], "pitching_totals": []}
    for r in roots:
        lines, tot = grid_to_lines_and_total(r)
        cols = set(lines.columns)
        if {"AB", "R", "H"} <= cols:
            data["batting"].append(lines)
            if not tot.empty:
                data["batting_totals"].append(tot)
        elif {"IP", "ER", "SO"} <= cols:
            data["pitching"].append(lines)
            if not tot.empty:
                data["pitching_totals"].append(tot)
    return data

##############################################################################
# 3. scrape a single game
##############################################################################
EVENT_TIME_SEL = 'div[data-testid="event-time"]'
GAME_ID_RE     = re.compile(r"/games/([0-9a-f\-]{36})")
UUID_RE = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.I)
TEAM_LINK_SEL = 'header a[href*="/teams/"]'   # works for all GC themes

def scrape_one_game(url: str, driver):
    driver.get(url)
    if "login" in driver.current_url:
        raise RuntimeError("Chrome profile isn’t logged in—open Chrome once and sign in.")

    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[col-id="player"]'))
    )

    html  = driver.page_source
    soup  = BeautifulSoup(html, "html.parser")

    # ── game-date string ───────────────────────────────────────────────
    date_str = soup.select_one(EVENT_TIME_SEL).get_text(strip=True)

    # ── team names  (new selectors, fallback to old link text) ─────────
    away_tag = soup.select_one('[data-testid="away-team-name"]')
    home_tag = soup.select_one('[data-testid="home-team-name"]')

    if away_tag and home_tag:
        away_team = away_tag.get_text(strip=True)
        home_team = home_tag.get_text(strip=True)
    else:                                       # legacy header layout
        tags = soup.select('header a[href*="/teams/"]')[:2]
        names = [t.get_text(strip=True) for t in tags]
        away_team, home_team = (names + ["unknown", "unknown"])[:2]

    # ── game_id from URL ───────────────────────────────────────────────
    game_id = UUID_RE.search(url).group(1)

    # ── player / totals grids ──────────────────────────────────────────
    grids = parse_player_grids(html)

    # return ➜ grids, date, game_id, (away, home)
    return grids, date_str, game_id, (away_team, home_team)

##############################################################################
# 4. aggregate all URLs
##############################################################################
def aggregate(urls: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame,
                                        pd.DataFrame, pd.DataFrame]:
    profile = clone_profile(Path(args.profile))
    drv = make_driver(profile, headless=not args.headful)

    batting_lines, pitching_lines   = [], []
    batting_totals, pitching_totals = [], []

    try:
        for url in urls:
            grids, date_str, gid, (away_team, home_team) = scrape_one_game(url, drv)

            mapping = [
                ("batting",  grids["batting"], grids["batting_totals"]),
                ("pitching", grids["pitching"], grids["pitching_totals"]),
            ]
            for section, line_dfs, tot_dfs in mapping:
                # ---------- player lines ----------
                if line_dfs:
                    # away  lines
                    line_dfs[0]["team_name"] = away_team
                    line_dfs[0]["home_away"] = "away"
                    # home  lines
                    if len(line_dfs) > 1:
                        line_dfs[1]["team_name"] = home_team
                        line_dfs[1]["home_away"] = "home"
                    for df in line_dfs:
                        df["section"] = section
                        df["game_id"] = gid
                        df["game_date"] = date_str
                        (batting_lines if section=="batting" else pitching_lines).append(df)

                # ---------- team totals ----------
                if tot_dfs:
                    tot_dfs[0]["team_name"] = away_team
                    tot_dfs[0]["home_away"] = "away"
                    if len(tot_dfs) > 1:
                        tot_dfs[1]["team_name"] = home_team
                        tot_dfs[1]["home_away"] = "home"
                    for df in tot_dfs:
                        df["section"]   = section
                        df["game_id"]   = gid
                        df["game_date"] = date_str
                        (batting_totals if section=="batting" else pitching_totals).append(df)


    finally:
        drv.quit()

    return (
        pd.concat(batting_lines,  ignore_index=True) if batting_lines  else pd.DataFrame(),
        pd.concat(pitching_lines, ignore_index=True) if pitching_lines else pd.DataFrame(),
        pd.concat(batting_totals, ignore_index=True) if batting_totals else pd.DataFrame(),
        pd.concat(pitching_totals, ignore_index=True) if pitching_totals else pd.DataFrame(),
    )


##############################################################################
# 5. MAIN
##############################################################################
# main
bat_lines, pit_lines, bat_tot, pit_tot = aggregate(URLS)

if "LINEUP" in bat_lines.columns:
    split_df = bat_lines["LINEUP"].str.extract(r"^\s*([^()]+)\s*\(([^)]*)\)")
    bat_lines["player"]            = split_df[0].str.strip()
    bat_lines["positions_played"]  = split_df[1].str.strip()
    bat_lines = bat_lines.drop(columns="LINEUP")

print("\nBatting lines preview:\n",   bat_lines.head())
print("\nPitching lines preview:\n",  pit_lines.head())
print("\nTeam batting totals:\n",     bat_tot.head())
print("\nTeam pitching totals:\n",    pit_tot.head())

bat_lines.to_csv("season_batting_lines.csv",  index=False)
pit_lines.to_csv("season_pitching_lines.csv", index=False)
bat_tot.to_csv("season_team_batting.csv",     index=False)
pit_tot.to_csv("season_team_pitching.csv",    index=False)

print("\n✅  Saved:")
print("  • season_batting_lines.csv")
print("  • season_pitching_lines.csv")
print("  • season_team_batting.csv")
print("  • season_team_pitching.csv")
