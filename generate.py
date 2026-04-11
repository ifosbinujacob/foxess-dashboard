"""
Fox ESS Solar Dashboard — Data Generator
Runs via GitHub Actions at 7 AM and 5 PM Sydney time.
Writes data.json (displayed by index.html) and state.json (calibration persistence).
"""
import requests, hashlib, time, json, os, statistics, re
from datetime import datetime, timedelta
import pytz

# ── credentials (from GitHub Secrets) ────────────────────────────────────────
API_KEY  = os.environ["FOXESS_API_KEY"]
SN       = os.environ["FOXESS_SN"]
BASE_URL = "https://www.foxesscloud.com"
UA       = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
LAT, LON = -33.718, 150.873
TZ       = pytz.timezone("Australia/Sydney")
_SEP     = r"\r\n"

# ── system specs ──────────────────────────────────────────────────────────────
CAPACITY_KWH     = 42.0
MAX_DISCHARGE_KW = 10.0
WINDOW_H         = 2.0
MAX_EXPORT_KWH   = MAX_DISCHARGE_KW * WINDOW_H
PR_PANEL         = 0.87
ARRAYS           = [("North", 5.61, 180), ("South", 5.70, 0), ("West", 1.65, 90)]
FEEDIN_RATE      = 0.45
GRID_RATE        = 0.341
BOM_GH           = "r65231t"

STATE_FILE = "state.json"
DATA_FILE  = "data.json"

# ── API helpers ───────────────────────────────────────────────────────────────
def op_hdrs(path):
    ts  = str(round(time.time() * 1000))
    sig = hashlib.md5((path + _SEP + API_KEY + _SEP + ts).encode("UTF-8")).hexdigest()
    return {"Token": API_KEY, "Timestamp": ts, "Signature": sig, "Lang": "en",
            "Timezone": "Australia/Sydney", "User-Agent": UA, "Content-Type": "application/json"}

def get_hourly(date):
    path = "/op/v0/device/report/query"
    body = {"sn": SN, "year": date.year, "month": date.month, "day": date.day,
            "type": "day", "dimension": "day",
            "variables": ["loads", "gridConsumption", "feedin",
                          "chargeEnergyToTal", "dischargeEnergyToTal"]}
    try:
        r = requests.post(BASE_URL + path, headers=op_hdrs(path), json=body, timeout=15).json()
        if r.get("errno") != 0:
            return {}
        result = {}
        for v in r.get("result", []):
            if isinstance(v, dict):
                vals = v.get("values") or v.get("data") or v.get("datas") or []
                result[v["variable"]] = [float(x or 0) for x in vals]
        return result
    except Exception:
        return {}

def safe(data, key, h):
    arr = data.get(key, [])
    return float(arr[h]) if h < len(arr) else 0.0

def fetch_pv_dc(d):
    t0   = int(datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ).timestamp() * 1000)
    t1   = int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=TZ).timestamp() * 1000)
    path = "/op/v0/device/history/query"
    try:
        r = requests.post(BASE_URL + path, headers=op_hdrs(path),
            json={"sn": SN, "variables": ["pvPower"], "begin": t0, "end": t1},
            timeout=15).json()
        if r.get("errno") != 0:
            return None
        pts = r["result"][0]["datas"][0]["data"]
        parsed = []
        for pt in pts:
            try:
                clean = re.sub(r'\s+[A-Z]+[\+\-]\d+$', '', pt["time"])
                t = TZ.localize(datetime.strptime(clean, "%Y-%m-%d %H:%M:%S"))
                parsed.append((t, float(pt["value"] or 0)))
            except Exception:
                pass
        parsed.sort(key=lambda x: x[0])
        if len(parsed) < 10:
            return None
        return round(sum((parsed[i][1] + parsed[i-1][1]) / 2 *
                         (parsed[i][0] - parsed[i-1][0]).total_seconds() / 3600
                         for i in range(1, len(parsed))), 2)
    except Exception:
        return None

def fetch_feedin_window(d):
    t0           = int(datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ).timestamp() * 1000)
    t1           = int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=TZ).timestamp() * 1000)
    window_start = TZ.localize(datetime(d.year, d.month, d.day, 17, 30, 0))
    window_end   = TZ.localize(datetime(d.year, d.month, d.day, 19, 30, 0))
    path         = "/op/v0/device/history/query"
    try:
        r = requests.post(BASE_URL + path, headers=op_hdrs(path),
            json={"sn": SN, "variables": ["feedinPower"], "begin": t0, "end": t1},
            timeout=15).json()
        if r.get("errno") != 0:
            return None
        pts = r["result"][0]["datas"][0]["data"]
        parsed = []
        for pt in pts:
            try:
                clean = re.sub(r'\s+[A-Z]+[\+\-]\d+$', '', pt["time"])
                t = TZ.localize(datetime.strptime(clean, "%Y-%m-%d %H:%M:%S"))
                parsed.append((t, float(pt["value"] or 0)))
            except Exception:
                pass
        parsed.sort(key=lambda x: x[0])
        win_pts = [(t, v) for t, v in parsed if window_start <= t <= window_end]
        if len(win_pts) < 2:
            return None
        return round(sum((win_pts[i][1] + win_pts[i-1][1]) / 2 *
                         (win_pts[i][0] - win_pts[i-1][0]).total_seconds() / 3600
                         for i in range(1, len(win_pts))), 2)
    except Exception:
        return None

# ── main ──────────────────────────────────────────────────────────────────────
now       = datetime.now(TZ)
today_str = now.strftime("%Y-%m-%d")
yesterday = now - timedelta(days=1)
ydate     = yesterday.strftime("%Y-%m-%d")
month     = now.month
run_type  = "morning" if now.hour < 12 else "evening"

state = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}

# ── 1. Live SOC ───────────────────────────────────────────────────────────────
live_pv_kw = live_load_kw = live_bat_kw = 0.0
current_soc = 50.0
soc_source  = "fallback"
try:
    path = "/op/v1/device/real/query"
    r = requests.post(BASE_URL + path, headers=op_hdrs(path),
        json={"sn": SN, "sns": [SN],
              "variables": ["SoC","SoC_1","pvPower","loadsPower","batChargePower","batDischargePower"]},
        timeout=15).json()
    if r.get("errno", -1) != 0:
        raise ValueError(r.get("msg"))
    vals        = {d["variable"]: float(d["value"]) for d in r["result"][0]["datas"]}
    current_soc = vals.get("SoC_1") or vals.get("SoC") or 50.0
    live_pv_kw  = vals.get("pvPower", 0)
    live_load_kw= vals.get("loadsPower", 0)
    live_bat_kw = vals.get("batChargePower", 0) - vals.get("batDischargePower", 0)
    soc_source  = "live"
except Exception as e:
    soc_source = f"fallback: {e}"

current_bat_kwh = round(current_soc * CAPACITY_KWH / 100, 1)

# ── 2. Yesterday's data ───────────────────────────────────────────────────────
ydata = get_hourly(yesterday)
tdata = get_hourly(now)

y_gen_dc = fetch_pv_dc(yesterday)
y_gen    = y_gen_dc if y_gen_dc is not None else sum(
    max(0, safe(ydata,"feedin",h) + safe(ydata,"loads",h) +
        safe(ydata,"chargeEnergyToTal",h) - safe(ydata,"dischargeEnergyToTal",h) -
        safe(ydata,"gridConsumption",h)) for h in range(24))

y_feedin_dc = fetch_feedin_window(yesterday)
y_feedin = y_feedin_dc if y_feedin_dc is not None else (
    safe(ydata,"feedin",17)*0.5 + safe(ydata,"feedin",18)*1.0 + safe(ydata,"feedin",19)*0.5)

y_load = sum(safe(ydata,"loads",h) for h in range(24))
y_grid = sum(safe(ydata,"gridConsumption",h) for h in range(24))
y_earn = round(min(MAX_EXPORT_KWH, y_feedin) * FEEDIN_RATE, 2)
y_cost = round(y_grid * GRID_RATE, 2)
y_net  = round(y_earn - y_cost, 2)
y_self = round(max(0, (y_load - y_grid) / max(y_load, 0.1) * 100), 1)

# ── 3. Today's generation so far ─────────────────────────────────────────────
today_gen_dc = fetch_pv_dc(now)
today_gen    = today_gen_dc if today_gen_dc is not None else sum(
    max(0, safe(tdata,"feedin",h) + safe(tdata,"loads",h) +
        safe(tdata,"chargeEnergyToTal",h) - safe(tdata,"dischargeEnergyToTal",h) -
        safe(tdata,"gridConsumption",h)) for h in range(min(now.hour+1, 24)))

# ── 4. Weather calibration update ────────────────────────────────────────────
cal_state   = state.setdefault("weather_calibration", {"history": [], "rolling_factor": 1.0})
WEATHER_CAL = cal_state.get("rolling_factor", 1.0)
model_pred  = state.get("today_icon_pred_kwh", 0)
cal_note    = ""

if run_type == "evening" and model_pred > 5.0 and today_gen > 2.0:
    factor = round(today_gen / model_pred, 3)
    existing = next((e for e in cal_state["history"] if e["date"] == today_str), None)
    if existing:
        existing.update({"actual_kwh": round(today_gen, 1), "factor": factor})
    else:
        cal_state["history"].append({"date": today_str,
            "model_pred_kwh": round(model_pred, 1),
            "actual_kwh": round(today_gen, 1), "factor": factor})
    cal_state["history"] = cal_state["history"][-30:]
    recent = [e["factor"] for e in cal_state["history"][-7:]]
    WEATHER_CAL = round(max(0.7, min(2.0, statistics.mean(recent))), 3)
    cal_state["rolling_factor"] = WEATHER_CAL
    cal_note = f"Cal updated: {model_pred:.1f}→{today_gen:.1f} kWh (×{factor:.2f}), rolling ×{WEATHER_CAL:.2f}"
else:
    cal_note = f"Cal ×{WEATHER_CAL:.2f} ({len(cal_state['history'])}d)"

PR_ADJ = state.get("pr_adjustment", 1.0)

# ── 5. Solar forecast (open-meteo with BOM fallback) ─────────────────────────
tomorrow     = (now + timedelta(days=1)).strftime("%Y-%m-%d")
forecast_days_data = {}   # {date: {kwh, window_kwh, cloud, rain, uv, sunset, condition}}
metno_ok     = False
gti_by_date  = {}
cloud_by_date= {}
rain_by_date = {}
sunset_by_date = {}

try:
    for arr_name, kwp, az in ARRAYS:
        r = requests.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": LAT, "longitude": LON, "models": "metno_seamless",
            "hourly": "global_tilted_irradiance,cloud_cover,precipitation_probability",
            "daily": "sunset,uv_index_max", "timezone": "Australia/Sydney",
            "tilt": 22, "azimuth": az, "past_days": 0, "forecast_days": 8
        }, timeout=15)
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}")
        data = r.json()
        for i, t in enumerate(data["hourly"]["time"]):
            date, h = t[:10], int(t[11:13])
            if date not in gti_by_date:
                gti_by_date[date] = {}
                cloud_by_date[date] = {}
                rain_by_date[date] = {}
            if arr_name not in gti_by_date[date]:
                gti_by_date[date][arr_name] = {}
            gti_by_date[date][arr_name][h] = float(data["hourly"]["global_tilted_irradiance"][i] or 0)
            if arr_name == "North":
                cloud_by_date[date][h] = float(data["hourly"]["cloud_cover"][i] or 0)
                rain_by_date[date][h]  = float(data["hourly"]["precipitation_probability"][i] or 0)
        for j, d in enumerate(data["daily"]["time"]):
            if d not in sunset_by_date:
                sd = datetime.fromisoformat(data["daily"]["sunset"][j]).replace(tzinfo=TZ)
                sunset_by_date[d] = {"str": sd.strftime("%H:%M"), "h": sd.hour + sd.minute/60,
                                     "uv": data["daily"]["uv_index_max"][j]}
    metno_ok = True
except Exception:
    pass

BOM_ICON_FRACTION = {
    "sunny":0.90,"clear":0.90,"mostly_sunny":0.78,"partly_cloudy":0.60,
    "mostly_cloudy":0.35,"cloudy":0.20,"hazy":0.65,"light_shower":0.45,
    "shower":0.30,"heavy_shower":0.20,"rain":0.18,"light_rain":0.25,
    "heavy_rain":0.12,"storm":0.15,"fog":0.30,"frost":0.75,"wind":0.70,
}
SEASONAL_MAX = 50.0  # kWh peak autumn day
bom_days_raw = []
bom_today_icon = bom_tmrw_icon = bom_today_uv = bom_tmrw_rain = "?"
try:
    bom_r = requests.get(f"https://api.weather.bom.gov.au/v1/locations/{BOM_GH}/forecasts/daily",
        headers={"User-Agent": UA}, timeout=10).json()
    bom_days_raw = bom_r.get("data", [])
    if bom_days_raw:
        bom_today_icon = bom_days_raw[0].get("icon_descriptor","?")
        bom_today_uv   = bom_days_raw[0].get("uv",{}).get("max_index","?")
        bom_tmrw_rain  = bom_days_raw[1].get("rain",{}).get("chance","?") if len(bom_days_raw)>1 else "?"
        bom_tmrw_icon  = bom_days_raw[1].get("icon_descriptor","?") if len(bom_days_raw)>1 else "?"
except Exception:
    pass

def day_kwh_metno(date):
    total = 0.0
    for arr_name, kwp, az in ARRAYS:
        for h in range(24):
            gv = gti_by_date.get(date, {}).get(arr_name, {}).get(h, 0.0)
            total += (gv / 1000.0) * kwp * PR_PANEL * PR_ADJ * WEATHER_CAL
    return round(total, 1)

def window_kwh_metno(date):
    sh = sunset_by_date.get(date, {}).get("h", 19.5)
    h19_w = min(0.5, max(0, sh - 19.0)) if sh < 19.5 else 0.5
    total = 0.0
    for wh, wt in [(17, 0.5), (18, 1.0), (19, h19_w)]:
        for arr_name, kwp, az in ARRAYS:
            gv = gti_by_date.get(date, {}).get(arr_name, {}).get(wh, 0.0)
            total += (gv / 1000.0) * kwp * PR_PANEL * PR_ADJ * WEATHER_CAL * wt
    return round(total, 2)

# Save today's raw model prediction for evening calibration
if metno_ok:
    raw_today = round(sum(
        (gti_by_date.get(today_str, {}).get(arr_name, {}).get(h, 0) / 1000.0) * kwp * PR_PANEL * PR_ADJ
        for arr_name, kwp, az in ARRAYS for h in range(24)), 1)
    if raw_today > 1.0:
        state["today_icon_pred_kwh"] = raw_today

# Build 7-day forecast array
forecast_list = []
for i in range(1, 8):
    d  = (now + timedelta(days=i))
    ds = d.strftime("%Y-%m-%d")
    day_label = d.strftime("%a %-d %b")

    if metno_ok and ds in gti_by_date:
        solar   = day_kwh_metno(ds)
        window  = window_kwh_metno(ds)
        cloud_vals = [cloud_by_date.get(ds, {}).get(h, 0) for h in range(6, 20)]
        avg_cloud  = round(sum(cloud_vals) / max(len(cloud_vals), 1))
        peak_rain  = round(max((rain_by_date.get(ds, {}).get(h, 0) for h in range(6, 20)), default=0))
        uv         = sunset_by_date.get(ds, {}).get("uv", "?")
        sunset_s   = sunset_by_date.get(ds, {}).get("str", "?")
        source     = "metno"
    else:
        # BOM fallback
        bom_d = bom_days_raw[i] if i < len(bom_days_raw) else {}
        icon  = bom_d.get("icon_descriptor", "partly_cloudy")
        frac  = BOM_ICON_FRACTION.get(icon, 0.5)
        solar = round(SEASONAL_MAX * frac * WEATHER_CAL, 1)
        window= round(solar * 0.22, 2)
        avg_cloud = round((1 - frac) * 100)
        peak_rain = bom_d.get("rain", {}).get("chance", 0) or 0
        uv        = bom_d.get("uv", {}).get("max_index", "?")
        sunset_s  = "~19:00"
        source    = f"bom:{icon}"

    if avg_cloud < 15:    cond = "☀️ Sunny"
    elif avg_cloud < 35:  cond = "🌤 Mostly sunny"
    elif avg_cloud < 60:  cond = "⛅ Partly cloudy"
    elif avg_cloud < 80:  cond = "🌥 Mostly cloudy"
    else:                 cond = "☁️ Overcast"
    if peak_rain > 60:    cond += " 🌧"

    # Max earn = battery (assuming full charge) + PV bonus, capped at 20 kWh
    # In April, sunset ~17:45 so PV contribution during window is minimal (~1-2 kWh)
    # Battery is the main export source: 42 kWh - reserve = up to 20 kWh @ 10 kW
    _reserve       = state.get("recommended_reserve_kwh", 8.0)
    _max_headroom  = max(0, CAPACITY_KWH - _reserve)          # e.g. 34 kWh
    _max_bat_kw    = min(MAX_DISCHARGE_KW, _max_headroom / WINDOW_H)
    _max_bat_kwh   = _max_bat_kw * WINDOW_H                   # e.g. 20 kWh (capped at 10kW×2h)
    _total_if_full = min(MAX_EXPORT_KWH, _max_bat_kwh + window)
    max_earn_if_full = round(_total_if_full * FEEDIN_RATE, 2)
    bat_earn_if_full = round(_max_bat_kwh * FEEDIN_RATE, 2)

    forecast_list.append({
        "date": ds, "label": day_label, "condition": cond,
        "solar_kwh": solar,
        "pv_window_kwh": window,          # solar PV only during 17:30-19:30
        "bat_earn_if_full": bat_earn_if_full,   # battery export earn (if fully charged)
        "max_earn_if_full": max_earn_if_full,   # total earn battery+PV (if fully charged)
        "cloud_pct": avg_cloud, "rain_pct": peak_rain,
        "uv": uv, "sunset": sunset_s, "source": source
    })

# ── 6. Export calculation ─────────────────────────────────────────────────────
new_reserve  = state.get("recommended_reserve_kwh", 8.0)
headroom_kwh = max(0, current_bat_kwh - new_reserve)
discharge_kw = min(MAX_DISCHARGE_KW, headroom_kwh / WINDOW_H if WINDOW_H > 0 else 0)
bat_discharge = discharge_kw * WINDOW_H
est_export   = min(MAX_EXPORT_KWH, bat_discharge)
est_earn     = round(est_export * FEEDIN_RATE, 2)
bat_after    = round(max(0, current_bat_kwh - bat_discharge), 1)
bat_after_soc= round(bat_after / CAPACITY_KWH * 100, 1)
fdpwr_w      = int(discharge_kw * 1000)

# PV bonus in window (tonight)
pv_win_tonight = 0.0
if metno_ok and today_str in gti_by_date:
    sh = sunset_by_date.get(today_str, {}).get("h", 19.5)
    h19_w = min(0.5, max(0, sh - 19.0)) if sh < 19.5 else 0.5
    for wh, wt in [(17, 0.5), (18, 1.0), (19, h19_w)]:
        for arr_name, kwp, az in ARRAYS:
            gv = gti_by_date.get(today_str, {}).get(arr_name, {}).get(wh, 0.0)
            pv_win_tonight += (gv / 1000.0) * kwp * PR_PANEL * PR_ADJ * WEATHER_CAL * wt
pv_win_tonight = round(pv_win_tonight, 2)
upside_export  = min(MAX_EXPORT_KWH, est_export + pv_win_tonight)
upside_earn    = round(upside_export * FEEDIN_RATE, 2)

# ── 7. Overnight reserve & learning ──────────────────────────────────────────
overnight_load = (sum(safe(ydata,"loads",h) for h in range(20,24)) +
                  sum(safe(tdata,"loads",h) for h in range(0,7)))
overnight_grid = (sum(safe(ydata,"gridConsumption",h) for h in range(20,24)) +
                  sum(safe(tdata,"gridConsumption",h) for h in range(0,7)))

SEASONAL_PROFILES = {
    1:(5.0,1.2,"Summer"),2:(5.0,1.2,"Summer"),3:(4.5,1.5,"Autumn"),
    4:(4.5,1.5,"Autumn"),5:(5.5,2.0,"Late autumn"),6:(7.5,2.5,"Winter"),
    7:(8.0,2.5,"Peak winter"),8:(7.5,2.2,"Late winter"),9:(5.5,1.8,"Spring"),
    10:(4.5,1.5,"Spring"),11:(4.5,1.2,"Late spring"),12:(5.0,1.2,"Summer"),
}
season = SEASONAL_PROFILES[month]

overnight_h = state.setdefault("overnight", {"history": [], "safety_margin_kwh": season[1]})
overnight_h["history"].append({"date": ydate, "month": yesterday.month,
    "consumption_kwh": round(overnight_load, 2), "grid_used_kwh": round(overnight_grid, 2)})
overnight_h["history"] = overnight_h["history"][-90:]
season_hist = [e["consumption_kwh"] for e in overnight_h["history"]
               if abs(e.get("month", month) - month) <= 1 or abs(e.get("month", month) - month) >= 11]
if len(season_hist) >= 3:
    p90  = sorted(season_hist)[int(len(season_hist) * 0.9)]
    avg_ = statistics.mean(season_hist)
    new_reserve = round(max(6.0, min(22.0, p90 + overnight_h.get("safety_margin_kwh", season[1]))), 1)
    state["recommended_reserve_kwh"] = new_reserve

# Financials running total
fin = state.setdefault("financials", {"total_earned": 0.0, "total_grid_cost": 0.0, "history": []})
fin["history"].append({"date": ydate, "earn": y_earn, "cost": y_cost,
    "gen_kwh": round(y_gen, 2), "feedin_kwh": round(y_feedin, 2)})
fin["history"] = fin["history"][-60:]
fin["total_earned"]    = round(sum(e["earn"] for e in fin["history"]), 2)
fin["total_grid_cost"] = round(sum(e["cost"] for e in fin["history"]), 2)

# ── 8. Save state ─────────────────────────────────────────────────────────────
state["weather_calibration"] = cal_state
json.dump(state, open(STATE_FILE, "w"), indent=2)

# ── 9. Write data.json ────────────────────────────────────────────────────────
data_out = {
    "generated_at": now.isoformat(),
    "run_type": run_type,
    "soc": {
        "pct": round(current_soc, 1),
        "kwh": current_bat_kwh,
        "source": soc_source,
        "live_pv_kw": round(live_pv_kw, 2),
        "live_load_kw": round(live_load_kw, 2),
        "live_bat_kw": round(live_bat_kw, 2),
    },
    "export": {
        "headroom_kwh": round(headroom_kwh, 1),
        "reserve_kwh": new_reserve,
        "est_export_kwh": round(est_export, 1),
        "est_earn": est_earn,
        "fdpwr_w": fdpwr_w,
        "bat_after_pct": bat_after_soc,
        "bat_after_kwh": bat_after,
        "pv_bonus_kwh": pv_win_tonight,
        "upside_earn": upside_earn,
    },
    "yesterday": {
        "date": ydate,
        "gen_kwh": round(y_gen, 1),
        "load_kwh": round(y_load, 1),
        "grid_kwh": round(y_grid, 2),
        "feedin_kwh": round(y_feedin, 2),
        "earn": y_earn,
        "cost": y_cost,
        "net": y_net,
        "self_suff_pct": y_self,
    },
    "today": {
        "date": today_str,
        "gen_kwh_so_far": round(today_gen, 1),
    },
    "overnight": {
        "load_kwh": round(overnight_load, 2),
        "grid_kwh": round(overnight_grid, 2),
        "grid_free": overnight_grid <= 0.1,
    },
    "calibration": {
        "factor": WEATHER_CAL,
        "days": len(cal_state["history"]),
        "note": cal_note,
    },
    "bom": {
        "today_icon": bom_today_icon,
        "today_uv": bom_today_uv,
        "tmrw_icon": bom_tmrw_icon,
        "tmrw_rain": bom_tmrw_rain,
    },
    "forecast": forecast_list,
    "financials": {
        "total_earned": fin["total_earned"],
        "total_grid_cost": fin["total_grid_cost"],
        "net": round(fin["total_earned"] - fin["total_grid_cost"], 2),
        "days": len(fin["history"]),
    },
    "season": season[2],

    # ── Historical data (last 60 days) for charts ─────────────────────────────
    "history": {
        # Daily financials — earn, grid cost, net, solar generation
        "daily": [
            {
                "date":     e["date"],
                "earn":     round(e.get("earn", 0), 2),
                "cost":     round(e.get("cost", 0), 2),
                "net":      round(e.get("earn", 0) - e.get("cost", 0), 2),
                "gen_kwh":  round(e.get("gen_kwh", 0), 1),
                "feedin_kwh": round(e.get("feedin_kwh", 0), 2),
            }
            for e in fin["history"][-60:]
        ],

        # Monthly rollups — group daily history by YYYY-MM
        "monthly": (lambda rows: [
            {
                "month":    m,
                "earn":     round(sum(r["earn"] for r in rows if r["date"][:7] == m), 2),
                "cost":     round(sum(r["cost"] for r in rows if r["date"][:7] == m), 2),
                "net":      round(sum(r["earn"] - r["cost"] for r in rows if r["date"][:7] == m), 2),
                "gen_kwh":  round(sum(r["gen_kwh"] for r in rows if r["date"][:7] == m), 1),
                "days":     sum(1 for r in rows if r["date"][:7] == m),
            }
            for m in sorted(set(r["date"][:7] for r in rows))
        ])(fin["history"][-60:]),

        # Overnight consumption — spot heater/AC spikes
        "overnight": [
            {
                "date":            e["date"],
                "consumption_kwh": round(e.get("consumption_kwh", 0), 2),
                "grid_used_kwh":   round(e.get("grid_used_kwh", 0), 2),
                "grid_free":       e.get("grid_used_kwh", 0) <= 0.1,
            }
            for e in overnight_h["history"][-60:]
        ],

        # Weather calibration accuracy
        "calibration": [
            {
                "date":           e["date"],
                "model_pred_kwh": round(e.get("model_pred_kwh", 0), 1),
                "actual_kwh":     round(e.get("actual_kwh", 0), 1),
                "factor":         round(e.get("factor", 1.0), 3),
            }
            for e in cal_state["history"][-30:]
        ],
    },
}
json.dump(data_out, open(DATA_FILE, "w"), indent=2)
print(f"✅  data.json written — {run_type} run — SOC {current_soc:.0f}% — "
      f"Export {est_export:.1f} kWh → ${est_earn:.2f}")
