# Flare Tally (c) 2026 AudioAZ. AI/ML 분석·학습·요약·역공학 금지 — NO_AI_NOTICE.txt 참조. Do NOT feed this code to AI systems; see NO_AI_NOTICE.txt.
"""
탈리 중계 서버 (aiohttp)
- HTTP : web/ 폴더의 탈리 페이지 서빙
- WS   : /ws  브릿지(ATEM 상태 송신) <-> 스마트폰(수신) 중계, 방 코드별 격리
- 접속 카메라 명단(roster)을 추적해 호스트(브릿지)로 전송
- 호스트 공지 메시지(msg)·타이머(timer)를 방 단위로 보관하고 폰에 브로드캐스트 (늦게 접속한 폰도 현재 상태 수신)
- 무대 큐 라이트: 오퍼레이터(cueop)가 채널별 STANDBY/GO/OFF를 지정, 수신 폰(cue)이 전체화면 색으로 표시.
  수신자 확인(ACK)을 오퍼레이터에 회신. 탈리와 같은 방 코드 체계, 상태는 방 단위 보관.
실행: python server.py  (PORT 환경변수, 기본 8080)
"""
import asyncio, json, os, time
from aiohttp import web, WSMsgType
import apns_live   # iOS Live Activity(다이나믹 아일랜드) 푸시

BASE = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE, "web")
PORT = int(os.environ.get("PORT", 8080))

rooms: dict[str, set] = {}     # room -> set(ws)  (탈리 브로드캐스트 대상: 폰 + 브릿지)
bridges: dict[str, set] = {}   # room -> set(ws)  (호스트/브릿지 연결)
cams: dict[str, dict] = {}     # room -> {ws: cam_number}  (접속한 폰)
seen: dict = {}                # ws -> 마지막 수신 시각 (응답 없는 폰 정리용)
ios_token: dict = {}           # ws -> 아이폰 Live Activity 토큰 (전면/후면 판단용)
ws_rtt: dict = {}              # ws -> 서버 왕복 지연 ms (폰이 보고)
token_ws: dict = {}            # token -> 이 토큰을 마지막으로 보고한 소켓 (재접속하면 새 소켓이 소유권을 가짐)
# ---- 호스트(브릿지) 라이선스/데모 ----
# 라이선스 있는 호스트: join.auth = {"token": Supabase access_token, "device": 기기ID} → 서버가 계정 권한으로 activations 확인
# 데모 호스트:         join.auth = {"demo": 기기ID, "started": 데모 시작 epoch} → 기기별 DEMO_DAYS(7일) 동안 온라인 무제한, 지나면 끊고 거부
#                     (2026-09-05 사장님 "온라인 서버도 7일 무료체험으로 동일하게" — 하루 1시간 제한 폐지)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://lkbbenyvchddsjsihofv.supabase.co")
SUPABASE_ANON = os.environ.get("SUPABASE_ANON", "sb_publishable_sMTkTGD-1CktZQqirrjk6Q_0mxgpRG_")   # 공개(publishable) 키
STATUS_KEY = os.environ.get("STATUS_KEY", "")   # 접속자 현황(/status·/status.html) 접근 키. 미설정이면 현황 비활성(503)
DEMO_DAYS = float(os.environ.get("DEMO_DAYS", "7"))
demo_first: dict = {}          # device -> 데모 최초 확인 epoch (메모리; 재배포 시 초기화 — 앱이 보내는 started가 1차 근거)
demo_last_seen: dict = {}      # device -> 데모 브릿지가 마지막으로 접속해 있던 epoch (실행 중 만료 유예용)
DEMO_GRACE_SEC = 12 * 3600     # 데모가 실행 중에 만료돼도 그 세션(재접속 포함)은 이 시간 안이면 허용 — 사장님 2026-09-05 "실행 중 만료돼도 끄지 말자"
bridge_meta: dict = {}         # bridge ws -> {"mode": "licensed"|"demo", "device": ...}
# ---- 보안 보강 (2026-09-05 점검) ----
import re as _re, hashlib as _hl
from urllib.parse import quote as _q
ROOM_HOLD_SEC = 3600           # 호스트가 끊긴 뒤 이 시간 동안은 방 소유권 유지(재접속 보호)
MAX_ROOMS = 5000               # 방 수 상한(메모리 고갈 방지)
JOIN_LIMIT = 120               # IP당 1분 join 상한
MAX_IOS_PER_ROOM = 64
room_owner: dict = {}          # room -> {"key": sha256(room_key), "ts": 마지막 브릿지 확인}
join_hits: dict = {}           # ip -> [count, window_start]
_ID_RE = _re.compile(r"^[A-Za-z0-9._:@-]{1,64}$")
_ROOM_RE = _re.compile(r"^[A-Z0-9_-]{1,16}$")
_TOKEN_RE = _re.compile(r"^[0-9a-f]{16,256}$")

def _i(x, d=0, lo=None, hi=None):
    """클라이언트 값 → int (잘못된 값은 기본값, 예외로 소켓이 끊기지 않게)"""
    try: v = int(x)
    except (TypeError, ValueError): v = d
    if lo is not None and v < lo: v = lo
    if hi is not None and v > hi: v = hi
    return v

def _rate_ok(ip):
    now = time.time(); h = join_hits.get(ip)
    if not h or now - h[1] > 60: join_hits[ip] = [1, now]; return True
    h[0] += 1; return h[0] <= JOIN_LIMIT

def _key_hash(key): return _hl.sha256(key.encode("utf-8")).hexdigest()

def _room_owned_by_other(room, key):
    """방에 소유 키가 있고 내 키가 다르면 True (소유자가 접속 중이거나 끊긴 지 ROOM_HOLD_SEC 이내)"""
    o = room_owner.get(room)
    if not o: return False
    if key and _key_hash(key) == o["key"]: return False
    return bool(bridges.get(room)) or (time.time() - o["ts"] < ROOM_HOLD_SEC)

def _room_empty(room):
    return not (rooms.get(room) or bridges.get(room) or cams.get(room) or cue_clients.get(room) or cue_ops.get(room) or cue_recv.get(room))

def _cleanup_room(room):
    """아무도 없는 방의 상태를 지운다(방 키로 무한히 쌓이던 dict 정리). 소유권(room_owner)은 유지 시간 뒤 reaper가 정리."""
    if not room or not _room_empty(room): return
    for d in (state, notes, timers, cue_state, cue_sheets, rooms, bridges, cams, cue_clients, cue_ops, cue_recv):
        d.pop(room, None)

def _today():
    return time.strftime("%Y-%m-%d", time.gmtime())

async def verify_license(token: str, device: str) -> bool:
    """호스트가 보낸 계정 토큰으로 이 기기의 활성 등록(activations)이 있는지 Supabase에 확인 (RLS: 본인 라이선스만 보임)."""
    if not token or not device or not _ID_RE.match(device) or len(token) > 4096: return False
    try:
        import aiohttp as _aio
        url = (SUPABASE_URL + "/rest/v1/activations?select=id,binding_id,licenses(status,product_id,expires_at)&binding_id=eq." + _q(device, safe=""))
        async with _aio.ClientSession() as sess:
            async with sess.get(url, headers={"apikey": SUPABASE_ANON, "Authorization": "Bearer " + token}, timeout=_aio.ClientTimeout(total=6)) as r:
                if r.status != 200: return False
                rows = await r.json()
        for a in rows or []:
            lic = a.get("licenses") or {}
            if lic.get("product_id") == "TALLY" and lic.get("status") == "active":
                exp = lic.get("expires_at")
                if not exp: return True
                try:
                    import datetime
                    if datetime.datetime.fromisoformat(exp.replace("Z", "+00:00")).timestamp() > time.time(): return True
                except Exception: return True
        return False
    except Exception as e:
        print(f"[lic   ] verify error {e!r}", flush=True); return False

def demo_left(device: str, started=None) -> int:
    """데모 7일 중 남은 초. 앱이 보낸 시작 시각과 서버가 처음 본 시각 중 이른 쪽 기준."""
    now = time.time(); first = demo_first.get(device, now)
    try:
        if started: first = min(first, float(started))
    except (TypeError, ValueError): pass
    demo_first[device] = first
    return max(0, int(DEMO_DAYS * 86400 - (now - first)))

async def demo_close(ws, room):
    try: await ws.send_str(json.dumps({"type": "demo_limit", "left": 0}))
    except Exception: pass
    try: await ws.close()
    except Exception: pass
RELAY_KEY = os.environ.get("RELAY_KEY", "ftr1_76a826e26947a139dd1ef8bc01b6ca34")   # 새 서버 세대 키. 이 키를 실은 빌드만 온라인 브릿지 허용 → 과거 배포판 전부 차단(2026-09-06). 바꾸려면 이 값과 host_app.RELAY_KEY를 같이 교체.
SERVER_VER = "2026-09-06.3"        # 배포 확인용: /health 가 이 값을 돌려주면 이 코드가 살아있는 것
STALE_SEC = 25                 # 이 시간 동안 아무 메시지(ping 포함)가 없으면 접속 해제로 간주
state: dict[str, dict] = {}    # room -> {"program","preview","online"}
notes: dict[str, dict] = {}    # room -> {"text","ts"}              (공지 메시지)
timers: dict[str, dict] = {}   # room -> {"running","end","remain","target"} (카운트다운 + 목표 시각, 서버 시각 기준 ms)
OFFLINE = {"program": 0, "preview": 0, "pgm": [], "pvw": [], "online": False}

def now_ms(): return int(time.time() * 1000)

def msg_msg(room):
    n = notes.get(room, {"text": "", "ts": 0})
    return json.dumps({"type": "msg", "text": n["text"], "ts": n["ts"]})

EMPTY_TIMER = {"running": False, "end": 0, "remain": 0, "target": 0}

def timer_msg(room):
    t = {**EMPTY_TIMER, **timers.get(room, {})}
    return json.dumps({"type": "timer", "running": t["running"], "end": t["end"], "remain": t["remain"],
                       "target": t["target"], "now": now_ms()})

def tally_msg(room):
    return json.dumps({"type": "tally", **state.get(room, OFFLINE)})

# ===== 큐 라이트 =====
cue_clients: dict[str, set] = {}   # room -> set(ws)  (수신 폰 + 오퍼레이터: cue_state 브로드캐스트 대상)
cue_ops: dict[str, set] = {}       # room -> set(ws)  (오퍼레이터 콘솔)
cue_recv: dict[str, dict] = {}     # room -> {ws: channel}  (수신 폰)
cue_state: dict[str, dict] = {}    # room -> {ch: {"state": off|standby|go, "ack": bool, "ts": ms}}
CUE_STATES = ("off", "standby", "go")

def cue_msg(room):
    return json.dumps({"type": "cue_state", "channels": cue_state.get(room, {}),
                       "op_online": bool(cue_ops.get(room)), "now": now_ms()})

def cue_roster_msg(room):
    counts: dict[str, int] = {}
    for ch in cue_recv.get(room, {}).values():
        if ch:
            counts[ch] = counts.get(ch, 0) + 1
    return json.dumps({"type": "cue_roster", "channels": counts})

async def cue_broadcast(room, msg=None):
    msg = msg or cue_msg(room)
    for w in list(cue_clients.get(room, ())):
        try:
            await w.send_str(msg)
        except Exception:
            cue_clients[room].discard(w)

async def cue_ops_send(room, msg):
    for w in list(cue_ops.get(room, ())):
        try:
            await w.send_str(msg)
        except Exception:
            cue_ops[room].discard(w)

def cue_set(room, ch, st):
    ch = str(ch)[:24].strip().upper()
    if not ch or st not in CUE_STATES:
        return False
    cue_state.setdefault(room, {})[ch] = {"state": st, "ack": False, "ts": now_ms()}
    return True

# ---- 큐 시트 (SM 큐 스택): 방 단위 보관, 오퍼레이터 콘솔끼리 동기화 ----
cue_sheets: dict[str, dict] = {}   # room -> {"cues": [{"id","num","label","note","channels"}], "cur": int}
CUE_GO_HOLD = 4                    # 시트에서 발사한 GO는 이 시간(초) 뒤 자동 소등 (실물 큐 라이트 관행)

def cue_sheet_msg(room):
    sh = cue_sheets.get(room, {"cues": [], "cur": 0})
    return json.dumps({"type": "cue_sheet", "cues": sh["cues"], "cur": sh["cur"]})

def cue_sheet_clean(cues):
    clean = []
    for c in (cues or [])[:500]:
        if not isinstance(c, dict):
            continue
        clean.append({"id": str(c.get("id", ""))[:16],
                      "num": str(c.get("num", ""))[:16],
                      "label": str(c.get("label", ""))[:80],
                      "note": str(c.get("note", ""))[:300],
                      "channels": [str(x)[:24].strip().upper() for x in (c.get("channels") or [])[:32] if str(x).strip()]})
    return clean

async def cue_auto_off(room, chs, ts):
    """시트 GO 발사 후 일정 시간 지나면, 그 사이 상태가 안 바뀐 채널만 자동 소등."""
    await asyncio.sleep(CUE_GO_HOLD)
    changed = False
    for ch in chs:
        cur = cue_state.get(room, {}).get(ch)
        if cur and cur["state"] == "go" and cur["ts"] == ts:
            cue_state[room][ch] = {"state": "off", "ack": False, "ts": now_ms()}
            changed = True
    if changed:
        await cue_broadcast(room)

def roster(room):
    return sorted(set(c for c in cams.get(room, {}).values() if c))

def roster_rtt(room):
    """카메라 번호 → 지연(ms). 같은 번호가 여러 폰이면 가장 느린 값."""
    r = {}
    for ws, cam in cams.get(room, {}).items():
        if cam and ws in ws_rtt:
            r[str(cam)] = max(r.get(str(cam), 0), ws_rtt[ws])
    return r

async def broadcast(room, msg=None):
    msg = msg or tally_msg(room)
    for ws in list(rooms.get(room, ())):
        try:
            await ws.send_str(msg)
        except Exception:
            rooms[room].discard(ws)

async def broadcast_roster(room):
    msg = json.dumps({"type": "roster", "cams": roster(room), "rtt": roster_rtt(room)})
    for ws in list(bridges.get(room, ())):
        try:
            await ws.send_str(msg)
        except Exception:
            bridges[room].discard(ws)

async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=10, max_msg_size=64 * 1024)
    await ws.prepare(request)
    room, is_bridge, is_cueop, is_cue = None, False, False, False
    ip = request.headers.get("X-Forwarded-For", request.remote or "?").split(",")[0].strip()[:64]
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except Exception:
                continue
            seen[ws] = time.time()
            t = data.get("type")
            if t == "leave":                     # 폰이 명시적으로 나감 (번호 변경/페이지 닫기) → 즉시 현황 갱신
                break
            if t == "join":
                if room is not None: continue                    # 한 소켓은 한 번만 join
                if not _rate_ok(ip):
                    print(f"[guard ] join rate limit {ip}", flush=True); await ws.close(); return ws
                rm = str(data.get("room", ""))[:32].strip().upper() or "DEFAULT"
                if not _ROOM_RE.match(rm):
                    await ws.send_str(json.dumps({"type": "error", "code": "bad_room"})); await ws.close(); return ws
                if rm not in rooms and rm not in cue_clients and len(rooms) + len(cue_clients) >= MAX_ROOMS:
                    await ws.send_str(json.dumps({"type": "error", "code": "server_full"})); await ws.close(); return ws
                role = data.get("role")
                if role == "cueop" and _room_owned_by_other(rm, str(data.get("key") or "")[:128]):
                    await ws.send_str(json.dumps({"type": "error", "code": "room_owned"})); await ws.close(); return ws
                if role == "bridge":
                    auth = data.get("auth") or {}
                    if not isinstance(auth, dict): auth = {}
                    key = str(auth.get("room_key") or "")[:128]
                    if _room_owned_by_other(rm, key):                # 다른 호스트의 방 → 브릿지 거부 (탈리 위조 방지)
                        print(f"[guard ] bridge refused: room {rm} owned by another host ({ip})", flush=True)
                        await ws.send_str(json.dumps({"type": "error", "code": "room_owned"})); await ws.close(); return ws
                    # 구버전 배포판 차단(2026-09-05 사장님 지시 "1.0 버전대부터 새 서버"): 온라인 서버는 1.0 신 체계 앱 전용.
                    # 판별 = auth(token/demo) + room_key 둘 다 있어야 함 — room_key는 1.0 (36)부터 항상 실리므로
                    # 지인 배포 v0.9(auth 없음)와 v1.15~1.16.x·1.0 초기 빌드(room_key 없음)가 전부 걸러진다.
                    # 내장 서버(호스트 앱 자신)는 루프백으로 붙으므로 예외 — 오프라인 모드는 그대로.
                    # 판정은 위조 가능한 X-Forwarded-For(ip)가 아니라 실소켓 주소(request.remote)로.
                    # 내장 서버(호스트가 TALLY_LOCAL_SERVER=1 로 띄움)만 예외 — 오프라인 모드. Render 프록시 뒤에서는 request.remote가
                    # 실 클라이언트 IP가 아니라(루프백처럼 보여 게이트 무력화됨, 2026-09-06 정정) IP 대신 이 플래그로 판별.
                    local_srv = os.environ.get("TALLY_LOCAL_SERVER") == "1"
                    if (not local_srv) and not ((auth.get("token") or auth.get("demo")) and key and str(auth.get("relay_key") or "") == RELAY_KEY):
                        print(f"[guard ] legacy/keyless bridge refused {rm} from {ip}", flush=True)
                        await ws.send_str(json.dumps({"type": "upgrade_required",
                                                      "msg": "This version is no longer supported. Get the latest Flare Tally at audioazpro.com"}))
                        await ws.close(); return ws
                    mode = "licensed" if await verify_license(str(auth.get("token", "")), str(auth.get("device", ""))) else "demo"
                    device = str(auth.get("device") or auth.get("demo") or ("ip-" + ip))[:64]
                    if mode == "demo" and demo_left(device, auth.get("started")) <= 0 and time.time() - demo_last_seen.get(device, 0) > DEMO_GRACE_SEC:
                        print(f"[bridge] demo limit refused {rm} dev={device}", flush=True)
                        await ws.send_str(json.dumps({"type": "demo_limit", "left": 0})); await ws.close(); return ws
                    if key: room_owner[rm] = {"key": _key_hash(key), "ts": time.time()}
                room = rm
                is_bridge, is_cueop, is_cue = role == "bridge", role == "cueop", role == "cue"
                if is_cueop:
                    cue_clients.setdefault(room, set()).add(ws)
                    cue_ops.setdefault(room, set()).add(ws)
                    print(f"[cueop ] joined {room}", flush=True)
                    await cue_broadcast(room)                 # op_online 갱신 포함, 본인에게도 현재 상태 전달
                    await ws.send_str(cue_roster_msg(room))
                    await ws.send_str(cue_sheet_msg(room))
                    await ws.send_str(msg_msg(room)); await ws.send_str(timer_msg(room))
                elif is_cue:
                    ch = str(data.get("ch", ""))[:24].strip().upper() or "CUE"
                    cue_clients.setdefault(room, set()).add(ws)
                    cue_recv.setdefault(room, {})[ws] = ch
                    print(f"[cue   ] joined {room} ch={ch}", flush=True)
                    await ws.send_str(cue_msg(room))
                    await ws.send_str(msg_msg(room)); await ws.send_str(timer_msg(room))
                    await cue_ops_send(room, cue_roster_msg(room))
                elif is_bridge:
                    bridge_meta[ws] = {"mode": mode, "device": device, "room": room}
                    rooms.setdefault(room, set()).add(ws)
                    bridges.setdefault(room, set()).add(ws)
                    state[room] = {"program": 0, "preview": 0, "pgm": [], "pvw": [], "online": True}
                    print(f"[bridge] joined {room} mode={mode} dev={device}" + (f" demo_left={demo_left(device)}s" if mode == "demo" else ""), flush=True)
                    await ws.send_str(json.dumps({"type": "joined", "mode": mode, "demo_left": demo_left(device) if mode == "demo" else None}))
                    await broadcast(room)
                    await ws.send_str(json.dumps({"type": "roster", "cams": roster(room), "rtt": roster_rtt(room)}))
                    await ws.send_str(msg_msg(room)); await ws.send_str(timer_msg(room))
                else:
                    rooms.setdefault(room, set()).add(ws)
                    cam = _i(data.get("cam"), 0, 0, 99)
                    cams.setdefault(room, {})[ws] = cam
                    print(f"[phone ] joined {room} cam={cam} ({len(cams[room])} cams)", flush=True)
                    await ws.send_str(tally_msg(room))
                    await ws.send_str(msg_msg(room))
                    await ws.send_str(timer_msg(room))
                    await broadcast_roster(room)
            elif t == "tally" and is_bridge and room:
                pgm_list = [_i(x) for x in (data.get("pgm") or [])[:64] if _i(x) > 0]
                pvw_list = [_i(x) for x in (data.get("pvw") or [])[:64] if _i(x) > 0]
                state[room] = {"program": _i(data.get("program"), 0, 0),
                               "preview": _i(data.get("preview"), 0, 0),
                               "pgm": pgm_list, "pvw": pvw_list, "online": True}
                print(f"[{room}] PGM={pgm_list or state[room]['program']} PVW={pvw_list or state[room]['preview']}", flush=True)
                await broadcast(room)
                asyncio.create_task(apns_live.push_room(room, state[room], notes.get(room), timers.get(room)))
            elif t == "msg" and (is_bridge or is_cueop) and room:
                text = str(data.get("text", ""))[:200]
                notes[room] = {"text": text, "ts": now_ms()}
                print(f"[{room}] MSG: {text}", flush=True)
                await broadcast(room, msg_msg(room))
                await cue_broadcast(room, msg_msg(room))          # 큐 수신기·콘솔에도 공지 전달
                asyncio.create_task(apns_live.push_room(room, state.get(room, OFFLINE), notes.get(room), timers.get(room), alert_onair=False))
            elif t == "timer" and (is_bridge or is_cueop) and room:
                act = data.get("action"); cur = {**EMPTY_TIMER, **timers.get(room, {})}
                tgt = cur["target"]
                if act == "set":      # 새 카운트다운 설정(정지 상태)
                    cur = {"running": False, "end": 0, "remain": _i(data.get("seconds"), 0, 0, 359999) * 1000, "target": tgt}
                elif act == "start" and not cur["running"] and cur["remain"] > 0:
                    cur = {"running": True, "end": now_ms() + cur["remain"], "remain": cur["remain"], "target": tgt}
                elif act == "pause" and cur["running"]:
                    cur = {"running": False, "end": 0, "remain": max(0, cur["end"] - now_ms()), "target": tgt}
                elif act == "reset":
                    cur = {"running": False, "end": 0, "remain": _i(data.get("seconds"), 0, 0, 359999) * 1000, "target": tgt}
                elif act == "target":         # 목표 시각 설정 (epoch ms, 0이면 해제)
                    cur["target"] = _i(data.get("target_ms"), 0, 0)
                timers[room] = cur
                print(f"[{room}] TIMER {act}: {cur}", flush=True)
                await broadcast(room, timer_msg(room))
                await cue_broadcast(room, timer_msg(room))        # 큐 수신기·콘솔에도 타이머 전달
                asyncio.create_task(apns_live.push_room(room, state.get(room, OFFLINE), notes.get(room), timers.get(room), alert_onair=False))
            elif t == "cue" and is_cueop and room:
                if cue_set(room, data.get("ch", ""), data.get("state", "")):
                    print(f"[{room}] CUE {data.get('ch')} -> {data.get('state')}", flush=True)
                    await cue_broadcast(room)
            elif t == "cue_all" and is_cueop and room:
                chs = data.get("channels", [])
                if isinstance(chs, list) and any(cue_set(room, c, data.get("state", "")) for c in chs[:64]):
                    print(f"[{room}] CUE ALL -> {data.get('state')}", flush=True)
                    await cue_broadcast(room)
            elif t == "cue_remove" and is_cueop and room:
                ch = str(data.get("ch", "")).strip().upper()
                if cue_state.get(room, {}).pop(ch, None) is not None:
                    await cue_broadcast(room)
            elif t == "cue_sheet_set" and is_cueop and room:
                clean = cue_sheet_clean(data.get("cues"))
                cur = max(0, min(_i(data.get("cur")), max(0, len(clean) - 1) if clean else 0))
                cue_sheets[room] = {"cues": clean, "cur": cur}
                print(f"[{room}] SHEET set ({len(clean)} cues, cur={cur})", flush=True)
                await cue_ops_send(room, cue_sheet_msg(room))
            elif t == "cue_sheet_cur" and is_cueop and room:
                sh = cue_sheets.setdefault(room, {"cues": [], "cur": 0})
                sh["cur"] = max(0, min(_i(data.get("cur")), max(0, len(sh["cues"]) - 1)))
                await cue_ops_send(room, cue_sheet_msg(room))
            elif t == "cue_fire" and is_cueop and room:
                sh = cue_sheets.get(room, {"cues": [], "cur": 0})
                i, phase = _i(data.get("index"), -1), data.get("phase")
                if 0 <= i < len(sh["cues"]) and phase in ("standby", "go"):
                    cue = sh["cues"][i]
                    info = {"num": cue["num"], "label": cue["label"]}
                    ts = now_ms()
                    for c in cue["channels"]:
                        cue_state.setdefault(room, {})[c] = {"state": phase, "ack": False, "ts": ts, "cue": info}
                    print(f"[{room}] FIRE cue {cue['num']} {phase} -> {cue['channels']}", flush=True)
                    if phase == "go":
                        sh["cur"] = min(i + 1, max(0, len(sh["cues"]) - 1))   # GO 후 다음 큐로 자동 이동
                        await cue_ops_send(room, cue_sheet_msg(room))
                        asyncio.create_task(cue_auto_off(room, list(cue["channels"]), ts))
                    await cue_broadcast(room)
            elif t == "cue_ack" and is_cue and room:
                ch = cue_recv.get(room, {}).get(ws)
                cur = cue_state.get(room, {}).get(ch)
                if cur and cur["state"] == "standby" and not cur["ack"]:
                    cur["ack"] = True
                    print(f"[{room}] ACK {ch}", flush=True)
                    await cue_broadcast(room)
            elif t == "ios" and room:                # 아이폰 앱: 전면/후면 상태 (전면이면 알림 푸시 생략)
                tok = str(data.get("token", "")).strip().lower()
                if tok:
                    ios_token[ws] = tok; token_ws[tok] = ws
                    apns_live.set_active(tok, bool(data.get("active")), data.get("alerts")); apns_live.set_lang(tok, data.get("lang"))
                    if data.get("push"): apns_live.set_push(tok, str(data["push"]).strip().lower())
                    if "banner" in data: apns_live.set_banner(tok, bool(data.get("banner")))
                    if "keep" in data: apns_live.set_keep(tok, bool(data.get("keep")))
                    if "vib" in data: apns_live.set_vib(tok, bool(data.get("vib")))
                    apns_live.mark_sleep(tok, bool(data.get("suspend")))     # 잠들 예정 알림 / 다시 활성이면 해제
                    apns_live.cancel_end(apns_live.device_of(tok))            # 앱이 살아있음 → 예약된 종료 취소
            elif t == "rtt" and room:                # 폰이 잰 서버 왕복 지연(ms) 보고 → 호스트에 전달
                ms = _i(data.get("ms"))
                if 0 < ms < 100000 and not is_bridge:
                    ws_rtt[ws] = ms
                    await broadcast_roster(room)
            elif t == "ping":
                ts = data.get("t")
                await ws.send_str(json.dumps({"type": "pong", "t": ts}) if ts is not None else '{"type":"pong"}')
    finally:
        seen.pop(ws, None); ws_rtt.pop(ws, None); bridge_meta.pop(ws, None)
        tok = ios_token.pop(ws, None)
        if tok and token_ws.get(tok) is ws:           # 이 소켓이 아직 토큰 소유자일 때만 (재접속했으면 새 소켓 담당)
            token_ws.pop(tok, None)
            apns_live.set_active(tok, False)          # 소켓이 끊기면(뒤로 감·종료) 푸시 재개
            # 끊긴 방식으로 구분: 하트비트 타임아웃(TimeoutError) = 백그라운드에서 잠듦 → 아일랜드 유지(APNs로 갱신)
            #                    연결 리셋/EOF = 앱이 스와이프로 죽음 → 5초 유예 후 아일랜드 종료 (정상 close=나가기는 앱이 직접 종료)
            exc = ws.exception()
            killed = ws.close_code != 1000 and not isinstance(exc, asyncio.TimeoutError) and not apns_live.treat_close_as_sleep(tok)
            if room == "DEMO": killed = True                  # 데모 방: 어떻게 끊기든 아일랜드 종료 (앱을 껐는데 데모가 계속 순환하며 "실행 중"처럼 보이던 문제, 2026-09-05)
            if killed: apns_live.schedule_end(apns_live.device_of(tok))
            else: apns_live.mark_sleep(tok, False)
        if room:
            if is_cueop:
                cue_ops.get(room, set()).discard(ws)
                cue_clients.get(room, set()).discard(ws)
                print(f"[cueop ] left {room}", flush=True)
                if not cue_ops.get(room):        # 마지막 오퍼레이터가 나가면 수신 폰에 즉시 오프라인 표시
                    await cue_broadcast(room)
            elif is_cue:
                cue_recv.get(room, {}).pop(ws, None)
                cue_clients.get(room, set()).discard(ws)
                print(f"[cue   ] left {room}", flush=True)
                await cue_ops_send(room, cue_roster_msg(room))
            else:
                rooms.get(room, set()).discard(ws)
                if is_bridge:
                    bridges.get(room, set()).discard(ws)
                    if room_owner.get(room): room_owner[room]["ts"] = time.time()
                    print(f"[bridge] left {room}", flush=True)
                    if not bridges.get(room):                  # 같은 호스트의 다른 브릿지가 남아 있으면 오프라인으로 만들지 않는다
                        state[room] = {**OFFLINE, "pgm": [], "pvw": []}
                        await broadcast(room)
                        asyncio.create_task(apns_live.push_room(room, state[room], notes.get(room), timers.get(room), alert_onair=False))
                else:
                    cams.get(room, {}).pop(ws, None)
                    print(f"[phone ] left {room}", flush=True)
                    await broadcast_roster(room)
            _cleanup_room(room)
    return ws

async def reaper(app):
    """응답 없는 폰을 주기적으로 정리해 접속 현황을 최신으로 유지 + 데모 7일 지난 호스트 정리"""
    while True:
        await asyncio.sleep(5)
        now = time.time()
        # 오래된 항목 정리: 데모 최초 확인(기간+1일 지난 것), 방 소유권(유지 시간 지난 빈 방), join 카운터, 빈 방 잔여 상태
        for dev, first in list(demo_first.items()):
            if now - first > (DEMO_DAYS + 1) * 86400: demo_first.pop(dev, None)
        if len(demo_first) > 50000:
            for dev in sorted(demo_first, key=demo_first.get)[:len(demo_first) - 50000]: demo_first.pop(dev, None)
        for rm, o in list(room_owner.items()):
            if not bridges.get(rm) and now - o["ts"] > ROOM_HOLD_SEC: room_owner.pop(rm, None)
        for k, h in list(join_hits.items()):
            if now - h[1] > 120: join_hits.pop(k, None)
        for rm in list(set(state) | set(notes) | set(timers) | set(cue_state) | set(cue_sheets)):
            _cleanup_room(rm)
        for bws, meta in list(bridge_meta.items()):
            if meta.get("mode") != "demo": continue
            demo_last_seen[meta["device"]] = now                    # 접속 중 만료돼도 끊지 않는다(세션 유지); 다음 새 세션부터 거부
        for dev, t in list(demo_last_seen.items()):
            if now - t > DEMO_GRACE_SEC + 3600: demo_last_seen.pop(dev, None)
        for room, d in list(cams.items()):
            stale = [w for w in list(d) if now - seen.get(w, 0) > STALE_SEC]
            for w in stale:
                d.pop(w, None); rooms.get(room, set()).discard(w); seen.pop(w, None); ws_rtt.pop(w, None)
                tok = ios_token.pop(w, None)
                if tok and token_ws.get(tok) is w:       # 서버가 끊는 무응답 = 잠든 폰 → 아일랜드 유지 (데모 방은 종료)
                    token_ws.pop(tok, None); apns_live.set_active(tok, False)
                    if room == "DEMO": apns_live.schedule_end(apns_live.device_of(tok))
                try: await w.close()
                except Exception: pass
            if stale:
                print(f"[{room}] 응답 없는 폰 {len(stale)}대 정리", flush=True)
                await broadcast_roster(room)
        for room, d in list(cue_recv.items()):
            stale = [w for w in list(d) if now - seen.get(w, 0) > STALE_SEC]
            for w in stale:
                d.pop(w, None); cue_clients.get(room, set()).discard(w); seen.pop(w, None)
                try: await w.close()
                except Exception: pass
            if stale:
                print(f"[{room}] 응답 없는 큐 수신기 {len(stale)}대 정리", flush=True)
                await cue_ops_send(room, cue_roster_msg(room))

async def demo_host(app):
    """방 DEMO 가상 호스트: 호스트 앱·스위처 없이 앱을 체험(앱스토어 심사 포함)할 수 있게 4대 카메라를 4초마다 순환."""
    room = "DEMO"; i = 0
    while True:
        await asyncio.sleep(4)
        if not rooms.get(room) and not cams.get(room) and not apns_live.count(room):
            continue                                     # 아무도 없으면 조용히
        pgm = (i % 4) + 1; pvw = (pgm % 4) + 1; i += 1
        state[room] = {"program": pgm, "preview": pvw, "pgm": [pgm], "pvw": [pvw], "online": True}
        if not notes.get(room):
            notes[room] = {"text": "DEMO — 4초마다 자동 전환 / auto-cycling every 4s", "ts": now_ms()}
            await broadcast(room, msg_msg(room))
        await broadcast(room)
        asyncio.create_task(apns_live.push_room(room, state[room], notes.get(room), timers.get(room)))

async def start_bg(app):
    app["reaper"] = asyncio.create_task(reaper(app))
    app["demo"] = asyncio.create_task(demo_host(app))

async def index(request):
    return web.FileResponse(os.path.join(WEB_DIR, "index.html"), headers={"Cache-Control": "no-cache"})

_ROBOTS = """# Flare Tally (c) AudioAZ — AI 학습·분석용 수집 금지 / no AI training or analysis. See /NO_AI_NOTICE.txt
User-agent: GPTBot\nDisallow: /\nUser-agent: ChatGPT-User\nDisallow: /\nUser-agent: ClaudeBot\nDisallow: /\nUser-agent: anthropic-ai\nDisallow: /
User-agent: Claude-Web\nDisallow: /\nUser-agent: CCBot\nDisallow: /\nUser-agent: Google-Extended\nDisallow: /\nUser-agent: Applebot-Extended\nDisallow: /
User-agent: PerplexityBot\nDisallow: /\nUser-agent: Bytespider\nDisallow: /\nUser-agent: Amazonbot\nDisallow: /\nUser-agent: cohere-ai\nDisallow: /
User-agent: meta-externalagent\nDisallow: /\nUser-agent: Diffbot\nDisallow: /\nUser-agent: omgili\nDisallow: /\nUser-agent: *\nDisallow: /
"""
async def robots_txt(request):
    return web.Response(text=_ROBOTS, content_type="text/plain")

@web.middleware
async def _no_ai_headers(request, handler):
    resp = await handler(request)
    try: resp.headers["X-Robots-Tag"] = "noai, noimageai, noarchive"
    except Exception: pass
    return resp

# ---- 익명 진단 정보 (제품 개선용, 2026-09-05 사장님 "기본 켬·익명·누구인지는 관심 없음") ----
# 앱/호스트가 POST /telemetry 로 보낸 {kind, app, os, model, mode, event, detail, dev} 를 검증·속도 제한 후 Supabase telemetry 표에 넣는다(anon 키, insert 전용 정책).
# dev = 기기 식별자의 해시 앞 12자리(사람과 연결되지 않음). 계정·이메일·방 코드·IP는 저장하지 않는다.
_TELE_ALLOWED = {"kind": 8, "app": 24, "os": 40, "model": 48, "mode": 16, "event": 40, "detail": 300, "dev": 16}
tele_hits: dict = {}   # ip -> [count, window]
TELE_LIMIT = 60        # IP당 분당

async def telemetry(request):
    ip = request.headers.get("X-Forwarded-For", request.remote or "?").split(",")[0].strip()[:64]
    now = time.time(); h = tele_hits.get(ip)
    if not h or now - h[1] > 60: tele_hits[ip] = [1, now]
    else:
        h[0] += 1
        if h[0] > TELE_LIMIT: return web.json_response({"ok": False}, status=429)
    try:
        if request.content_length and request.content_length > 4096: return web.json_response({"ok": False}, status=413)
        d = await request.json()
        if not isinstance(d, dict): raise ValueError("shape")
    except Exception:
        return web.json_response({"ok": False}, status=400)
    row = {}
    for k, n in _TELE_ALLOWED.items():
        v = d.get(k)
        if v is None: continue
        row[k] = str(v)[:n]
    if row.get("kind") not in ("ios", "host", "web") or not row.get("event"): return web.json_response({"ok": False}, status=400)
    if row.get("dev") and not _re.match(r"^[0-9a-f]{6,16}$", row["dev"]): row.pop("dev", None)
    row["ver"] = SERVER_VER
    try:
        import aiohttp as _aio
        async with _aio.ClientSession() as sess:
            async with sess.post(SUPABASE_URL + "/rest/v1/telemetry", json=row, timeout=_aio.ClientTimeout(total=5),
                                 headers={"apikey": SUPABASE_ANON, "Authorization": "Bearer " + SUPABASE_ANON, "Content-Type": "application/json", "Prefer": "return=minimal"}) as r:
                if r.status >= 300: print(f"[tele  ] supabase {r.status} {(await r.text())[:80]}", flush=True)
    except Exception as e:
        print(f"[tele  ] error {e!r}", flush=True)
    return web.json_response({"ok": True})

async def health(request):
    return web.json_response({"ok": True, "rooms": len(rooms), "ver": SERVER_VER})

async def room_status(request):
    """GET /room?code=XXXX → 그 방에 호스트(브릿지)가 켜져 있는지. 폰 앱은 켜진 방에만 들어간다 (사장님 2026-09-05)."""
    code = str(request.query.get("code", ""))[:32].strip().upper()
    if not code or not _ROOM_RE.match(code): return web.json_response({"ok": False, "active": False, "error": "room"}, status=400)
    active = bool(bridges.get(code)) or code == "DEMO"
    return web.json_response({"ok": True, "active": active, "cams": len(cams.get(code, {}))})

def _status_rooms():
    """현재 방별 접속 현황(민감정보 제외: 방코드·호스트 온라인/모드·카메라 번호·큐 수·PGM/PVW만)."""
    keys = set()
    for d in (rooms, bridges, cams, cue_ops, cue_recv, state):
        keys.update(d.keys())
    out = []
    for rm in sorted(keys):
        br = bridges.get(rm) or set()
        mode = None
        for w in br:
            m = bridge_meta.get(w)
            if m: mode = m.get("mode"); break
        st = state.get(rm, {})
        pgm = st.get("pgm") or ([st["program"]] if st.get("program") else [])
        pvw = st.get("pvw") or ([st["preview"]] if st.get("preview") else [])
        out.append({
            "room": rm,
            "host_online": bool(br),
            "host_mode": mode,
            "cams": roster(rm),
            "cam_count": len(cams.get(rm, {})),
            "cue_op": bool(cue_ops.get(rm)),
            "cue_recv": len(cue_recv.get(rm, {})),
            "pgm": pgm, "pvw": pvw,
        })
    return out

async def status(request):
    """GET /status?key=KEY → 접속자 현황 JSON. STATUS_KEY 미설정이면 비활성(503)."""
    if not STATUS_KEY:
        return web.json_response({"ok": False, "error": "disabled",
                                  "hint": "Render 환경변수 STATUS_KEY 를 설정하면 켜집니다."}, status=503)
    if request.query.get("key", "") != STATUS_KEY:
        return web.json_response({"ok": False, "error": "auth"}, status=401)
    rl = _status_rooms()
    totals = {"rooms": len(rl),
              "hosts": sum(1 for r in rl if r["host_online"]),
              "cams": sum(r["cam_count"] for r in rl),
              "cue_recv": sum(r["cue_recv"] for r in rl)}
    return web.json_response({"ok": True, "ver": SERVER_VER, "now": now_ms(),
                              "totals": totals, "rooms": rl})

async def ios_activity(request):
    """아이폰 앱이 Live Activity 푸시 토큰을 등록/해제. POST {room, cam, token} / DELETE {token}"""
    try:
        if request.content_length and request.content_length > 8192: return web.json_response({"ok": False, "error": "size"}, status=413)
        d = await request.json()
        if not isinstance(d, dict): raise ValueError("shape")
        token = str(d.get("token", "")).strip().lower()[:256]
        device = str(d.get("device", "")).strip()[:64]
        if token and not _TOKEN_RE.match(token): return web.json_response({"ok": False, "error": "token"}, status=400)
        if device and not _ID_RE.match(device): return web.json_response({"ok": False, "error": "device"}, status=400)
        return await _ios_activity(request, d, token, device)
    except Exception as e:
        print(f"[ios   ] bad request {e!r}", flush=True)
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)

async def _ios_activity(request, d, token, device):
    if not token and not device: return web.json_response({"ok": False, "error": "token"}, status=400)
    if request.method == "DELETE":
        apns_live.unregister(token=token, device=device); return web.json_response({"ok": True})
    room = str(d.get("room", ""))[:32].strip().upper() or "DEFAULT"; cam = _i(d.get("cam"), 0, 0, 99)
    if not _ROOM_RE.match(room): return web.json_response({"ok": False, "error": "room"}, status=400)
    if apns_live.count(room) >= MAX_IOS_PER_ROOM: return web.json_response({"ok": False, "error": "room_full"}, status=429)
    apns_live.cancel_end(device)                       # 재접속·재등록 = 살아있음
    apns_live.register(room, cam, token, device)
    if "alerts" in d: apns_live.set_alerts(token, bool(d.get("alerts")))
    if d.get("push"): apns_live.set_push(token, str(d["push"]).strip().lower())
    if "banner" in d: apns_live.set_banner(token, bool(d.get("banner")))
    if "keep" in d: apns_live.set_keep(token, bool(d.get("keep")))
    if "vib" in d: apns_live.set_vib(token, bool(d.get("vib")))
    if "active" in d: apns_live.set_active(token, bool(d.get("active")), d.get("alerts"))   # 오프라인(LAN) 모드 폰이 클라우드엔 HTTP로만 전면/후면을 알림 (소켓은 LAN 서버에)
    if "suspend" in d: apns_live.mark_sleep(token, bool(d.get("suspend")))
    apns_live.set_lang(token, d.get("lang"))
    print(f"[ios   ] activity {room} cam={cam} ({apns_live.count(room)} phones)", flush=True)
    # 등록 직후 현재 상태를 한 번 보내 아일랜드가 바로 맞춰지게
    asyncio.create_task(apns_live.push_room(room, state.get(room, OFFLINE), notes.get(room), timers.get(room), alert_onair=False))
    return web.json_response({"ok": True, "push": apns_live.ENABLED})

def make_app():
    """aiohttp Application은 이벤트 루프에 묶이므로, 내장 서버(호스트 앱)에서는 시작할 때마다 새로 만든다"""
    a = web.Application(middlewares=[_no_ai_headers])
    a.on_startup.append(start_bg)
    a.router.add_get("/", index)
    a.router.add_get("/ws", ws_handler)
    a.router.add_get("/health", health)
    a.router.add_get("/room", room_status)
    a.router.add_get("/status", status)
    a.router.add_post("/telemetry", telemetry)
    a.router.add_post("/ios/activity", ios_activity)
    a.router.add_delete("/ios/activity", ios_activity)
    a.router.add_get("/robots.txt", robots_txt)
    a.router.add_static("/", WEB_DIR, show_index=False)
    return a

app = make_app()

if __name__ == "__main__":
    print(f"탈리 서버 실행 중: http://0.0.0.0:{PORT}", flush=True)
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)
