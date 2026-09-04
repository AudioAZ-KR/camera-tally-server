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
SERVER_VER = "2026-09-04.3"        # 배포 확인용: /health 가 이 값을 돌려주면 이 코드가 살아있는 것
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
    ws = web.WebSocketResponse(heartbeat=10)
    await ws.prepare(request)
    room, is_bridge, is_cueop, is_cue = None, False, False, False
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
                room = str(data.get("room", "")).strip().upper() or "DEFAULT"
                role = data.get("role")
                is_bridge, is_cueop, is_cue = role == "bridge", role == "cueop", role == "cue"
                if is_cueop:
                    cue_clients.setdefault(room, set()).add(ws)
                    cue_ops.setdefault(room, set()).add(ws)
                    print(f"[cueop ] joined {room}", flush=True)
                    await cue_broadcast(room)                 # op_online 갱신 포함, 본인에게도 현재 상태 전달
                    await ws.send_str(cue_roster_msg(room))
                elif is_cue:
                    ch = str(data.get("ch", ""))[:24].strip().upper() or "CUE"
                    cue_clients.setdefault(room, set()).add(ws)
                    cue_recv.setdefault(room, {})[ws] = ch
                    print(f"[cue   ] joined {room} ch={ch}", flush=True)
                    await ws.send_str(cue_msg(room))
                    await cue_ops_send(room, cue_roster_msg(room))
                elif is_bridge:
                    rooms.setdefault(room, set()).add(ws)
                    bridges.setdefault(room, set()).add(ws)
                    state[room] = {"program": 0, "preview": 0, "pgm": [], "pvw": [], "online": True}
                    print(f"[bridge] joined {room}", flush=True)
                    await broadcast(room)
                    await ws.send_str(json.dumps({"type": "roster", "cams": roster(room), "rtt": roster_rtt(room)}))
                    await ws.send_str(msg_msg(room)); await ws.send_str(timer_msg(room))
                else:
                    rooms.setdefault(room, set()).add(ws)
                    cam = int(data.get("cam", 0) or 0)
                    cams.setdefault(room, {})[ws] = cam
                    print(f"[phone ] joined {room} cam={cam} ({len(cams[room])} cams)", flush=True)
                    await ws.send_str(tally_msg(room))
                    await ws.send_str(msg_msg(room))
                    await ws.send_str(timer_msg(room))
                    await broadcast_roster(room)
            elif t == "tally" and is_bridge and room:
                pgm_list = [int(x) for x in data.get("pgm", []) if x]
                pvw_list = [int(x) for x in data.get("pvw", []) if x]
                state[room] = {"program": int(data.get("program", 0)),
                               "preview": int(data.get("preview", 0)),
                               "pgm": pgm_list, "pvw": pvw_list, "online": True}
                print(f"[{room}] PGM={pgm_list or state[room]['program']} PVW={pvw_list or state[room]['preview']}", flush=True)
                await broadcast(room)
                asyncio.create_task(apns_live.push_room(room, state[room], notes.get(room), timers.get(room)))
            elif t == "msg" and is_bridge and room:
                text = str(data.get("text", ""))[:200]
                notes[room] = {"text": text, "ts": now_ms()}
                print(f"[{room}] MSG: {text}", flush=True)
                await broadcast(room, msg_msg(room))
                asyncio.create_task(apns_live.push_room(room, state.get(room, OFFLINE), notes.get(room), timers.get(room), alert_onair=False))
            elif t == "timer" and is_bridge and room:
                act = data.get("action"); cur = {**EMPTY_TIMER, **timers.get(room, {})}
                tgt = cur["target"]
                if act == "set":      # 새 카운트다운 설정(정지 상태)
                    cur = {"running": False, "end": 0, "remain": max(0, int(data.get("seconds", 0))) * 1000, "target": tgt}
                elif act == "start" and not cur["running"] and cur["remain"] > 0:
                    cur = {"running": True, "end": now_ms() + cur["remain"], "remain": cur["remain"], "target": tgt}
                elif act == "pause" and cur["running"]:
                    cur = {"running": False, "end": 0, "remain": max(0, cur["end"] - now_ms()), "target": tgt}
                elif act == "reset":
                    cur = {"running": False, "end": 0, "remain": max(0, int(data.get("seconds", 0))) * 1000, "target": tgt}
                elif act == "target":         # 목표 시각 설정 (epoch ms, 0이면 해제)
                    cur["target"] = max(0, int(data.get("target_ms", 0)))
                timers[room] = cur
                print(f"[{room}] TIMER {act}: {cur}", flush=True)
                await broadcast(room, timer_msg(room))
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
                    apns_live.mark_sleep(tok, bool(data.get("suspend")))     # 잠들 예정 알림 / 다시 활성이면 해제
                    apns_live.cancel_end(apns_live.device_of(tok))            # 앱이 살아있음 → 예약된 종료 취소
            elif t == "rtt" and room:                # 폰이 잰 서버 왕복 지연(ms) 보고 → 호스트에 전달
                ms = int(data.get("ms", 0) or 0)
                if 0 < ms < 100000 and not is_bridge:
                    ws_rtt[ws] = ms
                    await broadcast_roster(room)
            elif t == "ping":
                ts = data.get("t")
                await ws.send_str(json.dumps({"type": "pong", "t": ts}) if ts is not None else '{"type":"pong"}')
    finally:
        seen.pop(ws, None); ws_rtt.pop(ws, None)
        tok = ios_token.pop(ws, None)
        if tok and token_ws.get(tok) is ws:           # 이 소켓이 아직 토큰 소유자일 때만 (재접속했으면 새 소켓 담당)
            token_ws.pop(tok, None)
            apns_live.set_active(tok, False)          # 소켓이 끊기면(뒤로 감·종료) 푸시 재개
            # 끊긴 방식으로 구분: 하트비트 타임아웃(TimeoutError) = 백그라운드에서 잠듦 → 아일랜드 유지(APNs로 갱신)
            #                    연결 리셋/EOF = 앱이 스와이프로 죽음 → 5초 유예 후 아일랜드 종료 (정상 close=나가기는 앱이 직접 종료)
            exc = ws.exception()
            killed = ws.close_code != 1000 and not isinstance(exc, asyncio.TimeoutError) and not apns_live.treat_close_as_sleep(tok)
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
                    state[room] = dict(OFFLINE)
                    print(f"[bridge] left {room}", flush=True)
                    await broadcast(room)
                    asyncio.create_task(apns_live.push_room(room, state[room], notes.get(room), timers.get(room), alert_onair=False))
                else:
                    cams.get(room, {}).pop(ws, None)
                    print(f"[phone ] left {room}", flush=True)
                    await broadcast_roster(room)
    return ws

async def reaper(app):
    """응답 없는 폰을 주기적으로 정리해 접속 현황을 최신으로 유지"""
    while True:
        await asyncio.sleep(5)
        now = time.time()
        for room, d in list(cams.items()):
            stale = [w for w in list(d) if now - seen.get(w, 0) > STALE_SEC]
            for w in stale:
                d.pop(w, None); rooms.get(room, set()).discard(w); seen.pop(w, None); ws_rtt.pop(w, None)
                tok = ios_token.pop(w, None)
                if tok and token_ws.get(tok) is w:       # 서버가 끊는 무응답 = 잠든 폰 → 아일랜드 유지
                    token_ws.pop(tok, None); apns_live.set_active(tok, False)
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

async def start_bg(app):
    app["reaper"] = asyncio.create_task(reaper(app))

async def index(request):
    return web.FileResponse(os.path.join(WEB_DIR, "index.html"), headers={"Cache-Control": "no-cache"})

async def health(request):
    return web.json_response({"ok": True, "rooms": len(rooms), "ver": SERVER_VER})

async def ios_activity(request):
    """아이폰 앱이 Live Activity 푸시 토큰을 등록/해제. POST {room, cam, token} / DELETE {token}"""
    try: d = await request.json()
    except Exception: return web.json_response({"ok": False, "error": "json"}, status=400)
    token = str(d.get("token", "")).strip().lower()
    if not token and not d.get("device"): return web.json_response({"ok": False, "error": "token"}, status=400)
    device = str(d.get("device", "")).strip()
    if request.method == "DELETE":
        apns_live.unregister(token=token, device=device); return web.json_response({"ok": True})
    room = str(d.get("room", "")).strip().upper() or "DEFAULT"; cam = int(d.get("cam", 0) or 0)
    apns_live.cancel_end(device)                       # 재접속·재등록 = 살아있음
    apns_live.register(room, cam, token, device)
    if "alerts" in d: apns_live.set_alerts(token, bool(d.get("alerts")))
    if d.get("push"): apns_live.set_push(token, str(d["push"]).strip().lower())
    if "banner" in d: apns_live.set_banner(token, bool(d.get("banner")))
    if "keep" in d: apns_live.set_keep(token, bool(d.get("keep")))
    apns_live.set_lang(token, d.get("lang"))
    print(f"[ios   ] activity {room} cam={cam} ({apns_live.count(room)} phones)", flush=True)
    # 등록 직후 현재 상태를 한 번 보내 아일랜드가 바로 맞춰지게
    asyncio.create_task(apns_live.push_room(room, state.get(room, OFFLINE), notes.get(room), timers.get(room), alert_onair=False))
    return web.json_response({"ok": True, "push": apns_live.ENABLED})

def make_app():
    """aiohttp Application은 이벤트 루프에 묶이므로, 내장 서버(호스트 앱)에서는 시작할 때마다 새로 만든다"""
    a = web.Application()
    a.on_startup.append(start_bg)
    a.router.add_get("/", index)
    a.router.add_get("/ws", ws_handler)
    a.router.add_get("/health", health)
    a.router.add_post("/ios/activity", ios_activity)
    a.router.add_delete("/ios/activity", ios_activity)
    a.router.add_static("/", WEB_DIR, show_index=False)
    return a

app = make_app()

if __name__ == "__main__":
    print(f"탈리 서버 실행 중: http://0.0.0.0:{PORT}", flush=True)
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)
