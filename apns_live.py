"""
apns_live.py — iOS Live Activity(다이나믹 아일랜드) 푸시
아이폰 앱이 Live Activity를 시작하면 푸시 토큰을 서버에 등록(POST /ios/activity)하고,
서버는 방의 탈리·공지·타이머가 바뀔 때마다 APNs로 활동 내용을 갱신한다.
앱이 잠들어 있어도(다른 앱 사용·잠금화면) 아일랜드가 바뀌는 건 이 경로뿐이다.

설정(환경변수):
  APNS_TEAM_ID    Apple 팀 ID (92PGKJFTU9)
  APNS_KEY_ID     APNs 인증 키 ID (개발자 포털 → Keys → Apple Push Notifications service)
  APNS_KEY_P8     .p8 키 파일 내용(PEM 전체)   ※ 또는 APNS_KEY_PATH=파일경로
  APNS_BUNDLE_ID  앱 번들 ID (기본 kr.audioaz.flaretally)
  APNS_ENV        production | sandbox (기본 production; Xcode에서 직접 설치한 빌드는 sandbox)
설정이 없으면 등록만 받고 푸시는 보내지 않는다(로그 한 줄).
"""
import asyncio, json, os, time

TEAM_ID = os.environ.get("APNS_TEAM_ID", "")
KEY_ID = os.environ.get("APNS_KEY_ID", "")
BUNDLE_ID = os.environ.get("APNS_BUNDLE_ID", "kr.audioaz.flaretally")
ENV = os.environ.get("APNS_ENV", "production")
_key_pem = os.environ.get("APNS_KEY_P8", "")
if not _key_pem and os.environ.get("APNS_KEY_PATH"):
    try: _key_pem = open(os.environ["APNS_KEY_PATH"]).read()
    except OSError: _key_pem = ""

HOSTS = {"production": "https://api.push.apple.com", "sandbox": "https://api.sandbox.push.apple.com"}
HOST = HOSTS.get(ENV, HOSTS["production"])
_env_of: dict[str, str] = {}
_active: dict[str, bool] = {}       # token -> 앱이 앞에 떠 있음(앱이 직접 갱신·햅틱하므로 푸시 생략)
_alerts: dict[str, bool] = {}       # token -> 백그라운드 알림(진동·배너) 허용 (앱 안 '알림' 스위치)
_lang: dict[str, str] = {}          # token -> 앱 언어(ko/en) — 알림 문구용
_device_token: dict[str, str] = {}  # deviceId -> 현재 활성 토큰 (기기당 하나만 유지, 좀비 방지)
_token_device: dict[str, str] = {}  # token -> deviceId
_end_tasks: dict[str, asyncio.Task] = {}
_push_tok: dict[str, str] = {}      # LA token -> 일반 알림용 기기 토큰 (가로 화면 배너)
_banner: dict[str, bool] = {}
_vib: dict[str, bool] = {}          # LA token -> 알림에 진동·소리를 붙일지 (기본 켬). 끄면 조용히 표시만 (사장님 2026-09-05)
_keep: dict[str, bool] = {}         # LA token -> '잠금 유지': 소켓이 어떻게 끊겨도 종료로 보지 않음 (나가기·호스트 종료·410만 종료)
_sleeping: dict[str, bool] = {}     # LA token -> 앱이 "잠들 예정"을 알림 (뒤로 간 뒤 몇 초) → 이후 끊김은 잠듦       # LA token -> 배너 모드(앱 '배너' 스위치): 가로 화면에선 아일랜드가 안 그려지므로 일반 알림 배너로   # deviceId -> 유예 후 활동 종료 작업 (앱 종료·소켓 끊김 대비)         # token -> 실제로 통한 환경. TestFlight/앱스토어=production, Xcode 직접 설치=sandbox — 둘 다 자동 처리
ENABLED = bool(TEAM_ID and KEY_ID and _key_pem)

# room -> {token: cam}
_tokens: dict[str, dict[str, int]] = {}
_last: dict[str, dict] = {}          # token -> 마지막으로 보낸 content-state (같으면 안 보냄: iOS 갱신 예산 절약 = 지연 감소)
_jwt_cache = {"token": "", "ts": 0.0}
_client = None


def register(room: str, cam: int, token: str, device: str = ""):
    room = (room or "").strip().upper() or "DEFAULT"
    if device:
        old = _device_token.get(device)
        if old and old != token:
            _unregister_token(old)                 # 같은 기기의 이전(좀비) 토큰 제거
        _device_token[device] = token; _token_device[token] = device
    _tokens.setdefault(room, {})[token] = int(cam)


def _unregister_token(token: str):
    for d in _tokens.values():
        d.pop(token, None)
    for m in (_last, _env_of, _active, _alerts, _lang, _push_tok, _banner, _keep, _sleeping, _vib):
        m.pop(token, None)
    dev = _token_device.pop(token, None)
    if dev and _device_token.get(dev) == token:
        _device_token.pop(dev, None)


def unregister(token: str = "", device: str = ""):
    """토큰 하나 또는 기기(deviceId)에 딸린 모든 토큰 해제 — 나가기 시 좀비까지 확실히 정리"""
    if device:
        tok = _device_token.get(device)
        if tok: _unregister_token(tok)
    if token: _unregister_token(token)


def device_of(token: str) -> str:
    return _token_device.get(token, "")


async def _end_device(device: str, delay: float):
    """소켓이 끊긴 뒤 delay초 안에 앱이 다시 살아나지 않으면 이 기기의 아일랜드를 강제 종료.
    iOS는 앱을 스와이프로 완전히 꺼도 Live Activity를 남기므로, 서버가 대신 끝내준다."""
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return
    _end_tasks.pop(device, None)
    token = _device_token.get(device)
    if not token:
        return
    cam = next((c for d in _tokens.values() for t, c in d.items() if t == token), 0)
    print(f"[apns] grace expired → end device {device[:8]} cam={cam}", flush=True)
    if ENABLED:
        now = int(time.time())
        cs = content_state(cam, {"online": False}, None, None)
        try:
            await _send(token, {"aps": {"timestamp": now, "event": "end", "content-state": cs, "dismissal-date": now}})
        except Exception as e:
            print(f"[apns] end error: {e!r}", flush=True)
    _unregister_token(token)


def schedule_end(device: str, delay: float = 0.7):
    """소켓 끊김 → 유예 타이머 시작 (같은 기기 타이머는 갱신)"""
    if not device:
        return
    cancel_end(device)
    _end_tasks[device] = asyncio.create_task(_end_device(device, delay))


def cancel_end(device: str):
    """앱이 살아있다는 신호(재접속·토큰 등록) → 종료 취소"""
    t = _end_tasks.pop(device or "", None)
    if t: t.cancel()


def set_active(token: str, active: bool, alerts=None):
    if token:
        _active[token] = bool(active)
        if alerts is not None: _alerts[token] = bool(alerts)


def set_alerts(token: str, alerts: bool):
    if token: _alerts[token] = bool(alerts)


def set_push(token: str, hex_: str):
    if token and hex_: _push_tok[token] = hex_


def set_keep(token: str, on: bool):
    if token: _keep[token] = bool(on)


def mark_sleep(token: str, on: bool = True):
    if token: _sleeping[token] = bool(on)


def treat_close_as_sleep(token: str) -> bool:
    """소켓 끊김을 '잠듦'으로 볼지: 잠금 유지 ON 또는 앱이 잠들 예정을 알린 경우"""
    return bool(_keep.get(token) or _sleeping.get(token))


def set_vib(token: str, on: bool):
    if token: _vib[token] = bool(on)

def set_banner(token: str, on: bool):
    if token: _banner[token] = bool(on)


def set_lang(token: str, lang):
    if token and lang in ("ko", "en"): _lang[token] = lang


_MSG = {"ko": {"pgm": ("CAM {cam} ON AIR", "지금 방송 중"), "idle": ("CAM {cam} 대기", "온에어 해제"), "pvw": ("CAM {cam} PREVIEW", "다음 컷 대기")},
        "en": {"pgm": ("CAM {cam} ON AIR", "You are live"), "idle": ("CAM {cam} STANDBY", "Off air"), "pvw": ("CAM {cam} PREVIEW", "Up next")}}


def count(room: str) -> int:
    return len(_tokens.get(room, {}))


def _jwt():
    """APNs 토큰 인증용 JWT(ES256). 50분마다 갱신(Apple 허용 20~60분)."""
    if time.time() - _jwt_cache["ts"] < 50 * 60 and _jwt_cache["token"]:
        return _jwt_cache["token"]
    import jwt  # PyJWT + cryptography
    tok = jwt.encode({"iss": TEAM_ID, "iat": int(time.time())}, _key_pem, algorithm="ES256", headers={"kid": KEY_ID})
    _jwt_cache.update(token=tok, ts=time.time())
    return tok


def cam_state(cam: int, st: dict) -> str:
    """방 상태(dict) → 이 카메라의 탈리 상태 문자열"""
    if not st.get("online"):
        return "off"
    pgm = st.get("pgm") or ([st["program"]] if st.get("program") else [])
    pvw = st.get("pvw") or ([st["preview"]] if st.get("preview") else [])
    if cam in pgm: return "pgm"
    if cam in pvw: return "pvw"
    return "idle"


def content_state(cam: int, st: dict, note: dict | None, timer: dict | None) -> dict:
    """아이폰 Live Activity의 ContentState와 필드가 1:1로 맞아야 한다 (Shared/TallyAttributes.swift)"""
    t = timer or {}
    return {
        "state": cam_state(cam, st),
        "pgm": [int(x) for x in (st.get("pgm") or [])][:8],
        "pvw": [int(x) for x in (st.get("pvw") or [])][:8],
        "notice": (note or {}).get("text", "")[:120],
        "timerEnd": int(t.get("end") or 0),
        "timerRemain": int(t.get("remain") or 0),
        "timerRunning": bool(t.get("running")),
        "target": int(t.get("target") or 0),
    }


async def _send(token: str, payload: dict):
    """토큰의 환경(production/sandbox)을 모르면 기본 환경부터 시도하고, BadDeviceToken이면 반대 환경으로 한 번 더."""
    global _client
    import httpx
    if _client is None:
        _client = httpx.AsyncClient(http2=True, timeout=10)
    headers = {
        "authorization": f"bearer {_jwt()}",
        "apns-topic": f"{BUNDLE_ID}.push-type.liveactivity",
        "apns-push-type": "liveactivity",
        "apns-priority": "10",
        "apns-expiration": "0",
    }
    first = _env_of.get(token) or ENV
    order = [first] + [e for e in HOSTS if e != first]
    for env in order:
        t0 = time.time()
        r = await _client.post(f"{HOSTS[env]}/3/device/{token}", headers=headers, content=json.dumps(payload))
        st = payload["aps"].get("content-state", {}).get("state", "")
        if r.status_code == 200:
            _env_of[token] = env
            print(f"[apns] 200 {env} {int((time.time()-t0)*1000)}ms {st}", flush=True)
            return
        print(f"[apns] {r.status_code} {env} {int((time.time()-t0)*1000)}ms {st} {r.text[:80]}", flush=True)
        if r.status_code == 410:
            unregister(token); return                     # 활동 종료·앱 삭제
        if r.status_code == 400 and b"BadDeviceToken" in r.content:
            continue                                      # 환경이 다를 수 있음 → 다른 환경 시도
        return                                            # 그 외 오류는 재시도 없음
    unregister(token)                                     # 양쪽 다 거부 → 무효 토큰

async def _send_banner(la_token: str, dev_token: str, title: str, body: str, collapse: str, collapse_state: str = "idle", cam: int = 0):
    """일반 알림 푸시(배너). iOS는 가로 화면에서 다이나믹 아일랜드를 그리지 않으므로 촬영 앱을 가로로 쓸 때 이걸로 탈리를 알린다."""
    global _client
    import httpx
    if _client is None:
        _client = httpx.AsyncClient(http2=True, timeout=10)
    headers = {"authorization": f"bearer {_jwt()}", "apns-topic": BUNDLE_ID, "apns-push-type": "alert",
               "apns-priority": "10", "apns-expiration": "0", "apns-collapse-id": collapse}
    aps_ = {"alert": {"title": title, "body": body}, "mutable-content": 1}
    if _vib.get(la_token, True): aps_["sound"] = "default"       # 알림 진동 OFF면 소리·진동 없이 배너만
    payload = {"aps": aps_,
               "tally": {"state": collapse_state, "cam": cam}}          # 알림 서비스 확장이 색판(빨강/초록/회색+번호) 썸네일을 붙인다
    first = _env_of.get(la_token) or ENV
    for env in [first] + [e for e in HOSTS if e != first]:
        r = await _client.post(f"{HOSTS[env]}/3/device/{dev_token}", headers=headers, content=json.dumps(payload))
        print(f"[apns] banner {r.status_code} {env} {title}", flush=True)
        if r.status_code == 200: return
        if r.status_code == 400 and b"BadDeviceToken" in r.content: continue
        if r.status_code == 410: _push_tok.pop(la_token, None)
        return


async def _send_repeat(token: str, payload: dict, times: int, gap: float):
    """같은 알림을 짧은 간격으로 여러 번 → 백그라운드에서 '톡톡톡' 느낌. 매번 timestamp를 올려 iOS가 새 갱신으로 받게 한다."""
    async def one(i):
        await asyncio.sleep(gap * i)                       # 응답을 기다리지 않고 동시에 쏘되 gap만큼 어긋나게
        p = json.loads(json.dumps(payload)); p["aps"]["timestamp"] = int(time.time()) + i
        await _send(token, p)
    await asyncio.gather(*(one(i) for i in range(times)))


async def push_room(room: str, st: dict, note: dict | None = None, timer: dict | None = None, alert_onair=True):
    """방의 모든 아이폰에 현재 상태를 푸시. 온에어로 바뀐 폰에는 진동 알림도 붙인다."""
    if not ENABLED:
        return
    regs = _tokens.get(room)
    if not regs:
        return
    now = int(time.time())
    tasks = []
    for token, cam in list(regs.items()):
        cs = content_state(cam, st, note, timer)
        prev = _last.get(token)
        if prev == cs:
            continue                                   # 이 폰의 표시 내용이 그대로면 푸시 생략
        _last[token] = cs
        if _active.get(token):
            continue                                   # 앱이 앞에 있음: 소켓으로 즉시 갱신·앱 햅틱 → 푸시(알림 진동) 생략
        aps = {"timestamp": now, "event": "update", "content-state": cs, "relevance-score": 100 if cs["state"] == "pgm" else 50}
        ps = (prev or {}).get("state")
        do_alert = alert_onair and _alerts.get(token, True)     # 이 폰의 알림 스위치 (루프 지역 변수 — 다른 폰에 영향 없음)
        m = _MSG.get(_lang.get(token, "ko"), _MSG["ko"])
        def alert(kind):
            t, b = m[kind]; a = {"title": t.format(cam=cam), "body": b}
            if _vib.get(token, True): a["sound"] = "default"      # 알림 진동 OFF면 조용히
            return a
        kind = "pgm" if (cs["state"] == "pgm" and ps != "pgm") else "idle" if (cs["state"] == "idle" and ps == "pgm") else "pvw" if (cs["state"] == "pvw" and ps not in ("pvw", "pgm")) else None
        if alert_onair and kind and _banner.get(token) and _push_tok.get(token):
            # 배너 모드: 일반 알림 1건(가로에서도 보임, 진동 1회) + 아일랜드는 알림 없이 갱신 (이중 진동 방지)
            t_, b_ = m[kind]
            tasks.append(_send_banner(token, _push_tok[token], t_.format(cam=cam), b_, f"tally-{cam}", kind, cam))
            tasks.append(_send(token, {"aps": aps}))
            continue
        if do_alert and cs["state"] == "pgm" and ps != "pgm":
            aps["alert"] = alert("pgm")                    # 온에어로 '바뀔 때'만 알림(진동 1회)
        elif do_alert and cs["state"] == "idle" and ps == "pgm":
            aps["alert"] = alert("idle")                   # 온에어 해제 = 진동 1회
        elif do_alert and cs["state"] == "pvw" and ps not in ("pvw", "pgm"):
            aps["alert"] = alert("pvw")
            tasks.append(_send_repeat(token, {"aps": aps}, times=3, gap=0.15))   # 프리뷰 진입 = 진동 3번(알림 푸시 3연타)
            continue
        tasks.append(_send(token, {"aps": aps}))
    if not tasks:
        return
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f"[apns] send error: {e!r}", flush=True)


async def end_room(room: str):
    """호스트가 탈리를 끝내면 활동도 종료 표시"""
    if not ENABLED:
        return
    regs = _tokens.get(room) or {}
    now = int(time.time())
    for token, cam in list(regs.items()):
        cs = content_state(cam, {"online": False}, None, None)
        await _send(token, {"aps": {"timestamp": now, "event": "end", "content-state": cs, "dismissal-date": now + 300}})


print(f"[apns] {'enabled' if ENABLED else 'not configured (등록만 받음)'} env={ENV} bundle={BUNDLE_ID}", flush=True)
