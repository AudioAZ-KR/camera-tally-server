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
_token_device: dict[str, str] = {}  # token -> deviceId         # token -> 실제로 통한 환경. TestFlight/앱스토어=production, Xcode 직접 설치=sandbox — 둘 다 자동 처리
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
    for m in (_last, _env_of, _active, _alerts, _lang):
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


def set_active(token: str, active: bool, alerts=None):
    if token:
        _active[token] = bool(active)
        if alerts is not None: _alerts[token] = bool(alerts)


def set_alerts(token: str, alerts: bool):
    if token: _alerts[token] = bool(alerts)


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
        def alert(kind): t, b = m[kind]; return {"title": t.format(cam=cam), "body": b, "sound": "default"}
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
