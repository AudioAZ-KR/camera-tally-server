# Flare Tally — 중계 서버 + 스마트폰 웹 클라이언트

Flare Tally(맥 호스트 앱)와 카메라맨 스마트폰 사이를 잇는 WebSocket 중계 서버와, 폰에서 열리는 탈리·큐 라이트 웹 페이지입니다.

- 서비스 주소: https://camera-tally.onrender.com
- 호스트 앱 다운로드: https://audioazpro.com/product-flare-tally.html

## 구성
- `server.py` — aiohttp 서버. `/ws`로 호스트(브릿지)와 폰을 방 코드별로 중계, 공지·타이머·큐 라이트 상태 보관
- `web/` — `index.html`(방 만들기·접속), `tally.html`(탈리 화면), `cue.html`·`cue-op.html`(Flare Cue)

## 실행
```bash
pip install -r requirements.txt
python server.py            # PORT 환경변수, 기본 8080
```

Render 배포는 `render.yaml`(Docker)로 자동. 호스트 앱 소스는 이 저장소에 포함되지 않습니다.
