# camera-tally-server

AudioAZ 카메라 탈리 **중계 서버**만 담은 배포 전용 리포입니다.
(호스트 앱·ATEM 클라이언트·라이선스 등 제품 소스는 포함하지 않습니다.)

- `server.py` — WebSocket 중계 서버 (aiohttp)
- `web/` — 카메라맨용 탈리 페이지(tally.html 등)
- Render(Docker)로 배포. 헬스체크 `/`.
