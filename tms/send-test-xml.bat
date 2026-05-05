@echo off
title PAS-SCADA Kafka Bridge - TEST XML SENDER
color 0B

echo ============================================================
echo   SEND TEST XML to Artemis (Simulate TMS Server)
echo ============================================================
echo.
echo  NOTE: TestXmlPublisher removed (production code only).
echo.
echo  To test in LOCAL mode:
echo  ─────────────────────────────────────────────────────────
echo  Use Artemis Web Console to manually publish XML:
echo.
echo    1. Open browser: http://localhost:8161
echo    2. Login: pasbridge / testpass123
echo    3. Go to: Addresses tab
echo    4. Click topic: TMS.PISInfo
echo    5. Click: Send Message
echo    6. Paste XML and click Send
echo.
echo  Sample XML for TMS.PISInfo (ATRTimeTable):
echo  ─────────────────────────────────────────────────────────
echo  ^<ATRTimeTable^>
echo    ^<dateTime^>20260411T140000^</dateTime^>
echo    ^<StartIndex^>0^</StartIndex^>
echo    ^<TotalCount^>1^</TotalCount^>
echo    ^<GraphID^>42^</GraphID^>
echo    ^<Trains^>
echo      ^<Tg^>
echo        ^<TTGUID^>dc-occ.eclrt-train-6250-guid^</TTGUID^>
echo        ^<TripNo^>678^</TripNo^>
echo        ^<CTD lpid="101" tn="678"/^>
echo        ^<Evts F="3" Id="2201" As="3600" Ds="3660"/^>
echo      ^</Tg^>
echo    ^</Trains^>
echo  ^</ATRTimeTable^>
echo.
echo  In PRODUCTION:
echo  ─────────────────────────────────────────────────────────
echo  Real TMS server sends XML automatically.
echo  No manual step needed.
echo.
pause
