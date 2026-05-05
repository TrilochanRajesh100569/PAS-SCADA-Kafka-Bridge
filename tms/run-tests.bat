@echo off
title PAS-SCADA Kafka Bridge - RUN TESTS
color 0D

echo ============================================================
echo   RUN UNIT TESTS (No Docker needed)
echo ============================================================
echo.
echo  Tests: XmlToJsonProcessorTest
echo   - ATRTimeTable convert to ICD JSON
echo   - SingleArrival convert to ICD JSON
echo   - SingleDeparture convert to ICD JSON
echo   - RouteInfo convert to ICD JSON
echo   - History events skipped correctly
echo.

cd /d "%~dp0"
mvn test

IF %ERRORLEVEL% EQU 0 (
    echo.
    echo  ALL TESTS PASSED
) ELSE (
    echo.
    echo  TESTS FAILED - check output above
)

echo.
pause
