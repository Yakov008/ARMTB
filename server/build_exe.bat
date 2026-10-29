@echo off
chcp 65001 >nul
rem Сборка LockServer.exe (один файл). Результат: dist\LockServer.exe
cd /d "%~dp0"
python -m pip install -r requirements.txt || goto :err
python -m PyInstaller --onefile --name LockServer --add-data "static;static" ^
  --collect-submodules uvicorn --collect-submodules websockets app.py || goto :err
echo.
echo Готово: %~dp0dist\LockServer.exe
echo Рядом с exe при первом запуске появятся devices.json, users.json и events.db
pause
exit /b 0
:err
echo Ошибка сборки
pause
exit /b 1
