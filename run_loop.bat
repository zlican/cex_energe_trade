@echo off
:loop
energe.exe
echo 程序崩溃，5 秒后重启...
timeout /t 5
goto loop
