@echo off
rem One-click: check/restore the scheduled workflow, then trigger once to verify
"C:\Users\Tsti\.workbuddy\binaries\python\versions\3.13.12\python.exe" "%~dp0enable_workflow.py" --trigger
pause
