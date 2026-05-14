@echo off
chcp 65001 >nul
title 数据可视化查看器

cd /d %~dp0

echo ============================================
echo   数据可视化查看器 - 正在启动...
echo ============================================
echo.

:: 检查 Python 是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到 Python，请先安装 Python 3.8+
    echo 下载地址: https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

echo [1/3] 检查依赖包...

:: 检查 flask 是否已安装
python -m pip show flask >nul 2>&1
if errorlevel 1 (
    echo       flask 未安装，正在自动安装依赖，请稍候...
    python -m pip install flask flask-cors duckdb
    if errorlevel 1 (
        echo [错误] 依赖安装失败，请检查网络连接或手动运行：
        echo       python -m pip install flask flask-cors duckdb
        echo.
        pause
        exit /b 1
    )
    echo       依赖安装完成！
) else (
    echo       依赖已就绪 ✓
)

:: 检查 duckdb 是否已安装
python -m pip show duckdb >nul 2>&1
if errorlevel 1 (
    echo       duckdb 未安装，正在安装...
    python -m pip install duckdb
)

echo.
echo [2/3] 启动后端服务 (端口 5678)...
echo.
echo ============================================
echo   服务已启动！稍后浏览器将自动打开
echo   访问地址: http://localhost:5678
echo   关闭此窗口将停止服务
echo ============================================
echo.

:: 先在后台启动浏览器（延迟3秒，等服务器就绪）
start /b cmd /c "timeout /t 3 /nobreak >nul && start \"\" http://localhost:5678"

echo [3/3] 正在打开浏览器...
echo.

:: 启动后端服务（保持窗口，阻塞在此）
python viewer/server.py

echo.
echo [提示] 服务已停止。
pause
