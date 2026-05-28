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

echo [0/5] 更新 pip...
python -m pip install --upgrade pip -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com
echo.

:: 使用 Python 脚本检查和安装依赖
echo [1/5] 检查依赖包...
python check_deps.py
if errorlevel 1 (
    echo.
    echo [错误] 依赖检查/安装失败
    echo.
    pause
    exit /b 1
)
echo.

echo [2/5] 启动后端服务 (端口 5678)...
echo.
echo ============================================
echo   服务已启动！稍后浏览器将自动打开
echo   访问地址: http://localhost:5678
echo   关闭此窗口将停止服务
echo ============================================
echo.

echo [3/5] 正在打开浏览器...
:: 延迟2秒后打开浏览器，避免服务未就绪
timeout /t 2 /nobreak >nul
start "" http://localhost:5678
echo.

echo [4/5] 启动后端服务...
echo.

:: 启动后端服务（保持窗口，阻塞在此）
python viewer/server.py

if errorlevel 1 (
    echo.
    echo [错误] 服务异常退出！
    echo.
    pause
)

echo.
echo [提示] 服务已停止。
pause
