#!/usr/bin/env python3
import sys
import subprocess
import importlib.util

def is_installed(package):
    """检查包是否已安装"""
    if package == 'flask-cors':
        # flask-cors 的模块名是 flask_cors
        package_name = 'flask_cors'
    elif package == 'pyyaml':
        # pyyaml 的模块名是 yaml
        package_name = 'yaml'
    else:
        package_name = package
    
    try:
        spec = importlib.util.find_spec(package_name)
        return spec is not None
    except (ImportError, ModuleNotFoundError):
        return False

def main():
    required_packages = [
        'flask',
        'flask-cors', 
        'duckdb',
        'pyyaml',
    ]
    
    mirror_url = "https://mirrors.aliyun.com/pypi/simple/"
    trusted_host = "mirrors.aliyun.com"
    
    print()
    missing_packages = []
    
    for package in required_packages:
        if is_installed(package):
            print(f"      {package} 已就绪 ✓")
        else:
            print(f"      {package} 未安装")
            missing_packages.append(package)
    
    # 检查 xtquant（可选）
    if is_installed('xtquant'):
        print(f"      xtquant 已就绪 ✓")
    else:
        print()
        print(f"[提示] xtquant (QMT数据源) 未安装")
        print(f"      正在尝试自动安装...")
        
        try:
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install', 'xtquant',
                '-i', mirror_url,
                '--trusted-host', trusted_host
            ], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            print(f"      xtquant 安装完成！")
        except subprocess.CalledProcessError:
            print()
            print(f"[警告] xtquant 自动安装失败")
            print(f"      下载功能需要国金QMT/miniQMT客户端及xtquant SDK")
            print(f"      但Web界面仍可正常浏览现有数据")
            print()
    
    # 安装缺失的必需包
    if missing_packages:
        print()
        print(f"      正在自动安装缺失依赖，请稍候...")
        
        try:
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install'
            ] + missing_packages + [
                '-i', mirror_url,
                '--trusted-host', trusted_host
            ], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            print(f"      依赖安装完成！")
        except subprocess.CalledProcessError:
            print(f"[错误] 依赖安装失败，请检查网络连接")
            print()
            print(f"      请手动运行：")
            print(f"      python -m pip install {' '.join(missing_packages)} -i {mirror_url} --trusted-host {trusted_host}")
            print()
            return 1
    
    print()
    return 0

if __name__ == '__main__':
    sys.exit(main())
