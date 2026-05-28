
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
诊断脚本来查看为什么无法进行全量下载
"""
import os
import sys

def check_config_paths():
    print("="*60)
    print("1. 检查配置路径")
    print("="*60)
    
    from config import (
        QMT_CODE_LIST_DATA_DIR,
        QMT_DATA_DIR,
        STOCK_DB_PATH,
        ETF_DB_PATH
    )
    
    paths_to_check = [
        ("QMT代码列表目录", QMT_CODE_LIST_DATA_DIR),
        ("QMT数据目录", QMT_DATA_DIR),
        ("股票数据库路径", STOCK_DB_PATH),
        ("ETF数据库路径", ETF_DB_PATH),
    ]
    
    for name, path in paths_to_check:
        if path:
            exists = os.path.exists(path)
            is_dir = os.path.isdir(path) if exists else False
            print(f"  {name}: {path}")
            print(f"    存在: {exists}, 是目录: {is_dir}")
        else:
            print(f"  {name}: 未配置")
    print()

def check_xtquant():
    print("="*60)
    print("2. 检查 xtquant 模块")
    print("="*60)
    
    try:
        from xtquant import xtdata
        print("  ✅ xtquant 模块已安装")
        
        # 尝试连接
        from config import QMT_IP, QMT_PORT
        print(f"  尝试连接 QMT: {QMT_IP}:{QMT_PORT}")
        
        if hasattr(xtdata, "connect"):
            result = xtdata.connect(QMT_IP, int(QMT_PORT) if QMT_PORT else None)
            print(f"  ✅ 连接结果: {result}")
        else:
            print(f"  ⚠️ xtdata.connect 方法不存在，可能版本差异")
            
        # 检查数据目录
        if hasattr(xtdata, "get_data_dir"):
            try:
                data_dir = xtdata.get_data_dir()
                print(f"  xtquant数据目录: {data_dir}")
                print(f"  目录存在: {os.path.exists(data_dir) if data_dir else False}")
            except Exception as e:
                print(f"  ⚠️ 获取数据目录失败: {e}")
        return True
        
    except ImportError as e:
        print(f"  ❌ xtquant 模块未安装: {e}")
        print("\n  提示: 请确保已安装国金QMT/miniQMT及对应的Python SDK")
        return False

def test_get_stock_list():
    print("\n" + "="*60)
    print("3. 测试获取股票列表")
    print("="*60)
    
    try:
        from data_source import QMTClient
        client = QMTClient()
        client.login()
        
        print("\n  尝试获取股票列表...")
        stocks = client.get_stock_list()
        print(f"  ✅ 获取到 {len(stocks)} 只股票")
        if len(stocks) > 0:
            print(f"  前5只: {list(stocks.head().code)}")
            
        print("\n  尝试获取ETF列表...")
        etfs = client.get_etf_list()
        print(f"  ✅ 获取到 {len(etfs)} 只ETF")
        if len(etfs) > 0:
            print(f"  前5只: {list(etfs.head().code)}")
            
        return True
        
    except Exception as e:
        print(f"  ❌ 获取失败: {e}")
        import traceback
        print(f"  {traceback.format_exc()}")
        return False

def main():
    print("\n" + "="*60)
    print("  数据下载问题诊断工具")
    print("="*60 + "\n")
    
    check_config_paths()
    
    xt_ok = check_xtquant()
    
    if xt_ok:
        test_get_stock_list()
    
    print("\n" + "="*60)
    print("  诊断完成")
    print("="*60)

if __name__ == "__main__":
    main()

