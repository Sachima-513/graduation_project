#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复Python路径问题，确保PySpark使用正确的Python解释器
"""

import os
import sys
import subprocess
import platform

def get_python_version():
    """获取当前Python版本"""
    return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

def get_python_path():
    """获取当前Python解释器路径"""
    return sys.executable

def check_environment():
    """检查环境变量"""
    print("检查环境变量...")
    
    # 检查PYSPARK_PYTHON和PYSPARK_DRIVER_PYTHON环境变量
    pyspark_python = os.environ.get("PYSPARK_PYTHON", "未设置")
    pyspark_driver_python = os.environ.get("PYSPARK_DRIVER_PYTHON", "未设置")
    
    print(f"PYSPARK_PYTHON = {pyspark_python}")
    print(f"PYSPARK_DRIVER_PYTHON = {pyspark_driver_python}")
    
    # 获取当前Python信息
    current_python = get_python_path()
    current_version = get_python_version()
    
    print(f"当前Python解释器: {current_python}")
    print(f"当前Python版本: {current_version}")
    
    # 检查是否有多个Python版本
    if platform.system() == "Windows":
        try:
            # 检查Python 3.9
            result = subprocess.run(["where", "python"], capture_output=True, text=True)
            python_paths = result.stdout.strip().split("\n")
            
            print("\n系统中的Python解释器:")
            for path in python_paths:
                if os.path.exists(path):
                    try:
                        version = subprocess.run([path, "--version"], capture_output=True, text=True)
                        print(f"{path} - {version.stdout.strip()}")
                    except:
                        print(f"{path} - 无法获取版本")
        except:
            print("无法检查系统中的Python解释器")
    else:
        try:
            # 在Linux/Mac上使用which
            result = subprocess.run(["which", "-a", "python3"], capture_output=True, text=True)
            python_paths = result.stdout.strip().split("\n")
            
            print("\n系统中的Python解释器:")
            for path in python_paths:
                if os.path.exists(path):
                    try:
                        version = subprocess.run([path, "--version"], capture_output=True, text=True)
                        print(f"{path} - {version.stdout.strip()}")
                    except:
                        print(f"{path} - 无法获取版本")
        except:
            print("无法检查系统中的Python解释器")

def fix_environment():
    """修复环境变量"""
    print("\n修复环境变量...")
    
    # 获取当前Python解释器路径
    python_path = get_python_path()
    
    # 设置环境变量
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
    
    # 写入到.env文件
    with open(".env", "w") as f:
        f.write(f"PYSPARK_PYTHON={python_path}\n")
        f.write(f"PYSPARK_DRIVER_PYTHON={python_path}\n")
    
    print(f"已设置PYSPARK_PYTHON = {python_path}")
    print(f"已设置PYSPARK_DRIVER_PYTHON = {python_path}")
    print(f"这些设置已保存到.env文件中")
    
    # 提示用户
    if platform.system() == "Windows":
        print("\n请在命令行中运行以下命令设置环境变量:")
        print(f"set PYSPARK_PYTHON={python_path}")
        print(f"set PYSPARK_DRIVER_PYTHON={python_path}")
    else:
        print("\n请在终端中运行以下命令设置环境变量:")
        print(f"export PYSPARK_PYTHON={python_path}")
        print(f"export PYSPARK_DRIVER_PYTHON={python_path}")

def main():
    """主函数"""
    print("Python路径修复工具")
    print("=" * 50)
    
    # 检查环境
    check_environment()
    
    # 修复环境
    fix_environment()
    
    print("\n完成！请重新启动应用程序以应用更改。")

if __name__ == "__main__":
    main() 