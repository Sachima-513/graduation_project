#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
import sys
import time
from pyhdfs import HdfsClient
from config import Config

def check_hadoop_available():
    """检查Hadoop是否可用"""
    try:
        # 尝试ping HDFS
        subprocess.run(["hadoop", "fs", "-ls", "/"], 
                       stdout=subprocess.PIPE, 
                       stderr=subprocess.PIPE, 
                       check=True,
                       shell=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def check_safe_mode():
    """检查HDFS是否处于安全模式"""
    try:
        result = subprocess.run("hadoop dfsadmin -safemode get", 
                               shell=True, 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.PIPE, 
                               universal_newlines=True)
        return "Safe mode is ON" in result.stdout
    except subprocess.CalledProcessError:
        return False

def leave_safe_mode():
    """尝试退出安全模式"""
    print("ℹ️ 正在尝试退出安全模式...")
    try:
        result = subprocess.run("hadoop dfsadmin -safemode leave", 
                               shell=True, 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.PIPE, 
                               universal_newlines=True,
                               check=True)
        if "Safe mode is OFF" in result.stdout:
            print("✅ 已成功退出安全模式")
            return True
        else:
            print(f"⚠️ 无法确认安全模式状态: {result.stdout}")
            return False
    except subprocess.CalledProcessError as e:
        print(f"❌ 退出安全模式失败: {e}")
        return False

def create_hdfs_dirs():
    """创建HDFS目录结构"""
    # 检查安全模式
    if check_safe_mode():
        print("⚠️ HDFS当前处于安全模式，尝试退出...")
        if not leave_safe_mode():
            print("✳️ 提示: 如果您没有管理员权限，可能需要以管理员身份运行或等待安全模式自动退出")
            print("✳️ 您可以尝试手动执行: hadoop dfsadmin -safemode leave")
            user_input = input("是否继续尝试创建目录? (y/n): ")
            if user_input.lower() != 'y':
                return False
    
    try:
        cmd = f"hadoop fs -mkdir -p {Config.HDFS_BASE_PATH}"
        subprocess.run(cmd, shell=True, check=True)
        print(f"✅ 已创建HDFS基础目录: {Config.HDFS_BASE_PATH}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ 创建HDFS目录失败: {e}")
        
        # 如果失败，检查错误是否是权限问题
        if "Permission denied" in str(e):
            print("⚠️ 可能是权限问题，请确保您有权限写入HDFS")
        elif "Name node is in safe mode" in str(e):
            print("⚠️ Hadoop仍处于安全模式，无法创建目录")
            print("✳️ 提示: 您可以等待一段时间再尝试，安全模式通常会自动退出")
            print("✳️ 或者以管理员身份运行: hadoop dfsadmin -safemode leave")
        
        return False

def upload_file_to_hdfs(local_path, hdfs_path):
    """将本地文件上传到HDFS"""
    try:
        cmd = f'hadoop fs -put -f "{local_path}" "{hdfs_path}"'
        subprocess.run(cmd, shell=True, check=True)
        file_size = os.path.getsize(local_path) / 1024  # KB
        print(f"✅ 已上传: {os.path.basename(local_path)} ({file_size:.2f} KB)")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ 上传失败 {os.path.basename(local_path)}: {e}")
        return False

def check_hdfs_file_exists(hdfs_path):
    """检查HDFS文件是否存在"""
    try:
        cmd = f'hadoop fs -test -e "{hdfs_path}"'
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0
    except Exception:
        return False

def upload_data_directory():
    """上传data目录中的所有Excel文件到HDFS"""
    if not check_hadoop_available():
        print("❌ 无法连接到Hadoop。请确保Hadoop服务已启动。")
        return False
    
    print("ℹ️ Hadoop服务已连接")
    
    if not create_hdfs_dirs():
        return False
    
    # 获取本地数据文件列表
    data_dir = Config.DATA_DIR
    if not os.path.exists(data_dir):
        print(f"❌ 本地数据目录不存在: {data_dir}")
        return False
    
    files = [f for f in os.listdir(data_dir) 
             if f.endswith(('.xls', '.xlsx')) and os.path.isfile(os.path.join(data_dir, f))]
    
    if not files:
        print(f"⚠️ 未在 {data_dir} 中找到Excel文件")
        return False
    
    print(f"ℹ️ 发现 {len(files)} 个Excel文件，准备上传...")
    
    # 上传文件到HDFS
    successful_uploads = 0
    for filename in files:
        local_path = os.path.join(data_dir, filename)
        hdfs_path = f"{Config.HDFS_BASE_PATH}/{filename}"
        
        if upload_file_to_hdfs(local_path, hdfs_path):
            successful_uploads += 1
    
    print(f"\n🎉 上传完成! 成功上传 {successful_uploads}/{len(files)} 个文件到 {Config.HDFS_BASE_PATH}")
    print(f"📋 可以通过以下命令查看文件列表:")
    print(f"   hadoop fs -ls {Config.HDFS_BASE_PATH}")
    
    return successful_uploads == len(files)

def use_local_mode_fallback():
    """创建一个使用本地模式的配置备份，以防HDFS无法访问"""
    print("ℹ️ 正在为Spark创建本地模式备份选项...")
    
    # 创建备份目录
    local_data_dir = os.path.join(os.path.dirname(Config.BASE_DIR), 'local_data_backup')
    os.makedirs(local_data_dir, exist_ok=True)
    
    # 将反斜杠替换为正斜杠
    local_data_dir_path = local_data_dir.replace('\\', '/')
    
    # 更新配置文件
    with open(os.path.join(os.path.dirname(Config.BASE_DIR), 'local_mode.py'), 'w') as f:
        f.write(f"""
# 本地模式配置，当HDFS不可用时使用
LOCAL_DATA_DIR = '{local_data_dir_path}'
USE_LOCAL_MODE = True  # 设置为True表示使用本地模式
""")
    
    print(f"✅ 已创建本地模式备份配置: {local_data_dir}")
    print("✳️ 如果需要使用本地模式，请在应用中导入local_mode并检查USE_LOCAL_MODE标志")

if __name__ == "__main__":
    print("🚀 开始将制造业数据上传到HDFS...")
    
    # 如果第一次尝试失败，询问是否使用本地模式
    if not upload_data_directory():
        print("\n⚠️ HDFS上传遇到问题")
        user_input = input("您希望为应用创建本地模式备份吗? (y/n): ")
        if user_input.lower() == 'y':
            use_local_mode_fallback()
    
    print("\n✨ 完成! 您可以运行应用程序或查看HDFS上的数据。") 