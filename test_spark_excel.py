#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试Spark读取Excel文件
"""

import os
import sys
from app.models.spark_utils import init_spark_session, read_excel_from_hdfs

def test_read_excel():
    """测试从HDFS读取Excel文件"""
    spark = None
    try:
        # 初始化Spark会话
        spark = init_spark_session()
        
        if not spark:
            print("Spark会话初始化失败，无法进行测试")
            return
        
        # 测试文件路径
        hdfs_path = "hdfs://localhost:9000/manufacture_data/分省年度数据-GDP.xls"
        
        # 检查文件是否存在
        import subprocess
        cmd = f"hadoop fs -test -e {hdfs_path}"
        process = subprocess.run(cmd, shell=True)
        
        if process.returncode != 0:
            print(f"HDFS文件不存在: {hdfs_path}")
            print("尝试列出可用文件...")
            
            cmd = f"hadoop fs -ls hdfs://localhost:9000/manufacture_data/"
            process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
            print(process.stdout.decode('utf-8'))
            return
        
        # 尝试使用pandas直接读取
        try:
            print("\n尝试使用pandas直接读取...")
            
            # 从HDFS下载文件到临时目录
            import tempfile
            
            temp_dir = tempfile.gettempdir()
            temp_file = os.path.join(temp_dir, os.path.basename(hdfs_path))
            
            # 使用hadoop命令下载文件
            cmd = f"hadoop fs -get {hdfs_path} {temp_file}"
            process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if process.returncode != 0:
                print(f"无法从HDFS下载文件: {process.stderr.decode('utf-8')}")
            else:
                # 使用pandas读取
                import pandas as pd
                if temp_file.endswith('.xlsx'):
                    df_pandas = pd.read_excel(temp_file)
                else:
                    df_pandas = pd.read_excel(temp_file, engine='xlrd')
                
                print(f"pandas读取成功，数据形状: {df_pandas.shape}")
                print(f"列名: {df_pandas.columns.tolist()}")
                print(f"前5行数据:\n{df_pandas.head()}")
                
                # 删除临时文件
                os.remove(temp_file)
        except Exception as e:
            print(f"pandas读取失败: {str(e)}")
        
        # 尝试使用Spark读取
        sheet_names = ["Sheet1", "Sheet 1", "工作表1", None]
        
        for sheet_name in sheet_names:
            try:
                print(f"\n尝试读取工作表: {sheet_name}")
                df = read_excel_from_hdfs(spark, hdfs_path, sheet_name=sheet_name)
                
                # 打印数据信息
                print(f"成功读取Excel，行数: {df.count()}")
                print(f"列名: {df.columns}")
                
                # 显示数据预览
                print("\n数据预览:")
                df.show(5, truncate=False)
                
                # 成功读取后退出循环
                break
            except Exception as e:
                print(f"读取工作表 {sheet_name} 失败: {str(e)}")
                continue
    
    finally:
        # 确保在任何情况下都关闭Spark会话
        if spark:
            try:
                spark.stop()
                print("Spark会话已关闭")
            except:
                pass

if __name__ == "__main__":
    test_read_excel() 