#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

# 设置Python路径环境变量
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from flask import Flask
from app.routes import register_routes
from app.models.data_processor import DataProcessor
from app.models.spark_utils import init_spark_session

# 创建Flask应用
app = Flask(__name__, 
            static_folder='app/static',
            template_folder='app/templates')

# 注册路由
register_routes(app)

# 初始化数据处理器
data_processor = DataProcessor()

# 在应用程序启动时创建一个全局的Spark会话
global_spark = init_spark_session()

def cleanup():
    """清理函数，确保应用程序退出时正确关闭资源"""
    try:
        if hasattr(data_processor, 'close'):
            data_processor.close()
        
        # 在应用程序退出时关闭全局Spark会话
        global global_spark
        if global_spark:
            global_spark.stop()
            print("全局Spark会话已关闭")
    except:
        pass

# 注册清理函数
import atexit
atexit.register(cleanup)

if __name__ == '__main__':
    try:
        # 打印环境信息
        print(f"Python版本: {sys.version}")
        print(f"Python路径: {sys.executable}")
        print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', '未设置')}")
        print(f"PYSPARK_DRIVER_PYTHON: {os.environ.get('PYSPARK_DRIVER_PYTHON', '未设置')}")
        
        # 启动应用
        app.run(debug=True)
    finally:
        cleanup() 