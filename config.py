#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

class Config:
    # 基础配置
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    
    # 项目路径配置
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    
    # Hadoop配置
    HADOOP_HOST = 'localhost'
    HADOOP_PORT = 9000
    HADOOP_USER = 'hadoop'  # Hadoop用户名
    HDFS_BASE_PATH = '/manufacture_data'  # HDFS上的基础数据目录
    
    # Spark配置
    SPARK_MASTER = 'local[*]'  # 本地模式，使用所有可用核心
    SPARK_APP_NAME = 'ManufactureAnalysis'
    SPARK_EXECUTOR_MEMORY = '2g'  # 执行器内存
    SPARK_DRIVER_MEMORY = '2g'    # 驱动程序内存
    SPARK_CONF = {
        'spark.executor.memory': SPARK_EXECUTOR_MEMORY,
        'spark.driver.memory': SPARK_DRIVER_MEMORY,
        'spark.driver.maxResultSize': '1g',
        'spark.python.worker.memory': '1g',
        'spark.local.dir': os.path.join(os.path.dirname(BASE_DIR), 'spark_temp'),
        'spark.sql.warehouse.dir': os.path.join(os.path.dirname(BASE_DIR), 'spark_warehouse'),
        # Hadoop集成配置
        'spark.hadoop.fs.defaultFS': f'hdfs://{HADOOP_HOST}:{HADOOP_PORT}',

    }
    
    # 数据处理配置
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 最大上传文件大小：16MB
    ALLOWED_EXTENSIONS = {'xls', 'xlsx'}    # 允许的文件类型
    
    @staticmethod
    def init_app(app):
        """初始化应用配置"""
        # 确保必要的目录存在
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(Config.SPARK_CONF['spark.local.dir']), exist_ok=True)
        os.makedirs(os.path.dirname(Config.SPARK_CONF['spark.sql.warehouse.dir']), exist_ok=True) 