#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
下载Spark Excel读取所需的所有JAR文件
"""

import os
import sys
import requests
import shutil
from tqdm import tqdm

# JAR文件信息：(文件名, 下载URL)
JAR_FILES = [
    # 使用旧版本的POI (4.1.2)，这个版本与XMLBeans兼容性更好
    ("poi-4.1.2.jar", "https://repo1.maven.org/maven2/org/apache/poi/poi/4.1.2/poi-4.1.2.jar"),
    ("poi-ooxml-4.1.2.jar", "https://repo1.maven.org/maven2/org/apache/poi/poi-ooxml/4.1.2/poi-ooxml-4.1.2.jar"),
    ("poi-ooxml-schemas-4.1.2.jar", "https://repo1.maven.org/maven2/org/apache/poi/poi-ooxml-schemas/4.1.2/poi-ooxml-schemas-4.1.2.jar"),
    
    # XMLBeans 3.1.0 (与POI 4.1.2兼容)
    ("xmlbeans-3.1.0.jar", "https://repo1.maven.org/maven2/org/apache/xmlbeans/xmlbeans/3.1.0/xmlbeans-3.1.0.jar"),
    
    # 其他依赖
    ("commons-compress-1.19.jar", "https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.19/commons-compress-1.19.jar"),
    ("commons-collections4-4.4.jar", "https://repo1.maven.org/maven2/org/apache/commons/commons-collections4/4.4/commons-collections4-4.4.jar"),
    ("commons-math3-3.6.1.jar", "https://repo1.maven.org/maven2/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar"),
    ("curvesapi-1.06.jar", "https://repo1.maven.org/maven2/com/github/virtuald/curvesapi/1.06/curvesapi-1.06.jar"),
    
    # Spark Excel读取库 (使用与POI 4.1.2兼容的版本)
    ("spark-excel_2.12-0.13.7.jar", "https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/0.13.7/spark-excel_2.12-0.13.7.jar"),
    
    # 日志库
    ("commons-logging-1.2.jar", "https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar"),
    ("commons-io-2.6.jar", "https://repo1.maven.org/maven2/commons-io/commons-io/2.6/commons-io-2.6.jar"),
    ("commons-codec-1.13.jar", "https://repo1.maven.org/maven2/commons-codec/commons-codec/1.13/commons-codec-1.13.jar"),
    
    # Log4j
    ("log4j-api-2.17.2.jar", "https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.17.2/log4j-api-2.17.2.jar"),
    ("log4j-core-2.17.2.jar", "https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.17.2/log4j-core-2.17.2.jar"),
]

def download_file(url, destination):
    """下载文件并显示进度条"""
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    with open(destination, 'wb') as f, tqdm(
            desc=os.path.basename(destination),
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
        for data in response.iter_content(chunk_size=1024):
            size = f.write(data)
            bar.update(size)

def main():
    # 确定JAR文件目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    app_dir = os.path.join(script_dir, 'app')
    jars_dir = os.path.join(app_dir, 'jars')
    
    # 创建jars目录（如果不存在）
    os.makedirs(jars_dir, exist_ok=True)
    
    # 清空jars目录，确保没有版本冲突
    print(f"清空JAR文件目录: {jars_dir}")
    for file in os.listdir(jars_dir):
        file_path = os.path.join(jars_dir, file)
        if os.path.isfile(file_path) and file.endswith('.jar'):
            os.remove(file_path)
            print(f"已删除: {file}")
    
    print(f"下载JAR文件到: {jars_dir}")
    
    # 下载所有JAR文件
    for jar_file, url in JAR_FILES:
        jar_path = os.path.join(jars_dir, jar_file)
        
        print(f"下载: {jar_file}")
        try:
            download_file(url, jar_path)
        except Exception as e:
            print(f"下载 {jar_file} 失败: {str(e)}")
    
    print("所有JAR文件下载完成！")

if __name__ == "__main__":
    main() 