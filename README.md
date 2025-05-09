# 制造业数据分析平台

这是一个基于Flask+Hadoop+Spark+Bootstrap的制造业数据可视化与分析平台，使用机器学习方法对制造业相关数据进行分析。

## 功能特点

- **多源数据整合**：支持多种制造业指标数据集的加载和分析
- **机器学习分析**：K-means聚类和线性回归预测分析
- **大数据处理能力**：基于Hadoop和Spark的分布式数据处理
- **交互式可视化**：多种图表类型支持，直观展示数据特征
- **响应式界面**：基于Bootstrap 5的响应式设计，适应各种设备

## 技术架构

- **Web框架**：Flask (MVT架构)
- **前端**：Bootstrap 5 (在线CDN)
- **大数据处理**：Hadoop + Spark
- **机器学习**：Scikit-learn, Spark ML
- **数据可视化**：Matplotlib, Seaborn

## 项目结构

```
制造业数据分析平台/
│
├── app/                        # 应用主目录
│   ├── models/                 # 数据模型
│   │   ├── data_processor.py   # 数据处理器
│   │   ├── hadoop_utils.py     # Hadoop工具
│   │   └── spark_utils.py      # Spark工具
│   │
│   ├── static/                 # 静态文件
│   │   ├── css/                # CSS样式
│   │   ├── js/                 # JavaScript文件
│   │   └── images/             # 图片和生成的图表
│   │
│   ├── templates/              # HTML模板
│   │   ├── base.html           # 基础模板
│   │   ├── index.html          # 首页
│   │   ├── data_preview.html   # 数据预览页
│   │   ├── analysis.html       # 数据分析页
│   │   └── visualization.html  # 数据可视化页
│   │
│   ├── views/                  # 视图函数
│   │   └── views.py            # 路由和控制器
│   │
│   └── __init__.py             # 应用初始化
│
├── config/                     # 配置文件
│   └── config.py               # 应用配置
│
├── data/                       # 数据文件目录
│   └── *.xls, *.xlsx           # 数据文件
│
├── run.py                      # 应用入口
└── requirements.txt            # 依赖项
```

## 安装和运行

1. 首先安装所需依赖项：

```bash
pip install -r requirements.txt
```

2. 确保Hadoop和Spark环境已正确配置：

如果需要，修改 `config/config.py` 中的配置信息以匹配您的Hadoop和Spark环境。

3. 运行应用：

```bash
python run.py
```

4. 在浏览器中访问：

```
http://localhost:5000
```

## 数据分析功能

### 数据预览
- 查看原始数据和基本统计信息
- 自动处理数据中的缺失值和异常值

### 数据分析
- K-means聚类分析：发现数据中的自然分组
- 线性回归分析：根据特征预测目标变量

### 数据可视化
- 支持多种图表类型：柱状图、折线图、散点图、饼图、热力图
- 自定义标题和轴标签
- 导出图表为PNG格式 