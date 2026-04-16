# ATS数据监控平台

## 项目结构

```
D:\Project\test1\
├── index.html           # 前端页面
├── backend.py           # 后端API服务 (FastAPI)
├── requirements.txt    # Python依赖
├── pgsql-config.json    # 数据库配置文件
├── start.bat           # Windows启动脚本
└── SPEC.md             # 规格说明
```

## 快速启动

双击运行 `start.bat` 即可启动所有服务。

## 手动启动

### 1. 安装Python依赖

```bash
pip install -r requirements.txt
```

### 2. 配置数据库

编辑 `pgsql-config.json` 或通过前端界面配置：

```json
{
    "host": "localhost",
    "port": 5432,
    "database": "ats_monitor",
    "user": "postgres",
    "password": "your_password"
}
```

### 3. 启动后端服务

```bash
python backend.py
```

### 4. 启动前端服务

```bash
python -m http.server 8080
```

## 访问地址

- **前端页面**: http://localhost:8080
- **后端API**: http://localhost:8000
- **API文档**: http://localhost:8000/docs

## API接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/dashboard` | GET | 获取仪表盘统计数据 |
| `/api/services` | GET | 获取服务列表 |
| `/api/lines` | GET | 获取线路列表 |
| `/api/query` | POST | 执行SQL查询 |
| `/health` | GET | 健康检查 |

## 数据库表要求

### ats_information_status

```sql
CREATE TABLE ats_information_status (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    line_name VARCHAR(100),
    type VARCHAR(50),
    status VARCHAR(50),
    latency INTEGER,
    daily_volume BIGINT,
    update_time TIMESTAMP DEFAULT NOW()
);
```

### 线路数量查询

```sql
SELECT COUNT(DISTINCT line_name) FROM ats_information_status;
```

## 技术栈

- **前端**: HTML5 + CSS3 + ECharts
- **后端**: Python FastAPI
- **数据库**: PostgreSQL
