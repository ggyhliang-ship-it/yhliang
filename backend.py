"""
ATS数据监控运维平台 - 后端服务
使用FastAPI框架，提供数据查询API
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import asynccontextmanager
import json
import os

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ats_monitor",
    "user": "postgres",
    "password": ""
}

def load_db_config():
    """从配置文件加载数据库配置"""
    config_file = os.path.join(os.path.dirname(__file__), "pgsql-config.json")
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            if config.get("host"):
                DB_CONFIG["host"] = config["host"]
                DB_CONFIG["port"] = int(config.get("port", 5432))
                DB_CONFIG["database"] = config.get("database", "ats_monitor")
                DB_CONFIG["user"] = config.get("user", "postgres")
                DB_CONFIG["password"] = config.get("password", "")
                DB_CONFIG["schema"] = config.get("schema", "public")

def get_db_connection():
    """获取数据库连接"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

def execute_query(sql: str):
    """执行SQL查询"""
    conn = get_db_connection()
    if not conn:
        return {"success": False, "error": "数据库连接失败", "data": []}
    
    try:
        with conn.cursor() as cursor:
            # 设置schema
            schema = DB_CONFIG.get("schema", "public")
            cursor.execute(f'SET search_path TO {schema}')
            
            cursor.execute(sql)
            if cursor.description:
                results = cursor.fetchall()
                return {"success": True, "data": [dict(row) for row in results]}
            else:
                conn.commit()
                return {"success": True, "affected_rows": cursor.rowcount, "data": []}
    except Exception as e:
        return {"success": False, "error": str(e), "data": []}
    finally:
        conn.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时加载配置
    load_db_config()
    yield

app = FastAPI(
    title="ATS数据监控运维平台API",
    description="提供数据查询接口服务",
    version="1.1.0",
    lifespan=lifespan
)

# 允许跨域
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    sql: str
    params: Optional[dict] = None
    session_id: Optional[str] = None

@app.get("/")
async def root():
    return {"message": "ATS数据监控运维平台API服务", "version": "1.1.0"}

@app.get("/health")
async def health_check():
    """健康检查"""
    conn = get_db_connection()
    if conn:
        conn.close()
        return {"status": "healthy", "database": "connected"}
    return {"status": "unhealthy", "database": "disconnected"}

@app.get("/config")
async def get_config():
    """获取数据库配置（不含密码）"""
    return {
        "host": DB_CONFIG.get("host", ""),
        "port": DB_CONFIG.get("port", 5432),
        "database": DB_CONFIG.get("database", ""),
        "user": DB_CONFIG.get("user", ""),
        "schema": DB_CONFIG.get("schema", "public"),
        "configured": bool(DB_CONFIG.get("host"))
    }

@app.post("/config")
async def update_config(config: dict):
    """更新数据库配置"""
    global DB_CONFIG
    DB_CONFIG["host"] = config.get("host", "localhost")
    DB_CONFIG["port"] = int(config.get("port", 5432))
    DB_CONFIG["database"] = config.get("database", "ats_monitor")
    DB_CONFIG["user"] = config.get("user", "postgres")
    DB_CONFIG["password"] = config.get("password", "")
    DB_CONFIG["schema"] = config.get("schema", "public")
    
    # 保存到配置文件
    config_file = os.path.join(os.path.dirname(__file__), "pgsql-config.json")
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(DB_CONFIG, f, ensure_ascii=False, indent=2)
    
    return {"success": True, "message": "配置已更新"}

@app.post("/api/query")
async def query_data(request: QueryRequest):
    """执行SQL查询"""
    sql = request.sql.strip()
    
    # 检测是否需要将自然语言转换为SQL
    keywords = ["查询", "多少", "有几个", "查看", "获取", "显示", "统计", "线路", "服务", "在线", "离线", "报警", "运行图", "时刻表"]
    is_natural = any(kw in sql for kw in keywords) and not sql.lower().startswith("select")
    
    if is_natural:
        sql = convert_natural_to_sql(sql)
        if not sql:
            return {"success": False, "error": "无法理解您的问题，请尝试更具体的描述", "data": []}
    
    # 简单的SQL注入防护
    dangerous_keywords = ["drop", "delete", "update", "insert", "alter", "truncate", "create"]
    sql_lower = sql.lower()
    for keyword in dangerous_keywords:
        if keyword in sql_lower:
            raise HTTPException(status_code=400, detail=f"不允许执行包含 '{keyword}' 的SQL语句")
    
    result = execute_query(sql)
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
    return result

def convert_natural_to_sql(question: str) -> str:
    """将自然语言问题转换为SQL查询"""
    question = question.lower().strip()
    schema = DB_CONFIG.get("schema", "public")
    
    # 线路相关
    if "线路数量" in question or "有多少线路" in question or "线路数" in question:
        return f"SELECT COUNT(DISTINCT line_name) as count FROM {schema}.ats_information_status"
    
    # 服务相关
    if "总服务" in question or "所有服务" in question or "服务列表" in question:
        return f"SELECT id, line_name, server_name, server_id, status FROM {schema}.ats_information_status ORDER BY id"
    
    if "在线服务" in question or "在线数" in question:
        return f"SELECT COUNT(*) as count FROM {schema}.ats_information_status WHERE status = 1"
    
    if "离线服务" in question or "离线数" in question:
        return f"SELECT COUNT(*) as count FROM {schema}.ats_information_status WHERE status = 0"
    
    # 线路列表
    if "线路列表" in question or "有哪些线路" in question:
        return f"SELECT DISTINCT line_name FROM {schema}.ats_information_status ORDER BY line_name"
    
    # 报警相关
    if "报警" in question:
        return f"SELECT id, line_name, alarm_content, gen_time FROM {schema}.message_alarm ORDER BY gen_time DESC LIMIT 100"
    
    # 时刻表相关
    if "时刻表" in question or "运行图" in question:
        return f"SELECT id, line_name, inused_schedule_name, inused_date FROM {schema}.inused_schedule_parameter WHERE valid = 1 AND inused_date = CURRENT_DATE ORDER BY line_name"
    
    # 默认返回空，让前端判断为通用问题
    return ""

# 通用问答知识库
GENERAL_KNOWLEDGE = {
    "你好|hello|hi": "您好！我是ATS数据监控运维平台的智能助手，我可以帮您查询业务数据或回答日常问题。请告诉我您想了解什么？",
    "你是谁": "我是ATS数据监控运维平台的智能问数助手，可以回答日常问题，也可以查询数据库中的业务数据。",
    "帮助|help|如何使用": "您可以用自然语言提问，例如：\n- 查询线路数量\n- 查看在线服务\n- 有哪些���路\n- 报警情况如何\n也可以直接输入SQL语句查询。",
    "平台功能": "本平台提供以下功能：\n- 监控线路和服务状态\n- 时刻表接入统计\n- 实时报警统计\n- 智能问数查询\n您可以在首页查看监控数据，或使用智能问数功能查询。",
    "系统状态": "您可以在顶部状态栏查看数据库连接状态，确保数据库已配置并连接成功。",
    "感谢": "不客气！很高兴为您服务。",
    "再见": "再见！有问题随时找我。",
    "天气": "抱歉，我无法查询天气信息。但我可以帮您查询业务相关的数据。",
    "时间|现在几点": "抱歉，我没有获取当前时间的功能。但您可以在页面顶部查看系统时间。",
    "状态|监控": "本平台实时监控各线路和服务状态，您可以在首页查看线路数量、在线/离线服务数等统计信息。",
    "默认": "抱歉，我不太理解您的问题。您可以尝试：\n1. 查询线路数量\n2. 查看在线服务\n3. 有哪些线路\n4. 报警情况\n或者输入具体的SQL语句。"
}

@app.post("/api/chat")
async def chat(request: QueryRequest):
    """智能问答 - 带上下文的深度理解"""
    question = request.sql.strip()
    question_lower = question.lower()
    schema = DB_CONFIG.get("schema", "public")

    # ===== 获取上下文 =====
    session_id = request.session_id if request.session_id else f"session_{hash(question) % 10000}"
    context = get_session_context(session_id)

    # ===== 深度分析问题 =====
    analysis = deep_analyze_question(question_lower, context)

    # ===== 构建SQL并执行 =====
    sql_result = build_intelligent_sql(question_lower, schema, context)
    
    if sql_result["sql"]:
        dangerous_keywords = ["drop", "delete", "update", "insert", "alter", "truncate", "create"]
        if any(kw in sql_result["sql"].lower() for kw in dangerous_keywords):
            return {"success": False, "error": "不允许执行危险的SQL语句"}

        result = execute_query(sql_result["sql"])
        if result["success"]:
            answer = format_result_naturally(result, question_lower, sql_result["intent"])
            # 保存到上下文
            save_to_context(session_id, question, answer, result.get("data", []))
            return {
                "success": True,
                "analysis": analysis,
                "intent": sql_result["intent"],
                "sql": sql_result["sql"],
                "answer": answer,
                "data": result.get("data", [])[:10],
                "history": context.get("history", [])[-5:]  # 返回最近5条历史
            }
        else:
            return {"success": False, "error": result["error"], "analysis": analysis}

    # ===== 知识库回答 =====
    knowledge =search_knowledge_base(question_lower)
    if knowledge:
        return {
            "success": True,
            "analysis": analysis,
            "intent": "knowledge",
            "answer": knowledge
        }

    # ===== 模糊匹配 =====
    fuzzy = fuzzy_match(question_lower, context)
    if fuzzy:
        return {
            "success": True,
            "analysis": analysis,
            "intent": "fuzzy",
            "answer": fuzzy
        }

    # ===== 智能建议 =====
    suggestions = get_suggestions(question_lower, context)
    return {
        "success": True,
        "analysis": analysis,
        "intent": "unknown",
        "answer": f"我理解您想问「{question}」，但需要更多信息。{suggestions}"
    }

# ===== 上下文管理 =====
SESSION_CONTEXTS = {}

def get_session_context(session_id: str) -> dict:
    """获取会话上下文"""
    if session_id not in SESSION_CONTEXTS:
        SESSION_CONTEXTS[session_id] = {"history": [], "last_topic": None, "last_result": None}
    return SESSION_CONTEXTS[session_id]

def save_to_context(session_id: str, question: str, answer: str, result_data: list = None):
    """保存到上下文"""
    if session_id and session_id in SESSION_CONTEXTS:
        ctx = SESSION_CONTEXTS[session_id]
        ctx["history"].append({"q": question, "a": answer, "data": result_data})
        ctx["last_topic"] = question
        ctx["last_result"] = result_data
        if len(ctx["history"]) > 10:
            ctx["history"] = ctx["history"][-10:]

def deep_analyze_question(question: str, context: dict) -> str:
    """深度分析问题"""
    history = context.get("history", [])
    analysis_parts = []

    # 上下文引用检测
    if any(kw in question for kw in ["它", "那个", "这些", "刚才"]):
        if history:
            analysis_parts.append(f"【上下文引用】参考上一条: {history[-1]['q']}")

    # 实体识别
    entities = []
    entity_rules = {
        "线路": ["线路", "号线", "line"],
        "服务": ["服务", "server", "服务名"],
        "报警": ["报警", "告警", "alarm"],
        "时刻表": ["时刻表", "运行图", "timetable"],
        "状态": ["在线", "离线", "status"],
        "数量": ["多少", "几个", "数量", "count"],
        "列表": ["列表", "有哪些", "查看"],
    }
    for entity, keywords in entity_rules.items():
        if any(kw in question for kw in keywords):
            entities.append(entity)
    if entities:
        analysis_parts.append(f"【实体】{', '.join(entities)}")

    # 意图识别
    intents = []
    intent_rules = {
        "统计": ["多少", "几个", "数量", "统计", "total"],
        "查询": ["查", "看", "获取"],
        "监控": ["状态", "监控", "实时"],
        "列表": ["列表", "所有", "有哪些"],
    }
    for intent, keywords in intent_rules.items():
        if any(kw in question for kw in keywords):
            intents.append(intent)
    if intents:
        analysis_parts.append(f"【意图】{', '.join(intents)}")

    # 情感分析
    if any(kw in question for kw in ["?", "？", "吗", "?", "请问"]):
        analysis_parts.append("【类型】疑问句")

    return "，".join(analysis_parts) if analysis_parts else "【意图】通用查询"

def build_intelligent_sql(question: str, schema: str, context: dict) -> dict:
    """智能SQL构建"""
    history = context.get("history", [])
    last_topic = context.get("last_topic")

    # 数据库表结构映射
    table_mappings = {
        "ats_information_status": {
            "keywords": ["服务", "服务状态", "server", "线路服务"],
            "fields": ["line_name", "server_name", "server_id", "status", "update_time"]
        },
        "message_alarm": {
            "keywords": ["报警", "告警", "alarm"],
            "fields": ["line_name", "alarm_content", "gen_time", "alarm_level"]
        },
        "inused_schedule_parameter": {
            "keywords": ["时刻表", "运行图", "timetable", "计划"],
            "fields": ["line_name", "inused_schedule_name", "inused_date", "valid"]
        },
    }

    # 根据问题动态构建SQL
    queries = []

    # 1. 线路数量
    if any(kw in question for kw in ["线路数量", "有多少线路", "线路数", "几条线路"]):
        return {
            "sql": f"SELECT COUNT(DISTINCT line_name) as cnt FROM {schema}.ats_information_status",
            "intent": "统计线路数量",
            "table": "ats_information_status"
        }

    # 2. 服务统计
    if any(kw in question for kw in ["总服务", "所有服务", "服务列表"]):
        return {
            "sql": f"SELECT line_name, server_name, status FROM {schema}.ats_information_status ORDER BY line_name",
            "intent": "查询服务列表",
            "table": "ats_information_status"
        }

    # 3. 在线服务
    if any(kw in question for kw in ["在线服务", "在线多少", "在线���"]):
        return {
            "sql": f"SELECT COUNT(*) as cnt FROM {schema}.ats_information_status WHERE status = 1",
            "intent": "统计在线服务",
            "table": "ats_information_status"
        }

    # 4. 离线服务
    if any(kw in question for kw in ["离线服务", "离线多少", "离线数"]):
        return {
            "sql": f"SELECT COUNT(*) as cnt FROM {schema}.ats_information_status WHERE status = 0",
            "intent": "统计离线服务",
            "table": "ats_information_status"
        }

    # 5. 线路列表
    if any(kw in question for kw in ["线路列表", "有哪些线路", "全部线路"]):
        return {
            "sql": f"SELECT DISTINCT line_name FROM {schema}.ats_information_status ORDER BY line_name",
            "intent": "查询线路列表",
            "table": "ats_information_status"
        }

    # 6. 报警查询
    if any(kw in question for kw in ["报警", "告警", "alarm"]):
        return {
            "sql": f"SELECT line_name, alarm_content, alarm_level, gen_time FROM {schema}.message_alarm ORDER BY gen_time DESC LIMIT 20",
            "intent": "查询报警信息",
            "table": "message_alarm"
        }

    # 7. 时刻表
    if any(kw in question for kw in ["时刻表", "运行图", "timetable"]):
        return {
            "sql": f"SELECT line_name, inused_schedule_name, inused_date FROM {schema}.inused_schedule_parameter WHERE valid = 1 ORDER BY line_name",
            "intent": "查询时刻表",
            "table": "inused_schedule_parameter"
        }

    # 8. 上下文引用 - 继续上一个话题
    if any(kw in question for kw in ["它", "那个", "这些"]) and history:
        last_q = history[-1]["q"]
        if "线路" in last_q:
            return {
                "sql": f"SELECT * FROM {schema}.ats_information_status WHERE line_name = (SELECT DISTINCT line_name FROM {schema}.ats_information_status LIMIT 1)",
                "intent": "继续查询",
                "table": "ats_information_status"
            }

    # 9. 直接SQL
    if question.strip().lower().startswith("select"):
        return {"sql": question, "intent": "直接执行SQL", "table": "unknown"}

    return {"sql": "", "intent": "", "table": ""}

def search_knowledge_base(question: str) -> str:
    """知识库搜索"""
    kb = {
        "你好|hello|hi|您好": "您好！我是ATS数据监控运维平台的智能助手，可以回答日常问题，也可以帮您查询业务数据。",
        "你是谁": "我是智能问数助手，基于知识库和数据库查询为您提供服务。有问题尽管问我！",
        "帮助": "您可以这样问我：\n1. 查询线路数量\n2. 查看在线服务有哪些\n3. 有哪些线路\n4. 最近报警情况\n5. 时刻表接入情况\n6. 直接输入SQL语句",
        "平台|系统|软件": "这是ATS数据监控运维平台，监控线路和服务状态，提供时刻表接入统计和报警统计功能。",
        "登录|密码": "本平台无需登录，直接访问。数据库配置在「数据库配置」页面设置。",
        "数据来源": "数据来自：ats_information_status（服务状态）、message_alarm（报警）、inused_schedule_parameter（时刻表）等表。",
        "感谢": "不客气！有问题随时问我。",
        "再见": "再见！欢迎下次使用。",
    }
    for key, answer in kb.items():
        if any(k in question for k in key.split("|")):
            return answer
    return ""

def fuzzy_match(question: str, context: dict) -> str:
    """模糊匹配"""
    fuzzy_rules = {
        ("平台", "系统"): "这是ATS数据监控运维平台，提供监控、统计等功能。",
        ("怎么", "使用"): "在首页查看监控数据，使用智能问数功能可以查询业务数据。",
        ("数据", "来源"): "数据来源于数据库表，可在「数据库配置」页面查看。",
    }
    for keywords, answer in fuzzy_rules.items():
        if any(k in question for k in keywords):
            return answer
    return ""

def get_suggestions(question: str, context: dict) -> str:
    """获取智能建议"""
    suggestions = [
        "可以问：「查询线路数量」",
        "可以问：「有哪些线路」",
        "可以问：「查看在线服务」",
        "可以问：「最近报警情况」",
    ]
    return "或者您可以尝试：" + "，".join(suggestions[:2])

def format_result_naturally(result: dict, question: str, intent: str) -> str:
    """自然语言格式化结果 - 根据问题关键词返回人性化答案"""
    if not result.get("data") or len(result["data"]) == 0:
        return "查询成功，但没有返回数据。"

    data = result["data"]
    count = len(data)
    q = question.lower()

    # ===== 线路数量相关 =====
    if any(kw in q for kw in ["线路数量", "线路数", "多少线路", "几条线路"]):
        if count == 1:
            val = data[0].get("cnt") or data[0].get("count") or list(data[0].values())[0]
            return f"📊 当前系统共监控着 {val} 条线路。"
        return f"📊 查询到 {count} 条线路。"

    # ===== 服务相关 =====
    if any(kw in q for kw in ["总服务", "所有服务", "服务列表"]):
        if count == 1:
            first = data[0]
            return f"📋 共 {count} 个服务：{first}"
        lines = []
        for i, row in enumerate(data[:10], 1):
            line = row.get("line_name", "")
            server = row.get("server_name", "")
            status = row.get("status", "")
            status_text = "在线" if status == 1 else "离线"
            lines.append(f"  {i}. {line} - {server} ({status_text})")
        return f"📋 共 {count} 个服务：\n" + "\n".join(lines)

    # ===== 在线服务 =====
    if any(kw in q for kw in ["在线服务", "在线多少", "在线数"]):
        if count == 1:
            val = data[0].get("cnt") or data[0].get("count") or list(data[0].values())[0]
            return f"✅ 当前在线服务数量：{val} 个。"
        return f"✅ 共 {count} 个在线服务。"

    # ===== 离线服务 =====
    if any(kw in q for kw in ["离线服务", "离线多少", "离线数"]):
        if count == 1:
            val = data[0].get("cnt") or data[0].get("count") or list(data[0].values())[0]
            return f"❌ 当前离线服务数量：{val} 个。"
        return f"❌ 共 {count} 个离线服务。"

    # ===== 线路列表 =====
    if any(kw in q for kw in ["线路列表", "有哪些线路", "全部线路", "线路"]):
        if count == 1:
            name = data[0].get("line_name") or data[0].get("linename") or list(data[0].values())[0]
            return f"🚇 线路：{name}"
        names = [row.get("line_name") or row.get("linename") or list(row.values())[0] for row in data]
        return f"🚇 共 {count} 条线路：\n  " + "，".join(names)

    # ===== 报警相关 =====
    if any(kw in q for kw in ["报警", "告警"]):
        lines = []
        for i, row in enumerate(data[:10], 1):
            line = row.get("line_name", "")
            content = row.get("alarm_content", "")[:30]
            level = row.get("alarm_level", "")
            time = str(row.get("gen_time", ""))[:16]
            lines.append(f"  {i}. [{level}] {line} - {content}... ({time})")
        return f"🚨 最近 {count} 条报警：\n" + "\n".join(lines)

    # ===== 时刻表相关 =====
    if any(kw in q for kw in ["时刻表", "运行图", "timetable"]):
        lines = []
        for i, row in enumerate(data[:10], 1):
            line = row.get("line_name", "")
            name = row.get("inused_schedule_name", "")
            date = row.get("inused_date", "")
            status = "有效" if row.get("valid") == 1 else "无效"
            lines.append(f"  {i}. {line}: {name} ({date}) - {status}")
        return f"📅 时刻表接入情况（{count}条）：\n" + "\n".join(lines)

    # ===== 数量结果默认 =====
    if count == 1:
        first = data[0]
        if any(k in str(first).lower() for k in ["cnt", "count", "total", "sum"]):
            val = first.get("cnt") or first.get("count") or first.get("total") or first.get("sum") or list(first.values())[0]
            return f"📊 查询结果：共 {val} 条记录。"

    # ===== 单条详情 =====
    if count == 1:
        first = data[0]
        items = []
        for k, v in first.items():
            if v is not None:
                items.append(f"{k}: {v}")
        return "📋 查询结果：" + "，".join(items[:5])

    # ===== 多条列表 =====
    lines = []
    for i, row in enumerate(data[:10], 1):
        vals = [f"{k}: {v}" for k, v in row.items() if v is not None]
        lines.append(f"  {i}. " + " / ".join(vals[:2]))
    return f"📋 共 {count} 条记录：\n" + "\n".join(lines)

@app.get("/api/dashboard")
async def get_dashboard_data():
    """获取仪表盘统计数据"""
    results = {}
    schema = DB_CONFIG.get("schema", "public")
    table = f"{schema}.ats_information_status"
    
    # 线路总数
    line_count = execute_query(f"SELECT COUNT(DISTINCT line_name) as line_count FROM {table}")
    results["line_count"] = line_count["data"][0]["line_count"] if line_count["success"] and line_count["data"] else 0
    
    # 服务总数
    service_count = execute_query(f"SELECT COUNT(*) as service_count FROM {table}")
    results["service_count"] = service_count["data"][0]["service_count"] if service_count["success"] and service_count["data"] else 0
    
    # 在线服务数 (status = 1)
    online_count = execute_query(f"SELECT COUNT(*) as online_count FROM {table} WHERE status = 1")
    results["online_count"] = online_count["data"][0]["online_count"] if online_count["success"] and online_count["data"] else 0
    
    # 离线服务数 (status = 0)
    offline_count = execute_query(f"SELECT COUNT(*) as offline_count FROM {table} WHERE status = 0")
    results["offline_count"] = offline_count["data"][0]["offline_count"] if offline_count["success"] and offline_count["data"] else 0
    
    return results

@app.get("/api/lines")
async def get_lines():
    """获取线路列表"""
    schema = DB_CONFIG.get("schema", "public")
    result = execute_query(f"SELECT DISTINCT line_name FROM {schema}.ats_information_status ORDER BY line_name")
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    return result

@app.get("/api/services")
async def get_services():
    """获取服务列表"""
    schema = DB_CONFIG.get("schema", "public")
    result = execute_query(f"SELECT id, line_name, server_name, server_id, status, update_time FROM {schema}.ats_information_status ORDER BY id")
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    return result

@app.get("/api/traffic")
async def get_traffic_data(hours: int = 24):
    """获取流量数据"""
    result = execute_query(f"""
        SELECT 
            DATE_TRUNC('hour', update_time) as hour,
            SUM(data_count) as total_count
        FROM ats_traffic_log
        WHERE update_time >= NOW() - INTERVAL '{hours} hours'
        GROUP BY DATE_TRUNC('hour', update_time)
        ORDER BY hour
    """)
    if not result["success"]:
        return {"success": True, "data": []}
    return result

@app.get("/api/line-tables")
async def get_line_tables():
    """获取线路映射表"""
    result = execute_query("""
        SELECT DISTINCT linename, linecode 
        FROM atstimetable 
        WHERE diagram_date = (SELECT MAX(diagram_date) FROM atstimetable)
        ORDER BY linename
    """)
    return result

@app.get("/api/history-columns")
async def get_history_columns():
    """获取history_schedule表结构"""
    result = execute_query("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'itos_theme_ats' AND table_name = 'history_schedule' ORDER BY ordinal_position")
    return result

@app.get("/api/alarm-columns")
async def get_alarm_columns():
    """获取message_alarm表结构"""
    result = execute_query("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'itos_theme_ats' AND table_name = 'message_alarm' ORDER BY ordinal_position")
    return result

@app.get("/api/alarms")
async def get_alarms(line_name: str = "", alarm_date: str = ""):
    """获取实时报警统计"""
    schema = DB_CONFIG.get("schema", "public")
    
    query = f"SELECT id, line_name, alarm_content, gen_time FROM {schema}.message_alarm WHERE 1=1"
    
    if line_name and line_name != '全部':
        query += f" AND line_name = '{line_name}'"
    
    if alarm_date:
        query += f" AND DATE(gen_time) = '{alarm_date}'"
    
    query += " ORDER BY gen_time DESC LIMIT 100"
    
    result = execute_query(query)
    return result

@app.get("/api/alarm-lines")
async def get_alarm_lines(alarm_date: str = ""):
    """获取报警线路列表"""
    schema = DB_CONFIG.get("schema", "public")
    query = f"SELECT DISTINCT line_name FROM {schema}.message_alarm"
    
    if alarm_date:
        query += f" WHERE DATE(gen_time) = '{alarm_date}'"
    
    query += " ORDER BY line_name"
    
    result = execute_query(query)
    return result

@app.get("/api/timetable")
async def get_timetable_data():
    """获取时刻表接入统计"""
    schema = DB_CONFIG.get("schema", "public")
    
    plan_result = execute_query(f"""
        SELECT id, line_name, inused_schedule_name, inused_date 
        FROM {schema}.inused_schedule_parameter 
        WHERE valid = 1 
        AND inused_date = CURRENT_DATE
        ORDER BY line_name, id
    """)
    
    actual_result = execute_query("""
        SELECT linename as line_id, diagramname, diagram_date as inused_date
        FROM atstimetable 
        WHERE diagram_date = TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'YYYY-MM-DD')
        GROUP BY linename, diagramname, diagram_date
        ORDER BY linename
    """)
    
    actual_diagrams = actual_result.get("data", []) if actual_result.get("success") else []
    
    plan_diagrams = plan_result.get("data", []) if plan_result.get("success") else []
    
    return {
        "success": True,
        "plan_diagrams": plan_diagrams,
        "actual_diagrams": actual_diagrams
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
