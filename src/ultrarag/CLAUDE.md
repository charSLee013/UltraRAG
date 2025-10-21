# UltraRAG 核心引擎模块

← [项目主页](../CLAUDE.md) | [模块详情](./CLAUDE.md)

## 模块概述

UltraRAG 的核心执行引擎，负责处理 YAML Pipeline 配置、管理 MCP Servers 集群，并提供完整的流程控制系统。

## 主要接口

### 1. 客户端初始化
```python
from ultrarag import initialize

# 初始化服务器集群
initialize(
    servers=["retriever", "llm", "evaluator"],
    server_root="/path/to/servers"
)
```

### 2. Pipeline 执行引擎
```python
from ultrarag import pipeline, ToolCall

# 直接运行 Pipeline
pipeline("examples/rag.yaml")

# 函数式调用
result = ToolCall.retriever.search(query="test")
```

## 核心组件

### 1. Configuration 类
- 配置加载与环境管理
- YAML 参数文件解析

### 2. UltraData 类
- 全局变量管理与状态跟踪
- 内存快照记录
- IO 参数提取与验证

## 数据流与控制

### Pipeline 结构
```yaml
pipeline:
  - step1.server.tool
  - loop:
      times: 3
      steps:
        - loop_step.server.tool
  - branch:
      router: branch_router_server.tool
      branches:
        branch1: [step1, step2]
  - step2.server.tool
```

## 依赖管理

### 主要依赖
- `fastmcp==2.11.3` - MCP 协议客户端
- `Jinja2` - 模板处理
- `yaml` - 配置解析

## 使用示例

### 基础流程
```python
import asyncio
from ultrarag import run

# 直接运行 Pipeline
result = asyncio.run(run("config.yaml"))
```

## 关键文件

- `client.py` - 主执行引擎
- `server.py` - 服务端管理
- `utils.py` - 工具函数

### 运行模式
1. **构建模式**: `ultrarag build config.yaml`
2. **运行模式**: `ultrarag run config.yaml`

## 测试入口

运行测试检查核心功能完整性。

## 注意事项

- 支持多服务器并行执行
- 内置流程控制(循环/条件分支)
- 自动变量依赖解析