# UltraRAG Pipeline 模板模块

← [项目主页](../CLAUDE.md) | [模块详情](./CLAUDE.md)

## 模块概述

存放标准 Pipeline 配置模板，展现 UltraRAG 在复杂推理任务中的强大流程控制能力。

## 可用 Pipeline

### 1. Search-o1
- **路径**: `search_o1/`
- **描述**: 复杂多轮检索增强流程
- **核心特性**: 循环、条件分支、内存状态管理

## Pipeline 架构模式

### 1. 串行执行
```yaml
- step1.server.tool
- step2.server.tool
```

### 2. 循环控制
```yaml
- loop:
    times: 2
    steps:
      - branch_router_server.tool
      branches:
        branch1: [step1, step2]
```

## 运行机制

### 1. 配置解析
- 加载 YAML 配置文件
- 解析服务器路径与参数

### 2. 状态跟踪
- 全局变量管理
- 内存快照记录

## 开发建议

### 1. 模板创建
- 参考现有 `search_o1/run.yaml` 结构

## 配置文件结构

```bash
pipelines/├── search_o1/│   ├── run.yaml           # 主流水线编排
│   ├── server/                 # 服务端配置
│   └── parameter/               # 运行时参数
```

## 扩展能力

用户可基于现有模板创建新的 Pipeline 配置，快速实现定制化 RAG 流程。

## 集成要点

- **参数注入**: 通过 `$variable_name` 语法
- **流程组合**: 自由组合串行、循环、分支

## 测试验证

使用示例文件验证 Pipeline 功能完整性。