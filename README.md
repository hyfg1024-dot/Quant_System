# Quant_System

`Quant_System` 是一个基于 Streamlit 构建的本地量化研究工作台，面向日常股票研究、盘中交易观察、条件筛选、组合风控与策略回测场景。项目以 macOS 本地运行方式为主，强调可落地、可视化和低门槛部署。

## 项目概览

当前版本包含 6 个核心模块：

| 模块 | 说明 | 入口 |
| --- | --- | --- |
| Trading | 交易面分析台，聚合行情快照、盘口、分时结构与多智能体分析流程 | `apps/trading/app.py` |
| Fundamental | 基本面研究台，提供八维评分、新闻/研报催化剂摘要与 AI 解读 | `apps/fundamental/app.py` |
| Filter | 全市场条件筛选器，使用 DuckDB SQL 做快筛、模板保存与导出 | `apps/filter/app.py` |
| Portfolio | 仓位风控台，管理持仓、浮动盈亏、仓位权重与 ATR 风险约束 | `apps/portfolio/app.py` |
| Backtest | 通用策略回测台，支持策略配置、数据更新、执行回测与 HTML 报告 | `apps/backtest/run_backtest.py` |
| Paper Trade | 模拟实盘台，按策略逐日推进模拟持仓并跟踪执行结果 | `apps/backtest/paper_trade.py` |

## 主要能力

- 多模块本地量化研究界面
- 股票池管理，支持持仓与观察分组
- 基本面八维评分、新闻/研报催化剂整合
- DeepSeek 多智能体分析接入，支持本地保存用户配置
- DuckDB 本地数据底座与 SQL 条件快筛
- 仓位风控看板、浮动盈亏与 ATR 仓单规模建议器
- 通用回测、模拟实盘与交互式报告输出
- macOS 一键启动脚本与桌面快捷入口

## 目录结构

```text
Quant_System/
├── apps/
│   ├── trading/          # 交易观察模块
│   ├── fundamental/      # 基本面研究模块
│   ├── filter/           # 条件筛选模块
│   ├── portfolio/        # 仓位风控模块
│   └── backtest/         # 回测 / 模拟实盘模块
├── shared/               # 共享 UI / 通用逻辑
├── data/                 # 本地数据库、缓存、用户配置（不提交）
├── docs/                 # 附加文档
├── create_desktop_launcher.command
└── README.md
```

## 运行要求

- macOS
- Python 3.9+
- 终端可用 `python3` 与 `pip`
- 首次安装依赖时可正常访问 Python 包源
- 若使用仓位风控 / DuckDB 快筛，需要可安装 `duckdb`
- 若使用 QMT 数据适配层，需要本机具备 `xtquant` 运行环境

## 快速开始

### 方式一：使用桌面启动脚本

在项目根目录执行：

```bash
chmod +x create_desktop_launcher.command
xattr -d com.apple.quarantine create_desktop_launcher.command 2>/dev/null || true
./create_desktop_launcher.command
```

执行完成后，桌面会生成启动入口：

- `启动Quant_System.command`

双击后即可启动默认页面。

### 方式二：手动启动模块

#### Trading / 主控台

```bash
cd apps/trading
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
streamlit run app.py
```

#### Fundamental

```bash
cd apps/fundamental
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
streamlit run app.py
```

#### Filter

```bash
cd apps/filter
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
streamlit run app.py
```

#### Portfolio

```bash
cd apps/trading
python3 -m venv venv
source venv/bin/activate
python3 -m pip install duckdb streamlit pandas altair
streamlit run app.py
```

## 默认访问地址

启动后默认访问：

- [http://localhost:8501](http://localhost:8501)

如果端口被占用，可以改为其他端口，例如：

```bash
streamlit run app.py --server.port 8510
```

## 后台预警 Worker

后台预警进程入口：

- `daemon/alert_worker.py`
- 规则文件：`config/alert_rules.yaml`

### 依赖

当前交易模块依赖文件已经包含告警 worker 所需包：

- `APScheduler`
- `PyYAML`
- `requests`

### 配置推送环境变量

以 Telegram Bot 为例，在本机 shell 中配置：

```bash
export TELEGRAM_BOT_TOKEN="你的 bot token"
export TELEGRAM_CHAT_ID="你的 chat id"
```

PushPlus / ServerChan 也支持，环境变量名见：

- `config/alert_rules.yaml`

### 手工运行

只跑一轮并打印，不真实推送：

```bash
python3 daemon/alert_worker.py --once --dry-run
```

只跑一轮并真实推送：

```bash
python3 daemon/alert_worker.py --once
```

常驻调度运行：

```bash
python3 daemon/alert_worker.py
```

### macOS 常驻启动

安装 `launchd` 后台任务：

```bash
chmod +x daemon/install_launch_agent.command
./daemon/install_launch_agent.command
```

卸载：

```bash
chmod +x daemon/uninstall_launch_agent.command
./daemon/uninstall_launch_agent.command
```

日志输出位置：

- `data/logs/alert_worker.out.log`
- `data/logs/alert_worker.err.log`

## macOS 安全提示

如果首次执行脚本时遇到“Apple 无法验证开发者”或系统拦截，通常是 macOS 的隔离属性导致。可以执行：

```bash
chmod +x create_desktop_launcher.command
xattr -d com.apple.quarantine create_desktop_launcher.command 2>/dev/null || true
./create_desktop_launcher.command
```

如果桌面生成的启动入口被拦截，可继续执行：

```bash
xattr -d com.apple.quarantine ~/Desktop/启动Quant_System.command
chmod +x ~/Desktop/启动Quant_System.command
```


## 数据安全红线

本项目把本地数据视为资产，代码重构不得破坏已下载的数据。后续开发必须遵守：

1. 代码可以随便重构，数据资产不能被代码重构影响。
2. 快照可以替换，深补只能增量 upsert。
3. 任何批量写入前，必须生成恢复点。
4. Git 永远不管理数据库、缓存、API Key、本地策略草稿。
5. 页面按钮不直接清库，所有危险操作必须走备份和确认。

当前实现：

- 本地备份统一写入 `data/backups/`，该目录不会提交到 GitHub。
- 大过滤器运维台提供“数据保险箱”，支持手动完整备份、查看备份、确认后恢复。
- 批量快照/深补写入前会自动创建数据库恢复点。
- DeepSeek API Key 等本地密钥默认不进入备份包。

## 配置与本地数据

- DeepSeek 用户名与 API Key 仅保存在本地
- 本地偏好文件默认位于 `data/local_user_prefs.json`
- DuckDB 数据库默认位于 `data/quant_system.duckdb`
- Telegram / PushPlus / ServerChan 推送凭证建议仅保存在本机环境变量
- 分析缓存、任务文件、回测产物和模拟实盘快照都保存在本地目录，不会自动上传到 GitHub

建议将以下内容视为本地运行态数据，而不是源码的一部分：

- `data/`
- `apps/backtest/paper_trades/`
- 各模块下的 `venv/`
- `.env` / `.env.*`
- 本地缓存、数据库、导出结果

## 常见问题

### 1. 依赖安装较慢

首次安装会下载较多 Python 包，等待时间取决于网络环境。

### 2. 页面无法访问

请确认：

- 当前模块依赖已安装完成
- Streamlit 已正常启动
- 端口未被其他程序占用

### 3. API Key 是否会进入仓库

不会。项目默认将本地用户配置、运行缓存、DuckDB 数据库和模拟实盘产物排除在 Git 之外。

## 开发说明

- 项目当前以本地运行和单仓库维护为主
- UI 共享逻辑位于 `shared/`
- 数据适配与本地数据库能力位于 `shared/data_provider.py` 与 `shared/db_manager.py`
- 各模块均可独立启动，也可在主工作流中联动使用
- 若修改模块逻辑，建议优先在对应 `apps/<module>/` 下完成验证

## 许可证与使用说明

本仓库当前未在 README 中单独声明开源许可证。如需对外分发、商用或二次发布，建议先补充明确的许可证文件与使用条款。
