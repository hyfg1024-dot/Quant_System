# Quant_System（Mac 安装必成版）

这是一个多模块量化工作台，目前包含：
- `Trading`：交易面 + 深度行情 + DeepSeek 分析
- `Fundamental`：基本面八维评分 + DeepSeek 解读
- `Filter`：大过滤器（全市场条件筛选）

## 模块入口
- 交易面：`apps/trading/app.py`
- 基本面：`apps/fundamental/app.py`
- 大过滤器：`apps/filter/app.py`

下面按 **macOS** 给出最稳妥的安装方式。

## 1) 下载项目

两种方式任选其一：

1. Git 克隆：
```bash
git clone https://github.com/hyfg1024-dot/Quant_System.git
cd Quant_System
```

2. GitHub 下载 ZIP：
- 点击 `Code -> Download ZIP`
- 解压后进入项目根目录（里面要能看到 `create_desktop_launcher.command`）

---

## 2) 第一次启动（推荐：终端方式，100%绕开双击拦截）

在项目根目录执行：

```bash
chmod +x create_desktop_launcher.command
xattr -d com.apple.quarantine create_desktop_launcher.command 2>/dev/null || true
./create_desktop_launcher.command
```

执行成功后，桌面会生成：
- `启动Quant_System.command`

双击这个桌面按钮即可启动程序。

---

## 3) 浏览器访问

启动后打开：
- 默认启动交易面：[http://localhost:8501](http://localhost:8501)

---

## 4) 如果遇到“Apple 无法验证”

这是 macOS 安全策略，不是程序问题。执行下面命令一次即可：

```bash
cd /你的/Quant_System/目录
chmod +x create_desktop_launcher.command
xattr -d com.apple.quarantine create_desktop_launcher.command 2>/dev/null || true
./create_desktop_launcher.command
```

如果是桌面启动按钮被拦截，再执行：

```bash
xattr -d com.apple.quarantine ~/Desktop/启动Quant_System.command
chmod +x ~/Desktop/启动Quant_System.command
```

---

## 5) 手动启动（不用桌面按钮也可以）

```bash
cd apps/trading
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
streamlit run app.py
```

切换其他模块只需替换目录：

```bash
# 基本面
cd apps/fundamental
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
streamlit run app.py

# 大过滤器
cd apps/filter
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
streamlit run app.py
```

---

## 6) 常见问题

1. 端口被占用（8501）
```bash
cd apps/trading
source venv/bin/activate
streamlit run app.py --server.port 8510
```

2. 依赖安装慢
- 多等几分钟，首次安装会下载很多包。

3. API Key 是否会上传
- 不会。本地保存文件已忽略上传：`data/local_user_prefs.json`

---

## 版本

- Trading：`QDB-20260323-DSWIN-03`
- Filter：`FLT-20260327-01`
