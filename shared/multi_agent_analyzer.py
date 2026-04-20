from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List

OPENAI_AVAILABLE = True
OPENAI_IMPORT_ERROR = None
try:
    from openai import APIConnectionError, APITimeoutError, OpenAI
except Exception as _exc:  # pragma: no cover
    OPENAI_AVAILABLE = False
    OPENAI_IMPORT_ERROR = _exc
    OpenAI = None  # type: ignore[assignment]

    class APIConnectionError(Exception):
        pass

    class APITimeoutError(Exception):
        pass


FUNDAMENTAL_AGENT_PROMPT = """你是 FundamentalAgent（基本面空头专家）。
目标：只负责提出看空逻辑，必须挑刺，不允许“中性打太极”。

工作要求：
1) 聚焦高估值、现金流压力、分红偏低、盈利质量波动、杠杆风险等空头证据；
2) 必须引用输入中的具体数字或字段，禁止空话；
3) 若数据不足，明确指出“缺失数据导致的不确定性”，但仍给出保守空头判断。

输出格式（中文，Markdown）：
- 空头结论（<=80字）
- 关键证据（3-5条）
- 反证与失效条件（2条）
- 未来一周下行触发点（2条）"""

TECHNICAL_AGENT_PROMPT = """你是 TechnicalAgent（技术面多头专家）。
目标：只负责提出看多逻辑，必须给出交易结构上的上行理由。

工作要求：
1) 聚焦均线趋势、RSI、MACD、量价关系、盘口失衡、资金流向等多头证据；
2) 必须引用输入中的具体数字或字段，禁止模板化描述；
3) 若指标冲突，仍要给出“多头主线 + 风险控制位”。

输出格式（中文，Markdown）：
- 多头结论（<=80字）
- 关键证据（3-5条）
- 反证与失效条件（2条）
- 未来一周上行触发点（2条）"""

EVENT_AGENT_PROMPT = """你是 EventAgent（消息面与催化剂专家）。
目标：评估事件冲击力和情绪持续性，不站队，但必须给出方向性概率倾向。

工作要求：
1) 识别政策、行业、公司公告、研报预期差等事件催化；
2) 输出“冲击强度(低/中/高)”与“情绪半衰期(天)”；
3) 给出对未来一周情绪方向的判断（偏多/偏空/震荡偏空/震荡偏多），并说明原因。

输出格式（中文，Markdown）：
- 事件主线摘要（<=100字）
- 催化/风险清单（各2-3条）
- 冲击强度与半衰期
- 一周情绪方向判断"""

JUDGE_AGENT_PROMPT = """你是 JudgeAgent（法官智能体）。
你将收到三位专家的结论：基本面空头、技术面多头、消息面评估。
你的任务是审判，不是复读，不允许“和稀泥”。

强制规则：
1) 先提炼三位专家最强论点（每位最多2条）；
2) 明确给出概率评估：做多胜率X% vs 做空胜率Y%，且 X+Y=100；
3) 必须给出唯一主结论：做多 / 做空 / 观望（三选一）；
4) 若给“观望”，必须给出可执行触发条件（价格/指标/事件阈值）；
5) 必须说明你为何否决另一侧观点，不能模糊措辞。

输出格式（中文，Markdown）：
## 法官裁决
- 做多胜率: X%
- 做空胜率: Y%
- 最终结论: 做多/做空/观望
- 核心依据: 3-5条
- 48小时行动计划: 2-3条
- 一周跟踪清单: 3条"""


@dataclass
class AgentConfig:
    key: str
    title: str
    system_prompt: str
    temperature: float
    max_tokens: int


def _usage_to_dict(usage: Any) -> Dict[str, int]:
    if usage is None:
        return {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "prompt_cache_hit_tokens": 0,
            "prompt_cache_miss_tokens": 0,
        }
    prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
    completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
    cache_hit = int(getattr(usage, "prompt_cache_hit_tokens", 0) or 0)
    cache_miss = int(getattr(usage, "prompt_cache_miss_tokens", 0) or 0)
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "prompt_cache_hit_tokens": cache_hit,
        "prompt_cache_miss_tokens": cache_miss,
    }


def _sum_usage(usages: List[Dict[str, int]]) -> Dict[str, int]:
    out = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "prompt_cache_hit_tokens": 0,
        "prompt_cache_miss_tokens": 0,
    }
    for usage in usages:
        out["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
        out["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
        out["total_tokens"] += int(usage.get("total_tokens", 0) or 0)
        out["prompt_cache_hit_tokens"] += int(usage.get("prompt_cache_hit_tokens", 0) or 0)
        out["prompt_cache_miss_tokens"] += int(usage.get("prompt_cache_miss_tokens", 0) or 0)
    return out


def _estimate_cost(usage: Dict[str, int]) -> float:
    cache_hit_tokens = int(usage.get("prompt_cache_hit_tokens", 0) or 0)
    cache_miss_tokens = int(usage.get("prompt_cache_miss_tokens", 0) or 0)
    completion_tokens = int(usage.get("completion_tokens", 0) or 0)
    return (
        cache_hit_tokens / 1_000_000 * 0.028
        + cache_miss_tokens / 1_000_000 * 0.28
        + completion_tokens / 1_000_000 * 0.42
    )


class MultiAgentAnalyzer:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.deepseek.com/v1",
        model: str = "deepseek-chat",
        timeout_sec: float = 90.0,
        max_retries: int = 0,
    ) -> None:
        if (not OPENAI_AVAILABLE) or (OpenAI is None):
            if OPENAI_IMPORT_ERROR is not None:
                raise RuntimeError(f"openai 依赖不可用: {OPENAI_IMPORT_ERROR}")
            raise RuntimeError("openai 依赖不可用")
        self.api_key = str(api_key or "").strip()
        self.base_url = str(base_url or "").strip()
        self.model = str(model or "").strip() or "deepseek-chat"
        self.timeout_sec = float(timeout_sec)
        self.max_retries = int(max_retries)
        self.experts = [
            AgentConfig(
                key="fundamental_bear",
                title="FundamentalAgent（基本面空头）",
                system_prompt=FUNDAMENTAL_AGENT_PROMPT,
                temperature=0.25,
                max_tokens=900,
            ),
            AgentConfig(
                key="technical_bull",
                title="TechnicalAgent（技术面多头）",
                system_prompt=TECHNICAL_AGENT_PROMPT,
                temperature=0.25,
                max_tokens=900,
            ),
            AgentConfig(
                key="event_impact",
                title="EventAgent（消息面分析）",
                system_prompt=EVENT_AGENT_PROMPT,
                temperature=0.2,
                max_tokens=800,
            ),
        ]
        self.judge = AgentConfig(
            key="judge",
            title="JudgeAgent（法官裁决）",
            system_prompt=JUDGE_AGENT_PROMPT,
            temperature=0.15,
            max_tokens=1200,
        )

    def _build_client(self):
        return OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout_sec,
            max_retries=self.max_retries,
        )

    def _single_call(
        self,
        *,
        system_prompt: str,
        user_content: str,
        temperature: float,
        max_tokens: int,
        top_p: float = 0.9,
    ) -> Dict[str, Any]:
        client = self._build_client()
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        t0 = time.time()
        response = None
        last_exc = None
        for attempt in range(1, 4):
            try:
                response = client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    top_p=top_p,
                )
                break
            except (APIConnectionError, APITimeoutError) as exc:
                last_exc = exc
                if attempt < 3:
                    time.sleep(0.6 * attempt)
                    continue
            except Exception:
                raise

        if response is None:
            raise RuntimeError(f"模型请求失败: {last_exc}")

        content = (response.choices[0].message.content or "").strip()
        if not content:
            raise RuntimeError("模型未返回有效文本")
        usage = _usage_to_dict(response.usage)
        elapsed = time.time() - t0
        return {"text": content, "usage": usage, "elapsed": elapsed}

    def _build_expert_input(self, payload_json: str, stock_code: str, stock_name: str) -> str:
        return (
            f"标的: {stock_name} ({stock_code})\n"
            "以下是结构化行情与估值数据(JSON)：\n"
            f"{payload_json}\n\n"
            "请按你的角色要求输出，不要引用角色说明文本。"
        )

    def _build_judge_input(
        self,
        payload_json: str,
        stock_code: str,
        stock_name: str,
        expert_outputs: List[Dict[str, Any]],
    ) -> str:
        expert_pack = {
            item["agent_key"]: {
                "title": item["agent_title"],
                "report": item["text"],
            }
            for item in expert_outputs
        }
        return (
            f"标的: {stock_name} ({stock_code})\n\n"
            "原始数据(JSON)：\n"
            f"{payload_json}\n\n"
            "三位专家意见(JSON)：\n"
            f"{json.dumps(expert_pack, ensure_ascii=False, indent=2)}\n\n"
            "请输出最终审判结论。"
        )

    def _run_expert_sync(
        self,
        agent: AgentConfig,
        payload_json: str,
        stock_code: str,
        stock_name: str,
    ) -> Dict[str, Any]:
        call = self._single_call(
            system_prompt=agent.system_prompt,
            user_content=self._build_expert_input(payload_json, stock_code, stock_name),
            temperature=agent.temperature,
            max_tokens=agent.max_tokens,
            top_p=0.9,
        )
        return {
            "agent_key": agent.key,
            "agent_title": agent.title,
            "text": call["text"],
            "usage": call["usage"],
            "elapsed": call["elapsed"],
        }

    async def _run_expert_async(
        self,
        agent: AgentConfig,
        payload_json: str,
        stock_code: str,
        stock_name: str,
    ) -> Dict[str, Any]:
        try:
            return await asyncio.to_thread(
                self._run_expert_sync,
                agent,
                payload_json,
                stock_code,
                stock_name,
            )
        except Exception as exc:
            return {
                "agent_key": agent.key,
                "agent_title": agent.title,
                "text": f"该专家调用失败: {type(exc).__name__}: {exc}",
                "usage": _usage_to_dict(None),
                "elapsed": 0.0,
                "error": f"{type(exc).__name__}: {exc}",
            }

    @staticmethod
    def _compose_markdown(expert_outputs: List[Dict[str, Any]], judge_text: str) -> str:
        lines: List[str] = ["## 多智能体对抗报告", "", "### 专家观点"]
        for item in expert_outputs:
            lines.extend(
                [
                    "",
                    f"#### {item.get('agent_title', item.get('agent_key', 'Expert'))}",
                    str(item.get("text", "")).strip(),
                ]
            )
        lines.extend(["", str(judge_text or "").strip()])
        return "\n".join(lines).strip()

    async def analyze(self, payload_json: str, stock_code: str = "", stock_name: str = "") -> Dict[str, Any]:
        t0 = time.time()
        expert_outputs = await asyncio.gather(
            *[
                self._run_expert_async(agent, payload_json, stock_code, stock_name)
                for agent in self.experts
            ],
            return_exceptions=False,
        )

        judge_input = self._build_judge_input(payload_json, stock_code, stock_name, expert_outputs)
        judge_call = await asyncio.to_thread(
            self._single_call,
            system_prompt=self.judge.system_prompt,
            user_content=judge_input,
            temperature=self.judge.temperature,
            max_tokens=self.judge.max_tokens,
            top_p=0.9,
        )

        judge_text = judge_call["text"]
        markdown = self._compose_markdown(expert_outputs, judge_text)

        expert_usage = _sum_usage([item.get("usage", {}) for item in expert_outputs])
        judge_usage = judge_call.get("usage", {}) or _usage_to_dict(None)
        total_usage = _sum_usage([expert_usage, judge_usage])
        total_cost = _estimate_cost(total_usage)

        return {
            "experts": expert_outputs,
            "judge": {
                "agent_key": self.judge.key,
                "agent_title": self.judge.title,
                "text": judge_text,
                "usage": judge_usage,
                "elapsed": float(judge_call.get("elapsed", 0.0) or 0.0),
            },
            "final_markdown": markdown,
            "usage": total_usage,
            "cost": total_cost,
            "elapsed": time.time() - t0,
            "usage_breakdown": {
                "experts": expert_usage,
                "judge": judge_usage,
            },
        }
