# zed2api

将 Zed 编辑器的 LLM API 代理为 OpenAI / Anthropic 兼容接口的本地服务器。单文件二进制，内嵌 Web 管理界面。

> **维护分支说明（Zed2api 2.0）**
> 本仓库是 [yukmakoto/zed2api](https://github.com/yukmakoto/zed2api) 的社区维护分支。原项目已停止更新，本分支继续跟进修复 Zed 官方协议变更带来的兼容性问题（例如 llm_tokens 刷新需携带 organization_id），保持可正常使用。
> 本项目与 Zed 官方无关，仅供学习与个人使用；使用时请遵守 Zed 的服务条款，账号风险自担。
> This is an unofficial maintenance fork of yukmakoto/zed2api, kept working as Zed's client protocol evolves. Not affiliated with Zed.

## 功能

- OpenAI 兼容接口：`POST /v1/chat/completions`
- Anthropic 原生接口：`POST /v1/messages`
- 模型列表：`GET /v1/models`
- 多账号管理 + 自动故障转移
- SSE 流式输出
- 多模型供应商：Anthropic / OpenAI / Google / xAI
- 扩展思考 (thinking) 支持
- 内嵌 Web UI 管理界面
- HTTPS 代理支持（环境变量 / Windows 系统代理自动检测）
- GitHub OAuth 登录
- 跨平台：Windows + Linux

## 支持的模型

模型列表由 Zed 按账号实时下发，不同账号可能不同（Zed 分批灰度）。以下为实测汇总（2026-10），以 `GET /v1/models` 的实际返回为准：

| 供应商 | 模型 |
|---------|------|
| Anthropic | claude-opus-4-6, claude-opus-4-5, claude-opus-4-1, claude-sonnet-5-5, claude-sonnet-5, claude-sonnet-4-6, claude-sonnet-4-5, claude-sonnet-4, claude-3-7-sonnet, claude-haiku-5-5, claude-haiku-4-5 |
| OpenAI | gpt-6.1-sol, gpt-6-sol, gpt-6-luna, gpt-5.6-sol, gpt-5.6-terra, gpt-5.6-luna, gpt-5.5, gpt-5.4, gpt-5.3-codex, gpt-5.2, gpt-5.2-codex, gpt-5.1, gpt-5, gpt-5-mini, gpt-5-nano |
| Google | gemini-4-argon, gemini-3.1-pro-preview, gemini-3-pro-preview, gemini-3.8-flash, gemini-3.5-flash, gemini-3-flash, gemini-2.5-pro, gemini-2.5-flash |
| xAI | grok-4, grok-4-fast-reasoning, grok-4-fast-non-reasoning, grok-code-fast-1 |

注：部分账号已下发新一代模型（如 claude-sonnet-5、gpt-5.6-sol、gemini-4-argon），部分账号仍为上一代列表（如 claude-opus-4-6、gpt-5.2、grok-4）；同一账号的列表也会随 Zed 的调整而变化。

## 编译

需要 [Zig 0.15.x](https://ziglang.org/download/) 和 Node.js。

`zig build` 会自动编译 WebUI 并嵌入二进制文件，无需手动操作。

```bash
# 首次需要安装 WebUI 依赖
cd webui && npm install && cd ..

# 编译（当前平台）
zig build

# 交叉编译 Linux x86_64
zig build -Dtarget=x86_64-linux -Doptimize=ReleaseSafe
```

## 使用

```bash
# OAuth 登录添加账号
./zed2api login [账号名]

# 或手动创建 accounts.json（参考 accounts.example.json）

# 启动服务
./zed2api serve [端口]    # 默认 8000

# 查看账号列表
./zed2api accounts
```

打开 http://127.0.0.1:8000 进入 Web 管理界面。

## Claude Code 集成

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8000
export ANTHROPIC_AUTH_TOKEN=dummy
claude
```

## 代理设置

设置 `HTTPS_PROXY` 环境变量，或者服务器会自动从 Windows 注册表读取系统代理。

```bash
export HTTPS_PROXY=http://127.0.0.1:7890
./zed2api serve
```

## 项目结构

```
src/
  main.zig       - 入口，CLI 命令
  server.zig     - HTTP 服务器，路由，账号接口
  stream.zig     - SSE 流式代理
  socket.zig     - 跨平台 Socket I/O（Windows ws2_32 / POSIX）
  zed.zig        - Token 管理，计费查询，代理编排
  proxy.zig      - HTTPS 代理检测，curl HTTP 客户端
  providers.zig  - 多供应商请求构建 & 响应转换
  accounts.zig   - 账号管理，JSON 持久化
  auth.zig       - RSA 密钥对，OAuth 登录，浏览器启动
  models.json    - 内嵌模型列表
webui/           - Vite + TypeScript Web UI（编译为单 HTML 文件嵌入二进制）
```
