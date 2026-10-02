# CPA OpenAI Basis Points 插件

> 本项目 fork 自 [JaxsonWang/cpa-plugin-oai-basispoints](https://github.com/JaxsonWang/cpa-plugin-oai-basispoints)，在其基础上继续维护与扩展。

这是一个 CLIProxyAPI（CPA）原生插件，用 CPA 已有的 ChatGPT/Codex OAuth 凭据直接请求。

## 通过 CPA 插件商店安装（推荐）

在管理界面的「第三方插件源 → 插件源 registry URL (plugins.store-sources)」中添加以下地址并保存，然后刷新插件商店，搜索 **CPA OpenAI Basis Points**：

```text
https://raw.githubusercontent.com/anlostsheep/cpa-plugin-oai-basispoints/main/registry.json
```

也可合并到 CPA **宿主配置**（`config.yaml`，与下方插件配置共用同一个 `plugins` 节点）：

```yaml
plugins:
  enabled: true
  store-sources:
    - https://raw.githubusercontent.com/anlostsheep/cpa-plugin-oai-basispoints/main/registry.json
```

保留已有插件源，不要整体覆盖原有 `plugins` 配置；内置官方源由 CPA 自动保留。本源使用宿主原生的 `github-release` 安装方式，最新版本以本仓库已发布的 GitHub Release 为准，不在 registry 中另行维护版本号。CPA 会按运行平台下载 `oai-basispoints_<version>_<goos>_<goarch>.zip`，并使用同一 Release 的 `checksums.txt` 校验。

发行包覆盖 Linux、macOS、Windows 的 AMD64/ARM64。插件商店负责下载、校验和安装；更新已加载的动态库后仍需重启 CPA，使新代码及 OAuth 认证解析生效。

## 安装和配置

1. 将 `build/linux/amd64/oai-basispoints.so` 复制到 CPA 的 Linux amd64 插件目录。
2. 将 `config.example.yaml` 按需合并到 CPA 的 `config.yaml`；它是完整的宿主配置示例，不会由插件自动读取。插件内置默认暴露 `gpt-6-astra-basispoints`，示例同时配置 Astra 和 Sol，可继续增删模型。
3. 在插件配置 `dedicated_auth_files` 中列出要使用 Basis Points 的 `type: codex` OAuth 文件名（auth-dir 内的裸文件名）。只有列出的文件会被插件接管；插件只在内存中读取 token，不生成另一份 token 文件。列出的文件需配套外部刷新脚本（见下文）。
4. 客户端使用 Responses 协议调用 `gpt-6-astra-basispoints`。模型目录声明图像输入，以及 `low`、`medium`、`high`、`xhigh`、`max`、`ultra` 思考等级；`max` 映射为 `xhigh`，`ultra` 原样传递，未指定时默认 `medium`。

插件的 `auth.parse` 只接管 `dedicated_auth_files` 中列出的 `type: codex` OAuth 文件，并以「共享模式」展开两条内存认证：一条保留原生 `codex`（现有 Codex 模型继续使用 CPA 原生执行器），另一条是 `oai-basispoints` 虚拟认证。两条记录都**不携带 refresh_token**，因此 CPA 不会轮换该凭据（**前提：CPA 未以 Home 控制面模式运行**，即启动参数没有 `-home-jwt`；Home 刷新不依赖 refresh_token，启用 Home 时不要使用本模式）；刷新由外部脚本独占完成（fork 仓库配套 `cpa-codex-token-refresh`），脚本原子改写文件后，CPA 重新加载，两条记录同时拿到新的 access token。两条记录都由 CPA 标为 `plugin_virtual`，CPA 不会把它们写回 OAuth 文件；只有虚拟记录额外标记 `runtime_only`，native 记录保持可在面板上刷新额度、重置额度。未列出的 codex 文件插件不接管，由 CPA 原生加载、刷新并写回，也不提供 Basis Points 模型。之所以这样设计：CPA 会把插件展开出的多条记录都标记为虚拟认证、不持久化刷新结果，若由 CPA 刷新，新的 refresh_token 只留在内存，文件中的旧值随即作废，重启后凭据失效。流式响应遵循 Responses SSE 格式：http 传输下 message 正文边读边转发（逐段增量）；工具调用需在完整 item 上做安全转换，与推理、推理摘要一起在终态整批回放；ws 传输仍先读完上游再回放。`heartbeat_seconds: 0` 只关闭心跳，不影响正文增量。

## 构建

```bash
make test
make build
```

## 版本号

本 fork 自 `0.1.16.0` 起使用**四段纯数字**版本号 `<主>.<次>.<修订>.<fork 序号>`（如 `0.1.16.0`、`0.1.16.1`），tag 为 `v` 加版本号，并与 `internal/basispoints/types.go` 的 `Version` 一致（release 工作流会校验）。原因：CPA 插件商店只对全数字点分版本比较大小，带后缀的版本号会被当作「不同即更新」；原仓库始终用三段版本号，四段 tag 不会与之重名。

## 协议边界

- http 流式下 message 正文边读边转发；工具调用、推理与推理摘要在终态整批给出，终态须与已转发正文一致。`stream_tool_mode: buffered`（默认 `incremental`）时，本轮有可调用工具的回合改为整轮校验通过后再交付：失败的那次不交付，交付前仍可重新生成一次，代价是这些回合看不到逐字输出；无工具或 `tool_choice: none` 的回合不受影响，非流式与 ws 传输不受影响。
- 上游请求始终带 `Authorization: Bearer <access_token>`、`chatgpt-account-id`、`x-openai-account-id` 和 `x-basispoints-auth-mode: chatgpt`。
- `turn_id` 按会话和当前用户 turn 稳定生成；工具结果回合只递增 `agent_iteration`，不会把同一 turn 重新当成新计划。
- 工具中继（0.1.17.0 起）：外层 `run_officejs` 的 `references` 恰好列出一个完整工具名，`code` 只放该工具的载荷且始终是字符串——function 为参数 JSON 对象的文本，custom 为原文（逐字保留，不做 JSON 解码）。插件只解析它，不执行其中内容。
- 过渡期兼容：`references` 缺失或恰好为 `[]`，且 `code` 是合法的旧 `{"tool":…,"args":…}` 封装时，仍按旧路由接受（汇总里记为 `legacy`）；`references` 为其他任何值都严格按新格式处理。旧解析器的其他宽容（对象型 `code`、嵌套 `run_officejs`、对整个 `code` 再解码一层）不再接受；只有 function 参数恰好多套一层 JSON 字符串时允许解码一次。历史里的旧格式调用原样回放。
- 用户消息图片（0.1.17.1 起）：data URL 先上传到附件接口，客户端已有的 `file_id` 直接引用，两者发往上游时都只带 `{"type":"input_image","file_id":…}`。Basis Points 的文件引用不接受 `detail` 等字段（带 `detail` 会返回 422），所以客户端指定的 `detail`（含 `high`、`original`）不会转发；这只是为了匹配上游接受的形状，不代表精度语义与公开 Responses API 相同。同一张图的 `file_id` 和 `image_url` 都是非空字符串时，插件会在任何上传或请求之前返回 400 `invalid_image`，并给出 `input[i].content[j]` 位置（空值或非字符串值视为未提供）。上传格式按字节识别为 PNG / JPEG / GIF / WebP，使用固定后缀与 MIME（声明须为 `image/*`；别名或与字节不符时以字节为准；无法识别时在上传前报错）。远程 URL 图片和工具结果里的图片不做上述处理。上游拒绝带图片的请求时，错误附 `image_refs`（最多 16 个位置与引用类别），上游文本改为安全摘要：只取 JSON 中的错误字段，解码后替换凭据和本次请求的图片引用，再把 data URL、http(s) 链接、`file-…` 形式的回显替换掉，最后截断到 300 字节；原文不是 JSON 时只给固定摘要。代价是这类错误里的普通文档链接也会被替换。
- 工具调用失败的诊断区分「目录里有但本轮 `tool_choice` 不允许」（`tool_not_allowed_by_tool_choice`）与「未声明」（`tool_not_in_catalog`）；custom 调用的 item id 使用 `ctc_` 前缀。

## 中继汇总日志（0.1.17.0 起）

每次执行在收尾时经 `host.log` 发送一条汇总，正文形如 `basispoints: relay_summary {...}`（CPA 的日志格式只保留 message，所以 JSON 写在正文里）。正常收尾时在 `host.stream.close` 之前写出，日志行带宿主 request_id；被看门狗强制关流（超时、插件停止）时，关流不等待日志，汇总在往返协程收尾时补记，可能不带 request_id。`host.log` 失败时插件不重试，该条汇总会缺失。汇总是收尾时的快照：开始写之前若已被强关，以强关原因为准；开始写之后才发生的强关（例如 `host.log` 阻塞期间截止看门狗关流）不会再反映到这条记录里。字段只含类别与计数，不含请求或响应内容、工具参数、凭据：

| 字段 | 含义 |
|---|---|
| `v`、`version`、`model` | 格式版本、插件版本、客户端模型别名 |
| `transport`、`stream` | `http` / `ws`；是否流式 |
| `config_mode`、`delivery` | 配置的 `stream_tool_mode`；实际交付方式 `incremental` / `buffered` / `terminal`（ws）/ `non_stream` |
| `tool_callable` | 本轮是否有可调用的客户端工具；请求无法解析时为 `null` |
| `attempts_started` | 发起的上游往返次数（首次 + 重新生成） |
| `text_committed` | 工具校验前正文是否已进入提交 / 交付边界（插件开始把正文交给宿主流；不证明客户端已收到） |
| `first`、`final` | 本次执行内第一次有效的工具校验结果 / 最后一次尝试的工具校验结果：`not_run`、`no_call`、`ok`、`legacy`、`invalid:<原因>`。重新生成开始时 `final` 重置为 `not_run`，所以重新生成那次因上游错误中止时 `final` 是 `not_run`，不会残留第一次的 `invalid` |
| `regen` | 重新生成结局：`none`、`success`、`exhausted`（重新生成后仍失败）、`aborted`（重新生成后未走到校验） |
| `legacy_calls` | 按旧封装接受的调用数（只在整批校验通过时累计） |
| `exit` | `completed`（终态已交给宿主流，不代表客户端一定收到）、`incomplete`、`failed`、`cancelled`（客户端断开，包括失败事件没能交给宿主流）、`timeout`、`stopped`、`prepare_error`、`connect_error`、`upstream_error`。被看门狗强制关流时以强关原因为准（`timeout` / `stopped`），快照语义见上 |
| `error_kind` | 失败时的错误类别 |

`<原因>` 为白名单：`invalid_json`、`trailing_content`、`not_object`、`references_invalid`、`code_not_string`、`legacy_envelope_invalid`、`tool_not_in_catalog`、`tool_not_allowed_by_tool_choice`、`arguments_schema_mismatch`、`custom_args_not_string`、`missing_call_id`、`duplicate_call_id`、`parallel_limit`、`required_tool_choice_not_satisfied`、`outer_not_transport`、`other`。

统计口径：一条记录对应一次宿主执行；同一 request_id 有多条时，请求级结局取最后一条，尝试次数与重复执行数按全部记录统计。用户可见的最终工具失败须同时满足：`final` 以 `invalid:` 开头、`exit=failed`、`error_kind=invalid_tool_call`。`final` 只描述最后一次尝试的校验结果，不能单独使用：例如首次校验失败、决定重新生成后截止时间已到，`final` 仍是首次的 `invalid`，但 `exit=timeout`、`regen=aborted`。`aborted`、`timeout`、`stopped`、`cancelled` 按 `exit` / `error_kind` 分开报告，不计入工具失败。「工具校验通过」不等于「已成功交付」：交付时插件停止或客户端断开，`exit` 分别记为 `stopped`、`cancelled`。原有三条计数日志的文本保持不变。
- 未能从 OAuth JWT 或凭据字段得到账号 ID、token 过期、上游返回非 2xx、工具名不在客户端目录中时，插件会报告明确错误，不伪造成功。

---

## 版权与社区支持

本项目基于 [MIT License](LICENSE) 开源

感谢 [LINUX DO 社区](https://linux.do/) 的支持

<a href="https://linux.do/">
  <img src="docs/assets/linuxdo.png" alt="LINUX DO 社区" width="360" />
</a>
