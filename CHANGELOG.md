# 更新日志

## v0.1.15 — 2026-09-26（UTC）

### 新增

- **Codex 网页搜索改走原生 codex**（新配置 `alpha_search_model`，默认留空即关闭）：codex-tui 的网页搜索（`exec` 中调用 `tools.web__run`）单独请求 `/v1/alpha/search`，请求中的模型是当前会话模型。CPA 7.3.17 只从原生 codex 凭据中按模型挑选；Basis Points 模型只挂在本插件的虚拟记录上，因此返回 `auth_not_found`（503），sub2api 随即临时冷却「账号 + 该模型」约 10 秒，期间该模型的请求都报「没有可用账号」。
  - 配置一个原生 codex 能提供的模型名（如 `gpt-6-luna`）后，插件声明 `model_router` 能力，对 `SourceFormat=codex-alpha-search` 且模型属于本插件（按 CPA 规则从最后一个 `(` 去掉 `(xhigh)` 等档位后缀）的请求返回「改道到 provider `codex`、用该模型挑选凭据」。CPA 随即在原生 codex 凭据中重新挑选，经所选凭据的 `proxy_url` 转发到 ChatGPT 搜索后端，消耗所选凭据的额度。
  - **账号归属前提**：CPA 的路由契约无法指定凭据。只有当 CPA 中能提供该模型的原生 codex 凭据恰好是 Basis Points 凭据的同一账号时，搜索才一定由同一账号执行；存在其他原生 codex 凭据或获准 alpha search 的 codex API key 时，搜索可能由它们执行并消耗其额度（与原生模型自己的网页搜索相同）。增加 codex 凭据前应先评估，必要时把 `alpha_search_model` 设为 `""`。CPA 以 Home 模式运行（`-home-jwt`）时会从 Home 凭据池挑选，本地清点不足以证明账号归属，此时不要启用（共享模式本身也要求不以 Home 模式运行）。
  - **带凭据前缀的请求不改道**：固定的目标模型会丢掉前缀而选错或选不到凭据，且 OAuth 转发会原样保留请求体中的前缀，上游是否接受未经验证；这类请求保持 CPA 原有行为。
  - **目标可用性无法预检**：插件只能确认有 codex 提供方，看不到 CPA 的模型注册表与冷却状态；目标模型不可用时 CPA 挑选失败，结果与未改道时相同（503）。上线后需冒烟验证。
  - OAuth 凭据转发时 CPA 不改写请求体里的模型名，该配置只决定挑选哪个凭据；已实测 ChatGPT 搜索后端接受请求体中的 Basis Points 模型名。
  - 其余请求（普通 `/v1/responses`、原生模型、当前没有 codex 提供方时）一律返回「不处理」；判定只读 `SourceFormat`、`RequestedModel`、`AvailableProviders`，解析失败或 panic 都返回「不处理」，不返回错误。
  - **只在配置了该项时才声明 `model_router`**：声明后 CPA 处理每个请求都会先调用插件一次，并传入完整请求体。插件侧判定 1 MiB 请求体约 0.8 ms；CPA 克隆请求体、RPC 编码与跨 cgo 调用的开销另计，未测量。CPA 在 reconfigure 时按新注册结果重建能力，开关可热生效。
  - 校验：不能是 Basis Points 模型（改道后仍找不到原生凭据），不能含空白或控制字符，否则拒绝加载。关闭时设为 `""`；删除该键会被 `settings.json` 镜像补回旧值。

## v0.1.14 — 2026-09-26（UTC）

本版移植原仓库 JaxsonWang/cpa-plugin-oai-basispoints v0.1.10（及 v0.1.12 的 `count_tokens`）中本 fork 缺少的请求/响应契约修复。解析与诊断代码尽量原样采用，并移植了原仓库的契约测试；只在与本 fork 流式架构（延迟心跳、流会话、ws 传输）衔接处做了适配。原仓库 v0.1.12 的 Claude Code 兼容本版未移植。

### 修复

- **中转格式诊断 + 插件内重新生成一次**（原仓库 #6）：模型输出不符合 `run_officejs` 中转契约时，错误带结构化原因，例如 `code invalid_json byte_offset=N`、`tool_not_in_catalog`、`arguments_schema_mismatch`、`custom_args_not_string`，不含工具参数或正文。首次出现时，插件在请求末尾附加纠正提示和该原因，自动重新请求一次；仍失败才报错。http 非流式、http 流式、ws 三条路径都支持；流式重试期间心跳照常，每次往返都受客户端断开、插件停止约束，各自的上游流恰好关闭一次。首次往返与重新生成共享同一个 `timeout_seconds` 截止时间，重新生成不会重置超时，截止时间已过则不再发起请求。被拒绝的调用不写入回放缓存。上游因截断而 `incomplete` 时不重试。
- **中转错误统一为 422 `invalid_tool_call`**：原先非流式下多数中转错误是 502，可能让 CPA 冷却凭据；`invalid_tool_code` 并入 `invalid_tool_call`（诊断原因中的 `code …` 即对应原来的情形）。流式下仍以流内 `response.failed` 正常关闭。
- **终态解析**（原仓库 #8）：按正文实际格式解码 JSON 或 SSE（非流式遇到 SSE 正文不再失败）；拒绝空正文、HTML、非法 JSON/SSE 帧、多个终态、事件与状态不一致、尾随数据；错误只附内容类型类别和字节数，不回显正文。重新编码后的非流式响应改为 `application/json`，并去掉过时的 `Content-Length` 等实体头。上游在帧内报告的失败仍按原有分类映射为 401/403/429。能否把这些状态码交给 CPA 取决于错误何时被发现：非流式请求同步返回，带状态码；流式请求在延迟心跳窗口内只有建连阶段的错误（HTTP 非 2xx、WS 握手被拒）能同步带回状态码，而 200 响应正文或 WS 帧里报告的失败是在返回流头之后读取的，只能以无状态码的流错误关闭。重新生成那一轮的失败同样属于后者，与 v0.1.11 起延迟心跳的既有折中一致。
- **`response.incomplete` 是合法终态**：如达到 `max_output_tokens`，原样以 `response.incomplete` 交给客户端（http 与 ws 相同），不再当作失败，也不伪装成 `response.completed`。`response.cancelled` 按失败处理。
- **不支持的请求明确拒绝**（原仓库 #3、#9、#10）：
  - `previous_response_id` → 400 `unsupported_continuation`，避免只带增量 input 时静默丢失上下文。CPA 的 WebSocket 会话对未声明 `websockets` 的凭据会先合并历史、删除该字段再交给插件，不受影响。
  - `/responses/compact` → 400 `unsupported_compaction`，避免普通生成冒充压缩结果。
  - `service_tier` 只接受空、`auto`、`default`，且不再转发给上游；其余（`priority`、`fast` 等）→ 400 `unsupported_service_tier`。模型目录不再声明 Fast。
- **`count_tokens`** 改为 400 `unsupported_token_count`，不再返回伪造的 0。

### 说明

- 重新生成最多额外消耗一次上游请求。
- **未验证**：Codex 0.157 的远程压缩 v2 不走 `/responses/compact`，而是在普通 `/responses` 请求中追加 `compaction_trigger`，并要求返回恰好一个 compaction 输出项。插件对这条路径的处理与 v0.1.13 相同（原样转发），但 Basis Points 是否支持尚无实测；截至本版发布，现网尚未出现 Basis Points 模型触发压缩的记录。
- `run_officejs` 的 `code` 被多套一层 JSON 字符串转义时，仍按本 fork 原有的容错规则剥掉一层再解析（纯解析，无副作用）。

## v0.1.13 — 2026-09-26（UTC）

### 修复

- **Codex CLI 工具调用报 `invalid_tool_call`**：codex-tui 0.157 把工具目录放在 `input` 里的 `additional_tools` 条目（代码模式下为 `functions` 命名空间里的 custom `exec` 等），而不是顶层 `tools`。插件能从该条目解析目录，但转换请求时把原始条目原样转发给了 Basis Points。上游把它当作真实的原生工具定义，模型于是绕过 `run_officejs` 中继，直接返回原生 `custom_tool_call exec`，插件只能按契约以 `invalid_tool_call`（流内 `response.failed`，sub2api 记为上游 502）拒绝，同一轮重试会反复失败。现在 `additional_tools` 条目一律不转发（不论大小写、空白或 role），客户端工具只经中继协议说明传给模型；目录解析规则不变（只采信 developer 条目、顶层 `tools` 优先、冲突同名工具剔除）。修复方式参考原仓库 v0.1.11 的同类修复。
- 新增回归测试：按 0.157 的请求形态，端到端截获发往上游的请求体（http 非流式与流式），断言其中没有任何 `additional_tools` 条目或原生工具定义、中继目录列出全部工具、会话条目完整保留；ws 传输截获首帧 `response.create` 做同样断言；`functions.exec` 的中继调用仍还原为 custom 调用且输入保持 JS 字符串；绕过中继的原生直调仍被拒绝。

### 说明

- 会话标识：请求带 `prompt_cache_key`/`session_id`（Codex CLI 总会带 `prompt_cache_key`）时，`task_id` 与 `turn_id` 都以该键为会话部分，不受本次修改影响。未带时，`task_id` 由转换后第一个 input 条目的哈希推导；旧版本对 0.157 形态的请求取到的是 `additional_tools` 条目本身，现在取第一个真实会话条目，因此这类请求的 `task_id` 会与旧版本不同；`turn_id` 由会话键与 `turnState` 拼成，`turnState` 对原始 input 的计算未改变，但会话指纹变了，`turn_id` 也会随之不同。上游对此的影响未验证。

## v0.1.12 — 2026-09-26（UTC）

### 修复

- **共享模式的 native 记录不再标记 `runtime_only`**。CPA-Manager-Plus 等管理面板把 `runtime_only` 凭据整体视为只读：卡片上的「刷新额度」按钮、工具栏「刷新额度」、「重置额度」等操作都会被隐藏或跳过，导致标记凭据无法手动刷新额度。写回保护不受影响：插件为标记文件返回两条记录，CPA 会把它们都标为 `plugin_virtual`，`Manager.persist` 对此直接跳过写回；native 记录仍不携带 refresh_token，CPA 仍无法轮换它，外部刷新脚本仍是唯一刷新方。虚拟 `oai-basispoints` 记录保留 `runtime_only`。
- **面板操作约定（native 卡片恢复后）**：只使用「刷新额度」和「重置额度」。**不要**在该卡片上执行以下操作：
  - 禁用或启用：CPA 对插件展开的凭据会直接读出源文件、改 `disabled`、再写回，既不经过 `persist`，也不与刷新脚本互斥。若恰好与脚本的原子换 token 交错，旧 token 会被写回，凭据作废；
  - 删除：会删除实体凭据文件及两条运行时记录；
  - 上传或粘贴同名凭据：CPA 的上传路径不经插件解析，refresh_token 会重新进入运行时，并可能暂时丢失 `plugin_virtual` 标记；
  - 刷新凭证、配置保存：对这类凭据会失败，不起作用。

## v0.1.11 — 2026-09-25（UTC）

### 新增与改进

- **专用凭据（共享模式）**：新增 `dedicated_auth_files`。命中的 codex 凭据在 `auth.parse` 时同时返回 native codex 与 `oai-basispoints` 两条记录，原生模型照常可用；但两条记录都**不携带 refresh_token**（Metadata 与 StorageJSON 均剔除）。CPA 原生 Codex 刷新只从 Metadata 读 refresh_token，读不到就原样返回；401 后的补救刷新也要求 Metadata 里有 refresh_token。因此 CPA 永远不会轮换该凭据，外部刷新脚本是唯一刷新方；脚本原子改写文件后，watcher 重新解析，两条记录同时拿到新的 access token。两条记录都由 CPA 标为 `plugin_virtual`，CPA 永不把解析时的快照写回凭据文件（v0.1.11 另给两条都加了 `runtime_only`，v0.1.12 起只保留在虚拟记录上）。文件身份为 auth-dir 内的裸文件名，按 CPA 传入的原始文件名精确匹配（区分大小写、不做 trim）；非法条目（任意位置的空白或控制字符、路径分隔、重复）视为配置错误，不静默跳过。修改列表后需重启 CPA 才会对已加载凭据生效。依赖前提：CPA 未以 Home 控制面模式运行（启动参数没有 `-home-jwt`）。Home 刷新凭 auth_index 与 access token 摘要换取新认证，不依赖 refresh_token，会绕过这一保护，因此启用 Home 时不要使用本模式。CPA 升级后需复核上述刷新行为。
- **不兼容变更：未标记的 codex 凭据不再接管**。v0.1.10 会把每个 codex 文件展开成 native + 虚拟两条记录，CPA 会把多条记录都标为 `plugin_virtual` 并跳过写回：原生刷新得到的新 refresh_token 只留在内存里，文件中的旧值随即作废，CPA 重启后该凭据失效。v0.1.11 对未标记文件返回 `Handled:false`，交还 CPA 原生加载器（CPA 自行刷新并写回）；Basis Points 模型只对 `dedicated_auth_files` 中的凭据提供。升级前请先把需要 Basis Points 的凭据加入该列表，并配套部署外部刷新脚本。
- **面板为唯一配置来源**：插件持久化的 `settings.json` 改为「生效配置镜像」——宿主 YAML（面板）中出现的键一律以 YAML 为准，镜像只补 YAML 未提供的键，并改为原子写入。修复旧实现中 `settings.json` 覆盖面板修改的问题。镜像损坏（不可读、非 JSON 对象、补缺字段类型错误、非可空字段为 `null`）时插件拒绝加载，不会把损坏的列表当作空列表覆盖回去。外部刷新脚本读取该镜像中的 `dedicated_auth_files`。
- **流式心跳**（`heartbeat_seconds`，默认 15，0 关闭）：两种传输都需等上游完整回复后回放。建连（http 等待响应头 / ws 拨号 + 首帧）采用**延迟心跳**：最多同步等待一个心跳间隔——窗口内失败（http 以响应头到达为准，非 2xx 的错误正文只做有界读取；读取期间若守卫已因插件停止/客户端断开/总超时中止，以中止原因为准，只有正文读取上限造成的截断才保留原状态码；中止原因只有一个判定点（看守协程只负责被唤醒，与同步检查共用同一方法），按 插件停止 > 客户端断开 > 超时 的固定优先级判定，不依赖看守协程的调度时序或 select 的随机选择）时下游零字节，`execute_stream` 同步返回带 HTTP 状态码的错误，CPA 可按 401/403/429 正确换号或冷却；窗口到期仍未建连才先开流发出 `response.created`，之后建连失败只能以无状态码的流错误关闭（有意的折中）。开流后会话立即发出 `response.created`，缓冲期间定时发送 `response.in_progress`，防止 sub2api(180s)/Codex(300s) 空闲超时切断长回合；最终回放与开场事件序号连续、`response.id` 一致。心跳发送失败即判定客户端断开并取消上游（ws 发送 `cancel` 帧；http 由每个往返唯一的守卫统一中止：发请求前经 `host.http.operation_open` 申请 operation 并随 `do_stream` 携带，断开/停止/超时时调用 `host.http.cancel` 解除**等待响应头**的阻塞，拿到流后再 `stream_close` 解除阻塞中的读取；往返结束时守卫等待看守协程退出、恰好一次关闭上游，之后不再有宿主回调）。
- **错误分类（含流式）**：请求层面的错误（模型输出了非法 `run_officejs` code、工具调用不符合目录、上游半途失败/排空）在流式下改为向客户端发送 `response.failed` 后**正常关闭**，不再以无状态码的流错误关闭——后者会被 CPA 当作瞬时故障冷却凭据。凭据/限流/传输类错误（401/403/404/429、传输失败、超时）仍带 error 关闭，交给 CPA 换号或冷却。流已建立后上游在帧/事件内报告的失败（`response.failed`/`response.incomplete`/`error`）会先解析帧内状态与错误码：凭据失效、限流、额度耗尽、账号停用映射为 401/403/429 交给 CPA，（同时读取帧、`response` 对象及其 `error` 上的数值状态码）其余才按本次请求失败处理；错误串只保留短标识符形态的错误码。客户端断开时静默关闭。非流式下 `run_officejs` code 无法解析返回 `422 invalid_tool_code`。
- **网页版请求头对齐**：`client-runtime=web`、`client-platform-class=OfficeOnline`、`office-platform=OfficeOnline`，新增 `browser-name/browser-ua-platform/browser-ua-mobile/browser-ua-brands` 与 `x-openai-account-user-id`；`User-Agent` 等可配置（`user_agent`、`ua_platform`、`ua_brands`、`chrome_version`），不发 `referer`，`x-stainless-*` 不变。
- **WebSocket 传输**（`transport: ws`，默认仍 `http`）：按 HAR 实测的查询参数与 `responses, openai-bearer.<token>` 子协议握手（permessage-deflate）。每次 `execute_stream` 建一条连接、发一帧 `response.create`（全量 input，附 `basispoints_request_id`/`basispoints_resume_token`），读到 `response.completed` 透传 usage 后关闭，不复用连接。建连（拨号 + 首帧）按延迟心跳规则进行：一个心跳间隔内建连失败时下游零字节，`execute_stream` 同步返回带状态码的错误，CPA 可换号或冷却；超过窗口后的建连失败以无状态码的流错误关闭。取消通过独立读取上下文确保 `cancel` 帧能发出。`proxy_url` 仅用于 ws 传输（http 传输走 CPA 全局 `proxy-url`，两者应指向同一出口）。bearer 只出现在子协议，不入日志；握手被拒只返回固定文案与 HTTP 状态码（依赖库会把回显的子协议写进错误，旧的脱敏不识别 `openai-bearer.` 前缀），其他握手/写帧错误，以及 http 路径的宿主传输/读取错误，都先按当前令牌精确脱敏。`plugin.shutdown` 会取消当前这一代所有进行中的往返（ws 发送 `cancel` 帧，http 取消 operation / 关闭流）并等待其退出（上限 5 秒）后再返回，被中断的请求以 `response.failed(plugin_stopped)` 正常结束；最终回放前在持锁的输出边界再次检查停止状态。生命周期在任何宿主 HTTP 调用（含图片附件上传）之前登记，附件上传与非流式请求也经 operation 受 shutdown/超时取消。下游持续不读（宿主队列满、emit 阻塞）时，停止后的看门狗在 1 秒宽限期后强制关闭该流（宿主在队列满时仍接受 `host.stream.close` 并解除阻塞的 emit），`host.stream.close` 每个流恰好调用一次，保证插件卸载有界。生命周期按「代」隔离：每个往返绑定进入时那一代的 ctx（以 `errPluginStopped` 为取消 cause，据此与客户端断开、超时区分），停止（含等待）与重建串行执行，重建后的新一代不会被旧往返误用，也不会被旧往返拖住。
- **按凭据出口**：凭据文件顶层的 `proxy_url` 随 native 与虚拟记录交给 CPA（记录的 `ProxyURL`）。CPA 据此为执行上下文注入该出口的传输，插件经宿主发出的 http 请求随之走该出口，全局 `proxy-url` 可以为空。ws 传输同样优先使用凭据的 `proxy_url`，其次才是插件配置的 `proxy_url`；`direct`/`none` 表示显式直连，支持 http、https、socks5。ws 直连时不再读取环境变量代理。出口值按 CPA `proxyutil.Parse` 的同一规则校验（空、`direct`/`none`、或带主机的 socks5/socks5h/http/https）；凭据 `proxy_url` 无效时在任何上游调用（含附件上传）之前以 `invalid_proxy` 拒绝，不会退回直连。插件 http 请求只有在 CPA 全局 `proxy-url` 为空时才走按凭据出口（宿主优先使用全局值）。
- **作者署名**：`registry.json`、插件元数据 `Author`/`GitHubRepository` 指向 anlostsheep；`LICENSE` 保留原作者并增列 anlostsheep；README 注明 fork 来源。插件 ID 与 go module 路径不变。

### 兼容性

- `transport` 默认 `http`；除默认开启的流式心跳外，http 路径行为与 v0.1.10 一致。设 `heartbeat_seconds: 0` 可完全回到 v0.1.10 的一次性回放。

### 已知限制

- 仍为「缓冲后回放」：文本与推理不做逐 token 增量输出，工具调用参数一次性给出（增量流式计划在 v0.2.0）。
- `response.id` 在开启心跳时为插件生成的会话 id（上游 id 不透出）；`store:false` 下不影响续接。

## v0.1.10 — 2026-09-25（UTC）

### 修复

- 兼容 Codex CLI 将工具目录放在开发者 `input.additional_tools` 而非顶层 `tools` 的请求。保留显式顶层目录优先、`tool_choice` 限制和命名空间，拒绝冲突工具，并维持原生工具身份与结果回放。
- 为 `functions.exec`、`functions.wait` 及拒绝路径增加脱敏回归测试；本地测试通过，真实 Basis Points 工具回合仍需安装后验证。
- 将本 fork 的插件源、商店安装仓库和插件元数据指向 `anlostsheep/cpa-plugin-oai-basispoints`。

## v0.1.9 — 2026-09-24（UTC）

### 新增与改进

- 新增 `model_mappings`，支持任意数量的客户端别名分别映射到不同上游模型，可在同一插件实例中同时配置 Astra、Sol 及其他实际可用模型。
- 模型注册、普通请求、流式请求及 Codex 模型目录元数据使用同一份映射；上下文容量按各自规范模型读取，不复用其他模型的数值。
- 保留原有单上游配置：未单独映射的别名继续使用 `upstream_model`。校验未知别名、空映射和去除首尾空白后的重复映射键，避免误路由。
- 补充配置重载、持久化、并发隔离和 1、2、3、25、100 个模型的回归测试。
- 将 `config.example.yaml` 改为完整 CPA 宿主配置，并补全中文注释、模型增删方法及配置优先级说明；修正插件元数据中的仓库地址。

### 升级注意事项

- 配置位于 `plugins.configs.oai-basispoints`；`models` 声明启用的别名，`model_mappings` 声明对应上游，增加或删除模型时同步修改两处。
- 推荐按示例显式设置 `data_dir: ""`，仅使用 YAML 配置。若保留持久化目录，旧 `settings.json` 中的字段仍优先于 YAML，可能导致新增模型不显示或出现 `model_mappings alias is not enabled in models`。本版未改变持久化配置的优先级。
- 替换实际生效的插件动态库后重启 CPA，再刷新模型列表；不要通过改插件 ID 或另存同目录副本来替代原插件。
- 发布检查不等于真实上游联调；模型可用性仍受 Basis Points 和账号权限限制。Fast、流式缓冲及 OAuth 刷新同步等既有限制保持不变。

## v0.1.8 — 2026-09-25

本版统一发布此前工作区中的图片、上下文和工具协议修复，并新增 CPA 第三方插件源与多平台 GitHub Actions 打包。

### 修复与改进

- 用户消息中的内嵌图片先上传到 Basis Points 附件接口，再发送真实 `file_id`；保留图片字节和明确精度，并隔离账号/令牌缓存。
- 修正非流式宿主 HTTP 回调的 `StatusCode / Headers / Body` 字段匹配，避免上传成功却被误报 `HTTP 0`。
- 未指定、`null` 或空的 `context_management` 不再发送；保留显式非空策略，不注入隐式 200k 阈值。
- 使用 CPA v7.3.16 已有模型目录响应钩子修正本插件别名的上下文元数据，不要求修改 CPA 主程序。
- 修复客户端 function/custom 工具的命名空间、完整参数目录、结果与原生调用回放，以及 SSE 工具参数事件；保留多模态工具结果、大整数和空白。
- 新增根目录 `registry.json`，通过 `plugins.store-sources` 接入 CPA 插件商店；版本由 GitHub 最新 Release 决定。
- GitHub Actions 校验并构建 Linux/macOS/Windows 的 AMD64/ARM64 动态库，发布平台压缩包及 SHA-256 校验清单。

### 已知问题与边界

- **Fast 尚未修复，继续在 [#3](https://github.com/JaxsonWang/cpa-plugin-oai-basispoints/issues/3) 跟踪。** 真实对照中，不传 `service_tier` 成功；传 `default` 或 `priority` 均返回 422。请省略该字段，不能将 Fast 入口或参数透传当作优先调度已生效。
- [#2](https://github.com/JaxsonWang/cpa-plugin-oai-basispoints/issues/2) 按维护者实际使用反馈关闭，不代表全部线上场景、500k 实际容量或 Fast 已验收。
- 上游 SSE 仍全量缓冲后回放；token 计数及跨虚拟认证的 OAuth 刷新同步限制保持不变。
- 本地测试、Actions 打包及插件商店安装契约校验，不等于已自动更新用户的 CPA 部署。更换已加载动态库后请重启 CPA。
