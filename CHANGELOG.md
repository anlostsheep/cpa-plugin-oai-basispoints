# 更新日志

## v0.1.13 — 2026-09-26（UTC）

### 修复

- **Codex CLI 工具调用报 `invalid_tool_call`**：codex-tui 0.157 把工具目录放在 `input` 里的 `additional_tools` 条目（代码模式下为 `functions` 命名空间里的 custom `exec` 等），而不是顶层 `tools`。插件能从该条目解析目录，但转换请求时把原始条目原样转发给了 Basis Points。上游把它当作真实的原生工具定义，模型于是绕过 `run_officejs` 中继，直接返回原生 `custom_tool_call exec`，插件只能按契约以 `invalid_tool_call`（流内 `response.failed`，sub2api 记为上游 502）拒绝，同一轮重试会反复失败。现在 `additional_tools` 条目一律不转发（不论大小写、空白或 role），客户端工具只经中继协议说明传给模型；目录解析规则不变（只采信 developer 条目、顶层 `tools` 优先、冲突同名工具剔除）。修复方式参考原仓库 v0.1.11 的同类修复。
- 新增回归测试：按 0.157 的请求形态，端到端截获发往上游的请求体（http 非流式与流式），断言其中没有任何 `additional_tools` 条目或原生工具定义、中继目录列出全部工具、会话条目完整保留；`functions.exec` 的中继调用仍还原为 custom 调用且输入保持 JS 字符串；绕过中继的原生直调仍被拒绝。ws 传输复用同一转换产物。

### 说明

- 上游 input 中不再含 `additional_tools`，未显式提供会话键的请求，其由历史推导的 `task_id`/`turn_id` 会与旧版本不同。这只影响升级瞬间进行中的会话能否复用上游缓存，不影响正确性。

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
