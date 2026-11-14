# molq 1.0 架构铁律

现行规范：以下 50 条取消了 capability 层，统一 Scheduler 术语，并把监视策略移出 Cluster。一个 registry 正常对应一个 active serve 进程。实现合同见 [runtime spec](molq-1-0-runtime-spec.md)。本文的 `Scheduler` 指 molq 实现抽象；“原生调度器 / native scheduler”指外部 Slurm、PBS、LSF。

名称以现在的命令为准：业务进程是 `serve`，周期客户端是 `watch`。没有 Web UI，没有 1.0 之前的 API 版本，也没有 schema 迁移。配置是 JSON 或 YAML 文件，不使用 molcfg，不读取 `MOLQ_*`。下面条文里的 Runtime 指这个 serve 进程，Observer 指 `watch`。

## 一、单一实现，所有客户端平权

1. **molq 的业务逻辑只允许实现一次。**
   Scheduler、Transport、Job schema interpretation、资源映射、脚本渲染、Cluster registry、状态标准化等核心能力只能存在于唯一的 molq runtime 中。

2. **CLI、Python 同步/异步 SDK、TypeScript SDK、Observer 地位完全平等。**
   它们都是 molq client，不互相包装，也不拥有独立的 scheduler/transport 实现。

3. **RPC 是唯一稳定的跨进程、跨语言边界。**
   所有 client 都调用同一套公共 RPC contract。CLI 和 SDK 只是同一能力的不同 projection。

4. **不同语言不得重新实现 molq。**
   Python/TypeScript 等 SDK 只实现 RPC client、类型映射和符合语言习惯的 façade。可以生成的协议类型和 client code 应尽量从统一协议定义生成。

5. **Python SDK 必须提供 OOP 用户体验。**
   用户应操作 `Cluster`、`Job`、`JobCollection` 等领域对象；`submit(...)` / `asubmit(...)` 构造 Cluster 装饰器，保留 `yield JobSpec` 的使用风格。`Molq` / `AsyncMolq` 提供连接、注册与批量操作。RPC 是实现细节，不是 Python 用户模型。

6. **CLI 是 Agent-first 的正式接口，但不高于 SDK。**
   CLI 必须具备稳定 structured output、稳定 error/exit semantics、非交互运行和良好 discoverability；Agent 不应解析人类表格输出。

---

## 二、Runtime 是 molq 本体，部署方式不是架构

7. **只有 molq runtime 执行真正的 molq 操作。**
   Client 不直接运行 `sbatch`、`squeue`、`ssh`、`rsync` 或访问 Runtime 持久层。

8. **Runtime 跑在哪里是部署选择，不是业务架构。**
   它可以运行在：
   - 用户本机；
   - HPC login node；
   - 独立管理节点；
   - 短期 stdio 子进程；
   - 长期网络服务。

   这些模式必须使用同一套 runtime implementation。正常部署为一个 registry 一个 active Runtime；需要共享 cache/events/cursors/observation state 的 clients 连接同一 endpoint。无常驻 Runtime 的 native 使用可启动临时 stdio Runtime。SQLite 仅防护意外短暂重叠访问，不保证多个独立 Runtime 的观察状态同步，不建设分布式协调。

9. **本地无常驻服务不等于走第二套代码路径。**
   Native CLI 可以临时启动同一个 runtime，并通过 stdio RPC 调用它；不得退化成 CLI 直接调用 Scheduler。

10. **stdio、HTTP、WebSocket 只是同一 RPC contract 的不同 transport。**
    不能因为接入方式不同而产生不同业务语义。

11. **Runtime 的实现语言不是公共契约。**
    0.9.0 的唯一 runtime implementation 使用 Go。Python 与 TypeScript 只提供 RPC clients；公共 CLI/SDK/API 不依赖内部实现语言，不在 Python 中恢复业务实现。

---

## 三、Observer 是客户端，不是第二个 molq

12. **长期自动观察程序只是特殊 client。**
    Observer 通过公共 RPC 查询 Cluster、观察 Job、发送 notification；它不得直接访问 Scheduler、Transport、SSH、数据库或原生调度器。

13. **Observer 不拥有 Job 状态真相。**
    Job snapshot 和 observed change 必须来自 Runtime 通过 Scheduler 实际查询原生调度器的结果。

14. **Observer 不实现第二套 lifecycle diff。**
    状态比较、event generation 和 snapshot semantics 应在 runtime 中统一完成；Observer 只决定“何时观察”和“如何通知”。

15. **Notification 永远是观察层。**
    Nerve、邮件、UI 等失败不能影响 submit、cancel、query 或原生调度器上任务的正确运行。

16. **Observer 不是任务正确性的前提。**
    Observer 关闭后，提交、查询、取消、日志等普通功能仍必须完整可用。

---

## 四、原生调度器是 Job 真相，molq 不复制世界

17. **原生调度器是任务运行状态的事实来源。**
    queued、running、completed、failed、cancelled 等状态应直接来自原生调度器/live accounting；没有批调度器的 Shell 目标依赖原始 launch/process/exit 证据，不建立通用状态镜像。

18. **默认不持久化 Job 状态镜像。**
    molq 不维护一份长期 `JobState` 数据库去复制原生调度器已经拥有的信息。

19. **Cache 必须可丢弃。**
    Runtime 可以缓存 queue、job snapshot、health、freshness 等信息，但 cache 丢失后必须能够重新查询原生调度器恢复。

20. **只有 molq 自己拥有的信息才值得持久化。**
    例如：
    - Cluster registry；
    - molq settings；
    - presets/defaults；
    - 必要的 Runtime-owned 用户偏好。

    原生调度器已经拥有的状态不得为了方便查询而永久复制。

21. **Persistence 是实现细节，不是领域模型。**
    唯一的 SQLite 保存 Cluster registry、registry 身份和提交防重证据。settings/presets 使用 JSON 或 YAML 配置文件，但不能再次演化成隐藏的 Job lifecycle database。

22. **Client 永远不能直接访问 Runtime 持久层。**
    Cluster/Runtime-owned config 的读取和修改必须经过公共 runtime API；client-local 连接、观察 cadence 与通知路由配置由各 client 自行加载，不属于 registry。

---

## 五、Cluster 是显式配置，不做智能猜测

23. **Cluster 是 molq 持久化的一等领域对象。**
    它仅持久化计算目的地：stable ID/name、Scheduler binding、Transport kind/SSH alias、defaults、必要 site-specific Scheduler/rendering 配置与 target paths。Cluster 不含 monitor preferences、observation cadence、notification preferences 或 polling intervals；这些策略归 Observer/client 配置，SDK、前台 wait 与后台 Observer 可以用不同频率。

24. **Cluster 与 SSH Host 是不同概念。**
    SSH alias 描述“如何连接”；Cluster 描述“这是什么计算目标、绑定哪个 Scheduler、有哪些 molq 语义”。

25. **Cluster 不维护 capability 清单，也不自动发现运行环境。**
    Cluster 保存计算目标及必要配置。molq 不通过 SSH probe、命令试错或环境猜测选择高级功能的实现机制。

26. **Runtime 验证任务 schema，Scheduler 验证能否准确渲染。**
    无法表达的请求明确报错；资源实际是否存在、请求是否能被接受，由原生调度器判断。验证通过不保证资源、队列、权限或 MPI/MPS 配置可用；没有 support matrix object、feature registry、SchedulerCapabilities class 或功能协商。文档/conformance 覆盖表可保留，但不是 runtime state 或公共 API。molq 不偷偷选择另一种机制模拟功能。

27. **Cluster registry 只保存 molq-owned 信息。**
    不复制 OpenSSH 已经管理的 HostName、IdentityFile、ProxyJump、ControlPath 等配置，除非用户明确选择覆盖。

---

## 六、SSH 交给 OpenSSH

28. **molq 不重新实现 SSH stack。**
    默认使用系统 OpenSSH 和 rsync，而不是自建 SSH protocol、connection pool 或认证系统。

29. **OpenSSH 是 SSH 配置和 multiplexing 的事实来源。**
    `~/.ssh/config`、`ssh -G`、ControlMaster、ControlPath、ControlPersist、ProxyJump、IdentityFile 等都交给 OpenSSH。

30. **已有 ControlMaster 应被自然复用。**
    Terminal、molq runtime 和其他程序只要解析到同一 ControlPath，就应该共享 OpenSSH multiplex connection。

31. **molq 不拥有用户已有 SSH master。**
    不得因为 runtime 停止而关闭一个由用户或其他程序建立的 ControlMaster。

32. **Transport 与 Scheduler 必须正交。**
    SSH Transport 不理解 Slurm 语义；SlurmScheduler 不实现 SSH。Scheduler 只通过 Transport 执行操作。

---

## 七、Schema 是产品核心，不是 sbatch 参数集合

33. **所有标准任务必须由 molq schema 完整描述。**
    原生调度脚本应由 Scheduler 从 schema 渲染产生，而不是要求用户维护一份与 molq 平行的手写 sbatch 脚本。

34. **真实常见场景需要绕过 schema 时，优先认为是 schema 缺陷。**
    不应轻易以“高级用户可以自己写 shell”掩盖领域模型缺失。

35. **Schema 必须可退化。**
    最简单 Job 应保持简单，不因为 MPS、MPI、多进程等高级能力而承担额外复杂度。

36. **Schema 必须可扩展且正交。**
    高级功能通过可选、组合式领域语义加入，不能不断在顶层堆 `xxx=True`。

37. **Job 不等于单个 process。**
    Job 表示一次原生 scheduler allocation；一个 Job 可以包含一个或多个 execution units。

38. **Execution 必须原生支持多个 execution units。**
    一个 allocation 内可以执行多个程序，并能够表达必要的并行/顺序关系，而不是强迫用户把整个 execution topology 手写成 Bash。

39. **Resources、Scheduling、Execution 必须保持语义分离。**
    - Resources：向原生调度器申请什么；
    - Scheduling：原生调度器如何安排；
    - Execution：allocation 内实际运行什么。

40. **Scheduler-specific syntax 不得进入公共 schema。**
    公共 schema 表达领域意图；SlurmScheduler/PBSScheduler/LSFScheduler 负责转换成自己的 directive 和命令。

---

## 八、MPS 等高级资源必须遵守同一边界

41. **molq 只抽象原生调度器正式暴露的资源语义。**
    如果管理员将 NVIDIA MPS 暴露为原生 scheduler resource，molq 可以将它建模为 typed `nvidia_mps` 资源并由 Scheduler 渲染。

42. **管理员没有提供的资源能力，molq 不做代理实现。**
    如果原生调度器没有提供 managed MPS resource，molq 不自动启动 MPS、不模拟 MPS resource、不实现 fallback；资源请求由原生调度器接受或拒绝，失败时也不转换为整卡 GPU 请求。

43. **用户仍然可以通过普通 Execution 表达任意合法命令和环境变量。**
    用户自行启动 MPS、设置 NVIDIA 环境变量时，molq只把这些视为普通 execution 内容，不赋予特殊 MPS 语义。

44. **不能用一个字段同时表示 原生 scheduler resource 与用户 runtime 行为。**
    例如原生调度器管理的 `nvidia_mps` 资源 与用户设置的 CUDA/MPS 环境变量是不同层次，必须保持区分。

---

## 九、Scheduler 是唯一原生调度语义所在地

45. **所有 scheduler-specific 行为只存在于对应 Scheduler 实现。**
    包括：
    - resource mapping；
    - directive rendering；
    - dependency syntax；
    - submission；
    - query/list/history 与 queue parsing；
    - accounting；
    - cancel；
    - scheduler-native execution-step / launcher mapping；
    - 原生调度器状态标准化。

46. **上层不得根据 scheduler 名称分支实现业务。**
    新原生调度系统通过实现统一 Scheduler contract 接入；factory/registry 选择实现只是组装，不在上层增加调度语义。Scheduler 是普通进程内模块，不另套 Driver/Adapter/Provider，也不增加服务进程。

47. **同一个 Job schema 由不同 Scheduler 渲染时允许产生不同 native representation。**
    差异属于 Scheduler，而不是 client 或用户。

---

## 十、不重复成熟基础设施

48. **molq 只实现自己真正拥有的问题。**
    - SSH → OpenSSH；
    - Job execution truth → 原生调度器/accounting；
    - TLS/auth → 优先复用成熟部署设施；
    - dependency solver/container builder 等不属于 1.0 范围。

49. **禁止为了架构“整齐”而复制成熟能力。**
    如果已有系统已经提供可靠的配置、连接复用、状态或生命周期管理，molq 应集成它，而不是重新实现一套。

50. **每新增一个 subsystem，都必须回答：为什么现有系统不能承担这个职责？**
    如果答案只是“自己实现比较统一”，不足以成为新增实现的理由。

---

# 最核心的十句话

**Molq 的业务逻辑只能有一份实现。**

**CLI、SDK 和 Observer 都只是平等 client。**

**RPC 是公共边界，runtime 是 molq 本体。**

**部署位置和实现语言都不是领域架构。**

**原生调度器管 Job 真相，Scheduler 管原生语义，molq 不永久复制任务状态。**

**Cluster 持久化计算目的地，不维护 capability 清单或监视策略、不复制 SSH 配置。**

**SSH 交给 OpenSSH，molq 不重造连接管理。**

**Job 是 allocation，不等于一个 process。**

**Schema 必须简单可退化、高级可组合；常见场景无法表达就是 schema 缺陷。**

**已有基础设施能做的事情，molq 不重复实现。**
