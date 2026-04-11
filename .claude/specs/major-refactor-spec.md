# Spec: Submitor 重构与实施路线图

  ## 摘要

  - 目标文档路径：docs/spec/submitor-redesign.md
  - 本次为破坏式重构，不保留任何向后兼容层，不保留旧 decorator、全局 registry、legacy dict、cmdline 或 import-time 默认实例。
  - 新设计以 Submitor 实例代表一个 cluster；用户通过不同 Submitor 实例区分不同 cluster。
  - Submitor.submit(...) 是唯一提交通道；内部构造 JobSpec，用户不直接接触 JobSpec。
  - 命令模型固定为三类：argv、command、script。
  - backend 一词从设计中移除；内部统一称为 scheduler，且仅作为实现细节存在。

  ## 核心接口

  - 公开类：Submitor
  - 初始化签名：Submitor(cluster_name, scheduler, *, defaults=None, scheduler_options=None, store=None)
  - 语义：
      - cluster_name 标识一个明确的 cluster 目标与其命名空间
      - scheduler 固定为 local | slurm | pbs | lsf
      - defaults 是该 cluster 的默认资源与执行参数
      - scheduler_options 是该 scheduler 的内部实现配置
      - 不做全局缓存，不共享隐式实例状态
  - 公开方法：submit(...)、get(job_id)、list(include_terminal=False)、watch(job_ids, timeout=None)、cancel(job_id)、refresh()
  - submit(...) 固定签名：
      - submit(*, argv=None, command=None, script=None, resources=None, scheduling=None, execution=None, metadata=None) -> JobHandle
  - 参数约束：
      - argv、command、script 三者必须且只能提供一个
      - resources 类型为 JobResources
      - scheduling 类型为 JobScheduling
      - execution 类型为 JobExecution
      - metadata 类型为 dict[str, str]
  - 内部对象：
      - JobSpec 为内部 canonical 模型，仅在 submit(...) 内构造并向下传递
      - JobHandle 为公开返回对象
      - JobRecord 为公开持久化快照对象

  ## 命令模型

  - argv: list[str]
      - 表示结构化参数执行
      - 永不经过 shell 解释
      - 保留参数边界
  - command: str
      - 表示单行 shell command
      - 允许 shell 特性
      - 含换行直接校验失败
  - script: Script
      - Script.inline(text: str) 表示多行脚本内容
      - Script.path(path: str | Path) 表示现有脚本文件
      - Script 是单独公开类，专门承载脚本语义
      - Script.path(...) 在提交时必须复制到 job 私有目录形成快照后执行
      - Script.inline(...) 必须物化为 job 私有脚本文件后执行

  ## 公开数据模型

  - JobHandle
      - 字段：job_id、cluster_name、scheduler、scheduler_job_id | None
      - 方法：status() -> JobState、wait(timeout=None) -> JobRecord、cancel() -> None、refresh() -> JobHandle
      - 语义：单个 job 的操作句柄，所有方法仅影响自身指向的 job
      - status() 返回缓存状态，不触发 scheduler 查询
      - refresh() 触发一次 reconcile 后返回更新的 JobHandle
      - wait() 阻塞直到终态或超时
  - JobRecord
      - 字段：job_id、cluster_name、scheduler、state
      - 字段：scheduler_job_id | None、submitted_at、started_at、finished_at
      - 字段：exit_code | None、failure_reason | None、cwd、command_type、metadata
  - 状态枚举固定为：
      - created
      - submitted
      - queued
      - running
      - succeeded
      - failed
      - cancelled
      - timed_out
      - lost

  ## Submitor 方法与 JobHandle 方法职责划分

  Submitor 方法为管理级批量操作，JobHandle 方法为单 job 操作：

  | 方法 | 作用域 | 语义 |
  |------|--------|------|
  | Submitor.get(job_id) | 单 job | 从 store 读取 JobRecord 快照 |
  | Submitor.list(include_terminal) | 批量 | 返回本 cluster 下所有 JobRecord |
  | Submitor.watch(job_ids, timeout) | 批量 | 阻塞直到指定 job 全部终态；job_ids=None 时等待所有活跃 job |
  | Submitor.cancel(job_id) | 单 job | 向 scheduler 发送取消请求 |
  | Submitor.refresh() | 批量 | 对本 cluster 所有活跃 job 做一次 reconcile |
  | JobHandle.status() | 单 job | 返回缓存的 JobState，零 IO |
  | JobHandle.refresh() | 单 job | 对该 job 做一次 reconcile，返回更新后的 JobHandle |
  | JobHandle.wait(timeout) | 单 job | 阻塞直到该 job 终态或超时 |
  | JobHandle.cancel() | 单 job | 向 scheduler 发送取消请求 |

  ## Defaults 合并策略

  defaults 参数接受 SubmitorDefaults 类型：

  ```python
  @dataclass(frozen=True)
  class SubmitorDefaults:
      resources: JobResources | None = None
      scheduling: JobScheduling | None = None
      execution: JobExecution | None = None
  ```

  合并规则（per-submit 参数与 defaults 之间）：
  - **字段级浅覆盖**：per-submit 中显式提供的字段覆盖 defaults 中的同名字段
  - **None 字段不覆盖**：per-submit 中值为 None 的字段回退到 defaults
  - **不做深合并**：不会递归合并 dict 或嵌套对象
  - 合并顺序：defaults → per-submit 覆盖 → 构造 JobSpec

  示例：
  ```python
  slurm = Submitor("alpha", "slurm", defaults=SubmitorDefaults(
      resources=JobResources(cpu_count=4, memory=Memory.gb(8)),
      scheduling=JobScheduling(queue="normal"),
  ))

  # cpu_count=4 来自 defaults，memory=Memory.gb(32) 覆盖 defaults
  job = slurm.submit(
      argv=["python", "train.py"],
      resources=JobResources(memory=Memory.gb(32)),
  )
  # 最终 JobSpec.resources: cpu_count=4, memory=32GB
  ```

  合并实现为纯函数 `_merge_defaults(defaults, per_submit) -> merged`，不修改任何输入对象。

  ## Scheduler Options 类型定义

  每个 scheduler 定义自己的 frozen dataclass，禁止 **kwargs 或 dict[str, Any]：

  ```python
  @dataclass(frozen=True)
  class LocalSchedulerOptions:
      runner_shim: str | Path = "molq-runner"  # runner 可执行文件路径
      max_concurrent: int | None = None        # 并行 job 上限，None 表示无限

  @dataclass(frozen=True)
  class SlurmSchedulerOptions:
      sbatch_path: str = "sbatch"
      squeue_path: str = "squeue"
      scancel_path: str = "scancel"
      extra_sbatch_flags: tuple[str, ...] = ()  # 固定追加的全局 flags

  @dataclass(frozen=True)
  class PBSSchedulerOptions:
      qsub_path: str = "qsub"
      qstat_path: str = "qstat"
      qdel_path: str = "qdel"
      extra_qsub_flags: tuple[str, ...] = ()

  @dataclass(frozen=True)
  class LSFSchedulerOptions:
      bsub_path: str = "bsub"
      bjobs_path: str = "bjobs"
      bkill_path: str = "bkill"
      extra_bsub_flags: tuple[str, ...] = ()
  ```

  - Submitor.__init__ 根据 scheduler 参数校验 scheduler_options 类型是否匹配
  - scheduler_options=None 时使用对应类型的默认实例
  - 传入不匹配的类型直接抛出 TypeError

  ## 资源值类型

  memory 和 time_limit 使用结构化类型替代字符串解析：

  ### Memory

  ```python
  @dataclass(frozen=True, order=True)
  class Memory:
      bytes: int

      @classmethod
      def kb(cls, n: int | float) -> Memory: ...
      @classmethod
      def mb(cls, n: int | float) -> Memory: ...
      @classmethod
      def gb(cls, n: int | float) -> Memory: ...
      @classmethod
      def tb(cls, n: int | float) -> Memory: ...
      @classmethod
      def parse(cls, s: str) -> Memory: ...  # 兼容 "8GB" 字符串输入

      def to_slurm(self) -> str: ...   # "8G"
      def to_pbs(self) -> str: ...     # "8gb"
      def to_lsf_kb(self) -> int: ...  # KB 整数
  ```

  - 内部存储为 bytes (int)，无精度损失
  - 工厂方法为首选构造方式：`Memory.gb(8)`
  - `parse()` 作为从配置文件或 CLI 解析的入口，接受 "8GB"、"512MB" 等格式
  - 支持比较运算符（用于校验）

  ### Duration

  ```python
  @dataclass(frozen=True, order=True)
  class Duration:
      seconds: int

      @classmethod
      def minutes(cls, n: int) -> Duration: ...
      @classmethod
      def hours(cls, n: int) -> Duration: ...
      @classmethod
      def parse(cls, s: str) -> Duration: ...  # "2h30m", "01:30:00"

      def to_slurm(self) -> str: ...  # "HH:MM:SS"
      def to_pbs(self) -> str: ...    # "HH:MM:SS"
      def to_lsf_minutes(self) -> int: ...  # 整数分钟
  ```

  ### JobResources 更新后签名

  ```python
  @dataclass(frozen=True)
  class JobResources:
      cpu_count: int | None = None
      memory: Memory | None = None
      gpu_count: int | None = None
      gpu_type: str | None = None
      time_limit: Duration | None = None
  ```

  ## 错误模型

  定义统一异常层次，所有异常继承自 MolqError：

  ```
  MolqError (base)
  ├── ConfigError          # Submitor 初始化参数错误
  ├── SubmitError          # 提交失败（scheduler 拒绝、脚本物化失败等）
  │   ├── CommandError     # argv/command/script 校验失败（三选一违反、command 含换行等）
  │   └── ScriptError     # Script.path 文件不存在、快照复制失败等
  ├── SchedulerError       # scheduler 通信失败（命令执行失败、输出解析失败）
  ├── JobNotFoundError     # get/cancel/watch 指定的 job_id 不存在
  ├── TimeoutError         # watch/wait 超时（继承内置 TimeoutError）
  └── StoreError           # 数据库操作失败（schema 不兼容、IO 错误）
  ```

  规则：
  - Scheduler 内部实现只抛 SchedulerError，不泄漏 subprocess 细节
  - SchedulerError.stderr 保留原始错误输出供调试
  - 所有异常携带结构化上下文（job_id、cluster_name 等），不依赖 message 字符串解析
  - 用户代码只需 catch MolqError 即可兜底

  ## 并发与线程模型

  ### 设计原则

  - **同步优先**：公开 API 全部为同步阻塞，不暴露 async
  - **线程安全**：单个 Submitor 实例可在多线程间共享
  - **无全局锁**：不同 Submitor 实例完全独立

  ### 具体约束

  - **JobStore (SQLite)**：
      - WAL 模式，支持并发读
      - 写操作通过单个 connection 串行化（sqlite3 模块的 check_same_thread=False + 应用层写锁）
      - 每个 Submitor 实例持有独立 connection
  - **Scheduler 调用**：
      - subprocess.run 调用本身线程安全
      - 批量查询（poll_many）在调用侧串行，不引入线程池
  - **JobMonitor / watch**：
      - watch() 在调用线程阻塞，通过 threading.Event 支持外部中断
      - daemon 模式在主线程运行，通过信号处理优雅退出
      - 不引入后台线程池；如果用户需要并发等待多个 job，使用 Submitor.watch(job_ids=[...]) 批量接口
  - **本地 Scheduler 子进程管理**：
      - 使用 subprocess.Popen 启动，立即返回
      - runner shim 负责记录退出状态到约定路径（不依赖 ps 轮询推断退出码）
      - 取消操作发送 SIGTERM，等待 grace period 后 SIGKILL

  ### 不支持（显式排除）

  - 不提供 async API（如需 async 包装，由用户自行 run_in_executor）
  - 不提供 callback-based 非阻塞接口（EventBus 仅用于 monitor 内部事件分发）
  - 不提供跨进程的 Submitor 共享

  ## 数据库迁移策略

  由于本次为破坏式重构，新旧 schema 不兼容：

  ### 新 Schema

  ```sql
  -- 元数据表
  CREATE TABLE molq_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
  );
  -- 初始记录：INSERT INTO molq_meta VALUES ('schema_version', '2');

  -- 主表
  CREATE TABLE jobs (
      job_id TEXT PRIMARY KEY,             -- molq 自生成 UUID
      cluster_name TEXT NOT NULL,
      scheduler TEXT NOT NULL,
      scheduler_job_id TEXT,               -- scheduler 返回的 id
      state TEXT NOT NULL DEFAULT 'created',
      command_type TEXT NOT NULL,           -- 'argv' | 'command' | 'script'
      command_display TEXT NOT NULL,        -- 用于展示的命令摘要
      cwd TEXT NOT NULL,
      submitted_at REAL,
      started_at REAL,
      finished_at REAL,
      last_polled REAL,
      exit_code INTEGER,
      failure_reason TEXT,
      metadata TEXT DEFAULT '{}',           -- JSON dict[str, str]
      UNIQUE(cluster_name, scheduler_job_id)
  );

  CREATE TABLE status_transitions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL REFERENCES jobs(job_id),
      old_state TEXT,
      new_state TEXT NOT NULL,
      timestamp REAL NOT NULL,
      reason TEXT
  );

  CREATE INDEX idx_jobs_cluster_state ON jobs(cluster_name, state);
  CREATE INDEX idx_transitions_job ON status_transitions(job_id);
  ```

  ### 迁移行为

  - JobStore 启动时检查 molq_meta 表是否存在
  - **无 molq_meta 表**（旧库）：
      - 将旧库重命名为 `jobs.db.v1.bak`
      - 创建新库，写入 schema_version=2
      - 向 stderr 输出一行警告：`molq: migrated database to v2, old data backed up to ~/.molq/jobs.db.v1.bak`
  - **schema_version=2**（当前库）：正常启动
  - **schema_version > 2**（未来版本）：抛出 StoreError，提示用户升级 molq
  - 不尝试数据迁移（旧 schema 中 job_id 为 scheduler 分配的 int，语义不同，迁移无意义）

  ## 内部架构

  - Submitor
      - 负责参数校验、defaults 合并、构造 JobSpec、调用 scheduler、调用 store、返回 JobHandle
  - Scheduler
      - 内部协议，不对用户暴露
      - 具体实现为 LocalScheduler、SlurmScheduler、PBSScheduler、LSFScheduler
      - 职责仅限 submit/poll_many/cancel/resolve_terminal
  - JobStore
      - 唯一可信状态源
      - 负责保存 JobRecord 和状态迁移历史
  - JobReconciler
      - 从 JobStore 读取活跃 job
      - 调用 scheduler 批量查询
      - 计算状态迁移并写回 JobStore
  - JobMonitor
      - 提供阻塞等待、轮询策略和事件派发
      - 不拥有业务状态
  - 删除 BaseSubmitor
  - 删除 submit.CLUSTERS
  - 删除 get_submitor
  - 删除基于 cluster 名称猜 scheduler 的逻辑
  - 删除 import-time 默认实例与数据库初始化

  ## 关键行为与安全规则

  - Molq 内部主键必须是自身生成的 job_id；scheduler 返回的 id 只能存为 scheduler_job_id
  - 本地 scheduler 不得以裸 PID 作为公开 job identity
  - 本地执行必须通过 runner shim 或等价机制记录真实退出状态、子进程信息和取消目标
  - 终态只能基于明确证据推进：
      - 明确 exit code
      - 明确 scheduler terminal status
      - 明确取消操作结果
  - scheduler 中消失但无法确定终态时，状态必须进入 lost
  - argv 永不自动降级为 shell
  - command 与 script 是显式 shell 路径，不隐藏风险
  - job 私有脚本目录权限固定为 0700
  - 环境变量值默认不入库，只记录 key；日志和状态展示必须脱敏
  - extra 黑洞配置彻底删除，未知字段直接报错
  - import molq 必须零副作用

  ## 用户侧使用实例

  ```python
  from molq import Submitor, JobResources, Memory

  local = Submitor("devbox", "local")

  job = local.submit(
      argv=["python", "train.py", "--epochs", "10"],
      resources=JobResources(cpu_count=8, memory=Memory.gb(32)),
  )
  print(job.job_id)
  print(job.wait().state)
  ```

  ```python
  from molq import Submitor

  slurm = Submitor("alpha", "slurm")

  job = slurm.submit(
      command="python preprocess.py && python train.py",
  )
  ```

  ```python
  from molq import Submitor, Script

  slurm = Submitor("alpha", "slurm")

  job = slurm.submit(
      script=Script.inline(
          """
          set -euo pipefail
          python preprocess.py
          python train.py
          """
      ),
  )
  ```

  ```python
  from molq import Submitor, Script

  slurm = Submitor("alpha", "slurm")

  job = slurm.submit(
      script=Script.path("./jobs/train.sh"),
  )
  ```

  ```python
  # 带 defaults 的使用
  from molq import Submitor, SubmitorDefaults, JobResources, JobScheduling, Memory

  slurm = Submitor("alpha", "slurm", defaults=SubmitorDefaults(
      resources=JobResources(cpu_count=4, memory=Memory.gb(8)),
      scheduling=JobScheduling(queue="normal", account="team-ml"),
  ))

  # 只需指定差异部分
  job = slurm.submit(
      argv=["python", "train.py"],
      resources=JobResources(memory=Memory.gb(64)),  # cpu_count=4 来自 defaults
  )
  ```

  ```python
  # 错误处理
  from molq import Submitor, MolqError, SubmitError, JobNotFoundError

  slurm = Submitor("alpha", "slurm")

  try:
      job = slurm.submit(argv=["python", "train.py"])
      record = job.wait(timeout=3600)
  except SubmitError as e:
      print(f"提交失败: {e}")
  except TimeoutError:
      job.cancel()
  except MolqError as e:
      print(f"意外错误: {e}")
  ```

  ## Roadmap

  - Phase 1: 核心模型重建
      - 定义 Submitor、Script、JobHandle、JobRecord
      - 定义内部 JobSpec、JobResources、JobScheduling、JobExecution
      - 定义 Memory、Duration 值类型
      - 定义新的状态枚举与内部 job_id 规则（UUID v4）
      - 定义 SubmitorDefaults 与合并函数
      - 定义异常层次（MolqError 及子类）
      - 定义 SchedulerOptions 类型族
  - Phase 2: 状态存储重建
      - 用 job_id (UUID) + cluster_name 为中心重建 SQLite schema
      - 增加 molq_meta 表与 schema 版本检测
      - 实现旧库备份逻辑
      - 增加状态迁移历史表
  - Phase 3: Scheduler 协议与实现
      - 抽出 Scheduler 协议（submit/poll_many/cancel/resolve_terminal）
      - 实现 LocalScheduler（含 runner shim）
      - 迁移 SlurmScheduler、PBSScheduler、LSFScheduler
      - 移除现有 adapter/script generator 对公共接口的侵入
  - Phase 4: 提交与执行路径重建
      - 实现 Submitor.submit(...) → defaults 合并 → JobSpec 构造
      - 实现 argv、command、script 三类命令分发
      - 为 Script.path 增加快照复制
      - 为本地执行增加 runner shim 集成
  - Phase 5: 监控与状态对账重建
      - 实现 JobReconciler
      - 实现 JobMonitor
      - 统一 watch/cancel/refresh/list 语义
      - 将无法判定终态的 job 标准化为 lost
  - Phase 6: CLI 重写
      - 用 Submitor 新接口重写 submit/status/watch/cancel/list
      - 删除旧命令格式和基于名字猜 scheduler 的逻辑
  - Phase 7: 文档与测试重写
      - 重写 README、API、教程、spec 引用
      - 删除旧测试，按新模型重写
  - Phase 8: 清理与删旧
      - 删除 submit decorator、cmdline、BaseSubmitor、get_submitor
      - 删除 import-time 默认实例
      - 删除 legacy 配置与所有兼容代码

  ## 测试计划

  ### 覆盖率目标

  - 整体行覆盖率 ≥ 85%
  - 核心模块（Submitor、JobStore、Scheduler 协议实现）≥ 90%
  - CLI 模块 ≥ 70%（依赖终端交互，部分场景难以自动化）

  ### Mock 策略

  - **Scheduler 层以下 mock**：测试 Submitor/JobStore/Reconciler 时 mock Scheduler 协议
  - **Subprocess 层 mock**：测试具体 Scheduler 实现时 mock subprocess.run/Popen
  - **不 mock SQLite**：JobStore 测试使用真实内存数据库 (`:memory:`) 或 tmp_path 下的文件数据库
  - **不 mock 文件系统**：Script.path 快照测试使用 tmp_path 下的真实文件

  ### 测试矩阵

  - Submitor.__init__ 测试：
      - 不同 cluster_name 实例互不污染
      - 不存在全局复用
      - scheduler_options 类型不匹配时抛 TypeError
      - defaults 类型校验
  - submit(...) 测试：
      - argv/command/script 三选一校验（零个、两个、三个均报 CommandError）
      - command 含换行时报 CommandError
      - Script.inline/path 正常物化和快照
      - Script.path 文件不存在时报 ScriptError
      - defaults 合并：per-submit 覆盖、None 字段回退、不深合并
  - 命令执行测试：
      - argv 保留参数边界（含空格、引号、特殊字符的参数）
      - command 允许 shell 语义（管道、重定向、&&）
      - script 允许多行逻辑
  - Memory / Duration 测试：
      - 工厂方法正确性（Memory.gb(8).bytes == 8 * 1024^3）
      - parse 方法（"8GB"、"512MB"、"2h30m"、"01:30:00"）
      - 比较运算符
      - 各 scheduler 格式化输出
      - 无效输入抛 ValueError
  - 本地 scheduler 测试：
      - 成功、失败、超时、取消
      - runner shim 退出码记录
      - 进程消失时进入 lost
  - 集群 scheduler 测试：
      - 提交、批量轮询、终态解析、lost 判定
      - scheduler 命令路径可配置（SchedulerOptions）
      - 命令执行失败时抛 SchedulerError
      - 输出解析失败时抛 SchedulerError
  - JobStore 测试：
      - 主键唯一（job_id 为 UUID）
      - 并发读写稳定（多线程写入同一 store）
      - 状态迁移完整记录
      - 旧库检测与备份
      - schema_version 校验
  - 异常测试：
      - 所有自定义异常均继承 MolqError
      - 异常携带结构化上下文（job_id、cluster_name）
      - SchedulerError.stderr 保留原始输出
  - 生命周期测试：
      - import molq 零副作用（不创建文件、不连接数据库）
  - 文档验收：
      - README、教程、API 仅使用新接口
      - 必须覆盖 argv、command、Script.inline、Script.path

  ## 假设与默认选择

  - 本次不保留任何旧接口桥接
  - Submitor 是统一类，不再让用户直接操作具体 scheduler 子类
  - scheduler 是内部实现术语，不再把 backend 暴露给用户
  - 默认存储仍使用 SQLite，但围绕新的 job_id (UUID) 和 cluster_name 重建
  - 公开 API 仅提供同步接口，不提供 async
  - job_id 使用 UUID v4，由 molq 在 submit 时生成
