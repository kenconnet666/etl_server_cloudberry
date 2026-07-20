# ADR 0002：控制面和元数据权威

- 状态：Accepted
- 日期：2026-07-20

## 背景

产品需要一个服务管理多个 PostgreSQL source、多个 Cloudberry target、命名 prefix、table mapping、DDL/rebuild 操作和可视化状态。连接配置可以存入 PostgreSQL，但“第一个 source”不是稳定控制面：source 可能被删除、暂停、failover 或本身就是 Citus 集群。

同时，热路径 checkpoint 必须和 Cloudberry 数据提交一致，不能由远端控制库提前确认。

## 决策

使用三个明确的 metadata ownership 域：

| 位置 | 权威内容 | 非权威内容 |
| --- | --- | --- |
| central control PG18 database | desired config revision、encrypted secret、operation、audit、lease/fencing token | 已应用数据 LSN |
| 每个 source database 的 `pg2cb_meta` | source identity、安装版本、DDL helper、schema notice identity | Cloudberry applied checkpoint |
| 每个 Cloudberry target database 的 `pg2cb_meta` | object ownership、table generation、schema fingerprint、逐节点 applied checkpoint、quarantine | 用户密码和完整 source DSN |

central control database 由 bootstrap `control_dsn` 明确指定。它可以物理位于一个 source PG18 实例，但必须是独立 database/schema，不得加入业务 publication，不得分布成 Citus table，也不能由系统隐式选择。

## Bootstrap 与 secret

本地只保留最小 bootstrap 配置：

```toml
[server]
listen = "127.0.0.1:8080"

[admin]
username = "admin"
password_hash = "<argon2id PHC>"

[control]
database_url = "<PG18 control database>"

[security]
master_key_env = "PG2CB_MASTER_KEY"
```

多源和多目标 secret 存在 control database，但必须使用 AEAD envelope encryption：ciphertext、nonce、key version 和非敏感 metadata 分列保存。master key 只来自环境变量或受保护文件，不写入数据库、日志、错误或前端响应。密码保存后不回显；更新 secret 产生新的 config revision。

单管理员登录使用 Argon2id hash、HttpOnly/Secure/SameSite session cookie、CSRF 防护和登录限流。首版不实现多用户、RBAC 或 API token。默认由私网反向代理终止 TLS。

## Desired state 与变更

配置采用不可变 revision：

```text
Save draft -> Validate -> Apply revision -> Reconcile runtime
```

- draft 不影响运行态。
- Validate 执行源、目标、类型、冲突和 topology 预检，不创建 slot 或目标表。
- Apply 写入 immutable desired revision 和 operation，supervisor 负责收敛。
- prefix、mapping、source/target identity、PK、distribution 或 topology 改变会创建 generation/rebuild，不能热改 active table。
- batch、并发、限速和 credential 可以在验证后热更新。

所有破坏性操作，包括 adopt、DROP quarantine 清理、slot 删除、pipeline 删除和 rebuild，都需要二次确认、operation id 和审计事件。

## Checkpoint 权威与事务

每个 target database 保存：

```text
pipeline_id
topology_generation
source_node_identity
table_generation / completion state
applied_lsn
schema_fingerprint
fencing_token
updated_at
```

apply 事务先锁 checkpoint/fence row，验证 token、topology generation 和 table generation，再写业务数据并按连续完成前缀推进 applied LSN。只有事务提交成功，reader 才发送 slot ACK。

控制库中的 received/applied/lag 是 UI 缓存，可以延迟或丢失。进程恢复时必须从 Cloudberry target metadata 读取 checkpoint，再与 source slot 状态比对。

## Lease 与 fencing

即使首版只部署一个 active 服务，也必须实现 active/standby 安全边界：

1. 实例在 control DB 获取有 TTL 的 pipeline lease 和单调递增 fencing token。
2. 激活前把更高 token 写入 target metadata。
3. 每个 apply 事务锁定对应 row 并验证 token。
4. 旧实例持有较低 token 时不能继续提交。
5. lease 无法续期时，实例在本地截止时间前停止读取和 apply。

控制库和 target 的更新不需要分布式事务。目标 row lock 决定某一时刻哪个 token 能提交；新 active 必须完成目标激活后才开始读取。旧事务若先获得锁，只能在新 token 激活前完成，之后所有旧 token 都被拒绝。

## 可用性策略

- control DB 不可用：拒绝登录后的配置写入和新操作；运行 pipeline 只继续到 lease 过期，然后暂停。
- source-local metadata 不可用或被修改：阻塞对应 pipeline，publication 业务行不能替代 DDL/schema 身份。
- target metadata 不可用：立即停止 apply 和 ACK。
- UI/API 不可用：已持有且可续租的 pipeline 可以继续。
- observed status 写失败：记录限速错误，不影响已经由 target checkpoint 证明的 apply。

## Migration

control/source/target metadata 各有独立、单调、可校验的 migration version。生产启动不自动执行 DDL migration，发布流程必须先运行显式 `migrate` 命令，再启动兼容 binary。

每次启动执行只读 compatibility check：

- binary 支持的最小/最大 metadata version。
- event trigger/function checksum。
- target checkpoint schema 和 ownership constraint。
- 未完成 migration 或 generation operation。

不允许应用在未知新版本 metadata 上“尽力运行”。migration 失败保留诊断和原对象，不执行无证据的自动回滚。

## 目标 ownership 与生命周期

目标对象带稳定 ownership identity：pipeline、source database identity、source relation identity、generation 和 schema fingerprint。已有同名对象但 ownership 不匹配时默认拒绝。

删除 pipeline 默认：

- 先停止 reader，提交最后可证明 checkpoint 并释放 lease。
- 保留 slot、source metadata、target table 和 target metadata，直到显式清理操作。
- DROP TABLE 或 pipeline 清理进入默认 30 天 quarantine。
- 删除 slot 和目标数据分别二次确认，不能绑定为一个不可拆分动作。

## 结果

优点：

- 多源配置有稳定、显式的管理位置。
- 数据恢复依据与目标数据同库，避免控制库 checkpoint 超前。
- 控制面短暂故障不会篡改数据语义。
- lease/fencing 为以后双实例接管保留生产安全边界。

代价：

- 三处 metadata 都需要版本管理和健康检查。
- 控制库不可用超过 lease TTL 时数据面会有意暂停。
- 服务部署必须额外管理 master key 和显式 migration。

## 未选择的方案

- 配置隐式存入第一个 source：多源生命周期和 HA 语义不稳定。
- 所有 checkpoint 只存 control DB：不能与目标数据原子提交。
- secret 明文存储或 UI 回显：不符合最小安全边界。
- 首版不做 fencing：后续双实例或网络分区会允许并发 stale writer。

