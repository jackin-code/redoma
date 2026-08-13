# com.jackin — Java 捕获侧源码

捕获侧的 Java 源码（Spring Boot）。负责从关系库读取变更并写入 MongoDB 消息队列。

> ⚠️ 仓库只包含下面列出的部分类，`entity` / `repositories` / `config` / `MySqlUtil` / `EmbeddedConstant` 等被引用的类未提交，实际运行以同级目录的预编译 `embedded2Mongo.jar` 为准。

## 包结构

| 包 / 类 | 职责 |
|---|---|
| `Schedule/JobSchedule.java` | 作业调度器。`@Scheduled` 每秒扫描 `redoma.jobs`：把 `start-requested` 的作业拉起并提交到线程池，把 `interrupted` 的作业取消并标记 `stopped`；启动时把已是 `running` 的作业恢复运行。 |
| `common/EmbeddedUtil.java` | 捕获引擎工厂。按 `database.type` 分派：MySQL 走 Debezium `EmbeddedEngine`，Oracle 走 LogMiner（加 redo log → 启动分析 → 轮询 `V$LOGMNR_CONTENTS` → 解析 SQL）。 |

## 捕获引擎两条路径

- **MySQL** —— `EmbeddedEngine` 内嵌 Debezium，binlog 事件通过 `JobEntry::insertIntoMongo` 回调写入 MongoDB。
- **Oracle** —— 手写 LogMiner 流程：`DBMS_LOGMNR.ADD_LOGFILE` → `START_LOGMNR` → 查询归档日志内容 → 用 JSqlParser 解析出的 SQL 语句。
  > 已知局限：`parseOracleStatement()` 目前是空实现（识别出 Insert/Update/Delete/Truncate 但未落地处理），且轮询循环缺少退避 sleep。

## 命名约定

作业状态常量定义在未提交的 `EmbeddedConstant` 里：`RUNNING` / `START_REQUESTED` / `STOPPED` / `INTERRUPTED`，与 `rdm.py` 及 `redoma.jobs` 文档中的 `state` 字段一一对应。
