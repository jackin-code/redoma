# redoma

**RE**lational-to-**DO**cument **MA**pping — 一个基于 CDC（变更数据捕获）的工具，把关系型数据库（MySQL / Oracle）的实时变更流式同步到 MongoDB，并按配置把关系表**拍平或内嵌**成文档结构。

> A CDC-based tool that streams changes from relational databases (MySQL / Oracle) into MongoDB, reshaping relational rows into flat or embedded document structures.

---

## 它解决什么问题

关系库里 `customers` / `orders` / `products` 是分开的多张表，靠外键关联。搬到 MongoDB 时，往往希望按访问模式重新组织，例如把一个客户的所有订单**内嵌**进这个客户的文档里。redoma 在做实时同步的同时完成这种「关系 → 文档」的形变：

- **insert / upsert / merge** —— 表到集合的直连映射（可带 upsert 条件）
- **embedOne** —— 把一行内嵌成目标文档里的**单个子对象**（如把商品详情内嵌进订单）
- **embedMany** —— 把多行**追加进目标文档的数组**（如把订单数组内嵌进客户）

## 架构

redoma 分为**捕获**和**应用**两侧，中间用 MongoDB 里的集合当消息队列。MongoDB 同时充当**元数据库、消息队列、目标库**三种角色。

```mermaid
flowchart LR
    subgraph src[源库]
        MySQL[(MySQL)]
        Oracle[(Oracle)]
    end

    subgraph app[embedded2Mongo · Java/Spring Boot]
        CAP[捕获引擎<br/>MySQL: Debezium EmbeddedEngine<br/>Oracle: LogMiner]
    end

    subgraph mongo[(MongoDB)]
        JOBS[redoma.jobs<br/>作业定义/状态]
        QUEUE[message_* 集合<br/>CDC 消息队列]
        CDCLOG[cdc_change_log<br/>原始变更留档]
        TARGET[目标业务集合<br/>customers / orders ...]
    end

    APPLY[rdm.py · Python<br/>消费队列 + 关系→文档形变]

    MySQL & Oracle --> CAP
    CAP -->|写入 CDC 事件| QUEUE
    JOBS -.读取作业.-> CAP
    JOBS -.读取作业.-> APPLY
    QUEUE -->|消费 untreated| APPLY
    APPLY --> CDCLOG
    APPLY -->|insert/upsert/embedOne/embedMany| TARGET
```

**捕获侧 `embedded2Mongo`（Java）**：每秒扫描 `redoma.jobs`，把 `start-requested` 的作业拉起、`interrupted` 的作业停掉；每个作业跑在线程池里，MySQL 用 Debezium 内嵌引擎、Oracle 用 LogMiner 读 redo log，把变更事件写进 MongoDB 消息队列。

**应用侧 `rdm.py`（Python）**：从 `redoma.jobs` 读作业配置，消费队列里 `untreated` 的消息，按 `tables` 里的映射规则写入目标集合，同时把原始事件留档到 `cdc_change_log`。

> 早期版本用 Kafka + Debezium Connect 做消息总线（`rdm.py` 里仍保留了 `configureTopics` / `KafkaConsumer` 的注释代码），后来改为纯 MongoDB 队列，无需额外中间件。

## 快速开始

```bash
docker-compose up -d
```

`docker-compose.yml` 会拉起两个服务：
- `mongodb`（MongoDB 3.2，元数据 + 队列 + 目标库，暴露 `27017`）
- `embedded2Mongo`（捕获 + 应用容器，暴露 `9999`）

然后往 `redoma.jobs` 集合里插入一条作业定义即可开始同步。

## 作业配置

一条作业是 `redoma.jobs` 里的一个文档：

```json
{
  "_id": "demo-job",
  "state": "start-requested",
  "config": {
    "database.type": "oracle",
    "database.host": "192.168.71.43",
    "database.port": 1521,
    "database.name": "XE",
    "database.username": "LOGMINER",
    "database.password": "******",
    "mongo.uri": "mongodb://mongodb:27017",
    "mongo.database": "inventory"
  },
  "tables": [
    {
      "table": "customers",
      "collection": "customers",
      "insertionType": "insert",
      "fields": { "id": "id", "first_name": "first_name", "email": "email" }
    },
    {
      "table": "orders",
      "collection": "customers",
      "insertionType": "embedMany",
      "condition": { "id": "$purchaser" },
      "embedPath": "orders"
    },
    {
      "table": "products",
      "collection": "orders",
      "insertionType": "embedOne",
      "condition": { "product_id": "$id" },
      "embedPath": "product_detail"
    }
  ]
}
```

字段说明：

| 字段 | 说明 |
|---|---|
| `config.database.type` | `mysql` 或 `oracle` |
| `config.database.*` | 源库连接信息（host / port / name / username / password） |
| `config.mongo.uri` / `mongo.database` | 目标 MongoDB 地址与库名 |
| `tables[].table` | 源表名 |
| `tables[].collection` | 目标集合名 |
| `tables[].insertionType` | `insert` / `upsert` / `merge` / `embedOne` / `embedMany` |
| `tables[].condition` | 定位目标文档的条件，`$字段名` 表示取源行对应列的值 |
| `tables[].embedPath` | 内嵌到目标文档的哪个路径（不填默认用表名） |

作业状态（`state`）流转：`start-requested → running → stopped / interrupted`。

## 目录结构

| 目录 | 说明 |
|---|---|
| [`docker/embedded2Mongo/`](docker/embedded2Mongo/) | 核心服务：Java 捕获引擎 + Python 应用脚本 + 容器构建 |
| [`docker/embedded2Mongo/src/main/java/com/jackin/`](docker/embedded2Mongo/src/main/java/com/jackin/) | Java 源码（Spring Boot 调度 + Debezium/LogMiner 捕获） |
| [`docker/mongo/`](docker/mongo/) | docker-compose 用的 MongoDB 数据卷与配置 |
| [`mongo/`](mongo/) | ⚠️ 早期遗留的 MongoDB 数据卷（当前 compose 未挂载，见目录内说明） |
| `.github/ISSUE_TEMPLATE/` | GitHub Issue 模板（连接器插件 / 空白模板） |

每个代码目录内都有独立的 `README.md` 说明其职责。

## 现状与已知局限

这是一个**原型/实验性质**的项目，若要投产需注意：

- **Python 侧仅支持 Python 2**：`rdm.py` 用了 `print` 语句、`.iteritems()`、`unicode` 等，无法直接在 Python 3 运行。
- **Oracle DML 解析未完成**：`EmbeddedUtil.parseOracleStatement()` 目前只解析 SQL 但未落地 insert/update/delete 处理；Oracle 捕获循环也缺少退避 sleep（异常时会忙轮询）。
- **仓库内提交了运行期数据与二进制**：`mongo/data`、`docker/mongo/data`（2017 年的 WiredTiger 数据文件）和 35MB 的 `embedded2Mongo.jar` 被纳入了版本管理，导致仓库偏大；建议加 `.gitignore` 并从历史中清理。
- **Java 源码不完整**：仓库只包含部分类，构建依赖预编译好的 `embedded2Mongo.jar`，缺少 `pom.xml` / `build.gradle`，无法从源码直接构建。
- **中间件版本偏旧**：MongoDB 3.2、`docker-compose` v2 语法均已 EOL。

## 许可

未声明（No license specified）。
