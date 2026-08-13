# embedded2Mongo

redoma 的**核心服务容器**，一个镜像里同时跑捕获侧（Java）和应用侧（Python）两个进程。

## 内容

| 路径 | 说明 |
|---|---|
| `src/main/java/com/jackin/` | Java 源码：Spring Boot 调度器 + Debezium/LogMiner 捕获引擎（详见目录内 README） |
| `embedded2Mongo.jar` | 预编译好的 Java 捕获服务（Spring Boot fat jar，容器直接运行的就是它；仓库缺少构建脚本，源码不完整） |
| `rdm.py` | 应用侧脚本（Python）：消费 MongoDB 消息队列，按作业配置把关系变更映射/内嵌进目标集合 |
| `resources/application.yml` | Spring Boot 配置：MongoDB 连接、内嵌引擎存储路径、`instanceNo`、服务端口 `9999` |
| `resources/log4j2.properties` | 日志配置（注意：文件里混入了 log4j1 的残留配置，需清理） |
| `docker/Dockerfile` | **compose 实际使用的**构建文件：基于 `freejackin/jdk8-python`，同一容器内 `java -jar app.jar` 与 `python rdm.py` 一起启动 |
| `src/main/docker/Dockerfile` | 早期只跑 Java 的 Dockerfile（基于 alpine-oraclejdk8），已被上面的取代 |

## 容器如何启动

`docker/Dockerfile` 的 ENTRYPOINT 在同一容器里并行拉起两个进程：

```
java -jar /app.jar > extra.log   # 捕获侧：读源库 redo/binlog，写 MongoDB 队列
python -u rdm.py   > rdm.log      # 应用侧：读队列，写目标集合
```

两侧不直接通信，全部通过 MongoDB 里的 `redoma.jobs`（作业）和 `message_*`（队列）集合协调。
