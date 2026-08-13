# docker/mongo — MongoDB 数据卷与配置

`docker-compose.yml` 里 `mongodb` 服务挂载的宿主机目录。MongoDB 在 redoma 中同时充当**元数据库**（`redoma.jobs`）、**消息队列**（`message_*`）和**目标库**。

| 路径 | 说明 |
|---|---|
| `data/` | MongoDB 数据目录，挂载到容器 `/data/db`。 |
| `mongod.conf.orig` | 参考用的 mongod 配置样例，挂载到容器 `/etc/mongod.conf.orig`。 |
| `env` | 环境变量文件（当前为空占位）。 |
| `logs/` | 日志目录，挂载到容器 `/var/log/mongodb/`（运行时生成）。 |

> ⚠️ `data/` 里当前提交了 2017 年的 WiredTiger 运行期数据文件（约 13MB）。运行期数据不应纳入版本管理，建议加入 `.gitignore` 并从仓库中移除，仅保留空目录占位。
