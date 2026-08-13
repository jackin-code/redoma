# mongo — 早期遗留的 MongoDB 数据卷（已停用）

⚠️ **这是历史遗留目录，当前不再使用。**

`docker-compose.yml` 挂载的是 [`docker/mongo/data`](../docker/mongo/)，并非本目录。这里的 `data/`（2017 年的 WiredTiger 数据文件）是早期版本留下的，与当前 compose 无关。

建议：连同 `docker/mongo/data` 一起从版本管理中移除（加 `.gitignore`），避免运行期数据污染仓库、撑大体积。保留此说明仅为解释目录来历。
