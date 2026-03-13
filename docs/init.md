
## RabbitMQ 初始化

当前仓库默认 RabbitMQ 配置和现网一致：

- `host=127.0.0.1`
- `port=5672`
- `vhost=/orderflow`
- `user=guest`
- `password=guest`

对应位置：

- `config/config.yaml -> mq`
- `/etc/default/orderflow-common -> ORDERFLOW_MQ_PASSWORD`

新服务器初始化 RabbitMQ：

```bash
sudo apt-get update
sudo apt-get install -y rabbitmq-server
sudo systemctl enable --now rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
```

创建当前默认配置需要的 vhost 和权限：

```bash
sudo rabbitmqctl add_vhost /orderflow
sudo rabbitmqctl set_permissions -p /orderflow guest ".*" ".*" ".*"
sudo rabbitmqctl list_vhosts
sudo rabbitmqctl list_permissions -p /orderflow
```

如果你的新服务器没有 `guest` 用户，补创建：

```bash
sudo rabbitmqctl add_user guest guest
sudo rabbitmqctl set_permissions -p /orderflow guest ".*" ".*" ".*"
```

如果你想改成专用账号，例如 `orderflow`：

```bash
sudo rabbitmqctl add_user orderflow your_password
sudo rabbitmqctl set_permissions -p /orderflow orderflow ".*" ".*" ".*"
```

然后同步修改：

- `config/config.yaml -> mq.user`
- `config/config.yaml -> mq.password_env`
- `/etc/default/orderflow-common`

RabbitMQ 交换机和队列不需要手工在 UI 里创建。服务启动时会自动幂等声明。当前会自动声明的核心拓扑如下：

- exchanges: `x.md.live`, `x.md.replay`, `x.ind`, `x.dlx`
- queues: `q.ingestor.selfcheck`
- queues: `q.indicator.grp.ob_heavy`, `q.indicator.grp.flow`, `q.indicator.grp.deriv`
- queues: `q.strategy.ind.minute`, `q.llm.ind.minute`, `q.monitor.ind.events`
- replay 队列会按实例名动态创建，属于临时队列，不需要手工预建

## 安装 systemd 服务

仓库根目录下的 `system_services/` 已保存当前机器正在使用的 4 个服务 unit 文件：

- `system_services/orderflow-market-data-ingestor.service`
- `system_services/orderflow-indicator-engine.service`
- `system_services/orderflow-llm.service`
- `system_services/orderflow-api.service`
- `system_services/orderflow-common.env.example`

这些 unit 文件保留的是当前机器上的真实路径和用户：

- `User=ubuntu`
- `WorkingDirectory=/data`
- `orderflow-api` 的代码目录是 `/home/tools/codex-proxy`
- `market-data-ingestor` 的启动命令是 `/home/ubuntu/.cargo/bin/cargo run -p market_data_ingestor`

如果你的新服务器用户名或部署路径不同，请先修改 `system_services/` 里的 unit 文件，再复制到 `/etc/systemd/system/`。

新服务器安装方式：

```bash
cd /data
sudo cp system_services/orderflow-market-data-ingestor.service /etc/systemd/system/
sudo cp system_services/orderflow-indicator-engine.service /etc/systemd/system/
sudo cp system_services/orderflow-llm.service /etc/systemd/system/
sudo cp system_services/orderflow-api.service /etc/systemd/system/
sudo cp system_services/orderflow-common.env.example /etc/default/orderflow-common
sudo systemctl daemon-reload
sudo systemctl enable orderflow-market-data-ingestor
sudo systemctl enable orderflow-indicator-engine
sudo systemctl enable orderflow-llm
sudo systemctl enable orderflow-api
```

## 构建与数据库初始化

数据库重建脚本见：

- `sql/rebuild_orderflow.sql`
- `sql/rebuild_orderflow_ops.sql`
- `sql/rebuild_all_databases.sh`
- `sql/README_rebuild.md`

新服务器重建数据库：

```bash
cd /data
PGPASSWORD=your_password bash sql/rebuild_all_databases.sh
```