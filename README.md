# Orderflow 运行

## PostgreSQL

sudo service postgresql start


## 服务运行

运行 Market Data Ingestor：

```bash
cd /data
cargo build -p market_data_ingestor --release
sudo service orderflow-market-data-ingestor start
sudo service orderflow-market-data-ingestor stop
sudo service orderflow-market-data-ingestor restart
sudo journalctl -u orderflow-market-data-ingestor -f
```

注意：当前 `orderflow-market-data-ingestor.service` 的 `ExecStart` 仍是 `cargo run -p market_data_ingestor`，上面的 `release` 编译不会被该 unit 直接使用；如果后续要切到 `release` 二进制，需要同步修改 service 文件。

运行 Indicator Engine：

```bash
cd /data
cargo build -p indicator_engine --release
sudo service orderflow-indicator-engine start
sudo service orderflow-indicator-engine stop
sudo service orderflow-indicator-engine restart
sudo journalctl -u orderflow-indicator-engine -f
```

`orderflow-indicator-engine` 不要强制杀进程。当前实现会在退出阶段把内存状态回写数据库，以减少下次启动 backfill。停服务后请等待它自然退出，再重新启动。

运行 LLM 子系统：

```bash
cd /data
cargo build -p llm --release
sudo service orderflow-llm start
sudo service orderflow-llm stop
sudo service orderflow-llm restart
sudo journalctl -u orderflow-llm -f
```

运行 API：

```bash
cd /home/tools/codex-proxy
npm run build
sudo service orderflow-api start
sudo service orderflow-api stop
sudo service orderflow-api restart
sudo journalctl -u orderflow-api -f
```
