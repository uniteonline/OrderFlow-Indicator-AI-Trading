
重启postgresql
Restart-Service postgresql-x64-18

配置rabbitmq:
C:\Program Files\RabbitMQ Server\rabbitmq_server-4.2.4\sbin

运行Market Data Ingestor
sudo service orderflow-market-data-ingestor start
sudo service orderflow-market-data-ingestor stop
sudo journalctl -u orderflow-market-data-ingestor -f

运行 Indicator Engine
sudo service orderflow-indicator-engine start
sudo service orderflow-indicator-engine stop
sudo journalctl -u orderflow-indicator-engine -f
#该服务不可强制重启，我对服务做了内存数据缓存到数据库的操作，来提升下次重启服务后，减少backfill的事件的设计。所以不可强制重启
请耐心等待服务关闭，数据写入数据库后，再启动服务

运行 LLM 子系统
sudo service orderflow-llm start
sudo service orderflow-llm stop
sudo journalctl -u orderflow-llm -f

运行 api
sudo service orderflow-api start
sudo service orderflow-api stop
sudo journalctl -u orderflow-api -f

