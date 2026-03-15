我把Indicator Engine 子系统的产出结果存到了/data/systems/llm/temp_indicator/20260307T014100Z_ETHUSDT.json，请连接数据库查询原始数据，以及结合指标.md里的#1的指标部分，帮我确认一下当前执行结果是否正确，是否遗漏？是否有4h,1d相关指标的数据被遗漏？

通过sudo journalctl -u orderflow-llm -f查看日志，从上次重启到现在，帮我结合源数据 帮我分析一下，目前存在哪些问题，导致最后亏损的











模型返回20260314T111500Z_ETHUSDT_entry_custom_llm_custom_llm_scan_prompt_input_20260314T111712665Z.json单独存在llm下创建一个文件夹temp_model_output里，不混放到temp_model_input里