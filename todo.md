我把Indicator Engine 子系统的产出结果存到了/data/systems/llm/temp_indicator/20260307T014100Z_ETHUSDT.json，请连接数据库查询原始数据，以及结合指标.md里的#1的指标部分，帮我确认一下当前执行结果是否正确，是否遗漏？是否有4h,1d相关指标的数据被遗漏？

通过sudo journalctl -u orderflow-llm -f查看日志，从上次重启到现在，帮我结合源数据 帮我分析一下，目前存在哪些问题，导致最后亏损的


20260316T113100Z_ETHUSDT_entry_core_20260316T113319393Z.json  这是我的llm层stage2 开仓需要做tp，sl,entry的计算和交易决策的数据源，优化前文件是/data/systems/llm/temp_indicator/20260316T113100Z_ETHUSDT.json，我的需求是LLM模型可以通过优化后文件清晰的准确的给出交易的方向，tp,sl,entry。所以我写了一个/data/docs/core数据源过滤规则v4.md，请评估我的优化方案，是否可行？是否可以实现我的目的：stage2 开仓需要做tp，sl,entry的准确计算和清晰的交易决策的数据源。





请根据最新重启后stage2 core entry产出物: 20260316T121800Z_ETHUSDT_entry_core_20260316T122015301Z.json再次确认本次针对entry filter的优化实现了：llm阅读数据后清晰的准确的给出交易的方向，tp,sl,entry



是否可以让现在的stage2 的pending管理可以让模型准确的定义近15分钟的风险以及给出最优的挂单的方向，tp,sl,entry数据？

是否可以让现在的stage2 的management管理可以让模型准确的识别4h,1d的原有趋势是否延续，并关注15m的风险，能够对持仓的机会和风险做出很好的管理


请帮我按/data/docs/core_management数据源过滤规则v1.md 落地到代码