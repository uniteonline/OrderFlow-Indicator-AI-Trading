通过sudo journalctl -u orderflow-llm -f查看日志，从上次重启到现在，帮我结合源数据 帮我分析一下，目前存在哪些问题，导致最后亏损的



请根据最新重启后stage2 core entry产出物: 20260316T121800Z_ETHUSDT_entry_core_20260316T122015301Z.json再次确认本次针对entry filter的优化实现了：llm阅读数据后清晰的准确的给出交易的方向，tp,sl,entry



是否可以让现在的stage2 的pending管理可以让模型准确的定义近15分钟的风险以及给出最优的挂单的方向，tp,sl,entry数据？

是否可以让现在的stage2 的management管理可以让模型准确的识别4h,1d的原有趋势是否延续，并关注15m的风险，能够对持仓的机会和风险做出很好的管理

我的目的是scan filter -> 候选证据，让模型根据数据给出15m,4h,1d方向和区间



