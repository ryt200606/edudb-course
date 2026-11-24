# PostgreSQL 14.11 源码阅读实验

## 一条命令观察计划与回表

在 PostgreSQL 14.11 的练习数据库中，使用具有创建临时表权限的普通账号执行下列命令。脚本只创建本次会话的临时表与统计对象，会话结束后自动释放。请使用 psql 默认的逐条提交，使 VACUUM 可以执行。

```bash
psql -X -d your_practice_db -f mechanisms.sql
```

1. 两张表各有 1,000 行。两列各取十种等频值，第一张表的两列完全相关，第二张表的 100 种组合均匀出现。观察计划估算行数和实际行数。
2. 唯一索引保存 id，并将 label 放在 INCLUDE 中。查询只取这两列。修改未入索引的 payload 前后，比较目标 id=250 和未修改页上的 id=2250 的 Heap Fetches，再观察 VACUUM 后的结果。

## 完整复现四项实验

在 Linux 普通用户账户中准备 Python 3、PostgreSQL **14.11** 的服务端与客户端工具（包括 pg_config、initdb、pg_ctl、psql、pg_waldump），以及该安装的 libpq。若安装了标准 contrib 扩展 pg_visibility，脚本还会直接记录页的 all_visible 位；其余安装照常记录 Heap Fetches。无需 Python 第三方包或 GDB。

```bash
python3 run.py --pg-config /path/to/pg14/bin/pg_config --output ./observations
```

输出目录须为新目录。脚本在 /tmp 下建立只对当前用户开放的独立集簇，通过私有 UNIX socket 连接；它不接受已有数据库的数据目录或连接地址。正常结束及异常退出时都会关闭自己创建的服务器。只有第 4 项会对这个临时集簇执行两次 immediate 停止。单会话 SQL 的原始标准输出、完整执行计划、锁状态、WAL 记录与服务器日志写入输出目录。

| 问题 | 对照方法 | 本课程 PostgreSQL 14.11 实验观察 |
|---|---|---|
| 两个等值条件能否用独立概率相乘 | 完全相关表、均匀组合表；单列、联合 MCV、函数依赖三种统计分别 ANALYZE | 相关表 (2,2) 普通统计估 10 行、实际 100 行，MCV 与函数依赖均估 100。对矛盾组合 (2,3)，MCV 估 1，函数依赖估 100，实际 0。均匀表始终为 10 / 10。 |
| Index Only Scan 查齐列后会不会回表 | 固定索引和查询，依次测量初始 VACUUM、目标页修改、保留旧 RR 快照后 VACUUM、释放快照后 VACUUM | 目标页 Heap Fetches 依次为 0、1、1、0。对照页始终为 0。 |
| 为什么有的等待会升级为死锁 | RC 与 RR 两次读同一行。两事务分别按相反与相同顺序更新两行；用 pg_blocking_pids 确认阻塞边出现后才推进 | RC 两次读 10、11。RR 两次读 11、11，新连接读 12。反序形成两条相反边，一方报告 40P01，另一方提交后两行为 10。顺序对照只有一条等待边，最终均为 11。 |
| 未读提交应答后，事务是否已持久化 | 四笔事务各写两行，分别确认提交、确认回滚、已发送 COMMIT 但未读结果、保持打开。独立观察者确认第三笔已可见，随后即时停止、重启、核对并再次重启 | 已确认事务和已发未读事务各恢复两行；回滚和未提交事务恢复零行。此处观察者确定了第三笔的实际结果，写入客户端仍未取回应答。 |

执行器的可见性映射检查和取堆分支位于 PostgreSQL REL_14_11 的 src/backend/executor/nodeIndexonlyscan.c。联合估计见 src/backend/statistics/mcv.c 和 dependencies.c，等待环检测见 src/backend/storage/lmgr/deadlock.c；事务提交和 WAL 同步见 src/backend/access/transam/xact.c 与 xlog.c。课程调试报告记录了专门的用户关系断点。这里的公开脚本采集 SQL、锁和 WAL，不会附加调试器。

## 单独计时

```bash
python3 timing.py --pg-config /path/to/pg14/bin/pg_config --output ./timings
```

让其他基准测试先结束，再运行计时脚本。每种关系、统计模型和取值组合测试 30 次。覆盖查询重建 7 份独立初始数据，在每个修改阶段交替测目标与对照各 30 次。第一次预热不计入样本。输出包含 2,040 条保留执行计划指标的查询样本，以及 21 次独立轮次中的更新和 VACUUM 操作计时。服务器使用 EXPLAIN ANALYZE 测量含执行器计时开销的查询；另行记录 libpq 客户端经过的时间。P95 采用最近秩定义。

计划中的具体缓冲命中数、进程号、XID、页号和 LSN 随运行变化。比较行数、快照结果、阻塞方向及每笔事务的最终完整性。即时停机检验进程异常恢复，测试范围不包含主机断电。

2026 年 11 月 24 日另在 Ubuntu 24.04 / WSL2 的 ext4 文件系统完成公开入口的独立重跑。从 PostgreSQL 14.11 原始源码以 GCC 13.3.0 构建并安装 pg_visibility。临时表脚本执行完毕，四项实验全部通过，两次即时重启的四行指纹均以 b55a02799db5 开头。独立时段又执行 timing.py：12 个统计条件各 30 次、8 个覆盖查询场景各 210 次，以及 7 组事务外独立更新/清理，所有预期行数和回表次数均满足断言。
