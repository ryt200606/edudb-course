# PostgreSQL 14.11 专题实验

在已安装 14.11 的练习库运行 `psql -X -d 库名 -f experiments/mechanisms.sql`，即可比较两种联合分布下的估计，并观察一次 UPDATE 怎样改变覆盖索引访问的 Heap Fetches。要重现保留旧快照的 VACUUM、真实等待环以及客户端未取回应答的提交，使用 [四项实验的独立集簇入口](experiments/README.md)。其中的 Python 脚本会新建集簇、核对行集并导出原始观察值。

阅读基于上游 [PostgreSQL REL_14_11](https://github.com/postgres/postgres/tree/d1a2a93766b0a27eb4ffecdb63ed3879088dbfbe)。源代码属于 PostgreSQL 项目。课程分析脚本、SQL 和 GDB 文件位于本目录。[版本清单](source-lock.json) 记录使用的源码包和驱动版本。

### 索引计划

在专用空数据库中运行 `psql -X -v ON_ERROR_STOP=1 -f index-plans.sql`。12 万行数据按主键和重复键分布。三条自然计划分别返回 21、114000、120 行；原集簇的访问节点依次为 Index Scan、Seq Scan、Bitmap Heap Scan。成本设置、可用缓存、ANALYZE 采样等条件会影响其他集簇的选择。调试时实际执行的查询带 `ORDER BY id`，使用脚本末尾得到的索引 OID 替换 [GDB 条件](debug/index-user.gdb) 中的历史值 16389。该文件中的源码目录也需指向本机的 14.11 源树。

### 事务快照与等待

[现场 SQL](recorded-session-order.sql) 保留 2026-10-31 的会话标签、PID 和语句；并行请求按返回时刻登记，不能把整个文件作为单连接脚本。建立 `mv_probe(id integer PRIMARY KEY,v integer NOT NULL)`，插入 (1,10)、(2,20)。由 A 开启 READ COMMITTED 并读取第 1 行，B 将该行改为 11 并提交，A 再读得到 11。重新由 A 开启 REPEATABLE READ 并先读取 11，B 改为 12 并提交，A 再读仍为 11，独立观察会话的新查询可读到 12。死锁专题由两个会话以相反顺序更新两行，第三个会话通过 `pg_blocking_pids` 检查等待环。原实验的探针分别见 [MVCC](debug/mvcc-user.gdb) 和 [锁等待](debug/lock-user-wait.gdb)。

### 提交与恢复

在可独立停止的专用集簇中建立 `wal_probe(id integer PRIMARY KEY,value text NOT NULL)`，写入 (0,'baseline') 并执行 CHECKPOINT。会话 A 插入 (1001,'committed-a') 后提交。会话 B 插入 (1002,'uncommitted-b') 并保持事务开启。观察会话记录 WAL 位置，原实验执行 `pg_switch_wal()` 后以 immediate 模式停止该集簇，再启动并按 id 查询。原记录最终只保留 0 和 1001 两行。重放过程可以处理未提交事务的数据页变化，版本最终可见性仍取决于事务提交状态。用户 COMMIT 采集的 [GDB 命令](debug/wal-user-commit.gdb) 需要已启用调试符号的 PostgreSQL。

### 调用链定位

- 普通 SELECT 的启动和执行位于 [pquery.c](https://github.com/postgres/postgres/blob/d1a2a93766b0a27eb4ffecdb63ed3879088dbfbe/src/backend/tcop/pquery.c#L514)，Portal 清理位于 [portalcmds.c](https://github.com/postgres/postgres/blob/d1a2a93766b0a27eb4ffecdb63ed3879088dbfbe/src/backend/commands/portalcmds.c#L298-L299)。
- 可见性判定见 [heapam_visibility.c](https://github.com/postgres/postgres/blob/d1a2a93766b0a27eb4ffecdb63ed3879088dbfbe/src/backend/access/heap/heapam_visibility.c#L1053-L1063)。
- 提交时同步日志并更新状态的条件见 [xact.c](https://github.com/postgres/postgres/blob/d1a2a93766b0a27eb4ffecdb63ed3879088dbfbe/src/backend/access/transam/xact.c#L1399-L1409)。
