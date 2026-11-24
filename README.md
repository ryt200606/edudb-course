# EduDB 4.1.0 与 PostgreSQL 14.11 源码实验

EduDB 是用 C++17 编写的教学数据库。命令行与网页把 SQL 发给同一个服务进程。解析、计划选择、执行、页存储、索引和事务恢复的源码位于 [apps/edu-dbms](apps/edu-dbms)。另一项课设从 PostgreSQL 的真实断点、SQL 输出和崩溃重放记录解释相同的数据库问题。实验入口见 [PostgreSQL 源码阅读](postgresql-reading/README.md)。

## 启动与选课演示

准备 CMake、支持 C++17 的编译器和 Ninja。HTTP 与 JSON 库已随源码提供，版本和许可证见 [third_party](apps/edu-dbms/third_party)。在 Windows 的 PowerShell 中执行：

```powershell
cd apps/edu-dbms
.\build.ps1 -Test
.\start.ps1 -Port 8765 -Data .\demo-data
```

从浏览器打开 <http://127.0.0.1:8765>。在另一个终端进入同一应用目录，调用已编译的命令行程序：

```powershell
.\.build\windows\edudb.exe run --url http://127.0.0.1:8765 --file demo/course-integrity.sql
.\.build\windows\edudb.exe shell --url http://127.0.0.1:8765
```

选课脚本在新库中建立学生、课程和选课表。执行 `DESCRIBE enrolment` 可核对成绩检查和复合外键。可在事务内删父行、查看级联结果，再执行 `ROLLBACK`。在 Linux 文件系统中的源码目录执行下列命令可构建相同内核并运行原生测试：

```sh
cd apps/edu-dbms
bash build.sh
.build/linux/edudb serve --data ./demo-data --web ./web --port 8765
```

本版控制格式为 4、物理页格式为 3。4.1 新库的最低读取器为 5。升级已有 4.0 库时，先关停旧库，再用 `edudb migrate-v4 --source 旧目录 --data 新目录` 生成独立副本。

## 从 SQL 追到内核

| 课堂问题 | 可复核的入口 | 读源码时观察什么 |
|---|---|---|
| 两个会话同时删父行、插子行会怎样？ | [选课 SQL](apps/edu-dbms/demo/course-integrity.sql) · [行为与并发测试](apps/edu-dbms/tests/integrity_sql_test.cpp) | [约束执行](apps/edu-dbms/src/sql_integrity.inc)中的锁定范围、立即校验和级联删除。检查失败与显式回滚后的行集。 |
| 两列各有十个值，为什么联合等值谓词可以命中 100 行？ | [联合统计演示](apps/edu-dbms/demo/joint-statistics.sql) · [千行对照](apps/edu-dbms/tests/joint_statistics_sql_test.cpp) | [双列统计](apps/edu-dbms/src/sql_joint_statistics.inc)如何使用同一次采集的频数。比较预计输出行数、实际行数和单列索引必须访问的条目数。 |
| 四个写事务能否共享一次日志同步？ | [组提交测试](apps/edu-dbms/tests/group_commit_test.cpp) · [六个故障窗口](apps/edu-dbms/tests/commit_history.py) | [组提交](apps/edu-dbms/src/storage_group_commit.inc)保留各事务的提交记号，随后同步该批 WAL。启动参数 `--wal-group-size 1` 逐笔同步，`--wal-group-size 8 --wal-group-wait-us 500` 用于同版本对照。 |
| 主库失联时备用库凭什么接管？ | [Windows 与 WSL2 的交接测试](apps/edu-dbms/tests/cross_system_handoff.py) · [交接协议](apps/edu-dbms/src/fencing_handoff.inc) | 对照日志位置、移交证明和任期。旧主持久放弃写权后才签发移交证明，接管端校验目标和日志位置。这是一台宿主上的受控切换实验。 |
| 索引、外排序和覆盖访问怎样核验？ | [索引与散列回归](apps/edu-dbms/tests/sql_ch11_test.cpp) · [外排执行](apps/edu-dbms/src/storage_sort.inc) · [查询测试](apps/edu-dbms/tests) | 核对实际结果、用户堆页访问、临时段和资源上限。 |

## 独立复现

章节矩阵包含 198 项教材条目。178 项有原生实现，15 项采用有明确范围的替代方案，2 项是软件模型，3 项给出拒绝边界。每项都标出具体代码或测试。Windows 和 WSL2 的同版发行程序各通过 69 项原生测试；Windows 的网页观察和外部故障试验另行计数。[章节矩阵](apps/edu-dbms/docs/ch10-16/coverage-4.1.json) · [分项核验](apps/edu-dbms/docs/ch10-16/release-4.1-validation.json)

接管后还可验证旧主回归。Windows 原数据目录保留退役状态，在同一台 Windows 主机的新目录恢复 Linux 新主的快照，再追赶其日志。测试核对备库只读、拒绝旧令牌、两端行集指纹，以及反向交接后第 3 任期的写入。反向归档经 WSL 共享挂载转送到 Windows 本地接收器。[双向交接脚本](apps/edu-dbms/tests/cross_system_handoff.py)

从应用目录运行以下命令。故障脚本只启动自己的临时服务，输出目录须为新目录。26 步接口测试还使用 Node、Playwright 与 Chromium。公开版保留课内共同 SQL 的输入和预期结果。[业务用例](apps/edu-dbms/tests/course_integrity_workload.json)

```sh
python3 tests/commit_history.py --exe .build/linux/edudb --output ./history-run
python3 tests/optimizer_lifecycle.py --exe .build/linux/edudb --web ./web --output ./mcv-lifecycle.json
python3 tests/kernel_benchmark.py --exe .build/linux/edudb --label candidate --profile smoke --output ./bench-smoke
```

正式计时使用 [矩阵驱动](apps/edu-dbms/tests/kernel_benchmark_matrix.py)。在同一平台依次运行对照程序、新版组提交和新版逐笔同步；保留每次样本、配置、程序摘要与行集指纹。重开数据库进程会清空引擎缓冲，宿主文件缓存仍可能命中。计时结果需要连同负载和缓存条件解读。

每个规模从空库装载一次以准备封存夹具。正式批量写入在已有 1 千、1 万或 10 万行的库上再导入 1,000 行，每次恢复到同一关闭快照，独立重复 7 次。每种读查询分别收集 30 次重开后和 30 次立即重读的样本。开发者可换用自己的随机种子检查约束，`tests/course_integrity_model.py --help` 给出参数；每个事务结束后分别核对三张表的有序内容。

### 分开观察查询规划与执行的页访问

一条主键点查可以只访问四个 B+ 树索引页，同时在生成计划时产生更多读操作。`read_table_for_plan` 从普通堆表的事务内描述页读取精确堆页数，再从索引根取得规划所需信息。完整结构诊断会另外遍历索引页。[表描述页与规划读取](apps/edu-dbms/src/storage.cpp) · [回归断言](apps/edu-dbms/tests/sql_test.cpp)

以下命令从 `apps/edu-dbms` 运行。第一步准备关闭的十万行数据库，第二步在它的副本上执行点查和连续二十个主键的范围查询。两条 SQL 都在自动提交、新显式事务、同一事务内重读三个条件下执行。[可重复的页访问探针](apps/edu-dbms/tests/planner_io_probe.py)

```sh
python3 tests/kernel_benchmark.py --exe .build/linux/edudb --label planner-probe --sizes 100000 --joint --prepare-only --output ./probe-fixture
python3 tests/planner_io_probe.py --exe .build/linux/edudb --web ./web --fixture ./probe-fixture/fixtures/100000 --output ./probe-result.json
```

在同一份十万行快照上，两条查询的规划索引页访问均由 5,109 次降至 0 次，执行阶段均为 4 次。点查命中 1 条覆盖记录，范围扫描命中 20 条，两者回表均为 0 次。新事务的数据文件读取从 5,119 次降至 10 次，这里计量的是操作系统文件调用。范围条件 `id>=50000 AND id<50020` 的预计结果仍约为 25,009 行，实际为 20 行，反映了同一列上下界选择率近似的误差。

也可以在同一套本机夹具上观察内存取舍。运行下列探针，将算子内存预算从 256 KiB 改到 8 MiB。十万行表与 512 行标签表的宽字段哈希连接在两次执行中产生相同聚合结果。小预算触发一次分区，临时文件写入 19,361,868 字节；大预算在内存中完成。输出还分别记录算子估计占用峰值与操作系统报告的整个服务进程驻留峰值。[内存与临时 I/O 探针](apps/edu-dbms/tests/join_memory_probe.py)

```sh
python3 tests/join_memory_probe.py --exe .build/linux/edudb --web ./web --fixture ./probe-fixture/fixtures/100000 --output ./join-memory.json
```

### 在 Linux 重建对照版本

4.0.0 原源码依赖 Windows 系统调用。Linux 对照采用同一版本的 SQL 和 WAL 代码，只移植文件操作、文件锁、摘要及本地账户目录访问涉及的五个文件。以下命令从本仓库根目录运行，需要完整 Git 历史、Python 3、GNU patch、CMake、Ninja 和 C++17 编译器。准备器会对照实际参与实验的源树核验 116 个输入，忽略文本换行差异。[可审查的移植补丁](benchmark-control/linux-4.0.patch) · [校验清单](benchmark-control/control.json)

```sh
python3 benchmark-control/prepare.py --output /tmp/edudb-400-posix
cmake -S /tmp/edudb-400-posix -B /tmp/edudb-400-build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release '-DCMAKE_CXX_FLAGS_RELEASE=-O1 -DNDEBUG'
cmake --build /tmp/edudb-400-build --target edudb --parallel 2
```

用相同编译选项构建 4.1 后，可将两个程序传给矩阵驱动。Windows 对照直接使用本仓库历史提交 `66449516546f9e5e5ddcf551832f32c112e81772` 的原始程序源码。
