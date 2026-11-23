# EduDB 4.0.0 与 PostgreSQL 14.11 源码实验

本仓库配套《实验报告2 数据库系统实现》。第一部分是小组的教学数据库实现，第二部分是 PostgreSQL 14.11 源码阅读所用的 SQL 和调试命令。EduDB 的 SQL、索引、事务与恢复均由 C++17 内核实现。控制格式为 4，物理页格式为 3。两项第三方库的原始许可证和版本校验值在 [third_party](apps/edu-dbms/third_party) 中。

## Windows 构建和演示

需要已安装 Git（或解压本源码包）、CMake 3.16 及以上、Ninja 和支持 C++17 的编译器。以下 PowerShell 命令使用 GCC/MinGW-w64。依赖头文件已随代码提供。首次演示使用新数据目录，建议在同一个目录另开一个终端执行查询。

1. 在源码根目录打开 PowerShell，进入应用目录并编译。

~~~powershell
Set-Location apps/edu-dbms
.\build.ps1
~~~

2. 在该终端启动服务。浏览器网址为 http://127.0.0.1:8765 。在此终端按 Ctrl+C 结束本次服务。

~~~powershell
.\start.ps1 -Port 8765 -Data .\demo-data
~~~

3. 另开终端并进入同一应用目录，然后运行基础 SQL 脚本。

~~~powershell
.\.build\windows\edudb.exe run --url http://127.0.0.1:8765 --file demo/01_core.sql
~~~

脚本以三名学生演示含 NULL 的统计、成绩更新、中文课程连接和事务回滚。查看回滚后的查询，第 1 行学生的成绩应恢复为 86.5。再次运行完整脚本前请选择一个新数据目录。要运行原生用例，可在应用目录执行 `.\build.ps1 -Test`。

## 按报告阅读代码

| 问题 | 源码位置 | 阅读要点 |
|---|---|---|
| 为什么修改索引键不会把迁移后的行再次更新？ | [SQL 增量修改](apps/edu-dbms/src/sql_mutation.inc#L64-L94) | 游标先收集匹配行及旧值，再逐行写入。 |
| 如何处理内存放不下的索引构建？ | [索引排序与建树](apps/edu-dbms/src/storage_sort.inc#L54-L66) | 无临时段时使用内存排序；有临时段时进入归并，两者共用有序流接口。 |
| 为什么在写 WAL 之前预留缓冲帧？ | [缓冲预留](apps/edu-dbms/src/buffer_pool.cpp#L186-L193) · [提交流程](apps/edu-dbms/src/storage.cpp#L476-L487) | 先确认至少有一个可复用发布帧，再处理本事务 WAL 和已提交页面。 |
| 查询、并发、恢复如何核验？ | [原生用例](apps/edu-dbms/tests) · [演示 SQL](apps/edu-dbms/demo) | 用例与演示分别面向内核行为和服务入口。 |
| PostgreSQL 14.11 如何选择访问路径并处理并发、恢复？ | [专题复现与源码索引](postgresql-reading/README.md) | 区分自然计划与调试查询、语句快照与事务快照，以及 WAL 刷出与事务可见。 |
