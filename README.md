# Pipeline Execution Demo

pipeline exection demo. Inspired by Clickhouse, databend, datafusion.

## Features

* use DAG to represent the computing 

* Explicit schedule by traverse the DAG

* Parallel Computing using the thread pool

* push-based data transfer, data centric computing


## TBD

## Design Trade Off

1. 是否使用 channel 来传递数据？或者采用共享数据结构来传递数据，哪个会更好呢？

channel 的话，每次传递一个 batch，采用阻塞 channel 来进行传递，如果某个 processor 开始运行，那么将可以直接从 channel 中读取到对应的数据
当关闭的时候, 直接 close 掉，这样会比较依赖

需要考虑 merge processor 和 expand processor 的问题， merge processor 可以用 mpsc 的 channel 来做，

那么 expand processor 需要构建出多个 channel 管理关系

  1. 什么时候关闭 channel 呢？当当前结束运算的时候，当前结束运算，也就是当前计算完毕，而且上游 finished 之后就发送 close 信号

共享数据结构的话，去手动拿数据即可，不需要考虑 channel 关闭的问题，也是一样的实现，总体看下来好像没什么区别

要不先实现共享数据结构的实现，可能更舒服一点

2. 如何执行调度，让调度的开销更小？
