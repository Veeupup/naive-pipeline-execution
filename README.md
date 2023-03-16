# Naive Pipeline Execution (Toy)

> "What I can not create, I can not understand."
>               -- Richard Feynman
> 
>   Just write this to help me understand how pipeline really works.
>   
>   And I have an article on [知乎]() and [blog]() to summary pipeline.

pipeline exection demo. Inspired by Morsel, Clickhouse, databend, datafusion...

## Features

* use DAG to represent the computing 

* Explicit schedule by traverse the DAG

* Parallel Computing using the thread pool

* push-based data transfer, data centric computing

## TDB

This pipeline is so native so we have so much stuff to improve it, such as:

* more efficient scheduling algorithm.

* `pull combined with push` control.

* make processor NUMA-aware.

* dynamically change the DAG when running

* working stealing..(In fact we have some kind of this, because we only have one DAG globally)

and these things are mostly implemented in Clickhouse, databend. You can check them if you are interested.
