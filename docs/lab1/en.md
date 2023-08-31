# Introduction

There are two components here, one is coordinator and another is worker. If we want to keep worker's status in coordinator, it will be complex here. In contrast, we let worker keep track of health status of the coordinator.

If coodirnator is health, worker will try to retrive the task from it. After coordinator dipatch all the map & reduce task, it's status turn into unhealth. When worker receives the unhealth coordinator, work exits the program and leave.

Worker will `ACK` to coordinator when it finishes the task every time.

# Map/Reduce Task Bussiness Logic

One map task will generate the `nReduce` intermediate files. It will generate 80 intermediate files in this lab.

Map task puts the `Key` into specified intermediate file by using `ihash` and `nReduce`, for example:  
- `ihash(A) % nReduce = 4`, it means `A` will be put into bucket 4 which is `mr-{map_task_id-4}`
- `ihash(B) % nReduce = 2`, it means `b` will be put into bucket 4 which is `mr-{map_task_id-2}`

> It might have a chance all the keys located at same bucket.

Then coordinator collects all file name in the same bucket, such as `mr-{map_task_id_1}-4`, `mr-{map_task_id_2}-4`, `mr-{map_task_id_3}-4`. There will be 8 files in bucket 4. Coordinator will dispatch one bucket as a task to worker to handle. There will be 10 tasks cause of 10 buckets.