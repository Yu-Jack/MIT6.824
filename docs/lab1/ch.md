# 介紹

主要元件分為 Coordinator 以及 Worker 兩種，但要在 Coordinator 維護所有 Worker 狀態會過於複雜。所以這邊做法是反過來，只要讓 Worker 能知道 Coordinator health 狀態即可。

如果 Coordinator 還活著，Worker 就會嘗試跟 Coordinator 取得任務去完成它。而當 Coordinator 分派完所有 Map &Reduce 任務後，會把狀態改為 unhealthy，接著只要 Worker 收到 unhealthy 就會結束程式並離開。

而每次 Woker 做完 Map/Reduce 任務後，會呼叫 ACK 通知 Coordinator 代表做完。

# Map/Reduce 任務處理商業邏輯

以這個 lab 來說，1 個 map task 會產生 `nReduce` intermediate files。所以 lab 8 個檔案以及 10 nReduce 的設定來說，會出現 80 個 intermediate files。

Map 會根據 `ihash` 以及 `nReduce` 去決定這個 `Key` 要放在哪一個 intermediate file 裡面，舉例來說：
- `A` 透過 `ihash % nReduce` 得到第 4 bucket，就會被分配到 `mr-{map_task_id}-4`
- `B` 透過 `ihash % nReduce` 得到第 2 bucket，就會被分配到 `mr-{map_task_id}-2`

> 但很有可能這個 map task 得到的 Key 全部都被分到同一個 bucket  

然後 Coordinator 會把同個 bucket 的 file 整理起來，類似 `mr-{map_task_id_1}-4`, `mr-{map_task_id_2}-4`, `mr-{map_task_id_3}-4` 這樣，以第 4 個 bucket 來說，總共會有 8 個檔案。全部總共有 10 個 buckets，而一個 bucket 會被當成一個 task 丟給 worker 去處理。






