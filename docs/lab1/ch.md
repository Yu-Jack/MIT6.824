# 介紹

主要元件分為 Coordinator 以及 Worker 兩種，但要在 Coordinator 維護所有 Worker 狀態會過於複雜。所以這邊做法是反過來，只要讓 Worker 能知道 Coordinator health 狀態即可。

如果 Coordinator 還活著，Worker 就會嘗試跟 Coordinator 取得任務去完成它。而當 Coordinator 分派完所有 Map &Reduce 任務後，會把狀態改為 unhealthy，接著只要 Worker 收到 unhealthy 就會結束程式並離開。