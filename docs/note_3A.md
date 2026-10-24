# 3A: Raft 的 Leader 选举功能

## 1. 节点各角色定义以及转换条件

### Follower
1. 初始状态为 Follower，进入该状态时初始化一个计时器，倒计时结束进入 Candidate
2. 会回复来自 Candidate 的 RequestVoteRPC，遵循先到先得规则，把票投给收到的第一个请求的合法 Candidate
3. 收到同一个 Candidate 的投票请求时，继续回复同意，其它人的请求回复拒绝
4. 若 RPC 对应的 term 不合法，则拒绝投票
5. 当接收到来自 Leader 的合法 heartbeat（空 entry 的 log rpc）时重置计时器
6. 在发现 Leader 的 term 小于自身时，拒绝该信息并回复自身 term

### Candidate
1. 进入该状态时将 term++，并初始化一个计时器（时长是否随机？在一定范围内随机）
2. 知道当前节点的总数 n
3. 向其它节点发送 RequestVoteRPC 并附上 term
4. 投票开始时默认给自己一票，从1开始
5. 接收到超过一半（？确实是）的选票时进入 Leader 状态
6. 计时器倒计时结束后重新进入 Candidate 状态
7. 发现来自 Leader 的信息时（且 term >= 自身）进入 Follower 状态

### Leader
1. 刚进入该状态时立即向其它节点发送 heartbeat 信息
2. 在该状态下定时向其它所以节点发送 heartbeat 信息

### 通用操作
1. 当发现信息的 term > 自身时，进入 Follower 状态
