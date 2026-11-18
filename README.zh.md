# Aurora KCP

![许可](https://img.shields.io/github/license/NOMOAX/aurora.kcp)
![版本](https://img.shields.io/badge/version-0.1.1-blue)
![最低 Unity 版本](https://img.shields.io/badge/Unity-2021.2%2B-blue)

KCP 是一个快速可靠的自动重传请求（ARQ, Automatic Repeat reQuest）协议：它用 10%-20% 的额外带宽换取比 TCP 低 30%-40% 的平均延迟。

[English](README.md) | 中文

## 依赖

- [Aurora](https://github.com/NOMOAX/aurora.git)

## 安装

1. 打开 Unity package manager。
2. 点击左上角的 `+` 按钮，然后选择 `Add package from git URL...`。
3. 填入 `https://github.com/NOMOAX/aurora.kcp.git` 并点击 `Add` 按钮。

## 概览

`Kcp` 是本包中唯一持有状态的类型：一个实例就是一条连接。它是纯算法——既不持有 socket，也不读取时钟——因此两者都由调用方提供：负责发送数据报的 `KcpOutputHandler`，以及每次调用时传入的毫秒时间戳。正因如此，一个服务器才能用单个循环驱动成千上万条连接，或者通过 `Check` 用单个定时器做到同样的事。

| 类型                          | 职责                                     |
|-------------------------------|------------------------------------------|
| `Kcp`                         | 一条连接：状态机、收发队列与发送缓冲     |
| `KcpOutputHandler`            | 向对端发送一个数据报                     |
| `KcpInputResult`              | `Input` 在一个数据报中解析出的结果       |
| `KcpNoDelayLevel`             | 丢包时重传超时的增长方式                 |
| `KcpLogMask` / `KcpLogWriter` | 记录哪些协议事件，以及记录到哪里         |
| `IKcpCongestionControl`       | 可按连接替换的拥塞控制算法               |
| `Constant`                    | 协议常量，以及构造函数采用的默认值       |

`Kcp` **不保证线程安全**。请在单一线程上调用，或在外部把调用串行化。

## 设计说明

实现逐行对照 `ikcp.c`（包括 `IKCP_FASTACK_CONSERVE` 变体），因此可以与任何其它 KCP 实现逐字节互通。线上格式未做改动。

C 侧的分配器被 `Aurora.Pooling.Pool<T>` 取代：消息被分片时租用分段，被确认时归还，因此空闲连接不占内存，稳定运行中的连接不产生堆分配。

`IQUEUEHEAD` 侵入式链表被 `Aurora.Collections.Deque<T>` 取代，它提供了发送缓冲所需的按索引删除。C 结构体里的四个计数器已删除：队列自己就会报告数量。

KCP v2 的可插拔拥塞控制被暴露为 `IKcpCongestionControl`；`Kcp.CongestionControl` 为 `null` 时使用内置算法。

协议日志按事件类型选择性开启：`Kcp.LogMask` 决定 `Kcp.LogWriter` 是否被调用，因此关闭的日志只花一个分支。

## 创建连接

一条连接由会话标识符区分，两端必须一致。`Kcp.GetConversationId` 可以从数据报开头读出它，服务器正是靠它把一个收到的数据报分派给正确的连接。

```csharp
var kcp = new Kcp(conversationId, (buffer, offset, count) => udpSocket.Send(buffer, offset, count));

// 接收侧
var conversationId = Kcp.GetConversationId(datagram, 0);
```

输出处理器每个数据报被调用一次，而一个数据报可能携带多个分段。它不能阻塞：调用发生在 `Update` 和 `Flush` 内部。

## 驱动状态机

`Update` 推进状态机，并在刷新间隔到达时刷出待发的确认、窗口探测与数据分段。 **请反复调用它，例如每 10 到 100 毫秒一次**——没有它，就不会有任何发送、重传或超时。

```csharp
kcp.Update(currentTimeInMilliseconds);
```

时间戳是一个毫秒时钟，会 **像 `uint` 一样回绕**；可以使用基于 `Stopwatch` 的毫秒数强制转换为 `uint`，单机场景也可以使用 `Environment.TickCount`。`CurrentTime` 报告上一次调用传入的值。

`Check` 回答的是另一个问题：给定当前时间，如果期间既不调用 `Input` 也不调用 `Send`，那么下一次必须在什么时候调用 `Update`。因此服务器可以为成千上万条连接只保留一个定时器，而不必逐条轮询。

```csharp
var next = kcp.Check(currentTimeInMilliseconds);
```

`Flush` 编码并发送所有待发内容，不必等待刷新间隔。`Update` 会自动调用它；只有在确认必须立刻发出时才需要直接调用。在 `Update` 至少被调用一次之前，它什么都不做。

## 发送

`Send` 把一条消息排入队列，并按不超过 `MaximumSegmentSize` 的大小分片。它返回已排入队列的字节数，在流模式下这个数可能只是消息的前缀：剩余部分会追加到仍有空间的已有分段里。

```csharp
var queued = kcp.Send(payload, 0, payload.Length);
```

如果一条消息需要 `Constant.DefaultReceiveWindowSize` 个或更多分片，会抛出 `ArgumentException`，因为接收方永远无法拼装出来。消息只是被排入队列，实际发送发生在下一次 `Update` 或 `Flush`。

```csharp
kcp.WaitSendCount;        // 已排队或未确认的分段数
kcp.InFlightSegmentCount; // 已交给输出处理器但尚未确认的分段数
```

## 接收

`Input` 把一个收到的数据报喂给协议。它返回 `KcpInputResult`，因此调用方可以区分“不可用的数据报”和“与本连接无关的数据报”。

```csharp
var result = kcp.Input(datagram, 0, datagram.Length);
if (result != KcpInputResult.Ok)
{
    // InvalidDatagram、MalformedSegment 或 UnknownCommand
}
```

`PeekSize` 报告下一条完整消息的长度，没有可读消息时返回 -1。`Receive` 把它拷贝出来并返回长度，没有内容时返回 -1。

```csharp
var size = kcp.PeekSize();
if (size >= 0)
{
    var message = new byte[size];
    kcp.Receive(message, 0, size);
}
```

可用空间小于消息长度时，`Receive` 抛出 `ArgumentException`。它只会交出完整消息：分片在消息可见之前就已拼装完毕。

## 配置

### 用带宽换延迟

`SetNoDelay` 配置决定“多快发现丢包、多快修复”的三个参数。第一个是 `KcpNoDelayLevel`。

| 级别         | 重传超时的增长方式                       |
|--------------|------------------------------------------|
| `Disabled`   | 按“它自身与平滑往返时间的较大者”增长，相当于翻倍 |
| `Enabled`    | 按自身的一半增长                         |
| `Aggressive` | 按平滑往返时间的一半增长                 |

```csharp
// 常规模式连接：关闭 no-delay、间隔 40、不做快速重传、开启拥塞控制
kcp.SetNoDelay(KcpNoDelayLevel.Disabled, 40, 0, false);

// 最快模式连接：间隔 10 毫秒、后续 2 个确认后立即重传、关闭拥塞控制
kcp.SetNoDelay(KcpNoDelayLevel.Enabled, 10, 2, true);
```

第二个参数是 `Interval`，被限制在 10 到 5000 毫秒之间。第三个是 `FastResendThreshold`：后续分段被确认多少个之后，就在超时之前重传本分段；0 表示关闭快速重传。第四个参数关闭拥塞控制，用公平性与带宽利用率为代价，换取在丢包链路上稳定的发送速率。

这些参数背后的属性也可以单独赋值：`Interval`、`NoDelayLevel`、`FastResendThreshold`、`FastResendLimit` 与 `NoCongestionControl` 都可读可写。`MinimumRoundTripTimeout` 同样可写，且不能为负。

### 窗口

```csharp
kcp.SetWindowSize(32, 128);
```

发送窗口限制同时处于在途状态的分段数。接收窗口限制可以排给上层的分段数，并且会被抬到至少 `Constant.DefaultReceiveWindowSize`，因为它必须能容纳一条完整消息。`RemoteWindowSize` 报告对端在最后一个分段中通告的值。

拥塞窗口是另一回事：`CongestionWindowSize`、`SlowStartThreshold` 与 `CongestionWindowIncrement` 由拥塞控制算法维护；启用 `NoCongestionControl` 后，发送窗口成为唯一的限制。

### 最大传输单元

```csharp
kcp.SetMaximumTransmissionUnit(1400);
```

最大传输单元限制一个数据报能合并多少个分段，以及单个分片的大小。它不能小于 `Constant.MinimumTransmissionUnit`；其中 `HeaderSize`（24 字节）是协议开销，其余留给载荷，即 `MaximumSegmentSize`。分段会保持它被分片时的尺寸，因此在仍有分段排队或未确认时调低最大传输单元，这些分段仍会以超过新上限的数据报发出。

### 流模式

默认情况下每条消息都有边界：`Send` 交出去什么，`Receive` 就取回什么。流模式下消息没有边界——连续的发送可以合并进同一个分段，接收方也会把相邻分段合并——这正是连接上的字节流所需要的。

```csharp
kcp.StreamMode = true;
```

## 拥塞控制

给 `IKcpCongestionControl` 赋一个实现即可替换内置算法；赋 `null` 则回到内置算法。赋值时会先释放此前安装的算法，再用本连接初始化新的算法。

```csharp
kcp.CongestionControl = new MyCongestionControl(); // 由 Dispose 或下一次赋值释放
```

接口会收到确认、超时、快速重传、往返时间与应用受限的通知，并返回 pacing 预算，决定一次刷出能发出多少个包。

## 日志

日志按事件类型选择性开启，默认关闭，因此不需要它的连接只为每个事件付出一个分支的代价。

```csharp
kcp.LogMask = KcpLogMask.Input | KcpLogMask.Output | KcpLogMask.OutputData;
kcp.LogWriter = message => Debug.Log(message);
```

`KcpLogMask` 区分数据报与分段，也区分两个方向：`Input`、`InputData`、`InputAcknowledgment`、`InputProbe`、`InputWindowSize`，以及对应的 `Output` 系列。

## 链路健康

某个分段被发送 `DeadLinkTransmissionCount` 次仍未被确认时，连接被判为已死：`State` 变成 `uint.MaxValue`，`IsDead` 返回 `true`。该次数至少为 1。如何处理由应用决定——KCP 只负责报告。

```csharp
kcp.State;                     // 链路死亡后为 uint.MaxValue
kcp.IsDead;
kcp.RetransmissionCount;       // 到目前为止因超时发生的重传次数
kcp.SmoothedRoundTripTime;     // 毫秒
kcp.RoundTripTimeVariance;     // 毫秒
kcp.RoundTripTimeout;          // 毫秒
```

## 释放

`Dispose` 把连接仍持有的所有分段归还内部池，并释放拥塞控制算法。之后连接不可再用，任何需要它的成员都会抛出 `ObjectDisposedException`。

## 常量

`Constant` 中存放协议常量与构造函数采用的默认值。

| 常量                                  | 值        | 含义                                 |
|---------------------------------------|-----------|--------------------------------------|
| `HeaderSize`                          | 24        | 分段头部的字节数                     |
| `DefaultMaximumTransmissionUnit`      | 1400      | 初始最大传输单元                     |
| `MinimumTransmissionUnit`             | 50        | `SetMaximumTransmissionUnit` 接受的最小值 |
| `DefaultSendWindowSize`               | 32        | 初始发送窗口                         |
| `DefaultReceiveWindowSize`            | 128       | 初始接收窗口，也是其可被设置的最小值 |
| `MaximumFragmentCount`                | 255       | 8 位分片计数字段带来的上限           |
| `DefaultInterval`                     | 100       | 初始刷新间隔                         |
| `MinimumInterval` / `MaximumInterval` | 10 / 5000 | `Interval` 被限制到的范围            |
| `DefaultRoundTripTimeout`             | 200       | 初始重传超时                         |
| `MinimumRoundTripTimeout`             | 100       | 关闭 no-delay 时的下限               |
| `NoDelayMinimumRoundTripTimeout`      | 30        | 开启 no-delay 时的下限               |
| `MaximumRoundTripTimeout`             | 60000     | 重传超时的上限                       |
| `InitialSlowStartThreshold`           | 2         | 初始慢启动阈值                       |
| `MinimumSlowStartThreshold`           | 2         | 最小慢启动阈值                       |
| `DefaultDeadLinkRetransmissionCount`  | 20        | 发送多少次后判定链路已死             |
| `DefaultFastResendLimit`              | 5         | 发送次数超过它后不再快速重传         |
| `InitialWindowProbeInterval`          | 5000      | 初始窗口探测间隔                     |
| `MaximumWindowProbeInterval`          | 120000    | 窗口探测间隔的上限                   |
| `TimestampResetThreshold`             | 10000     | 差值达到它时重置刷新时间戳而非使用   |
