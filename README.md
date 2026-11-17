# Aurora KCP

![license](https://img.shields.io/github/license/NOMOAX/aurora.kcp)
![version](https://img.shields.io/badge/version-0.1.0-blue)
![lowest Unity version](https://img.shields.io/badge/Unity-2021.2%2B-blue)

KCP is a fast and reliable ARQ (Automatic Repeat reQuest) protocol: it trades 10%-20% of extra bandwidth for an average latency 30%-40% lower than TCP.

English | [中文](README.zh.md)

## Dependencies

- [Aurora](https://github.com/NOMOAX/aurora.git)

## Installation

1. Open Unity package manager.
2. Click the `+` button in the upper-left corner, then select `Add package from git URL...`.
3. Input `https://github.com/NOMOAX/aurora.kcp.git` and then click the `Add` button.

## Overview

`Kcp` is the only type that holds state: one instance is one connection. It is a pure algorithm — it owns no socket and reads no clock — so the caller supplies both, a `KcpOutputHandler` that sends a datagram and a millisecond timestamp on every call. That is what lets a server drive thousands of connections from one loop, or from one timer through `Check`.

| Type                                     | Role                                                                |
|------------------------------------------|---------------------------------------------------------------------|
| `Kcp`                                    | One connection: the state machine, the queues and the send buffer   |
| `KcpOutputHandler`                       | Sends one datagram to the remote endpoint                           |
| `KcpInputResult`                         | What `Input` found in a datagram                                    |
| `KcpNoDelayLevel`                        | How fast the retransmission timeout grows on a lossy link           |
| `KcpLogMask` / `KcpLogWriter`            | Which protocol events are logged, and where                         |
| `IKcpCongestionControl`                  | The congestion control algorithm, pluggable per connection          |
| `Constant`                               | Protocol constants and the defaults the constructor applies         |

`Kcp` is **not thread safe**. Call it from one thread, or serialize the calls externally.

## Design Notes

The implementation follows `ikcp.c` line by line, including the `IKCP_FASTACK_CONSERVE` variant, so a connection interoperates byte for byte with any other KCP implementation. The wire format is untouched.

The C allocator is replaced by `Aurora.Pooling.Pool<T>`: segments are rented when a message is fragmented and returned when they are acknowledged, so an idle connection holds no memory and a steady state connection performs no heap allocation.

The `IQUEUEHEAD` intrusive lists are replaced by `Aurora.Collections.Deque<T>`, which supplies the index based removal that the send buffer needs. The four counters of the C struct are gone: the queues report their own count.

The pluggable congestion control of KCP v2 is exposed as `IKcpCongestionControl`; the built-in algorithm is used while `Kcp.CongestionControl` is `null`.

Protocol logging is opt in per event kind: `Kcp.LogMask` gates `Kcp.LogWriter`, so a disabled log costs one branch.

## Creating a Connection

A connection is identified by its conversation identifier, which must be the same on both endpoints. `Kcp.GetConversationId` reads it from the beginning of a datagram, which is what a server needs in order to dispatch an incoming datagram to the right connection.

```csharp
var kcp = new Kcp(conversationId, (buffer, offset, count) => udpSocket.Send(buffer, offset, count));

// On the receiving side
var conversationId = Kcp.GetConversationId(datagram, 0);
```

The output handler is called once per datagram, and a datagram may carry several segments. It must not block: it is called from inside `Update` and `Flush`.

## Driving the State Machine

`Update` advances the state machine and flushes the pending acknowledgments, window probes and data segments when the flush interval has elapsed. **Call it repeatedly, for example every 10 to 100 milliseconds** — nothing is transmitted, retransmitted or timed out without it.

```csharp
kcp.Update(currentTimeInMilliseconds);
```

The timestamp is a millisecond clock that **wraps around like `uint`**; use `Stopwatch`-based milliseconds cast to `uint`, or `Environment.TickCount` on a single machine. `CurrentTime` reports the value of the last call.

`Check` answers a different question: given the current time, when must `Update` be called next, assuming that neither `Input` nor `Send` is called in between. A server can therefore keep one timer for thousands of connections instead of polling each one.

```csharp
var next = kcp.Check(currentTimeInMilliseconds);
```

`Flush` encodes and sends everything that is pending, without waiting for the flush interval. `Update` calls it automatically; call it directly only when an acknowledgment must leave immediately. It does nothing until `Update` has been called at least once.

## Sending

`Send` queues a message, splitting it into fragments no larger than `MaximumSegmentSize`. It returns the number of bytes that were queued, which in the stream mode may be a prefix of the message: the rest is appended to a segment that still has room.

```csharp
var queued = kcp.Send(payload, 0, payload.Length);
```

A message that would need `Constant.DefaultReceiveWindowSize` fragments or more throws `ArgumentException`, because the receiver could never assemble it. The message is only queued; it leaves on the next `Update` or `Flush`.

```csharp
kcp.WaitSendCount;        // Segments queued or unacknowledged
kcp.InFlightSegmentCount; // Segments handed to the output handler and not acknowledged yet
```

## Receiving

`Input` feeds one received datagram into the protocol. It returns a `KcpInputResult`, so a caller can tell an unusable datagram from one that was merely irrelevant to this connection.

```csharp
var result = kcp.Input(datagram, 0, datagram.Length);
if (result != KcpInputResult.Ok)
{
    // InvalidDatagram, MalformedSegment or UnknownCommand
}
```

`PeekSize` reports the length of the next complete message, or -1 when none is ready. `Receive` copies it out and returns the length, or -1 when there is nothing to read.

```csharp
var size = kcp.PeekSize();
if (size >= 0)
{
    var message = new byte[size];
    kcp.Receive(message, 0, size);
}
```

`Receive` throws `ArgumentException` when the available space is smaller than the message, and only ever returns a whole message: fragments are reassembled before the message becomes visible.

## Configuration

### Trading Bandwidth for Latency

`SetNoDelay` configures the three parameters that decide how fast a loss is detected and repaired. The first one is `KcpNoDelayLevel`.

| Level      | Retransmission timeout growth                                                 |
|------------|-------------------------------------------------------------------------------|
| `Disabled` | By the larger of itself and the smoothed round-trip time, which doubles it     |
| `Enabled`  | By half of itself                                                              |
| `Aggressive` | By half of the smoothed round-trip time                                      |

```csharp
// A normal mode connection: no delay disabled, interval 40, no fast retransmission, congestion control on
kcp.SetNoDelay(KcpNoDelayLevel.Disabled, 40, 0, false);

// A fastest mode connection: 10 ms interval, retransmit after 2 later acknowledgments, congestion control off
kcp.SetNoDelay(KcpNoDelayLevel.Enabled, 10, 2, true);
```

The second parameter is `Interval`, clamped to 10 to 5000 milliseconds. The third is `FastResendThreshold`: the number of acknowledgments of later segments after which a segment is retransmitted before its timeout expires, where 0 disables fast retransmission. The fourth disables congestion control, which trades fairness and bandwidth utilization for a stable rate on a lossy link.

The properties behind the parameters can be assigned one at a time as well; `Interval`, `NoDelayLevel`, `FastResendThreshold`, `FastResendLimit` and `NoCongestionControl` are all readable and writable.

### Windows

```csharp
kcp.SetWindowSize(32, 128);
```

The send window limits how many segments may be in flight. The receiving window limits how many segments may be queued for the upper layer, and it is clamped to at least `Constant.DefaultReceiveWindowSize`, because it must be able to hold a complete message. `RemoteWindowSize` reports the value the remote endpoint announced with its last segment.

The congestion window is separate: `CongestionWindowSize`, `SlowStartThreshold` and `CongestionWindowIncrement` are maintained by the congestion control algorithm, and `NoCongestionControl` makes the send window the only limit.

### Maximum Transmission Unit

```csharp
kcp.SetMaximumTransmissionUnit(1400);
```

The maximum transmission unit limits how many segments are merged into one datagram and how large a fragment is. It must be at least `Constant.MinimumTransmissionUnit`; `HeaderSize` (24 bytes) of it is protocol overhead, which leaves `MaximumSegmentSize` for payload.

### Stream Mode

By default every message is delimited: what `Send` hands over is what `Receive` gives back. In the stream mode messages have no boundary — consecutive sends may merge into one segment, and the receiver merges contiguous segments — which is what a byte stream over a connection wants.

```csharp
kcp.StreamMode = true;
```

## Congestion Control

Assign an `IKcpCongestionControl` to replace the built-in algorithm; assign `null` to go back to it. Assigning releases the previously installed algorithm and initializes the new one with this connection.

```csharp
kcp.CongestionControl = new MyCongestionControl(); // Released by Dispose, or by the next assignment
```

The interface receives acknowledgments, timeouts, fast retransmissions, round-trip times and application limited notifications, and returns the pacing budget that decides how many packets may leave in one flush.

## Logging

Logging is opt in per event kind and off by default, so a connection that does not need it pays one branch per event.

```csharp
kcp.LogMask = KcpLogMask.Input | KcpLogMask.Output | KcpLogMask.OutputData;
kcp.LogWriter = message => Debug.Log(message);
```

`KcpLogMask` distinguishes datagrams from segments, and the two directions from each other: `Input`, `InputData`, `InputAcknowledgment`, `InputProbe`, `InputWindowSize`, and the matching `Output` kinds.

## Link Health

A segment that is transmitted `DeadLinkTransmissionCount` times without being acknowledged marks the connection as dead: `State` becomes `uint.MaxValue` and `IsDead` reports `true`. The application decides what to do with it — KCP itself only reports it.

```csharp
kcp.State;                     // uint.MaxValue once the link is dead
kcp.IsDead;
kcp.RetransmissionCount;       // Timeouts so far
kcp.SmoothedRoundTripTime;     // Milliseconds
kcp.RoundTripTimeVariance;     // Milliseconds
kcp.RoundTripTimeout;          // Milliseconds
```

## Disposal

`Dispose` returns every segment the connection still holds to the internal pool and releases the congestion control algorithm. After that the connection is unusable, and every member that needs it throws `ObjectDisposedException`.

## Constants

`Constant` holds the protocol constants and the defaults the constructor applies.

| Constant                                 | Value   | Meaning                                                          |
|------------------------------------------|---------|------------------------------------------------------------------|
| `HeaderSize`                             | 24      | Segment header size in bytes                                     |
| `DefaultMaximumTransmissionUnit`         | 1400    | Initial maximum transmission unit                                |
| `MinimumTransmissionUnit`                | 50      | Smallest value `SetMaximumTransmissionUnit` accepts              |
| `DefaultSendWindowSize`                  | 32      | Initial send window                                              |
| `DefaultReceiveWindowSize`               | 128     | Initial receiving window, and the smallest it can be made        |
| `MaximumFragmentCount`                   | 255     | Limit imposed by the 8 bit fragment count field                  |
| `DefaultInterval`                        | 100     | Initial flush interval                                           |
| `MinimumInterval` / `MaximumInterval`    | 10 / 5000 | Range `Interval` is clamped to                                 |
| `DefaultRoundTripTimeout`                | 200     | Initial retransmission timeout                                   |
| `MinimumRoundTripTimeout`                | 100     | Floor when the no-delay mode is disabled                         |
| `NoDelayMinimumRoundTripTimeout`         | 30      | Floor when the no-delay mode is enabled                          |
| `MaximumRoundTripTimeout`                | 60000   | Ceiling of the retransmission timeout                            |
| `InitialSlowStartThreshold`              | 2       | Initial slow start threshold                                     |
| `MinimumSlowStartThreshold`              | 2       | Smallest slow start threshold                                    |
| `DefaultDeadLinkRetransmissionCount`     | 20      | Transmissions after which the link is dead                       |
| `DefaultFastResendLimit`                 | 5       | Transmissions above which fast retransmission stops              |
| `InitialWindowProbeInterval`             | 5000    | First window probe interval                                      |
| `MaximumWindowProbeInterval`             | 120000  | Ceiling of the window probe interval                             |
| `TimestampResetThreshold`                | 10000   | Difference at which the flush timestamp is reset instead of used  |
