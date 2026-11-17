using System;
using Aurora.Collections;
using Aurora.Pooling;

namespace Aurora.Kcp
{
    /// <summary>
    /// A KCP control block, which implements the KCP ARQ protocol for one connection.
    /// </summary>
    /// <remarks>
    /// KCP is a pure algorithm: it neither owns sockets nor reads the clock. The caller supplies the lower layer through a <see cref="KcpOutputHandler"/>,
    /// feeds received datagrams through <see cref="Input"/>, and drives the state machine by calling <see cref="Update"/> with the current time in milliseconds.
    /// <br/>
    /// An instance is not thread-safe. All members must be called from the same thread, or in an externally serialized manner.
    /// <br/>
    /// The implementation follows the upstream C implementation (kcp v2) closely, so it interoperates with any other KCP implementation.
    /// </remarks>
    public sealed class Kcp : IDisposable
    {
        /// <summary>
        /// The flag that indicates a window probe request must be sent.
        /// </summary>
        private const uint AskSend = 1;

        /// <summary>
        /// The flag that indicates the local window size must be told to the remote endpoint.
        /// </summary>
        private const uint AskTell = 2;

        /// <summary>
        /// The pool from which segments are rented, so that a steady state connection does not allocate.
        /// </summary>
        private static readonly IPool<KcpSegment> SegmentPool = new Pool<KcpSegment>(new PooledKcpSegmentPolicy());

        private readonly uint _conversationId;

        private readonly KcpOutputHandler _output;

        /// <summary>
        /// The queue of segments that have not been assigned a sequence number yet.
        /// </summary>
        private readonly Deque<KcpSegment> _sendQueue = new();

        /// <summary>
        /// The buffer of segments that have been sent but not acknowledged yet (ordered by sequence number).
        /// </summary>
        private readonly Deque<KcpSegment> _sendBuffer = new();

        /// <summary>
        /// The buffer of segments that have been received but cannot be queued yet because of a gap (ordered by sequence number).
        /// </summary>
        private readonly Deque<KcpSegment> _receiveBuffer = new();

        /// <summary>
        /// The queue of segments that the upper layer can receive.
        /// </summary>
        private readonly Deque<KcpSegment> _receiveQueue = new();

        /// <summary>
        /// The buffer that a flush encodes datagrams into.
        /// </summary>
        private byte[] _outputBuffer;

        /// <summary>
        /// The list of acknowledgments that the next flush sends (stored as pairs of sequence number and timestamp).
        /// </summary>
        private uint[] _acknowledgmentList = Array.Empty<uint>();

        /// <summary>
        /// The number of acknowledgments in <see cref="_acknowledgmentList"/>.
        /// </summary>
        private int _acknowledgmentCount;

        /// <summary>
        /// The number of payload bytes that have been acknowledged since the beginning of the current <see cref="Input"/> call.
        /// </summary>
        private int _acknowledgedLength;

        private uint _state;

        private uint _maximumTransmissionUnit;

        private uint _maximumSegmentSize;

        private uint _sendUnacknowledgedSequenceNumber;

        private uint _sendNextSequenceNumber;

        private uint _receiveNextSequenceNumber;

        private uint _slowStartThreshold;

        private int _smoothedRoundTripTime;

        private int _roundTripTimeVariance;

        private int _roundTripTimeout;

        private int _minimumRoundTripTimeout;

        private uint _sendWindowSize;

        private uint _receiveWindowSize;

        private uint _remoteWindowSize;

        private uint _congestionWindowSize;

        private uint _congestionWindowIncrement;

        private uint _probeFlags;

        private uint _currentTime;

        private uint _flushTimestamp;

        private uint _retransmissionCount;

        private int _interval;

        private KcpNoDelayLevel _noDelayLevel;

        private bool _updated;

        private bool _disposed;

        private uint _probeTimestamp;

        private uint _probeWait;

        private uint _deadLinkTransmissionCount;

        private int _fastResendThreshold;

        private int _fastResendLimit;

        private bool _noCongestionControl;

        private bool _streamMode;

        private IKcpCongestionControl _congestionControl;

        /// <summary>
        /// Initializes a new instance of the <see cref="Kcp"/> class.
        /// </summary>
        /// <param name="conversationId">The conversation identifier, which must be the same on both endpoints of the connection.</param>
        /// <param name="output">The handler that sends a datagram to the remote endpoint.</param>
        /// <exception cref="ArgumentNullException"><paramref name="output"/> is <see langword="null"/>.</exception>
        public Kcp(uint conversationId, KcpOutputHandler output)
        {
            _output                           = output ?? throw new ArgumentNullException(nameof(output));
            _conversationId                   = conversationId;
            _state                            = 0;
            _sendUnacknowledgedSequenceNumber = 0;
            _sendNextSequenceNumber           = 0;
            _receiveNextSequenceNumber        = 0;
            _sendWindowSize                   = Constant.DefaultSendWindowSize;
            _receiveWindowSize                = Constant.DefaultReceiveWindowSize;
            _remoteWindowSize                 = Constant.DefaultReceiveWindowSize;
            _congestionWindowSize             = 0;
            _congestionWindowIncrement        = 0;
            _probeFlags                       = 0;
            _maximumTransmissionUnit          = Constant.DefaultMaximumTransmissionUnit;
            _maximumSegmentSize               = _maximumTransmissionUnit - Constant.HeaderSize;
            _outputBuffer                     = new byte[(_maximumTransmissionUnit + Constant.HeaderSize) * 3];
            _smoothedRoundTripTime            = 0;
            _roundTripTimeVariance            = 0;
            _roundTripTimeout                 = Constant.DefaultRoundTripTimeout;
            _minimumRoundTripTimeout          = Constant.MinimumRoundTripTimeout;
            _currentTime                      = 0;
            _interval                         = Constant.DefaultInterval;
            _flushTimestamp                   = Constant.DefaultInterval;
            _noDelayLevel                     = KcpNoDelayLevel.Disabled;
            _updated                          = false;
            _slowStartThreshold               = Constant.InitialSlowStartThreshold;
            _fastResendThreshold              = 0;
            _fastResendLimit                  = Constant.DefaultFastResendLimit;
            _noCongestionControl              = false;
            _retransmissionCount              = 0;
            _deadLinkTransmissionCount        = Constant.DefaultDeadLinkRetransmissionCount;
            _streamMode                       = false;
            _probeTimestamp                   = 0;
            _probeWait                        = 0;
        }

        /// <summary>
        /// Gets the conversation identifier of this connection.
        /// </summary>
        public uint ConversationId => _conversationId;

        /// <summary>
        /// Gets the state of this connection.
        /// </summary>
        /// <remarks>The state becomes <see cref="uint.MaxValue"/> (that is, a dead link) once a segment has been transmitted <see cref="DeadLinkTransmissionCount"/> times without being acknowledged.</remarks>
        public uint State => _state;

        /// <summary>
        /// Gets a value indicating whether the link is considered dead.
        /// </summary>
        public bool IsDead => _state == uint.MaxValue;

        /// <summary>
        /// Gets the maximum transmission unit in bytes.
        /// </summary>
        public int MaximumTransmissionUnit => (int)_maximumTransmissionUnit;

        /// <summary>
        /// Gets the maximum segment size in bytes, which is <see cref="MaximumTransmissionUnit"/> minus <see cref="Constant.HeaderSize"/>.
        /// </summary>
        public int MaximumSegmentSize => (int)_maximumSegmentSize;

        /// <summary>
        /// Gets the send window size in segments.
        /// </summary>
        public int SendWindowSize => (int)_sendWindowSize;

        /// <summary>
        /// Gets the receiving window size in segments.
        /// </summary>
        public int ReceiveWindowSize => (int)_receiveWindowSize;

        /// <summary>
        /// Gets the window size of the remote endpoint in segments, as reported by the last received segment.
        /// </summary>
        public int RemoteWindowSize => (int)_remoteWindowSize;

        /// <summary>
        /// Gets the congestion window size in segments, which is maintained by <see cref="CongestionControl"/> or by the built-in algorithm.
        /// </summary>
        public int CongestionWindowSize
        {
            get => (int)_congestionWindowSize;
            set => _congestionWindowSize = (uint)Math.Max(value, 0);
        }

        /// <summary>
        /// Gets the slow start threshold in segments, which is maintained by <see cref="CongestionControl"/> or by the built-in algorithm.
        /// </summary>
        public int SlowStartThreshold
        {
            get => (int)_slowStartThreshold;
            set => _slowStartThreshold = (uint)Math.Max(value, 0);
        }

        /// <summary>
        /// Gets or sets the congestion window increment in bytes, which is maintained by <see cref="CongestionControl"/> or by the built-in algorithm.
        /// </summary>
        public int CongestionWindowIncrement
        {
            get => (int)_congestionWindowIncrement;
            set => _congestionWindowIncrement = (uint)Math.Max(value, 0);
        }

        /// <summary>
        /// Gets the sequence number below which all segments have been acknowledged by the remote endpoint.
        /// </summary>
        public uint SendUnacknowledgedSequenceNumber => _sendUnacknowledgedSequenceNumber;

        /// <summary>
        /// Gets the sequence number that the next sent segment receives.
        /// </summary>
        public uint SendNextSequenceNumber => _sendNextSequenceNumber;

        /// <summary>
        /// Gets the sequence number of the next segment that the local endpoint queues for the upper layer.
        /// </summary>
        public uint ReceiveNextSequenceNumber => _receiveNextSequenceNumber;

        /// <summary>
        /// Gets the number of segments that are waiting in flight (queued or unacknowledged).
        /// </summary>
        public int InFlightSegmentCount => _sendBuffer.Count;

        /// <summary>
        /// Gets the number of segments that are waiting to be sent (queued or unacknowledged).
        /// </summary>
        public int WaitSendCount => _sendBuffer.Count + _sendQueue.Count;

        /// <summary>
        /// Gets or sets the interval in milliseconds at which KCP flushes its state.
        /// </summary>
        /// <remarks>The value is clamped to the range [<see cref="Constant.MinimumInterval"/>, <see cref="Constant.MaximumInterval"/>].</remarks>
        public int Interval { get => _interval; set => SetInterval(value); }

        private void SetInterval(int interval)
        {
            _interval = Clamp(interval, Constant.MinimumInterval, Constant.MaximumInterval);
        }

        /// <summary>
        /// Gets or sets the level of the no-delay mode, which controls how fast a lost segment is retransmitted.
        /// </summary>
        public KcpNoDelayLevel NoDelayLevel { get => _noDelayLevel; set => SetNoDelayLevel(value); }

        private void SetNoDelayLevel(KcpNoDelayLevel noDelayLevel)
        {
            _noDelayLevel = noDelayLevel;
            _minimumRoundTripTimeout = noDelayLevel == KcpNoDelayLevel.Disabled
                                           ? Constant.MinimumRoundTripTimeout
                                           : Constant.NoDelayMinimumRoundTripTimeout;
        }

        /// <summary>
        /// Gets or sets the number of acknowledgments of later segments after which a segment is retransmitted before its timeout expires (0 disables fast retransmission).
        /// </summary>
        public int FastResendThreshold { get => _fastResendThreshold; set => _fastResendThreshold = value; }

        /// <summary>
        /// Gets or sets the maximum number of transmissions above which a segment is no longer fast-retransmitted (a value less than or equal to 0 removes the limit).
        /// </summary>
        public int FastResendLimit { get => _fastResendLimit; set => _fastResendLimit = value; }

        /// <summary>
        /// Gets or sets a value indicating whether congestion control is disabled.
        /// </summary>
        /// <remarks>Disabling congestion control trades fairness and bandwidth utilization for a stable rate on lossy links.</remarks>
        public bool NoCongestionControl { get => _noCongestionControl; set => _noCongestionControl = value; }

        /// <summary>
        /// Gets or sets a value indicating whether the stream mode is enabled.
        /// </summary>
        /// <remarks>In the stream mode a message is not delimited: consecutive sends may merge into a single segment, and the receiver merges a contiguous run of segments into a single message.</remarks>
        public bool StreamMode { get => _streamMode; set => _streamMode = value; }

        /// <summary>
        /// Gets or sets the smallest retransmission timeout in milliseconds.
        /// </summary>
        /// <remarks><see cref="NoDelayLevel"/> overwrites this value when it is assigned.</remarks>
        public int MinimumRoundTripTimeout { get => _minimumRoundTripTimeout; set => _minimumRoundTripTimeout = value; }

        /// <summary>
        /// Gets the smoothed round-trip time in milliseconds.
        /// </summary>
        public int SmoothedRoundTripTime => _smoothedRoundTripTime;

        /// <summary>
        /// Gets the round-trip time variance in milliseconds.
        /// </summary>
        public int RoundTripTimeVariance => _roundTripTimeVariance;

        /// <summary>
        /// Gets the current retransmission timeout in milliseconds.
        /// </summary>
        public int RoundTripTimeout => _roundTripTimeout;

        /// <summary>
        /// Gets or sets the number of transmissions of a segment after which the link is considered dead.
        /// </summary>
        public int DeadLinkTransmissionCount
        {
            get => (int)_deadLinkTransmissionCount;
            set => _deadLinkTransmissionCount = (uint)value;
        }

        /// <summary>
        /// Gets the number of retransmissions that have occurred because of timeouts.
        /// </summary>
        public uint RetransmissionCount => _retransmissionCount;

        /// <summary>
        /// Gets the timestamp in milliseconds that was supplied by the last <see cref="Update"/> call.
        /// </summary>
        public uint CurrentTime => _currentTime;

        /// <summary>
        /// Gets or sets the congestion control algorithm, or <see langword="null"/> to use the built-in algorithm.
        /// </summary>
        /// <remarks>
        /// Assigning an algorithm releases the previously installed one (if any), then initializes the new one.
        /// If the new one fails to initialize, it is removed and the built-in algorithm takes over again.
        /// </remarks>
        public IKcpCongestionControl CongestionControl
        {
            get => _congestionControl;
            set
            {
                if (ReferenceEquals(_congestionControl, value))
                {
                    return;
                }
                _congestionControl?.Release(this);
                _congestionControl = value;
                if (value != null && !value.Initialize(this))
                {
                    value.Release(this);
                    _congestionControl = null;
                }
                if (_congestionControl == null)
                {
                    EnsureCongestionWindowIsOpen();
                }
            }
        }

        /// <summary>
        /// Gets or sets the kinds of protocol events that <see cref="LogWriter"/> receives.
        /// </summary>
        /// <remarks>The default is <see cref="KcpLogMask.None"/>, which disables logging entirely.</remarks>
        public KcpLogMask LogMask { get; set; } = KcpLogMask.None;

        /// <summary>
        /// Gets or sets the handler that receives protocol log messages.
        /// </summary>
        /// <remarks>Messages are formatted and passed to the handler only when <see cref="LogMask"/> contains the corresponding kind of event.</remarks>
        public KcpLogWriter LogWriter { get; set; }

        /// <summary>
        /// Configures the parameters that trade bandwidth for latency.
        /// </summary>
        /// <param name="noDelayLevel">The level of the no-delay mode.</param>
        /// <param name="interval">The interval in milliseconds at which KCP flushes its state.</param>
        /// <param name="fastResendThreshold">The number of acknowledgments of later segments after which a segment is retransmitted before its timeout expires (0 disables fast retransmission).</param>
        /// <param name="noCongestionControl">A value indicating whether congestion control is disabled.</param>
        /// <remarks>
        /// A normal mode connection is configured with <c>(<see cref="KcpNoDelayLevel.Disabled"/>, 40, 0, <see langword="false"/>)</c>.
        /// <br/>
        /// The fastest mode connection is configured with <c>(<see cref="KcpNoDelayLevel.Enabled"/>, 10, 2, <see langword="true"/>)</c>.
        /// </remarks>
        public void SetNoDelay(
            KcpNoDelayLevel noDelayLevel,
            int             interval,
            int             fastResendThreshold,
            bool            noCongestionControl)
        {
            ThrowIfDisposed();
            SetNoDelayLevel(noDelayLevel);
            SetInterval(interval);
            FastResendThreshold = fastResendThreshold;
            NoCongestionControl = noCongestionControl;
        }

        /// <summary>
        /// Sets the maximum send and receive window sizes.
        /// </summary>
        /// <param name="sendWindowSize">The send window size in segments (a value less than or equal to 0 leaves it unchanged).</param>
        /// <param name="receiveWindowSize">The receiving window size in segments (a value less than or equal to 0 leaves it unchanged, otherwise it is clamped to at least <see cref="Constant.DefaultReceiveWindowSize"/>).</param>
        /// <remarks>The receiving window must be large enough to hold a complete message, otherwise that message can never be assembled.</remarks>
        public void SetWindowSize(int sendWindowSize, int receiveWindowSize)
        {
            ThrowIfDisposed();
            if (sendWindowSize > 0)
            {
                _sendWindowSize = (uint)sendWindowSize;
            }
            if (receiveWindowSize > 0)
            {
                _receiveWindowSize = (uint)Math.Max(receiveWindowSize, Constant.DefaultReceiveWindowSize);
            }
        }

        /// <summary>
        /// Sets the maximum transmission unit.
        /// </summary>
        /// <param name="maximumTransmissionUnit">The maximum transmission unit in bytes.</param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="maximumTransmissionUnit"/> is less than <see cref="Constant.MinimumTransmissionUnit"/>.</exception>
        /// <remarks>The maximum transmission unit limits how many segments are merged into one datagram and how large a fragment is.</remarks>
        public void SetMaximumTransmissionUnit(int maximumTransmissionUnit)
        {
            ThrowIfDisposed();
            if (maximumTransmissionUnit < Constant.MinimumTransmissionUnit)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(maximumTransmissionUnit),
                    maximumTransmissionUnit,
                    $"The maximum transmission unit must be at least {Constant.MinimumTransmissionUnit}."
                );
            }
            _maximumTransmissionUnit = (uint)maximumTransmissionUnit;
            _maximumSegmentSize      = _maximumTransmissionUnit - Constant.HeaderSize;
            _outputBuffer            = new byte[(maximumTransmissionUnit + Constant.HeaderSize) * 3];
        }

        /// <summary>
        /// Sends a message to the remote endpoint, splitting it into fragments as needed.
        /// </summary>
        /// <param name="buffer">The buffer that contains the message.</param>
        /// <param name="offset">The zero-based offset of the message in <paramref name="buffer"/>.</param>
        /// <param name="count">The length of the message in bytes.</param>
        /// <returns>The number of bytes that have been queued (in the stream mode a prefix of the message may be appended to a previously queued segment).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">The current <see cref="Kcp"/> is disposed.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> or <paramref name="count"/> is negative, or the range they denote is not within <paramref name="buffer"/>.</exception>
        /// <exception cref="ArgumentException">The message would require <see cref="Constant.DefaultReceiveWindowSize"/> or more fragments, which the receiver could never assemble.</exception>
        /// <remarks>The message is queued; it is actually transmitted by the next <see cref="Update"/> or <see cref="Flush"/> call.</remarks>
        public int Send(byte[] buffer, int offset, int count)
        {
            ThrowIfDisposed();
            ValidateRange(buffer, offset, count);

            var sent = 0;

            // Append to the previous segment in the stream mode when it still has room.
            if (_streamMode && _sendQueue.Count > 0)
            {
                var previous = _sendQueue.PeekLast();
                if (previous.Length < _maximumSegmentSize)
                {
                    var capacity = (int)_maximumSegmentSize - previous.Length;
                    var extend   = Math.Min(count, capacity);
                    var segment  = RentSegment();
                    segment.EnsureDataCapacity(previous.Length + extend);
                    Array.Copy(previous.Data, 0,      segment.Data, 0,               previous.Length);
                    Array.Copy(buffer,        offset, segment.Data, previous.Length, extend);
                    segment.Length        = previous.Length + extend;
                    segment.FragmentIndex = 0;
                    _sendQueue.RemoveAt(_sendQueue.Count - 1);
                    ReturnSegment(previous);
                    _sendQueue.EnqueueLast(segment);
                    offset += extend;
                    count  -= extend;
                    sent   =  extend;
                }
                if (count <= 0)
                {
                    return sent;
                }
            }

            var fragmentCount = count <= _maximumSegmentSize
                                    ? 1
                                    : (count + (int)_maximumSegmentSize - 1) / (int)_maximumSegmentSize;

            if (fragmentCount >= Constant.DefaultReceiveWindowSize)
            {
                if (sent > 0)
                {
                    return sent;
                }
                throw new ArgumentException(
                    $"The message requires {fragmentCount} fragments, but the receive window cannot hold {Constant.DefaultReceiveWindowSize} or more of them.",
                    nameof(count)
                );
            }

            for (var i = 0; i < fragmentCount; i++)
            {
                var size    = Math.Min(count, (int)_maximumSegmentSize);
                var segment = RentSegment();
                segment.EnsureDataCapacity(size);
                if (size > 0)
                {
                    Array.Copy(buffer, offset, segment.Data, 0, size);
                }
                segment.Length        = size;
                // Fragments are numbered backwards, so 0 always marks the last fragment of a message.
                segment.FragmentIndex = _streamMode ? (byte)0 : (byte)(fragmentCount - i - 1);
                _sendQueue.EnqueueLast(segment);
                offset += size;
                count  -= size;
                sent   += size;
            }

            return sent;
        }

        /// <summary>
        /// Feeds a received datagram into the protocol.
        /// </summary>
        /// <param name="buffer">The buffer that contains the datagram.</param>
        /// <param name="offset">The zero-based offset of the datagram in <paramref name="buffer"/>.</param>
        /// <param name="count">The length of the datagram in bytes.</param>
        /// <returns>The result of the parsing.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">The current <see cref="Kcp"/> is disposed.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> or <paramref name="count"/> is negative, or the range they denote is not within <paramref name="buffer"/>.</exception>
        /// <remarks>This method does not send anything by itself, except that a fast retransmission of an earlier flush may be triggered by the next <see cref="Flush"/>.</remarks>
        public KcpInputResult Input(byte[] buffer, int offset, int count)
        {
            ThrowIfDisposed();
            ValidateRange(buffer, offset, count);

            var previousUnacknowledged    = _sendUnacknowledgedSequenceNumber;
            var priorInFlightSegmentCount = _sendBuffer.Count;
            var maximumAcknowledged       = 0U;
            var latestTimestamp           = 0U;
            var hasAcknowledgment         = false;
            _acknowledgedLength = 0;

            if (IsLogging(KcpLogMask.Input))
            {
                WriteLog(KcpLogMask.Input, $"input: {count} bytes");
            }

            if (count < Constant.HeaderSize)
            {
                return KcpInputResult.InvalidDatagram;
            }

            var end = offset + count;
            while (end - offset >= Constant.HeaderSize)
            {
                var conversationId = ReadUInt32(buffer, ref offset);
                if (conversationId != _conversationId)
                {
                    return KcpInputResult.InvalidDatagram;
                }
                var command        = (KcpCommand)ReadUInt8(buffer, ref offset);
                var fragmentIndex  = ReadUInt8(buffer, ref offset);
                var window         = ReadUInt16(buffer, ref offset);
                var timestamp      = ReadUInt32(buffer, ref offset);
                var sequenceNumber = ReadUInt32(buffer, ref offset);
                var unacknowledged = ReadUInt32(buffer, ref offset);
                var length         = ReadUInt32(buffer, ref offset);

                if ((uint)(end - offset) < length)
                {
                    return KcpInputResult.MalformedSegment;
                }

                _remoteWindowSize = window;
                ParseUnacknowledged(unacknowledged);
                ShrinkSendBuffer();

                switch (command)
                {
                    case KcpCommand.Acknowledge:
                    {
                        if (TimeDifference(_currentTime, timestamp) >= 0)
                        {
                            UpdateRoundTripTime(TimeDifference(_currentTime, timestamp));
                        }
                        ParseAcknowledgment(sequenceNumber);
                        ShrinkSendBuffer();
                        if (!hasAcknowledgment)
                        {
                            hasAcknowledgment   = true;
                            maximumAcknowledged = sequenceNumber;
                            latestTimestamp     = timestamp;
                        }
                        else if (TimeDifference(sequenceNumber, maximumAcknowledged) > 0 &&
                                 TimeDifference(timestamp,      latestTimestamp) > 0)
                        {
                            // Only an acknowledgment that is both later and more recent may skip later segments.
                            maximumAcknowledged = sequenceNumber;
                            latestTimestamp     = timestamp;
                        }
                        if (IsLogging(KcpLogMask.InputAcknowledgment))
                        {
                            WriteLog(
                                KcpLogMask.InputAcknowledgment,
                                $"input acknowledgment: sequence number = {sequenceNumber}, round trip time = {TimeDifference(_currentTime, timestamp)}, retransmission timeout = {_roundTripTimeout}"
                            );
                        }
                        break;
                    }
                    case KcpCommand.Push:
                    {
                        if (IsLogging(KcpLogMask.InputData))
                        {
                            WriteLog(
                                KcpLogMask.InputData,
                                $"input push: sequence number = {sequenceNumber}, timestamp = {timestamp}"
                            );
                        }
                        if (TimeDifference(sequenceNumber, _receiveNextSequenceNumber + _receiveWindowSize) < 0)
                        {
                            PushAcknowledgment(sequenceNumber, timestamp);
                            if (TimeDifference(sequenceNumber, _receiveNextSequenceNumber) >= 0)
                            {
                                var segment = RentSegment();
                                segment.FragmentIndex  = fragmentIndex;
                                segment.SequenceNumber = sequenceNumber;
                                segment.Length         = (int)length;
                                segment.EnsureDataCapacity((int)length);
                                if (length > 0)
                                {
                                    Array.Copy(buffer, offset, segment.Data, 0, (int)length);
                                }
                                ParseData(segment);
                            }
                        }
                        break;
                    }
                    case KcpCommand.WindowProbeAsk:
                    {
                        // The next flush tells the remote endpoint the local window size.
                        _probeFlags |= AskTell;
                        WriteLog(KcpLogMask.InputProbe, "input probe");
                        break;
                    }
                    case KcpCommand.WindowProbeTell:
                    {
                        // The window size has already been stored in the remote window size field.
                        if (IsLogging(KcpLogMask.InputWindowSize))
                        {
                            WriteLog(KcpLogMask.InputWindowSize, $"input window size: {window}");
                        }
                        break;
                    }
                    default:
                    {
                        return KcpInputResult.UnknownCommand;
                    }
                }

                offset += (int)length;
            }

            if (hasAcknowledgment)
            {
                ParseFastAcknowledgment(maximumAcknowledged, latestTimestamp);
            }

            if (TimeDifference(_sendUnacknowledgedSequenceNumber, previousUnacknowledged) > 0)
            {
                if (_congestionControl != null)
                {
                    _congestionControl.OnAcknowledged(
                        this,
                        _sendUnacknowledgedSequenceNumber - previousUnacknowledged,
                        _acknowledgedLength,
                        priorInFlightSegmentCount
                    );
                }
                else
                {
                    GrowCongestionWindow();
                }
            }

            return KcpInputResult.Ok;
        }

        /// <summary>
        /// Gets the size in bytes of the next message that <see cref="Receive"/> would return.
        /// </summary>
        /// <returns>The size of the next message; or -1 if no complete message is available.</returns>
        /// <exception cref="ObjectDisposedException">The current <see cref="Kcp"/> is disposed.</exception>
        public int PeekSize()
        {
            ThrowIfDisposed();
            if (_receiveQueue.Count == 0)
            {
                return -1;
            }
            var first = _receiveQueue.PeekFirst();
            if (first.FragmentIndex == 0)
            {
                return first.Length;
            }
            if (_receiveQueue.Count < first.FragmentIndex + 1)
            {
                return -1;
            }
            var length = 0;
            for (var i = 0; i < _receiveQueue.Count; i++)
            {
                var segment = _receiveQueue[i];
                length += segment.Length;
                if (segment.FragmentIndex == 0)
                {
                    break;
                }
            }
            return length;
        }

        /// <summary>
        /// Receives the next message from the receiving queue.
        /// </summary>
        /// <param name="buffer">The buffer that receives the message.</param>
        /// <param name="offset">The zero-based offset in <paramref name="buffer"/> at which to write the message.</param>
        /// <param name="count">The available space in bytes.</param>
        /// <returns>The length of the message in bytes, or -1 if no complete message is available.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">The current <see cref="Kcp"/> is disposed.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> or <paramref name="count"/> is negative, or the range they denote is not within <paramref name="buffer"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="count"/> is smaller than <see cref="PeekSize"/>.</exception>
        public int Receive(byte[] buffer, int offset, int count)
        {
            ThrowIfDisposed();
            ValidateRange(buffer, offset, count);

            var messageSize = PeekSize();
            if (messageSize < 0)
            {
                return -1;
            }
            if (messageSize > count)
            {
                throw new ArgumentException(
                    $"The available space is {count} bytes, but the message is {messageSize} bytes.",
                    nameof(count)
                );
            }

            var wasWindowFull = _receiveQueue.Count >= _receiveWindowSize;
            var length        = 0;
            while (_receiveQueue.Count > 0)
            {
                var segment = _receiveQueue.DequeueFirst();
                if (segment.Length > 0)
                {
                    Array.Copy(segment.Data, 0, buffer, offset + length, segment.Length);
                }
                length += segment.Length;
                var fragmentIndex = segment.FragmentIndex;
                if (IsLogging(KcpLogMask.Receive))
                {
                    WriteLog(KcpLogMask.Receive, $"receive: sequence number = {segment.SequenceNumber}");
                }
                ReturnSegment(segment);
                if (fragmentIndex == 0)
                {
                    break;
                }
            }

            MoveAvailableSegmentsToReceiveQueue();

            if (wasWindowFull && _receiveQueue.Count < _receiveWindowSize)
            {
                // Tell the remote endpoint that the local window has room again.
                _probeFlags |= AskTell;
            }

            return length;
        }

        /// <summary>
        /// Updates the state and flushes when the flush interval has elapsed.
        /// </summary>
        /// <param name="current">The current time in milliseconds.</param>
        /// <exception cref="ObjectDisposedException">The current <see cref="Kcp"/> is disposed.</exception>
        /// <remarks>Call this method repeatedly (for example every 10 to 100 milliseconds), or call it when <see cref="Check"/> says so.</remarks>
        public void Update(uint current)
        {
            ThrowIfDisposed();
            _currentTime = current;
            if (!_updated)
            {
                _updated        = true;
                _flushTimestamp = _currentTime;
            }
            var elapsed = TimeDifference(_currentTime, _flushTimestamp);
            if (elapsed >= Constant.TimestampResetThreshold || elapsed < -Constant.TimestampResetThreshold)
            {
                _flushTimestamp = _currentTime;
                elapsed         = 0;
            }
            if (elapsed >= 0)
            {
                _flushTimestamp += (uint)_interval;
                if (TimeDifference(_currentTime, _flushTimestamp) >= 0)
                {
                    _flushTimestamp = _currentTime + (uint)_interval;
                }
                Flush();
            }
        }

        /// <summary>
        /// Determines when <see cref="Update"/> must be called next.
        /// </summary>
        /// <param name="current">The current time in milliseconds.</param>
        /// <returns>The timestamp at which <see cref="Update"/> should be called, assuming that neither <see cref="Send"/> nor <see cref="Input"/> is called in between.</returns>
        /// <exception cref="ObjectDisposedException">The current <see cref="Kcp"/> is disposed.</exception>
        public uint Check(uint current)
        {
            ThrowIfDisposed();
            var flushTimestamp = _flushTimestamp;
            if (!_updated)
            {
                return current;
            }
            if (TimeDifference(current, flushTimestamp) >= Constant.TimestampResetThreshold ||
                TimeDifference(current, flushTimestamp) < -Constant.TimestampResetThreshold)
            {
                flushTimestamp = current;
            }
            if (TimeDifference(current, flushTimestamp) >= 0)
            {
                return current;
            }
            var timeToFlush  = TimeDifference(flushTimestamp, current);
            var timeToPacket = int.MaxValue;
            for (var i = 0; i < _sendBuffer.Count; i++)
            {
                var difference = TimeDifference(_sendBuffer[i].ResendTimestamp, current);
                if (difference <= 0)
                {
                    return current;
                }
                if (difference < timeToPacket)
                {
                    timeToPacket = difference;
                }
            }
            var minimal = (uint)Math.Min(timeToPacket, timeToFlush);
            if (minimal >= (uint)_interval)
            {
                minimal = (uint)_interval;
            }
            return current + minimal;
        }

        /// <summary>
        /// Encodes and sends all pending acknowledgments, window probes and data segments.
        /// </summary>
        /// <exception cref="ObjectDisposedException">The current <see cref="Kcp"/> is disposed.</exception>
        /// <remarks>This method does nothing until <see cref="Update"/> has been called at least once. It is called by <see cref="Update"/> automatically.</remarks>
        public void Flush()
        {
            ThrowIfDisposed();
            if (!_updated)
            {
                return;
            }

            var current      = _currentTime;
            var buffer       = _outputBuffer;
            var offset       = 0;
            var unusedWindow = UnusedReceiveWindow();
            var pacingBudget = -1;

            _congestionControl?.OnTick(this);
            if (_congestionControl != null)
            {
                pacingBudget = _congestionControl.GetPacingBudget(this);
            }

            var priorCongestionWindow = _congestionWindowSize;

            // Flush acknowledgments.
            for (var i = 0; i < _acknowledgmentCount; i++)
            {
                if (offset + Constant.HeaderSize > _maximumTransmissionUnit)
                {
                    OutputDatagram(buffer, offset);
                    offset = 0;
                }
                offset = WriteHeader(
                    buffer,
                    offset,
                    KcpCommand.Acknowledge,
                    0,
                    (ushort)unusedWindow,
                    _acknowledgmentList[i * 2 + 1],
                    _acknowledgmentList[i * 2],
                    _receiveNextSequenceNumber,
                    0
                );
            }
            _acknowledgmentCount = 0;

            // Probe the remote window when it is closed.
            if (_remoteWindowSize == 0)
            {
                if (_probeWait == 0)
                {
                    _probeWait      = Constant.InitialWindowProbeInterval;
                    _probeTimestamp = _currentTime + _probeWait;
                }
                else if (TimeDifference(_currentTime, _probeTimestamp) >= 0)
                {
                    if (_probeWait < Constant.InitialWindowProbeInterval)
                    {
                        _probeWait = Constant.InitialWindowProbeInterval;
                    }
                    _probeWait += _probeWait / 2;
                    if (_probeWait > Constant.MaximumWindowProbeInterval)
                    {
                        _probeWait = Constant.MaximumWindowProbeInterval;
                    }
                    _probeTimestamp =  _currentTime + _probeWait;
                    _probeFlags     |= AskSend;
                }
            }
            else
            {
                _probeTimestamp = 0;
                _probeWait      = 0;
            }

            if ((_probeFlags & AskSend) != 0)
            {
                if (offset + Constant.HeaderSize > _maximumTransmissionUnit)
                {
                    OutputDatagram(buffer, offset);
                    offset = 0;
                }
                offset = WriteHeader(
                    buffer,
                    offset,
                    KcpCommand.WindowProbeAsk,
                    0,
                    (ushort)unusedWindow,
                    0,
                    0,
                    _receiveNextSequenceNumber,
                    0
                );
                WriteLog(KcpLogMask.OutputProbe, "output probe");
            }

            if ((_probeFlags & AskTell) != 0)
            {
                if (offset + Constant.HeaderSize > _maximumTransmissionUnit)
                {
                    OutputDatagram(buffer, offset);
                    offset = 0;
                }
                offset = WriteHeader(
                    buffer,
                    offset,
                    KcpCommand.WindowProbeTell,
                    0,
                    (ushort)unusedWindow,
                    0,
                    0,
                    _receiveNextSequenceNumber,
                    0
                );
                if (IsLogging(KcpLogMask.OutputWindowSize))
                {
                    WriteLog(KcpLogMask.OutputWindowSize, $"output window size: {unusedWindow}");
                }
            }

            _probeFlags = 0;

            var congestionWindow = Math.Min(_sendWindowSize, _remoteWindowSize);
            if (_congestionControl != null || !_noCongestionControl)
            {
                congestionWindow = Math.Min(_congestionWindowSize, congestionWindow);
            }

            // Move as many queued segments into the send buffer as the window allows.
            while (TimeDifference(_sendNextSequenceNumber, _sendUnacknowledgedSequenceNumber + congestionWindow) < 0)
            {
                if (_sendQueue.Count == 0)
                {
                    break;
                }
                var segment = _sendQueue.DequeueFirst();
                _sendBuffer.EnqueueLast(segment);
                segment.Timestamp               = current;
                segment.SequenceNumber          = _sendNextSequenceNumber++;
                segment.ResendTimestamp         = current;
                segment.RetransmissionTimeout   = (uint)_roundTripTimeout;
                segment.FastAcknowledgmentCount = 0;
                segment.TransmissionCount       = 0;
            }

            if (_congestionControl != null && _sendQueue.Count == 0)
            {
                var effectiveWindow = Math.Min(_sendWindowSize, _remoteWindowSize);
                effectiveWindow = Math.Min(_congestionWindowSize, effectiveWindow);
                if (_sendBuffer.Count < effectiveWindow)
                {
                    _congestionControl.OnApplicationLimited(this, _sendBuffer.Count);
                }
            }

            var fastResendThreshold = _fastResendThreshold > 0 ? (uint)_fastResendThreshold : uint.MaxValue;
            var resendJitter = _noDelayLevel == KcpNoDelayLevel.Disabled ? (uint)(_roundTripTimeout >> 3) : 0U;
            var fastRetransmissionCount = 0;
            var hasTimedOut = false;

            for (var i = 0; i < _sendBuffer.Count; i++)
            {
                var segment   = _sendBuffer[i];
                var needsSend = false;
                if (segment.TransmissionCount == 0)
                {
                    needsSend = true;
                    segment.TransmissionCount++;
                    segment.RetransmissionTimeout = (uint)_roundTripTimeout;
                    segment.ResendTimestamp       = current + segment.RetransmissionTimeout + resendJitter;
                }
                else if (TimeDifference(current, segment.ResendTimestamp) >= 0)
                {
                    needsSend = true;
                    segment.TransmissionCount++;
                    _retransmissionCount++;
                    if (_noDelayLevel == KcpNoDelayLevel.Disabled)
                    {
                        segment.RetransmissionTimeout += Math.Max(
                            segment.RetransmissionTimeout,
                            (uint)_roundTripTimeout
                        );
                    }
                    else
                    {
                        var step = _noDelayLevel == KcpNoDelayLevel.Enabled
                                       ? segment.RetransmissionTimeout
                                       : (uint)_roundTripTimeout;
                        segment.RetransmissionTimeout += step / 2;
                    }
                    segment.ResendTimestamp = current + segment.RetransmissionTimeout;
                    hasTimedOut             = true;
                }
                else if (segment.FastAcknowledgmentCount >= fastResendThreshold)
                {
                    if ((int)segment.TransmissionCount <= _fastResendLimit || _fastResendLimit <= 0)
                    {
                        needsSend = true;
                        segment.TransmissionCount++;
                        segment.FastAcknowledgmentCount = 0;
                        segment.ResendTimestamp         = current + segment.RetransmissionTimeout;
                        fastRetransmissionCount++;
                    }
                }

                if (!needsSend)
                {
                    continue;
                }

                segment.Timestamp = current;

                if (pacingBudget >= 0 && pacingBudget < segment.Length)
                {
                    break;
                }

                _congestionControl?.OnSegmentSent(
                    this,
                    segment.SequenceNumber,
                    current,
                    segment.Length,
                    _sendBuffer.Count,
                    segment.TransmissionCount
                );

                var required = Constant.HeaderSize + segment.Length;
                if (offset + required > _maximumTransmissionUnit)
                {
                    OutputDatagram(buffer, offset);
                    offset = 0;
                }
                offset = WriteHeader(
                    buffer,
                    offset,
                    KcpCommand.Push,
                    segment.FragmentIndex,
                    (ushort)unusedWindow,
                    current,
                    segment.SequenceNumber,
                    _receiveNextSequenceNumber,
                    segment.Length
                );
                if (segment.Length > 0)
                {
                    Array.Copy(segment.Data, 0, buffer, offset, segment.Length);
                    offset += segment.Length;
                }
                if (IsLogging(KcpLogMask.OutputData))
                {
                    WriteLog(
                        KcpLogMask.OutputData,
                        $"output push: sequence number = {segment.SequenceNumber}, transmission count = {segment.TransmissionCount}, length = {segment.Length}"
                    );
                }

                if (pacingBudget >= 0)
                {
                    pacingBudget -= segment.Length;
                }

                if (segment.TransmissionCount >= _deadLinkTransmissionCount)
                {
                    _state = uint.MaxValue;
                }
            }

            if (offset > 0)
            {
                OutputDatagram(buffer, offset);
            }

            if (fastRetransmissionCount > 0)
            {
                if (_congestionControl != null)
                {
                    _congestionControl.OnFastRetransmit(
                        this,
                        fastRetransmissionCount,
                        _sendBuffer.Count,
                        (int)priorCongestionWindow
                    );
                }
                else
                {
                    var inFlight = _sendNextSequenceNumber - _sendUnacknowledgedSequenceNumber;
                    _slowStartThreshold = inFlight / 2;
                    if (_slowStartThreshold < Constant.MinimumSlowStartThreshold)
                    {
                        _slowStartThreshold = Constant.MinimumSlowStartThreshold;
                    }
                    _congestionWindowSize      = _slowStartThreshold + fastResendThreshold;
                    _congestionWindowIncrement = _congestionWindowSize * _maximumSegmentSize;
                }
            }

            if (hasTimedOut)
            {
                if (_congestionControl != null)
                {
                    _congestionControl.OnTimeout(this, (int)priorCongestionWindow);
                }
                else
                {
                    _slowStartThreshold = priorCongestionWindow / 2;
                    if (_slowStartThreshold < Constant.MinimumSlowStartThreshold)
                    {
                        _slowStartThreshold = Constant.MinimumSlowStartThreshold;
                    }
                    _congestionWindowSize      = 1;
                    _congestionWindowIncrement = _maximumSegmentSize;
                }
            }

            if (_congestionControl == null)
            {
                EnsureCongestionWindowIsOpen();
            }
        }

        /// <summary>
        /// Reads the conversation identifier from the beginning of a datagram.
        /// </summary>
        /// <param name="buffer">The buffer that contains a datagram (or at least the beginning of one).</param>
        /// <param name="offset">The zero-based offset of the datagram in <paramref name="buffer"/>.</param>
        /// <returns>The conversation identifier.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is negative, or the four bytes at <paramref name="offset"/> are not within <paramref name="buffer"/>.</exception>
        public static uint GetConversationId(byte[] buffer, int offset)
        {
            ValidateRange(buffer, offset, sizeof(uint));
            return ReadUInt32(buffer, ref offset);
        }

        /// <summary>
        /// Returns all segments that this connection still holds to the internal pool, and releases the congestion control algorithm.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            _congestionControl?.Release(this);
            _congestionControl = null;
            ReturnSegments(_sendQueue);
            ReturnSegments(_sendBuffer);
            ReturnSegments(_receiveBuffer);
            ReturnSegments(_receiveQueue);
            _acknowledgmentCount = 0;
            _acknowledgmentList  = Array.Empty<uint>();
            _outputBuffer        = Array.Empty<byte>();
            LogWriter            = null;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(ConversationId)} = {_conversationId}, {nameof(WaitSendCount)} = {WaitSendCount}";
        }

        private void UpdateRoundTripTime(int roundTripTime)
        {
            if (_smoothedRoundTripTime == 0)
            {
                _smoothedRoundTripTime = roundTripTime;
                _roundTripTimeVariance = roundTripTime / 2;
            }
            else
            {
                var delta = roundTripTime - _smoothedRoundTripTime;
                if (delta < 0)
                {
                    delta = -delta;
                }
                _roundTripTimeVariance = (3 * _roundTripTimeVariance + delta) / 4;
                _smoothedRoundTripTime = (7 * _smoothedRoundTripTime + roundTripTime) / 8;
                if (_smoothedRoundTripTime < 1)
                {
                    _smoothedRoundTripTime = 1;
                }
            }
            var timeout = _smoothedRoundTripTime + Math.Max(_interval, 4 * _roundTripTimeVariance);
            _roundTripTimeout = (int)Clamp(
                (uint)timeout,
                (uint)_minimumRoundTripTimeout,
                Constant.MaximumRoundTripTimeout
            );
            _congestionControl?.OnRoundTripTime(this, roundTripTime);
        }

        /// <summary>
        /// The built-in congestion control: slow start, then congestion avoidance with the increment based on the maximum segment size.
        /// </summary>
        private void GrowCongestionWindow()
        {
            if (_congestionWindowSize >= _remoteWindowSize)
            {
                return;
            }
            if (_congestionWindowSize < _slowStartThreshold)
            {
                _congestionWindowSize++;
                _congestionWindowIncrement += _maximumSegmentSize;
            }
            else
            {
                if (_congestionWindowIncrement < _maximumSegmentSize)
                {
                    _congestionWindowIncrement = _maximumSegmentSize;
                }
                _congestionWindowIncrement += _maximumSegmentSize * _maximumSegmentSize / _congestionWindowIncrement +
                                              _maximumSegmentSize / 16;
                if ((_congestionWindowSize + 1) * _maximumSegmentSize <= _congestionWindowIncrement)
                {
                    _congestionWindowSize =
                        (_congestionWindowIncrement + _maximumSegmentSize - 1) / _maximumSegmentSize;
                }
            }
            if (_congestionWindowSize > _remoteWindowSize)
            {
                _congestionWindowSize      = _remoteWindowSize;
                _congestionWindowIncrement = _remoteWindowSize * _maximumSegmentSize;
            }
        }

        private void EnsureCongestionWindowIsOpen()
        {
            if (_congestionWindowSize < 1)
            {
                _congestionWindowSize = 1;
            }
            if (_congestionWindowIncrement < _maximumSegmentSize)
            {
                _congestionWindowIncrement = _maximumSegmentSize;
            }
        }

        private void ShrinkSendBuffer()
        {
            _sendUnacknowledgedSequenceNumber = _sendBuffer.Count > 0
                                                    ? _sendBuffer.PeekFirst().SequenceNumber
                                                    : _sendNextSequenceNumber;
        }

        private void ParseUnacknowledged(uint unacknowledged)
        {
            while (_sendBuffer.Count > 0)
            {
                var segment = _sendBuffer.PeekFirst();
                if (TimeDifference(unacknowledged, segment.SequenceNumber) <= 0)
                {
                    break;
                }
                _acknowledgedLength += segment.Length;
                _congestionControl?.OnSegmentAcknowledged(
                    this,
                    segment.SequenceNumber,
                    segment.Timestamp,
                    segment.Length,
                    -1,
                    segment.TransmissionCount
                );
                _sendBuffer.DequeueFirst();
                ReturnSegment(segment);
            }
        }

        private void ParseAcknowledgment(uint sequenceNumber)
        {
            if (TimeDifference(sequenceNumber, _sendUnacknowledgedSequenceNumber) < 0 ||
                TimeDifference(sequenceNumber, _sendNextSequenceNumber) >= 0)
            {
                return;
            }
            for (var i = 0; i < _sendBuffer.Count; i++)
            {
                var segment = _sendBuffer[i];
                if (segment.SequenceNumber == sequenceNumber)
                {
                    _acknowledgedLength += segment.Length;
                    if (_congestionControl != null)
                    {
                        _congestionControl.OnSegmentAcknowledged(
                            this,
                            segment.SequenceNumber,
                            segment.Timestamp,
                            segment.Length,
                            TimeDifference(_currentTime, segment.Timestamp),
                            segment.TransmissionCount
                        );
                    }
                    _sendBuffer.RemoveAt(i);
                    ReturnSegment(segment);
                    break;
                }
                if (TimeDifference(sequenceNumber, segment.SequenceNumber) < 0)
                {
                    break;
                }
            }
        }

        private void ParseFastAcknowledgment(uint sequenceNumber, uint timestamp)
        {
            if (TimeDifference(sequenceNumber, _sendUnacknowledgedSequenceNumber) < 0 ||
                TimeDifference(sequenceNumber, _sendNextSequenceNumber) >= 0)
            {
                return;
            }
            for (var i = 0; i < _sendBuffer.Count; i++)
            {
                var segment = _sendBuffer[i];
                if (TimeDifference(sequenceNumber, segment.SequenceNumber) < 0)
                {
                    break;
                }
                // The conservative variant counts a skip only when the acknowledgment is newer than the segment,
                // which prevents spurious fast retransmissions caused by reordered datagrams.
                if (sequenceNumber != segment.SequenceNumber && TimeDifference(timestamp, segment.Timestamp) >= 0)
                {
                    segment.FastAcknowledgmentCount++;
                }
            }
        }

        private void ParseData(KcpSegment segment)
        {
            var sequenceNumber = segment.SequenceNumber;
            if (TimeDifference(sequenceNumber, _receiveNextSequenceNumber + _receiveWindowSize) >= 0 ||
                TimeDifference(sequenceNumber, _receiveNextSequenceNumber) < 0)
            {
                ReturnSegment(segment);
                return;
            }

            // Insert the segment so that the buffer stays ordered by sequence number, and drop duplicates.
            var insertIndex = 0;
            var isRepeated  = false;
            for (var i = _receiveBuffer.Count - 1; i >= 0; i--)
            {
                var existing = _receiveBuffer[i];
                if (existing.SequenceNumber == sequenceNumber)
                {
                    isRepeated = true;
                    break;
                }
                if (TimeDifference(sequenceNumber, existing.SequenceNumber) > 0)
                {
                    insertIndex = i + 1;
                    break;
                }
            }

            if (isRepeated)
            {
                ReturnSegment(segment);
            }
            else
            {
                _receiveBuffer.Insert(insertIndex, segment);
            }

            MoveAvailableSegmentsToReceiveQueue();
        }

        private void MoveAvailableSegmentsToReceiveQueue()
        {
            while (_receiveBuffer.Count > 0)
            {
                var segment = _receiveBuffer.PeekFirst();
                if (segment.SequenceNumber != _receiveNextSequenceNumber || _receiveQueue.Count >= _receiveWindowSize)
                {
                    break;
                }
                _receiveBuffer.DequeueFirst();
                _receiveQueue.EnqueueLast(segment);
                _receiveNextSequenceNumber++;
            }
        }

        private void PushAcknowledgment(uint sequenceNumber, uint timestamp)
        {
            var required = (_acknowledgmentCount + 1) * 2;
            if (required > _acknowledgmentList.Length)
            {
                var capacity = Math.Max(8, _acknowledgmentList.Length);
                while (capacity < required)
                {
                    capacity <<= 1;
                }
                var list = new uint[capacity];
                Array.Copy(_acknowledgmentList, list, _acknowledgmentCount * 2);
                _acknowledgmentList = list;
            }
            _acknowledgmentList[_acknowledgmentCount * 2 + 0] = sequenceNumber;
            _acknowledgmentList[_acknowledgmentCount * 2 + 1] = timestamp;
            _acknowledgmentCount++;
        }

        private int UnusedReceiveWindow()
        {
            return _receiveQueue.Count < (int)_receiveWindowSize
                       ? (int)(_receiveWindowSize - (uint)_receiveQueue.Count)
                       : 0;
        }

        private void OutputDatagram(byte[] buffer, int count)
        {
            if (count == 0)
            {
                return;
            }
            if (IsLogging(KcpLogMask.Output))
            {
                WriteLog(KcpLogMask.Output, $"output: {count} bytes");
            }
            _output(buffer, 0, count);
        }

        private static KcpSegment RentSegment()
        {
            return SegmentPool.Get();
        }

        private static void ReturnSegment(KcpSegment segment)
        {
            SegmentPool.Return(segment);
        }

        private static void ReturnSegments(Deque<KcpSegment> segments)
        {
            while (segments.Count > 0)
            {
                SegmentPool.Return(segments.DequeueFirst());
            }
        }

        private bool IsLogging(KcpLogMask mask)
        {
            return LogWriter != null && (LogMask & mask) != 0;
        }

        private void WriteLog(KcpLogMask mask, string message)
        {
            if (!IsLogging(mask))
            {
                return;
            }
            LogWriter(message, this, mask);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        private static int TimeDifference(uint later, uint earlier)
        {
            return (int)(later - earlier);
        }

        private static int Clamp(int value, int minimum, int maximum)
        {
            if (value < minimum)
            {
                return minimum;
            }
            return value > maximum ? maximum : value;
        }

        private static uint Clamp(uint value, uint minimum, uint maximum)
        {
            if (value < minimum)
            {
                return minimum;
            }
            return value > maximum ? maximum : value;
        }

        private static void ValidateRange(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset), offset, null);
            }
            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count), count, null);
            }
            if (offset > buffer.Length - count)
            {
                throw new ArgumentOutOfRangeException(nameof(count), count, null);
            }
        }

        private int WriteHeader(
            byte[]     buffer,
            int        offset,
            KcpCommand command,
            byte       fragmentIndex,
            ushort     window,
            uint       timestamp,
            uint       sequenceNumber,
            uint       unacknowledged,
            int        length)
        {
            WriteUInt32(buffer, ref offset, _conversationId);
            WriteUInt8(buffer, ref offset, (byte)command);
            WriteUInt8(buffer, ref offset, fragmentIndex);
            WriteUInt16(buffer, ref offset, window);
            WriteUInt32(buffer, ref offset, timestamp);
            WriteUInt32(buffer, ref offset, sequenceNumber);
            WriteUInt32(buffer, ref offset, unacknowledged);
            WriteUInt32(buffer, ref offset, (uint)length);
            return offset;
        }

        private static void WriteUInt8(byte[] buffer, ref int offset, byte value)
        {
            buffer[offset++] = value;
        }

        private static byte ReadUInt8(byte[] buffer, ref int offset)
        {
            return buffer[offset++];
        }

        private static void WriteUInt16(byte[] buffer, ref int offset, ushort value)
        {
            buffer[offset++] = (byte)value;
            buffer[offset++] = (byte)(value >> 8);
        }

        private static ushort ReadUInt16(byte[] buffer, ref int offset)
        {
            var value = (ushort)(buffer[offset] | (buffer[offset + 1] << 8));
            offset += 2;
            return value;
        }

        private static void WriteUInt32(byte[] buffer, ref int offset, uint value)
        {
            buffer[offset++] = (byte)value;
            buffer[offset++] = (byte)(value >> 8);
            buffer[offset++] = (byte)(value >> 16);
            buffer[offset++] = (byte)(value >> 24);
        }

        private static uint ReadUInt32(byte[] buffer, ref int offset)
        {
            var value = (uint)(buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) |
                               (buffer[offset + 3] << 24));
            offset += 4;
            return value;
        }
    }
}
