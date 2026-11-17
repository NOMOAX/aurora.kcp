namespace Aurora.Kcp
{
    /// <summary>
    /// A pluggable congestion control algorithm of a KCP connection.
    /// </summary>
    /// <remarks>
    /// The built-in (NewReno-like) algorithm is used while <see cref="Kcp.CongestionControl"/> is <see langword="null"/>.
    /// Installing an implementation disables the built-in algorithm, and the implementation becomes responsible for maintaining
    /// <see cref="Kcp.CongestionWindowSize"/>, <see cref="Kcp.SlowStartThreshold"/> and <see cref="Kcp.CongestionWindowIncrement"/>.
    /// <br/>
    /// The methods are called from within <see cref="Kcp.Input"/>, <see cref="Kcp.Flush"/> and <see cref="Kcp.Update"/>, so they must not allocate or block.
    /// </remarks>
    public interface IKcpCongestionControl
    {
        /// <summary>
        /// Initializes the state of the algorithm for the specified connection.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <returns><see langword="true"/> if the algorithm has been initialized; otherwise, <see langword="false"/>, in which case the installation fails and the built-in algorithm is restored.</returns>
        bool Initialize(Kcp kcp);

        /// <summary>
        /// Releases the state of the algorithm for the specified connection.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        void Release(Kcp kcp);

        /// <summary>
        /// Notifies the algorithm that segments have been acknowledged, that is, that the in-flight window has shrunk.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <param name="acknowledgedSegmentCount">The number of segments that the <see cref="Kcp.SendUnacknowledgedSequenceNumber"/> has advanced by.</param>
        /// <param name="acknowledgedByteCount">The number of payload bytes that have been acknowledged.</param>
        /// <param name="priorInFlightSegmentCount">The number of segments that were in flight before the acknowledgment was processed.</param>
        void OnAcknowledged(
            Kcp  kcp,
            uint acknowledgedSegmentCount,
            int  acknowledgedByteCount,
            int  priorInFlightSegmentCount);

        /// <summary>
        /// Notifies the algorithm that segments have been fast-retransmitted.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <param name="fastRetransmittedSegmentCount">The number of segments that have been fast-retransmitted.</param>
        /// <param name="inFlightSegmentCount">The number of segments that are currently in flight.</param>
        /// <param name="priorCongestionWindowSize">The congestion window size before the flush.</param>
        void OnFastRetransmit(
            Kcp kcp,
            int fastRetransmittedSegmentCount,
            int inFlightSegmentCount,
            int priorCongestionWindowSize);

        /// <summary>
        /// Notifies the algorithm that a retransmission timeout has occurred.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <param name="priorCongestionWindowSize">The congestion window size before the flush.</param>
        void OnTimeout(Kcp kcp, int priorCongestionWindowSize);

        /// <summary>
        /// Notifies the algorithm at the beginning of every flush.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        void OnTick(Kcp kcp);

        /// <summary>
        /// Notifies the algorithm that the application has run out of data while the in-flight window was not full.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <param name="inFlightSegmentCount">The number of segments that are currently in flight.</param>
        void OnApplicationLimited(Kcp kcp, int inFlightSegmentCount);

        /// <summary>
        /// Notifies the algorithm of a new round-trip time sample.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <param name="roundTripTime">The round-trip time in milliseconds.</param>
        void OnRoundTripTime(Kcp kcp, int roundTripTime);

        /// <summary>
        /// Notifies the algorithm that a segment is transmitted.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <param name="sequenceNumber">The sequence number of the segment.</param>
        /// <param name="timestamp">The timestamp in milliseconds at which the segment is transmitted.</param>
        /// <param name="length">The payload length of the segment in bytes.</param>
        /// <param name="inFlightSegmentCount">The number of segments that are currently in flight.</param>
        /// <param name="transmissionCount">The number of times the segment has been transmitted, including this transmission.</param>
        void OnSegmentSent(
            Kcp  kcp,
            uint sequenceNumber,
            uint timestamp,
            int  length,
            int  inFlightSegmentCount,
            uint transmissionCount);

        /// <summary>
        /// Notifies the algorithm that a segment is acknowledged.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <param name="sequenceNumber">The sequence number of the segment.</param>
        /// <param name="timestamp">The timestamp in milliseconds at which the segment was last transmitted.</param>
        /// <param name="length">The payload length of the segment in bytes.</param>
        /// <param name="roundTripTime">The round-trip time in milliseconds, or -1 if the timestamp is later than the current time.</param>
        /// <param name="transmissionCount">The number of times the segment has been transmitted.</param>
        void OnSegmentAcknowledged(
            Kcp  kcp,
            uint sequenceNumber,
            uint timestamp,
            int  length,
            int  roundTripTime,
            uint transmissionCount);

        /// <summary>
        /// Writes implementation-specific information into the specified buffer.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <param name="buffer">The buffer to write into.</param>
        /// <param name="offset">The zero-based offset in <paramref name="buffer"/> at which to write.</param>
        /// <param name="count">The available space in bytes.</param>
        /// <returns>The number of bytes written.</returns>
        int GetInfo(Kcp kcp, byte[] buffer, int offset, int count);

        /// <summary>
        /// Gets the number of payload bytes that this flush may transmit.
        /// </summary>
        /// <param name="kcp">The connection.</param>
        /// <returns>The pacing budget in bytes for the current flush, or -1 if the algorithm does not pace the transmission.</returns>
        int GetPacingBudget(Kcp kcp);
    }
}
