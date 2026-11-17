using System;

namespace Aurora.Kcp
{
    /// <summary>
    /// The kinds of protocol events that <see cref="Kcp.LogWriter"/> can receive.
    /// </summary>
    [Flags]
    public enum KcpLogMask
    {
        /// <summary>
        /// No event is logged.
        /// </summary>
        None = 0,

        /// <summary>
        /// A datagram has been handed to the <see cref="KcpOutputHandler"/> of <see cref="Kcp"/>.
        /// </summary>
        Output = 1 << 0,

        /// <summary>
        /// A datagram has been handed to <see cref="Kcp.Input"/>.
        /// </summary>
        Input = 1 << 1,

        /// <summary>
        /// Data has been queued for sending.
        /// </summary>
        Send = 1 << 2,

        /// <summary>
        /// A message has been received from the receiving queue.
        /// </summary>
        Receive = 1 << 3,

        /// <summary>
        /// An incoming data segment has been parsed.
        /// </summary>
        InputData = 1 << 4,

        /// <summary>
        /// An incoming acknowledgment segment has been parsed.
        /// </summary>
        InputAcknowledgment = 1 << 5,

        /// <summary>
        /// An incoming window probe segment has been parsed.
        /// </summary>
        InputProbe = 1 << 6,

        /// <summary>
        /// An incoming window size segment has been parsed.
        /// </summary>
        InputWindowSize = 1 << 7,

        /// <summary>
        /// A data segment has been sent.
        /// </summary>
        OutputData = 1 << 8,

        /// <summary>
        /// An acknowledgment segment has been sent.
        /// </summary>
        OutputAcknowledgment = 1 << 9,

        /// <summary>
        /// A window probe segment has been sent.
        /// </summary>
        OutputProbe = 1 << 10,

        /// <summary>
        /// A window size segment has been sent.
        /// </summary>
        OutputWindowSize = 1 << 11
    }
}
