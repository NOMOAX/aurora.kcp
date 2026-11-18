using System;

namespace Aurora.Kcp
{
    /// <summary>
    /// A segment, which is the unit of transmission and retransmission of KCP.
    /// </summary>
    /// <remarks>The same type is used for the send queue, the send buffer, the receiving buffer and the receiving queue.</remarks>
    internal sealed class KcpSegment
    {
        /// <summary>
        /// The sequence number of the segment.
        /// </summary>
        internal uint SequenceNumber;

        /// <summary>
        /// The timestamp in milliseconds at which the segment was last sent.
        /// </summary>
        internal uint Timestamp;

        /// <summary>
        /// The timestamp in milliseconds at which the segment is retransmitted if it is not acknowledged before.
        /// </summary>
        internal uint ResendTimestamp;

        /// <summary>
        /// The retransmission timeout in milliseconds of the segment.
        /// </summary>
        internal uint RetransmissionTimeout;

        /// <summary>
        /// The number of times the segment has been skipped by acknowledgments of later segments.
        /// </summary>
        internal uint FastAcknowledgmentCount;

        /// <summary>
        /// The number of times the segment has been transmitted.
        /// </summary>
        internal uint TransmissionCount;

        /// <summary>
        /// The index of the fragment in its message, counted backwards from the last fragment (0 is the last fragment).
        /// </summary>
        /// <remarks>The receiver uses the index to delimit messages, and the protocol stores it in eight bits, so a message never has more fragments than <see cref="Constant.MaximumFragmentCount"/>; the receive window imposes a lower limit, which <see cref="Kcp.Send"/> enforces.</remarks>
        internal byte FragmentIndex;

        /// <summary>
        /// The length of the payload in bytes.
        /// </summary>
        internal int Length;

        /// <summary>
        /// The buffer that stores the payload (only the first <see cref="Length"/> bytes are meaningful).
        /// </summary>
        internal byte[] Data = Array.Empty<byte>();

        /// <summary>
        /// Ensures that <see cref="Data"/> can store the specified number of bytes.
        /// </summary>
        /// <param name="length">The required length of the payload.</param>
        internal void EnsureDataCapacity(int length)
        {
            if (Data.Length < length)
            {
                Data = new byte[length];
            }
        }

        /// <summary>
        /// Resets the segment so that it can be reused for another message.
        /// </summary>
        internal void Reset()
        {
            SequenceNumber          = 0;
            Timestamp               = 0;
            ResendTimestamp         = 0;
            RetransmissionTimeout   = 0;
            FastAcknowledgmentCount = 0;
            TransmissionCount       = 0;
            FragmentIndex           = 0;
            Length                  = 0;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(SequenceNumber)} = {SequenceNumber}, {nameof(Length)} = {Length}";
        }
    }
}
