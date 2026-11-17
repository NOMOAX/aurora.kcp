namespace Aurora.Kcp
{
    /// <summary>
    /// The way KCP increases the retransmission timeout of a segment after each retransmission.
    /// </summary>
    /// <remarks>
    /// The larger the level is, the fewer milliseconds are added to the retransmission timeout, and the sooner a lost segment is retransmitted.
    /// </remarks>
    public enum KcpNoDelayLevel : byte
    {
        /// <summary>
        /// The retransmission timeout grows by the larger of itself and the smoothed round-trip timeout, which effectively doubles it.
        /// </summary>
        Disabled = 0,

        /// <summary>
        /// The retransmission timeout grows by half of itself.
        /// </summary>
        Enabled = 1,

        /// <summary>
        /// The retransmission timeout grows by half of the smoothed round-trip timeout.
        /// </summary>
        Aggressive = 2
    }
}
