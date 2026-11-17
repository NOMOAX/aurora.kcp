namespace Aurora.Kcp
{
    /// <summary>
    /// The command of a segment.
    /// </summary>
    public enum KcpCommand : byte
    {
        /// <summary>
        /// Pushes data.
        /// </summary>
        Push = 81,

        /// <summary>
        /// Acknowledges a received segment.
        /// </summary>
        Acknowledge = 82,

        /// <summary>
        /// Asks the remote endpoint for its window size.
        /// </summary>
        WindowProbeAsk = 83,

        /// <summary>
        /// Tells the remote endpoint the local window size.
        /// </summary>
        WindowProbeTell = 84
    }
}
