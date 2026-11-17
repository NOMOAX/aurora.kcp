namespace Aurora.Kcp
{
    /// <summary>
    /// The result of <see cref="Kcp.Input"/>.
    /// </summary>
    public enum KcpInputResult
    {
        /// <summary>
        /// All segments of the datagram have been processed.
        /// </summary>
        Ok = 0,

        /// <summary>
        /// The datagram is shorter than a segment header, or contains a segment whose conversation identifier does not match <see cref="Kcp.ConversationId"/>.
        /// </summary>
        InvalidDatagram = -1,

        /// <summary>
        /// The payload length of a segment exceeds the remaining length of the datagram.
        /// </summary>
        MalformedSegment = -2,

        /// <summary>
        /// A segment carries an unknown command.
        /// </summary>
        UnknownCommand = -3
    }
}
