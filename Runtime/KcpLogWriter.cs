namespace Aurora.Kcp
{
    /// <summary>
    /// Writes a protocol log message of a KCP object.
    /// </summary>
    /// <param name="message">The log message.</param>
    /// <param name="kcp">The KCP object that produced the message.</param>
    /// <param name="mask">The kind of the event that produced the message.</param>
    public delegate void KcpLogWriter(string message, Kcp kcp, KcpLogMask mask);
}
