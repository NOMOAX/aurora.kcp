namespace Aurora.Kcp
{
    /// <summary>
    /// Sends a datagram to the remote endpoint of a KCP connection.
    /// </summary>
    /// <param name="buffer">The buffer that contains the datagram.</param>
    /// <param name="offset">The zero-based offset of the datagram in <paramref name="buffer"/>.</param>
    /// <param name="count">The length of the datagram in bytes.</param>
    /// <remarks>
    /// KCP calls this handler only from within <see cref="Kcp.Update"/>, <see cref="Kcp.Flush"/> and <see cref="Kcp.Input"/>.
    /// <br/>
    /// The handler must not access the buffer beyond the specified range, and must not modify the buffer.
    /// </remarks>
    public delegate void KcpOutputHandler(byte[] buffer, int offset, int count);
}
