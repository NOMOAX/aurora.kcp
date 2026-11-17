using System;
using Aurora.Pooling;

namespace Aurora.Kcp
{
    /// <summary>
    /// A strategy for managing <see cref="KcpSegment"/> objects in the pool.
    /// </summary>
    internal sealed class PooledKcpSegmentPolicy : IPooledObjectPolicy<KcpSegment>
    {
        /// <summary>
        /// The largest payload buffer that a pooled segment keeps, so that a connection with a huge maximum transmission unit does not pin that memory once it becomes idle.
        /// </summary>
        private const int RetainedDataCapacity = 4096;

        /// <inheritdoc />
        public KcpSegment Create()
        {
            return new KcpSegment();
        }

        /// <inheritdoc />
        public void Get(KcpSegment obj)
        {
            obj.Reset();
        }

        /// <inheritdoc />
        public bool Return(KcpSegment obj)
        {
            obj.Reset();
            if (obj.Data.Length > RetainedDataCapacity)
            {
                obj.Data = Array.Empty<byte>();
            }
            return true;
        }

        /// <inheritdoc />
        public void Dispose(KcpSegment obj)
        {
            obj.Reset();
            obj.Data = Array.Empty<byte>();
        }
    }
}
