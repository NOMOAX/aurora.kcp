namespace Aurora.Kcp
{
    /// <summary>
    /// Protocol constants of KCP.
    /// </summary>
    public static class Constant
    {
        /// <summary>
        /// The size of a segment header in bytes.
        /// </summary>
        public const int HeaderSize = 24;

        /// <summary>
        /// The default maximum transmission unit in bytes.
        /// </summary>
        public const int DefaultMaximumTransmissionUnit = 1400;

        /// <summary>
        /// The smallest maximum transmission unit that <see cref="Kcp.MaximumTransmissionUnit"/> accepts.
        /// </summary>
        public const int MinimumTransmissionUnit = 50;

        /// <summary>
        /// The default send window size in segments.
        /// </summary>
        public const int DefaultSendWindowSize = 32;

        /// <summary>
        /// The default receive window size in segments, which is also the smallest receiving window size that <see cref="Kcp.SetWindowSize"/> accepts.
        /// </summary>
        /// <remarks>It must be greater than the maximum length of a message in fragments, otherwise a message could never be completed.</remarks>
        public const int DefaultReceiveWindowSize = 128;

        /// <summary>
        /// The maximum fragment count of a single message, which is limited by the width of the fragment count field (8 bits).
        /// </summary>
        public const int MaximumFragmentCount = 255;

        /// <summary>
        /// The default interval in milliseconds at which KCP flushes its state.
        /// </summary>
        public const int DefaultInterval = 100;

        /// <summary>
        /// The smallest interval in milliseconds that <see cref="Kcp.Interval"/> accepts.
        /// </summary>
        public const int MinimumInterval = 10;

        /// <summary>
        /// The largest interval in milliseconds that <see cref="Kcp.Interval"/> accepts.
        /// </summary>
        public const int MaximumInterval = 5000;

        /// <summary>
        /// The default retransmission timeout in milliseconds.
        /// </summary>
        public const int DefaultRoundTripTimeout = 200;

        /// <summary>
        /// The retransmission timeout in milliseconds that <see cref="Kcp.MinimumRoundTripTimeout"/> defaults to when the no-delay mode is disabled.
        /// </summary>
        public const int MinimumRoundTripTimeout = 100;

        /// <summary>
        /// The retransmission timeout in milliseconds that <see cref="Kcp.NoDelayLevel"/> applies when the no-delay mode is enabled.
        /// </summary>
        public const int NoDelayMinimumRoundTripTimeout = 30;

        /// <summary>
        /// The largest retransmission timeout in milliseconds.
        /// </summary>
        public const int MaximumRoundTripTimeout = 60000;

        /// <summary>
        /// The initial slow start threshold.
        /// </summary>
        public const uint InitialSlowStartThreshold = 2;

        /// <summary>
        /// The smallest slow start threshold.
        /// </summary>
        public const uint MinimumSlowStartThreshold = 2;

        /// <summary>
        /// The default number of transmissions after which a segment is considered lost and the link dead.
        /// </summary>
        public const int DefaultDeadLinkRetransmissionCount = 20;

        /// <summary>
        /// The default upper limit of transmissions of a single segment that the fast retransmission mode allows.
        /// </summary>
        public const int DefaultFastResendLimit = 5;

        /// <summary>
        /// The initial interval in milliseconds between window probes that is used when the remote window size is zero.
        /// </summary>
        public const int InitialWindowProbeInterval = 5000;

        /// <summary>
        /// The largest interval in milliseconds between window probes that is used when the remote window size is zero.
        /// </summary>
        public const int MaximumWindowProbeInterval = 120000;

        /// <summary>
        /// The difference in milliseconds beyond which a supplied timestamp and the internal flush timestamp are considered desynchronized, and the internal flush timestamp is reset to the supplied timestamp.
        /// </summary>
        public const int TimestampResetThreshold = 10000;
    }
}
