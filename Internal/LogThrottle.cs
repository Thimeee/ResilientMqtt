namespace ResilientMqtt.Internal
{
    /// <summary>
    /// Limits a log line that can repeat once per message: the first in each window is written, the others are counted.
    /// Only the log line is limited, never the behaviour around it. At 500 kiosks one line per message fills disks (#68).
    /// </summary>
    internal sealed class LogThrottle
    {
        private long _lastLoggedTicks;
        private long _skipped;

        /// <summary>true = write the line now; <paramref name="skippedBefore"/> = how many were skipped since the last one.</summary>
        public bool ShouldLog(TimeSpan window, out long skippedBefore)
        {
            var now = DateTime.UtcNow.Ticks;
            var last = Interlocked.Read(ref _lastLoggedTicks);
            if (now - last < window.Ticks || Interlocked.CompareExchange(ref _lastLoggedTicks, now, last) != last)
            {
                Interlocked.Increment(ref _skipped);
                skippedBefore = 0;
                return false;
            }

            skippedBefore = Interlocked.Exchange(ref _skipped, 0);
            return true;
        }

        /// <summary>Returns what was skipped and not reported yet, once.</summary>
        public long TakeSkipped() => Interlocked.Read(ref _skipped) == 0 ? 0 : Interlocked.Exchange(ref _skipped, 0);

        public static string Suffix(long skipped) => skipped > 0 ? $" ({skipped} more since the last report)" : string.Empty;
    }
}
