using System;

namespace Apache.Arrow
{
    public static class TimeSpanExtensions
    {
        /// <summary>
        /// Formats a TimeSpan into an ISO 8601 compliant time offset string.
        /// </summary>
        /// <param name="timeSpan">timeSpan to format</param>
        /// <returns>ISO 8601 offset string</returns>
        public static string ToTimeZoneOffsetString(this TimeSpan timeSpan)
        {
            var sign = timeSpan.Hours >= 0 ? "+" : "-";
            var hours = Math.Abs(timeSpan.Hours);
            var minutes = Math.Abs(timeSpan.Minutes);
            return sign + hours.ToString("00") + ":" + minutes.ToString("00");
        }
           
    }
}
