using System.Diagnostics;

namespace CachePerformance.Helpers
{
    public static class Constants
    {
        public static readonly int Zero = 0;
        public static readonly int One = 1;
        public static readonly int Tenthousand = 10000;
        public static readonly int OneHundredThousand = 100000;
        public static readonly int OneMillion = 1000000;

        public static string GetTime(int count, Stopwatch watch) => $"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes";
    }
}
