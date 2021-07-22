using NCrontab;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("WarehouseTest")]
namespace Warehouse.Modules
{
    internal class NextRun
    {
        internal string FunctionAppName { get; }
        private static readonly string filePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "functionAppLog.json"));
        private static Dictionary<string, DateTime> runs;

        internal NextRun(string functionAppName)
        {
            FunctionAppName = functionAppName;
        }

        internal bool DoRun(DateTime now, string scheduleExpression)
        {
            var schedule = CrontabSchedule.Parse(scheduleExpression);
            var lastRun = GetLastRun();
            var nextRunFromLastRun = schedule.GetNextOccurrence(lastRun);
            var hourSpanBetweenRuns = (schedule.GetNextOccurrence(nextRunFromLastRun.AddSeconds(1)) - nextRunFromLastRun).TotalHours;
            var currentRun = schedule.GetNextOccurrence(now.AddHours(-hourSpanBetweenRuns));
            var minAgeLimitOnLastRun = lastRun < currentRun.AddHours(-hourSpanBetweenRuns + 1);
            var NotOlderThanOneHour = now > currentRun && now < currentRun.AddHours(1);
            return minAgeLimitOnLastRun && NotOlderThanOneHour;
        }

        internal static double GetHourSpanBetweenRuns(string scheduleExpression)
        {
            var schedule = CrontabSchedule.Parse(scheduleExpression);
            var nextRunFromLastRun = schedule.GetNextOccurrence(DateTime.MinValue);
            return (schedule.GetNextOccurrence(nextRunFromLastRun.AddSeconds(1)) - nextRunFromLastRun).TotalHours;
        }

        internal void SetLastRun(DateTime lastRunUtcDateTime)
        {
            if (runs == null)
                GetRuns();

            if (runs.ContainsKey(FunctionAppName))
                runs[FunctionAppName] = lastRunUtcDateTime;
            else
                runs.Add(FunctionAppName, lastRunUtcDateTime);

            using StreamWriter file = File.CreateText(filePath);
            JsonSerializer serializer = new JsonSerializer();
            serializer.Serialize(file, runs);
        }

        internal DateTime GetLastRun()
        {
            if (runs == null)
                GetRuns();

            return runs.TryGetValue(FunctionAppName, out DateTime res) ? res : DateTime.MinValue;
        }

        internal void PurgeLog()
        {
            runs = null; // new Dictionary<string, DateTime>();
            if (File.Exists(filePath))
                File.Delete(filePath);
        }

        private static void GetRuns()
        {
            if (!File.Exists(filePath))
                runs = new Dictionary<string, DateTime>();
            else
            {
                using StreamReader file = File.OpenText(filePath);
                JsonSerializer serializer = new JsonSerializer();
                runs = (Dictionary<string, DateTime>)serializer.Deserialize(file, typeof(Dictionary<string, DateTime>));
            }
        }
    }
}