using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Bygdrift.Warehouse.Helpers.Logs
{
    /// <summary>
    /// Handle all logs. With this log, you can read the log - you cannot do that in a nomral ILogger
    /// </summary>
    public class Log
    {
        private readonly List<LogModel> _logs;

        /// <summary>
        /// Th saved logs
        /// </summary>
        public List<LogModel> Logs
        {
            get { return _logs; }
        }

        /// <summary>
        /// The formal logger
        /// </summary>
        public ILogger Logger { get; set; }


        /// <summary>
        /// The constructor
        /// </summary>
        public Log(ILogger logger = null)
        {
            _logs = new List<LogModel>();
            Logger = logger;
        }

        /// <summary>
        /// Return all errors and critical logs
        /// </summary>
        /// <param name="inludeClassName">writes in from what class the log has been send</param>
        public IEnumerable<string> GetErrorsAndCriticals(bool inludeClassName = false)
        {
            foreach (var item in Logs.Where(o => o.LogType == LogType.Error || o.LogType == LogType.Critical))
                yield return inludeClassName ? item.MessageWithClassName : item.Message;
        }

        ///// <summary>
        ///// Return all informations
        ///// </summary>
        ///// <param name="inludeClassName">writes in from what class the log has been send</param>
        //public IEnumerable<string> GetInformations(bool inludeClassName = false)
        //{
        //    foreach (var item in Logs.Where(o => o.LogType == LogType.Information))
        //        yield return inludeClassName ? item.MessageWithClassName : item.Message;
        //}

        /// <summary>
        /// Get all logs
        /// </summary>
        /// <param name="inludeClassName">writes in from what class the log has been send</param>
        public IEnumerable<string> GetLogs(bool inludeClassName = false)
        {
            foreach (var item in Logs)
                yield return inludeClassName ? item.MessageWithClassName : item.Message;
        }

        /// <summary>
        /// Get all logs
        /// </summary>
        /// <param name="logType">What kind of log to return</param>
        /// <param name="inludeClassName">writes in from what class the log has been send</param>
        public IEnumerable<string> GetLogs(LogType logType, bool inludeClassName = false)
        {
            foreach (var item in Logs.Where(o => o.LogType == logType))
                yield return inludeClassName ? item.MessageWithClassName : item.Message;
        }

        ///// <summary>
        ///// Return all warnings
        ///// </summary>
        ///// <param name="inludeClassName">writes in from what class the log has been send</param>
        //public IEnumerable<string> GetWarnings(bool inludeClassName = false)
        //{
        //    foreach (var item in Logs.Where(o => o.LogType == LogType.Warning))
        //        yield return inludeClassName ? item.MessageWithClassName : item.Message;
        //}

        /// <summary>
        /// If there are any errors in the log
        /// </summary>
        /// <returns></returns>
        public bool HasErrorsOrCriticals()
        {
            return Logs.Any(o => o.LogType == LogType.Error || o.LogType == LogType.Critical);
        }

        /// <summary>
        /// Create a log information
        /// </summary>
        /// <param name="message"></param>
        /// <param name="args"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "<Pending>")]
        public void LogInformation(string message, params object[] args)
        {
            var classCallerName = new StackTrace().GetFrame(1).GetMethod().ReflectedType.FullName;
            var log = new LogModel(LogType.Information, message, null, classCallerName, args);
            _logs.Add(log);
            Debug.WriteLine("Info: " + log.Message);
            Logger?.LogInformation(log.Message);
        }

        /// <summary>
        /// Create a log information
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        /// <param name="args"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "<Pending>")]
        public void LogInformation(Exception exception, string message, params object[] args)
        {
            var classCallerName = new StackTrace().GetFrame(1).GetMethod().ReflectedType.FullName;
            var log = new LogModel(LogType.Information, message, exception, classCallerName, args);
            _logs.Add(log);
            Debug.WriteLine("Info: " + log.Message);
            Logger?.LogInformation(exception, log.Message);
        }

        /// <summary>
        /// Create a log warning
        /// </summary>
        /// <param name="message"></param>
        /// <param name="args"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "<Pending>")]
        public void LogWarning(string message, params object[] args)
        {
            var classCallerName = new StackTrace().GetFrame(1).GetMethod().ReflectedType.FullName;
            var log = new LogModel(LogType.Warning, message, null, classCallerName, args);
            _logs.Add(log);
            Debug.WriteLine("Warning: " + log.Message);
            Logger?.LogWarning(log.Message);
        }

        /// <summary>
        /// Create a log warning
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        /// <param name="args"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "<Pending>")]
        public void LogWarning(Exception exception, string message, params object[] args)
        {
            var classCallerName = new StackTrace().GetFrame(1).GetMethod().ReflectedType.FullName;
            var log = new LogModel(LogType.Warning, message, exception, classCallerName, args);
            _logs.Add(log);
            Debug.WriteLine("Warning: " + log.Message);
            Logger?.LogWarning(exception, log.Message);
        }

        /// <summary>
        /// Create a log error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="args"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "<Pending>")]
        public void LogError(string message, params object[] args)
        {
            var classCallerName = new StackTrace().GetFrame(1).GetMethod().ReflectedType.FullName;
            var log = new LogModel(LogType.Error, message, null, classCallerName, args);
            _logs.Add(log);
            Debug.WriteLine("Error: " + log.Message);
            Logger?.LogError(log.Message);
        }


        /// <summary>
        /// Create a log error
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        /// <param name="args"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "<Pending>")]
        public void LogError(Exception exception, string message, params object[] args)
        {
            var classCallerName = new StackTrace().GetFrame(1).GetMethod().ReflectedType.FullName;
            var log = new LogModel(LogType.Error, message, exception, classCallerName, args);
            _logs.Add(log);
            Debug.WriteLine("Error: " + log.Message);
            Logger?.LogError(exception, log.Message);

        }

        /// <summary>
        /// Create a log critical
        /// </summary>
        /// <param name="message"></param>
        /// <param name="args"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "<Pending>")]
        public void LogCritical(string message, params object[] args)
        {
            var classCallerName = new StackTrace().GetFrame(1).GetMethod().ReflectedType.FullName;
            var log = new LogModel(LogType.Critical, message, null, classCallerName, args);
            _logs.Add(log);
            Debug.WriteLine("Critical: " + log.Message);
            Logger?.LogCritical(log.Message);
        }

        /// <summary>
        /// Create a log critical
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        /// <param name="args"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "<Pending>")]
        public void LogCritical(Exception exception, string message, params object[] args)
        {
            var classCallerName = new StackTrace().GetFrame(1).GetMethod().ReflectedType.FullName;
            var log = new LogModel(LogType.Critical, message, exception, classCallerName, args);
            _logs.Add(log);
            Debug.WriteLine("Critical: " + log.Message);
            Logger?.LogCritical(exception, log.Message);
        }
    }

    /// <summary>
    /// The type og log
    /// </summary>
    public enum LogType
    {
        /// <summary>Information</summary>
        Information,
        /// <summary>Warning</summary>
        Warning,
        /// <summary>Error</summary>
        Error,
        /// <summary>Critical</summary>
        Critical
    }
}
