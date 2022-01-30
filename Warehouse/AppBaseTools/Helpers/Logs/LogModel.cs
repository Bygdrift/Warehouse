using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Bygdrift.Warehouse.Helpers.Logs
{
    /// <summary>
    /// Th log model
    /// </summary>
    public class LogModel
    {
        /// <summary>The name of the class that called this method</summary>
        public readonly string ClassCallerName;

        private string _message = null;

        /// <summary>
        /// The message in the log
        /// </summary>
        public string Message
        {
            get
            {
                _message ??= CreateMessage(false);
                return _message;
            }
        }

        /// <summary>
        /// The message in the log, starting with the name of the class, that send the log
        /// </summary>
        public string MessageWithClassName
        {
            get
            {
                _message ??= CreateMessage(true);
                return _message;
            }
        }

        /// <summary>
        /// Eventual arugments that followed the log
        /// </summary>
        public List<object> Arguments { get; }

        /// <summary>
        /// Eventual exceptions
        /// </summary>
        public Exception Exception { get; }

        /// <summary>
        /// The template that builds up the log. An log with this template: log.LogError("-{A}- {B}-", "a", 1), will produce "-a- 1-";
        /// </summary>
        public string MessageTemplate { get; }

        /// <summary>
        /// The type of log - is it an information or error etc.
        /// </summary>
        public LogType LogType { get; }

        /// <summary>
        /// The model - primary used internal
        /// </summary>
        /// <param name="logType"></param>
        /// <param name="messageTemplate"></param>
        /// <param name="exception"></param>
        /// <param name="classCallerName"></param>
        /// <param name="args"></param>
        public LogModel(LogType logType, string messageTemplate, Exception exception, string classCallerName, params object[] args)
        {
            LogType = logType;
            MessageTemplate = messageTemplate;
            Exception = exception;
            ClassCallerName = classCallerName;
            if (args != null && args.Any())
                Arguments = args.ToList();
        }

        private string CreateMessage(bool inludeClassName)
        {
            var res = ReplaceBracketContentWithArgs(MessageTemplate, Arguments);
            return inludeClassName ? ClassCallerName + ": " + res : res;
        }

        private string ReplaceBracketContentWithArgs(string input, List<object> args = null)
        {
            if (args == null)
                return input;

            var expression = new Regex(@"\{.*?\}");
            var matches = expression.Matches(input);
            var res = new StringBuilder();
            var start = 0;
            for (int i = 0; i < matches.Count; i++)
            {
                res.Append(input[start..matches[i].Index]);  //The sam as res.Append(input.Substring(start, matches[i].Index - start));
                if (args != null && args.Count - 1 >= i)
                    res.Append(args[i]);
                else
                    res.Append(matches[i].Value);

                start = matches[i].Index + matches[i].Length;
            }
            if (start < input.Length)
                res.Append(input[start..]);  //The same as res.Append(input.Substring(start, input.Length - start));

            return res.ToString();
        }

    }
}
