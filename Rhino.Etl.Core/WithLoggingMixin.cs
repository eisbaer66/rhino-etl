using System.Linq;
using Rhino.Etl.Core.Logging;

namespace Rhino.Etl.Core
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    /// <summary>
    /// A base class that expose easily logging events
    /// </summary>
    public class WithLoggingMixin
    {
        private readonly ILog log;
        readonly List<Exception> errors = new List<Exception>();

        /// <summary>
        /// Initializes a new instance of the <see cref="WithLoggingMixin"/> class.
        /// </summary>
        protected WithLoggingMixin()
        {
            log = LogProvider.GetLogger(GetType());
        }

        /// <summary>
        /// Logs an error message
        /// </summary>
        /// <param name="exception">The exception.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        protected void Error(Exception exception, string format, params Tuple<string, object>[] args)
        {
            string logtemplate = string.Format(CultureInfo.InvariantCulture, format, args.Select(a => "{"+ a.Item1+ "}").ToArray());
            string message = string.Format(CultureInfo.InvariantCulture, format, args.Select(a => a.Item2).ToArray());
            string errorMessage;
            if (exception != null)
                errorMessage = string.Format("{0}: {1}", message, exception.Message);
            else
                errorMessage = message.ToString();
            errors.Add(new RhinoEtlException(errorMessage, exception));
            if (log.IsErrorEnabled())
            {
                log.ErrorException(logtemplate, exception, args.Select(a => a.Item2).ToArray());
            }
        }

        /// <summary>
        /// Logs a warn message
        /// </summary>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        protected void Warn(string format, params object[] args)
        {
            if (log.IsWarnEnabled())
            {
                log.WarnFormat(format, args);
            }
        }

        /// <summary>
        /// Logs a debug message
        /// </summary>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        protected void Debug(string format, params object[] args)
        {
            if (log.IsDebugEnabled())
            {
                log.DebugFormat(format, args);
            }
        }

        
        /// <summary>
        /// Logs a notice message
        /// </summary>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        protected void Trace(string format, params object[] args)
        {
            if (log.IsTraceEnabled())
            {
                log.TraceFormat(format, args);
            }
        }


        /// <summary>
        /// Logs an information message
        /// </summary>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        protected void Info(string format, params object[] args)
        {
            if (log.IsInfoEnabled())
            {
                log.InfoFormat(format, args);
            }
        }

        /// <summary>
        /// Gets all the errors
        /// </summary>
        /// <value>The errors.</value>
        public Exception[] Errors
        {
            get { return errors.ToArray(); }
        }
    }
}