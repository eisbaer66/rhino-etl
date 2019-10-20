namespace Rhino.Etl.Cmd
{
    using System;
    using Logging;
    using Serilog;
    using Bootstrap;

    public class Program
    {
        private static ILog _log;

        private static void Main(string[] args)
        {
            bool verbose = false;
#if DEBUG
            verbose = true;
#endif

            try
            {
                SetupLogging(verbose);

                _log = LogProvider.GetCurrentClassLogger();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            try
            {
                new RhinoEtlSetup(SetupLogging).Execute(args);
            }
            catch (Exception e)
            {
                _log.ErrorException("unexpected Error", e);
                throw;
            }
        }
        private static void SetupLogging(bool verbose)
        {
            LoggerConfiguration loggerConfiguration = new LoggerConfiguration()
                .WriteTo.Console()
                .Enrich.FromLogContext()
                .MinimumLevel.Information();
            if (verbose) loggerConfiguration.MinimumLevel.Verbose();
            Log.Logger = loggerConfiguration.CreateLogger();
        }
    }
}

