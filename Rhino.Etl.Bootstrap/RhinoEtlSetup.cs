using System.Threading;
using System.Threading.Tasks;

namespace Rhino.Etl.Bootstrap
{
    using System;
    using System.IO;
    using System.Reflection;
    using Boo.Lang.Useful.CommandLine;
    using Core;
    using Dsl;
    using Logging;

    public class RhinoEtlSetup
    {
        private readonly ILog log = LogProvider.GetCurrentClassLogger();
        private readonly Action<bool> setupLogging;

        public RhinoEtlSetup(Action<bool> setupLogging)
        {
            this.setupLogging = setupLogging ?? throw new ArgumentNullException(nameof(setupLogging));
        }


        public async Task Execute(string[] args, CancellationToken cancellationToken = default)
        {
            RhinoEtlCommandLineOptions options = new RhinoEtlCommandLineOptions();
            try
            {
                options.Parse(args);
            }
            catch (CommandLineException e)
            {
                Console.WriteLine(e.Message);
                options.PrintOptions();
                return;
            }
            await Execute(options, cancellationToken);
        }

        public async Task Execute(RhinoEtlCommandLineOptions options, CancellationToken cancellationToken = default)
        {
            setupLogging(options.Verbose);

            log.DebugFormat("Starting with {OptionsFile}", options.File);
            string ext = Path.GetExtension(options.File).ToLower();
            Type processType;
            if(ext==".exe" || ext==".dll")
            {
                processType = GetFromAssembly(options);
            }
            else
            {
                processType = GetFromDslFile(options.File);
            }

            await ExecuteProcessInSeparateAppDomain(processType, options, cancellationToken);
        }

        private static Type GetFromAssembly(RhinoEtlCommandLineOptions options)
        {
            FileInfo _assemblyInfo = new FileInfo(options.File);
            Assembly asm = Assembly.LoadFile(_assemblyInfo.FullName);
            //Assembly asm = Assembly.Load(options.File);
            foreach (Type type in asm.GetTypes())
            {
                if(typeof(EtlProcess).IsAssignableFrom(type) && type.Name.Equals(options.Process, StringComparison.InvariantCultureIgnoreCase))
                    return type;
            }
            throw new InvalidOperationException("Could not find type nameed '" + options.Process + "' on: " +
                                                options.File);
        }

        private static Type GetFromDslFile(string filename)
        {
            Type processType;
            EtlProcess process = EtlDslEngine.Factory.Create<EtlProcess>(filename);
            processType = process.GetType();
            return processType;
        }

        private async Task ExecuteProcessInSeparateAppDomain(Type processType, RhinoEtlCommandLineOptions options,
            CancellationToken cancellationToken = default)
        {
            try
            {
                FileInfo _assemblyInfo = new FileInfo(options.File);
                //we have to run the code in another appdomain, because we want to
                //setup our own app.config for it
                AppDomainSetup appDomainSetup = new AppDomainSetup();
                //setting this to the current executing directory because that's where the dsl's dll gets created.
                appDomainSetup.ApplicationBase = Path.GetDirectoryName(Uri.UnescapeDataString(new UriBuilder(Assembly.GetExecutingAssembly().CodeBase).Path));
                appDomainSetup.ConfigurationFile = string.IsNullOrEmpty(options.Configuration) ? options.File + ".config" : options.Configuration;
                AppDomain appDomain = AppDomain.CreateDomain("etl.domain", null, appDomainSetup);
                appDomain.Load(processType.Assembly.GetName());
                RhinoEtlRunner runner = (RhinoEtlRunner)appDomain.CreateInstanceAndUnwrap(typeof (RhinoEtlRunner).Assembly.GetName().FullName,
                                                                                          typeof (RhinoEtlRunner).FullName);
                await runner.Start(processType, options.Verbose, setupLogging, cancellationToken);
            }
            catch (Exception e)
            {
                log.ErrorException(e.Message, e);
            }
        }

    }
}

