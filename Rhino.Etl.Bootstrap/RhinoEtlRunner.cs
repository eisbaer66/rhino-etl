using System.Threading;
using System.Threading.Tasks;

namespace Rhino.Etl.Bootstrap
{
    using System;
    using Core;
    using Logging;

    public class RhinoEtlRunner : MarshalByRefObject
    {
        private readonly ILog log = LogProvider.GetCurrentClassLogger();

        public async Task Start(Type type, bool verboseLogging, Action<bool> setupLogging,
            CancellationToken cancellationToken = default)
        {
            try
            {
                setupLogging(verboseLogging);
                EtlProcess process = (EtlProcess)Activator.CreateInstance(type);
                await process.Execute(cancellationToken);
                foreach (Exception error in process.GetAllErrors())
                {
                    log.ErrorException(error.Message, error);
                }
            }
            catch (Exception e)
            {
                log.ErrorException(e.Message, e);
            }
        }
    }
}