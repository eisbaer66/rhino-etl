namespace Rhino.Etl.Bootstrap
{
    using System;
    using Core;
    using Logging;

    public class RhinoEtlRunner : MarshalByRefObject
    {
        private readonly ILog log = LogProvider.GetCurrentClassLogger();

        public void Start(Type type, bool verboseLogging, Action<bool> setupLogging)
        {
            try
            {
                setupLogging(verboseLogging);
                EtlProcess process = (EtlProcess)Activator.CreateInstance(type);
                process.Execute();
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