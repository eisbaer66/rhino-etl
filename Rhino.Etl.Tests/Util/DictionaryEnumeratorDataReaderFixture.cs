using System.Threading;
using System.Threading.Tasks;

namespace Rhino.Etl.Tests.Util
{
    using System;
    using System.Collections.Generic;
    using Core;
    using Xunit;
    using Mocks;
    using Rhino.Etl.Core.DataReaders;

    
    public class DictionaryEnumeratorDataReaderFixture
    {
        [Fact]
        public void WillDisposeInternalEnumeratorAndEnumerableWhenDisposed()
        {
            MockRepository mocks = new MockRepository();
            IAsyncEnumerable<Row> enumerable = mocks.DynamicMultiMock<IAsyncEnumerable<Row>>(typeof(IAsyncDisposable));
            IAsyncEnumerator<Row> enumerator = mocks.DynamicMock<IAsyncEnumerator<Row>>();
            using(mocks.Record())
            {
                SetupResult.For(enumerable.GetAsyncEnumerator(Arg<CancellationToken>.Is.Anything)).Return(enumerator);
                SetupResult.For(enumerator.DisposeAsync()).Return(new ValueTask());
                SetupResult.For(((IAsyncDisposable)enumerable).DisposeAsync()).Return(new ValueTask());
            }
            using (mocks.Playback())
            {
                DictionaryEnumeratorDataReader reader =
                    new DictionaryEnumeratorDataReader(new Dictionary<string, Type>(), enumerable);
                reader.Dispose();
            }
        }
    }
}