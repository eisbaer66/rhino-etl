using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.Aggregation
{
    using System.Collections.Generic;
    using Core;
    using Xunit;

    
    public class AggregationFixture : BaseAggregationFixture
    {
        [Fact]
        public async Task AggregateRowCount()
        {
            using (RowCount rowCount = new RowCount())
            {
                IAsyncEnumerable<Row> result = rowCount.Execute(rows.ToAsyncEnumerable());
                List<Row> items = await result.ToListAsync();
                Assert.Single(items);
                Assert.Equal(6, items[0]["count"]);
            }
        }

        [Fact]
        public async Task AggregateCostPerProduct()
        {
            using (CostPerProductAggregation aggregation = new CostPerProductAggregation())
            {
                IAsyncEnumerable<Row> result = aggregation.Execute(rows.ToAsyncEnumerable());
                List<Row> items = await result.ToListAsync();
                Assert.Equal(3, items.Count);
                Assert.Equal("milk", items[0]["name"]);
                Assert.Equal("sugar", items[1]["name"]);
                Assert.Equal("coffee", items[2]["name"]);

                Assert.Equal(30, items[0]["cost"]);
                Assert.Equal(28, items[1]["cost"]);
                Assert.Equal(6, items[2]["cost"]);
            }
        }

        [Fact]
        public async Task SortedAggregateCostPerProduct()
        {
            using (SortedCostPerProductAggregation aggregation = new SortedCostPerProductAggregation())
            {
                IAsyncEnumerable<Row> result = aggregation.Execute(rows.ToAsyncEnumerable());
                List<Row> items = await result.ToListAsync();
                Assert.Equal(4, items.Count);
                Assert.Equal("milk", items[0]["name"]);
                Assert.Equal("sugar", items[1]["name"]);
                Assert.Equal("coffee", items[2]["name"]);
                Assert.Equal("sugar", items[3]["name"]);

                Assert.Equal(30, items[0]["cost"]);
                Assert.Equal(25, items[1]["cost"]);
                Assert.Equal(6, items[2]["cost"]);
                Assert.Equal(3, items[3]["cost"]);
            }
        }
    }
}