
namespace Sitecore.Support.ListManager.ProcessingPool
{
    using System.Collections.Generic;
    using Sitecore.Diagnostics;
    using Sitecore.Data;

    public class ListProcessingPoolItem : Sitecore.Analytics.Processing.ProcessingPool.ProcessingPoolItem
    {
        private HashSet<ID> bulkOperationIds;

        public ListProcessingPoolItem() : base()
        {
        }

        public ListProcessingPoolItem(byte[] key)
            : base(key)
        {
        }

        [NotNull]
        public HashSet<ID> BulkOperationIds
        {
            get
            {
                return this.bulkOperationIds ?? (this.bulkOperationIds = new HashSet<ID>());
            }

            [UsedImplicitly]
            private set
            {
                Assert.ArgumentNotNull(value, "value");
                this.bulkOperationIds = value;
            }
        }
    }
}
