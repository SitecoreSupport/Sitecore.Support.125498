
namespace Sitecore.Support.ListManager.ProcessingPool.Events
{
  using Sitecore.Diagnostics;
  using ProcessingPoolItem = Sitecore.Analytics.Processing.ProcessingPool.ProcessingPoolItem;

  public class AddingItemToProcessingPoolArgs : ProcessingPoolArgs
  {
    public AddingItemToProcessingPoolArgs(string poolName, ProcessingPoolItem poolItem) : base(poolName)
    {
      Assert.ArgumentNotNull(poolItem, "poolItem");
      this.PoolItem = poolItem;
    }

    public ProcessingPoolItem PoolItem { get; set; }
  }
}