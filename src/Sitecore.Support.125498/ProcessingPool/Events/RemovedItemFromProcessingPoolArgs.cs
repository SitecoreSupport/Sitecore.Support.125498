
namespace Sitecore.Support.ListManager.ProcessingPool.Events
{
  using Sitecore.Diagnostics;
  using ProcessingPoolItem = Sitecore.Analytics.Processing.ProcessingPool.ProcessingPoolItem;

  public class RemovedItemFromProcessingPoolArgs : ProcessingPoolArgs
  {
    public enum ProcessingPoolItemResult
    {
      MaxAttemptsReached,
      Successful
    }

    public RemovedItemFromProcessingPoolArgs(string poolName, ProcessingPoolItem poolItem, ProcessingPoolItemResult result)
      : base(poolName)
    {
      Assert.ArgumentNotNull(poolItem, "poolItem");
      this.PoolItem = poolItem;
      this.ProcessResult = result;
    }

    public ProcessingPoolItem PoolItem { get; protected set; }

    public ProcessingPoolItemResult ProcessResult { get; protected set; }
  }
}