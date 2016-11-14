
namespace Sitecore.Support.ListManager.ProcessingPool.Events
{
  using System;
  using Sitecore.Diagnostics;
  using Sitecore.Events;

  public class ProcessingPoolArgs : EventArgs, IPassNativeEventArgs
  {
    public ProcessingPoolArgs(string poolName)
    {
      Assert.ArgumentNotNull(poolName, "poolName");
      this.PoolName = poolName;
    }

    public string PoolName { get; protected set; }
  }
}