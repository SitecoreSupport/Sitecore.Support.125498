
namespace Sitecore.Support.ListManager
{
  using System;
  using System.Linq;
  using System.Threading;
  using Sitecore.Configuration;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Analytics.Models;
  using Sitecore.Diagnostics;
  using Sitecore.Events;
  using Sitecore.Events.Hooks;
  using Sitecore.Support.ListManager.Locking;
  using Sitecore.Support.ListManager.ProcessingPool;
  using Sitecore.Support.ListManager.ProcessingPool.Events;

  public class ContactListLockProcessingHook : IHook
  {
    protected IListOperationsCountingLock contactListLock;

    protected ContactListIndexObserver indexObserver;

    public string PoolName { get; private set; }

    // Events are handled based on the default behavior of List Manager: lock is added only if a list is modified on CM instance 
    public void Initialize()
    {
      this.contactListLock = Factory.CreateObject("contactListManagement/contactListCountingLock", true) as IListOperationsCountingLock;
      Assert.IsNotNullOrEmpty(this.PoolName, "PoolName is not set...");
      Event.Subscribe("processingpool:removeditem", this.RemovedPoolItemHandler);
      Event.Subscribe("processingpool:cancelall", this.CancellAllHandler);

      var indexName = Settings.GetSetting("ListManagement.ContactsIndexName", "sitecore_analytics_index");
      var index = ContentSearchManager.GetIndex(indexName);

      // Check only tag related crawlers
      Func<IProviderCrawler, bool> crawlerFilter = crawler => crawler is ObserverCrawler<IContactIndexable> || crawler is ObserverCrawler<IContactTagIndexable>;
      this.indexObserver = new ContactListIndexObserver(index, crawlerFilter);
      this.indexObserver.IndexJobFinished += IndexJobFinished;

      var startTime = DateTime.UtcNow;
      ThreadPool.QueueUserWorkItem(dummy => this.CleanUpReleasedOnStartUp(startTime));

      Log.Info("SUPPORT ContactListLockProcessingHook has been initialized...", this);
    }

    protected virtual void RemovedPoolItemHandler(object sender, EventArgs e)
    {
      var args = e as RemovedItemFromProcessingPoolArgs;

      if (args == null)
      {
        Log.Warn("SUPPORT RemovedPoolItemHandler: args are not RemovedItemFromProcessingPoolArgs.", this);
        return;
      }

      if (!string.Equals(this.PoolName, args.PoolName, StringComparison.InvariantCultureIgnoreCase))
      {
        return;
      }

      var listPoolItem = args.PoolItem as ListProcessingPoolItem;

      if (listPoolItem == null)
      {
        // The default ProcessingPoolItem -> Do nothing
        return;
      }

      var operationIds = listPoolItem.BulkOperationIds;
      foreach (var operId in operationIds)
      {
        this.contactListLock.Inc(operId);
      }
    }

    protected virtual void CancellAllHandler(object sender, EventArgs e)
    {
      var args = e as ProcessingPoolArgs;

      if (args == null)
      {
        Log.Warn("SUPPORT CancellAllHandler: args are not ProcessingPoolArgs.", this);
        return;
      }

      if (!string.Equals(this.PoolName, args.PoolName, StringComparison.InvariantCultureIgnoreCase))
      {
        return;
      }

      Log.Warn("SUPPORT All operations in processing pool have been cleared", this);
      this.contactListLock.ClearAll();
    }

    protected virtual void IndexJobFinished(object sender, Jobs.JobFinishedEventArgs e)
    {
      var operations = this.contactListLock.GetCompleteOperationIds();

      if (operations.Count < 1)
      {
        return;
      }

      DateTime start, end;
      this.indexObserver.GetTimeOfLastCompleteJob(out start, out end);

      var operationsToRelease = operations.Keys.Where(opId => operations[opId] < start);
      this.contactListLock.Release(operationsToRelease);
    }

    protected virtual void CleanUpReleasedOnStartUp(DateTime startTime)
    {
      if (this.indexObserver.IsIndexJobInQueue() || this.indexObserver.IndexHadUpdate)
      {
        return;
      }

      var releasedBulkOperationIds = this.contactListLock.GetCompleteOperationIds();

      if (releasedBulkOperationIds.Count <= 0)
      {
        return;
      }

      var operationIdsToRelease = releasedBulkOperationIds.Where(op => op.Value < startTime).Select(op => op.Key).ToArray();

      if (operationIdsToRelease.Length <= 0)
      {
        return;
      }

      this.contactListLock.Release(operationIdsToRelease);

      Log.Warn("SUPPORT Released operation ids during clean up: ", this);
      foreach (var operationId in operationIdsToRelease)
      {
        Log.Warn(operationId.ToString(), this);
      }
    }
  }
}