
namespace Sitecore.Support.ListManager
{
  using System;
  using Sitecore.Configuration;
  using Sitecore.Diagnostics;
  using Sitecore.Events;
  using Sitecore.Events.Hooks;
  using Sitecore.Support.ListManager.Contact.Bulk;
  using Sitecore.Support.ListManager.Locking;
  using Sitecore.Support.ListManager.ProcessingPool;
  using Sitecore.Support.ListManager.ProcessingPool.Events;

  public class ContactListLockManagementHook : IHook
  {
    protected IListOperationsCountingLock contactListLock;

    // Events are handled based on the default behavior of List Manager: lock is added only if a list is modified on CM instance 
    public void Initialize()
    {
      this.contactListLock = Factory.CreateObject("contactListManagement/contactListCountingLock", true) as IListOperationsCountingLock;
      Assert.IsNotNullOrEmpty(this.PoolName, "PoolName is not set...");

      Event.Subscribe("processingpool:cancelall", this.CancellAllHandler);
      Event.Subscribe("processingpool:addingitem", this.AddingPoolItemHandler);
      Event.Subscribe("processingpool:addeditem", this.AddedPoolItemHandler);
      Event.Subscribe("contactbulkupdate:batchprocessing", this.BatchProcessingHandler);
      Event.Subscribe("contactbulkupdate:batchprocessed", this.BatchProcessedHandler);

      Log.Info("SUPPORT ContactListLockManagementHook has been initialized...", this);
    }

    public string PoolName { get; private set; }

    protected virtual void BatchProcessingHandler(object sender, EventArgs e)
    {
      var args = e as ContactBulkUpdateArgs;

      if (args == null)
      {
        Log.Warn("SUPPORT BatchProcessingHandler: args are not ContactBulkUpdateArgs.", this);
        return;
      }

      this.contactListLock.Lock(args.OperationId);
    }

    protected virtual void BatchProcessedHandler(object sender, EventArgs e)
    {
      var args = e as ContactBulkUpdateArgs;

      if (args == null)
      {
        Log.Warn("SUPPORT BatchProcessedHandler: args are not ContactBulkUpdateArgs.", this);
        return;
      }

      this.contactListLock.UnLock(args.OperationId, args.TotalQueuedRecords);
    }

    protected virtual void AddingPoolItemHandler(object sender, EventArgs e)
    {
      var args = e as AddingItemToProcessingPoolArgs;

      if (args == null)
      {
        Log.Warn("SUPPORT AddingPoolItemHandler Can't process some parameters", this);
        return;
      }

      if (!string.Equals(this.PoolName, args.PoolName, StringComparison.InvariantCultureIgnoreCase))
      {
        return;
      }

      if (ContextListOperationSwitcher.CurrentValue == null)
      {
        return;
      }

      var poolItem = args.PoolItem;
      var listPoolItem = new ListProcessingPoolItem(poolItem.Key);

      foreach (var property in poolItem.Properties)
      {
        listPoolItem.Properties.Add(property.Key, property.Value);
      }

      var operationId = ContextListOperationSwitcher.CurrentValue.ListOperationID;
      listPoolItem.BulkOperationIds.Add(operationId);

      args.PoolItem = listPoolItem;
    }

    protected virtual void AddedPoolItemHandler(object sender, EventArgs e)
    {
      var args = e as AddedItemToProcessingPoolArgs;

      if (args == null)
      {
        Log.Warn("SUPPORT AddedPoolItemHandler: args are not AddedItemToProcessingPoolArgs.", this);
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

      var contextListOperation = ContextListOperationSwitcher.CurrentValue;

      if (contextListOperation == null)
      {
        Log.Warn("SUPPORT AddedPoolItemHandler: poolItem is ListProcessingPoolItem but a context list operation is null", this);
        return;
      }

      if (!listPoolItem.BulkOperationIds.Contains(contextListOperation.ListOperationID))
      {
        Log.Warn("SUPPORT AddedPoolItemHandler: a processing pool item doesn't contain the context ListOperationId.", this);
      }

      contextListOperation.ProcessedItemsCount++;
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
  }
}