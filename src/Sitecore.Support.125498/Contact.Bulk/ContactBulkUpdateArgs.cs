
namespace Sitecore.Support.ListManager.Contact.Bulk
{
  using System;
  using Sitecore.Data;
  using Sitecore.Diagnostics;
  using Sitecore.Events;

  public class ContactBulkUpdateArgs : EventArgs, IPassNativeEventArgs
  {
    public ContactBulkUpdateArgs(ID operationId, int totalQueuedRecords)
    {
      Assert.ArgumentNotNull(operationId, "operationId");
      Assert.ArgumentCondition(totalQueuedRecords >= 0, "totalQueuedRecords", "Processed records can't be a negative number");

      this.OperationId = operationId;
      this.TotalQueuedRecords = totalQueuedRecords;
    }

    public ID OperationId { get; protected set; }

    public int TotalQueuedRecords { get; protected set; }
  }
}