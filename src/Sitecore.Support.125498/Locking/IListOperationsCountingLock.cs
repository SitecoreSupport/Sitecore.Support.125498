
namespace Sitecore.Support.ListManager.Locking
{
  using System;
  using System.Collections.Generic;
  using Sitecore.Data;

  public interface IListOperationsCountingLock
  {
    void ClearAll();

    int Inc(ID operationId);

    int Update(ID operationId, int processedItems);

    bool IsRegistered(ID operationId);

    bool Lock(ID operationId);

    bool UnLock(ID operationId, int processedItems);

    bool Release(ID operationId);

    void Release(IEnumerable<ID> operationIds);

    Dictionary<ID, DateTime> GetCompleteOperationIds();
  }
}