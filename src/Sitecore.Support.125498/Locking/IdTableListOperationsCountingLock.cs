namespace Sitecore.Support.ListManager.Locking
{
  using System;
  using System.Collections.Generic;
  using System.Globalization;
  using System.Linq;
  using Sitecore.Data;
  using Sitecore.Data.IDTables;
  using Sitecore.Diagnostics;

  public class IdTableListOperationsCountingLock : IListOperationsCountingLock
  {
    protected const string inProgressPrefix = "LM.InProgress";
    protected const string completePrefix = "LM.Complete";

    protected const string isFixedValue = "1";

    protected readonly char[] dataSep = { '|' };

    public void ClearAll()
    {
      var operationIds = IDTable.GetKeys(inProgressPrefix).Select(entry => entry.Key);
      foreach (var opId in operationIds)
      {
        IDTable.RemoveKey(inProgressPrefix, opId);
      }

      operationIds = IDTable.GetKeys(completePrefix).Select(entry => entry.Key);
      foreach (var opId in operationIds)
      {
        IDTable.RemoveKey(completePrefix, opId);
      }
    }

    public bool Lock(ID operationId)
    {
      Assert.ArgumentNotNull(operationId, "operationId");
      var key = this.BuildKey(operationId);
      var entry = IDTable.GetID(inProgressPrefix, key);

      if (entry != null)
      {
        return false;
      }

      var strData = this.SerializeData(0, false);
      IDTable.Add(inProgressPrefix, key, ID.NewID, ID.Null, strData);

      return true;
    }

    public bool IsRegistered(ID operationId)
    {
      Assert.ArgumentNotNull(operationId, "operationId");
      var key = this.BuildKey(operationId);
      var entry = IDTable.GetID(inProgressPrefix, key);

      if (entry != null)
      {
        return true;
      }

      entry = IDTable.GetID(completePrefix, key);

      return entry != null;
    }

    public int Update(ID operationId, int processedItems)
    {
      Assert.ArgumentNotNull(operationId, "operationId");
      var key = this.BuildKey(operationId);
      var entry = IDTable.GetID(inProgressPrefix, key);

      if (entry == null)
      {
        return 0;
      }

      int counter;
      bool isUnlocked;
      this.ParseInProgressData(entry, out counter, out isUnlocked);

      counter -= processedItems;

      var strData = this.SerializeData(counter, true);
      this.UpdateCustomData(inProgressPrefix, key, entry.ID, strData);

      return counter;
    }

    public bool UnLock(ID operationId, int processedItems)
    {
      Assert.ArgumentNotNull(operationId, "operationId");
      var key = this.BuildKey(operationId);
      var entry = IDTable.GetID(inProgressPrefix, key);

      if (entry == null)
      {
        return true;
      }

      int counter;
      bool isUnlocked;
      this.ParseInProgressData(entry, out counter, out isUnlocked);

      counter -= processedItems;

      if (counter == 0)
      {
        this.MarkAsComplete(key, entry.ID);
        return true;
      }

      if (counter > 0)
      {
        Log.Warn(string.Format("SUPPORT Counter value has a positive value '{0}' after all records have been processed, operation id: {1}", counter, entry), this);
        this.MarkAsComplete(key, entry.ID);
        return true;
      }

      var strData = this.SerializeData(counter, true);
      this.UpdateCustomData(inProgressPrefix, key, entry.ID, strData);

      return false;
    }

    public bool Release(ID operationId)
    {
      Assert.ArgumentNotNull(operationId, "operationId");
      var key = this.BuildKey(operationId);
      var entry = IDTable.GetID(completePrefix, key);

      if (entry == null)
      {
        Log.Warn("SUPPORT Attempt to release uncomplete operation, operation id:" + operationId, this);
        return false;
      }

      IDTable.RemoveKey(completePrefix, key);
      
      return true;
    }

    public void Release(IEnumerable<ID> operationIds)
    {
      Assert.ArgumentNotNull(operationIds, "operationIds");
      
      foreach (var operationId in operationIds)
      {
        this.Release(operationId);
      }
    }

    public Dictionary<ID, DateTime> GetCompleteOperationIds()
    {

      Func<IDTableEntry, KeyValuePair<ID, DateTime>> composeElement = entry =>
      {
        ID operationID;
        DateTime completeTime;
        this.ParseCompleteData(entry, out operationID, out completeTime);

        return new KeyValuePair<ID, DateTime>(operationID, completeTime);
      };

      return IDTable.GetKeys(completePrefix).Select(entry => composeElement(entry)).ToDictionary(it=>it.Key, it=>it.Value);
    }

    public int Inc(ID operationId)
    {
      Assert.ArgumentNotNull(operationId, "operationId");
      var key = this.BuildKey(operationId);
      var entry = IDTable.GetID(inProgressPrefix, key);

      if (entry == null)
      {
        return 0;
      }

      int counter;
      bool fixedState;

      this.ParseInProgressData(entry, out counter, out fixedState);
      counter++;

      if (counter == 0 && fixedState)
      {
        this.MarkAsComplete(key, entry.ID);
        return counter;
      }

      var strData = this.SerializeData(counter, fixedState);
      this.UpdateCustomData(inProgressPrefix, key, entry.ID, strData);

      return counter;
    }

    protected virtual string BuildKey(ID operationId)
    {
      Assert.ArgumentNotNull(operationId, "operationId");
      return operationId.ToString();
    }

    protected virtual ID BuildId(string key)
    {
      Assert.ArgumentNotNullOrEmpty(key, "key");
      return ID.Parse(key);
    }

    protected virtual void ParseInProgressData(IDTableEntry entry, out int counter, out bool isUnlocked)
    {
      Assert.ArgumentNotNull(entry, "entry");
     
      string strData = entry.CustomData;
      var v = strData.Split(this.dataSep, StringSplitOptions.RemoveEmptyEntries);

      counter = int.Parse(v[0], NumberStyles.Integer, CultureInfo.InvariantCulture);
      isUnlocked = v.Length > 1 && v[1] == isFixedValue;
    }

    protected virtual void ParseCompleteData(IDTableEntry entry, out ID operationID, out DateTime completeTime)
    {
      Assert.ArgumentNotNull(entry, "entry");

      operationID = this.BuildId(entry.Key);

      string strData = entry.CustomData;
      completeTime = DateTime.Parse(strData, CultureInfo.InvariantCulture);
    }

    protected virtual string SerializeData(int counter, bool fixState)
    {
      var counterStr = counter.ToString(CultureInfo.InvariantCulture);
      return !fixState ? counterStr : string.Concat(counterStr, this.dataSep[0], isFixedValue);
    }

    protected virtual void UpdateCustomData(string prefix,string key, ID id, string newCustomData)
    {
      IDTable.RemoveKey(prefix, key);
      IDTable.Add(prefix, key, id, ID.Null, newCustomData);
    }

    protected void MarkAsComplete(string key, ID id)
    {
      // Potential place for optimization: Replace 'add-delete' with 'update'
      var completeTime = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);
      IDTable.RemoveKey(inProgressPrefix, key);
      IDTable.Add(completePrefix, key, id, ID.Null, completeTime);
    }
  }
}