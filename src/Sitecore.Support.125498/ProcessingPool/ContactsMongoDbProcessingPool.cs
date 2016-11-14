
namespace Sitecore.Support.ListManager.ProcessingPool
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using System.Reflection;
  using MongoDB.Bson;
  using MongoDB.Driver;
  using MongoDB.Driver.Builders;
  using Sitecore.Analytics.Data.DataAccess.MongoDb;
  using Sitecore.Analytics.Diagnostics.PerformanceCounters;
  using Sitecore.Analytics.Processing.ProcessingPool;
  using Sitecore.Collections;
  using Sitecore.Data;
  using Sitecore.Diagnostics;
  using Sitecore.Events;
  using Sitecore.Support.ListManager.ProcessingPool.Events;

  public class ContactsMongoDbProcessingPool : Sitecore.Analytics.Data.MongoDb.ProcessingPool.MongoDbProcessingPool
  {
    #region Service Types

    protected static class Fields
    {
      public const string _LastAccessed = "LastAccessed";
      public const string _State = "State";
      public const string Attempts = "Attempts";
      public const string Id = "_id";
      public const string Properties = "Properties";
      public const string Scheduled = "Scheduled";
    }

    protected static class CustomFields
    {
      public const string ContactListOperationIds = "ContactListOperationIds";
    }

    protected class Candidate
    {
      private readonly byte[] key;
      private readonly DateTime scheduled;

      [NotNull]
      public byte[] Key
      {
        get
        {
          return this.key;
        }
      }


      public DateTime Scheduled
      {
        get
        {
          return this.scheduled;
        }
      }

      public Candidate([NotNull] byte[] key, DateTime scheduled)
      {
        this.key = key;
        this.scheduled = scheduled;
      }
    }

    #endregion

    #region Fields

    private static readonly MethodInfo miWaitForCollectionReady;

    private Action dWaitForCollectionReady;

    private readonly Random random;

    #endregion

    #region Constructors

    static ContactsMongoDbProcessingPool()
    {
      miWaitForCollectionReady = typeof(Sitecore.Analytics.Data.MongoDb.ProcessingPool.MongoDbProcessingPool).GetMethod("WaitForCollectionReady", BindingFlags.Instance | BindingFlags.NonPublic);
    }

    public ContactsMongoDbProcessingPool(MongoDbCollection pool)
      : base(pool)
    {
      this.random = new Random();
      this.InitializeDelegates();
    }

    public ContactsMongoDbProcessingPool(string connectionStringName)
      : base(connectionStringName)
    {
      this.random = new Random();

      this.InitializeDelegates();
    }

    private void InitializeDelegates()
    {
      dWaitForCollectionReady = miWaitForCollectionReady.CreateDelegate(typeof(Action), this) as Action;
    }

    #endregion

    public override void CancelAll()
    {
      this.Pool.RemoveAll();
      var args = new ProcessingPoolArgs(this.Name);
      Event.RaiseEvent("processingpool:cancelall", args);
    }

    public override ProcessingPoolItem Add(Guid id, bool update = false)
    {
      ProcessingPoolItem item = null;
      if (this.Enabled)
      {
        item = this.CreateItem(id);
        item = this.Add(item, update);
      }

      return item;
    }

    public override ProcessingPoolItem Add(Guid id, TimeSpan delay, bool update = false)
    {
      ProcessingPoolItem item = null;
      if (this.Enabled)
      {
        item = this.CreateItem(id, delay);
        item = this.Add(item, update);
      }

      return item;
    }

    public override bool Add(ProcessingPoolItem workItem, SchedulingOptions options = null)
    {
      Assert.ArgumentNotNull(workItem, "workItem");
      Assert.IsFalse(this.DuplicateKeyStrategy == DuplicateKeyStrategy.Undefined, "Duplicate key strategy for the pool is not defined.");
      try
      {
        workItem.Scheduled = DateTime.UtcNow;
        if (options != null)
        {
          workItem.Scheduled += options.SchedulingDelay;
        }

        bool update = this.DuplicateKeyStrategy == DuplicateKeyStrategy.AllowAndMerge;
        this.Add(workItem, update);
        return true;
      }
      catch (DuplicateProcessingItemException)
      {
        return false;
      }
    }

    protected void PrepareUpdateOperation([NotNull] ProcessingPoolItem item, out IMongoQuery query, out IMongoUpdate update)
    {
      query = Query.EQ("_id", item.Key);

      var builder = new UpdateBuilder();
      builder.Set(Fields.Scheduled, item.Scheduled);
      builder.Set(Fields.Attempts, item.Attempts);

      // The original behavior has been inherited however Properties are merged incorrectly
      BsonValue properties = BsonUtilities.ToBsonValue(typeof(Dictionary<string, string>), item.Properties);
      builder.Set(Fields.Properties, properties);

      var listRelatedPoolItem = item as ListProcessingPoolItem;

      if (listRelatedPoolItem != null && listRelatedPoolItem.BulkOperationIds.Count > 0)
      {
        builder.AddToSetEach(
          CustomFields.ContactListOperationIds,
          listRelatedPoolItem.BulkOperationIds
            .Select(id => BsonUtilities.ToBsonValue(typeof(ID), id)));
      }

      update = builder;
    }

    protected ProcessingPoolItem TryMerge([NotNull] ProcessingPoolItem item)
    {
      IMongoQuery query;
      IMongoUpdate update;
      this.PrepareUpdateOperation(item, out query, out update);

      // TODO Check behavior if it can't update a document
      FindAndModifyResult famr = this.Pool.FindAndModify(query, null, update, true, false);
      if (famr == null || famr.ModifiedDocument == null)
      {
        return null;
      }

      // TODO Validate how it works if a record is not found
      // It is important to use the same processing item as for the 'processingpool:addingitem' item 
      var args = new AddedItemToProcessingPoolArgs(this.Name, item);
      Event.RaiseEvent("processingpool:addeditem", args);
      AnalyticsMongoDbCount.DataAggregationProcessingPoolAdd.Increment();

      return this.Deserialize(famr.ModifiedDocument);
    }

    protected void Insert([NotNull] BsonDocument document)
    {
      try
      {
        this.Pool.Insert(document);

        AnalyticsMongoDbCount.DataAggregationProcessingPoolAdd.Increment();
      }
      catch (WriteConcernException ex)
      {
        if (ex.CommandResult.Code == 11000)
        {
          throw new DuplicateProcessingItemException("The item is already contained in the processing pool.", ex);
        }

        throw;
      }
    }


    [NotNull]
    private ProcessingPoolItem Add([NotNull] ProcessingPoolItem item, bool update)
    {
      Debug.ArgumentNotNull(item, "item");

      if (!this.Enabled)
      {
        return item;
      }

      //this.WaitForCollectionReady();
      this.dWaitForCollectionReady();

      var operationMessage = "updated in";


      var args = new AddingItemToProcessingPoolArgs(this.Name, item);
      Event.RaiseEvent("processingpool:addingitem", args);
      item = args.PoolItem;

      BsonDocument document = this.Serialize(item);

      ProcessingPoolItem freshPoolItem = update ? this.TryMerge(item) : null;

      if (freshPoolItem == null)
      {
        operationMessage = "added to";
        this.Insert(document);
        // It is important to use the same processing item as for the 'processingpool:addingitem' item 
        var args2 = new AddedItemToProcessingPoolArgs(this.Name, item);
        Event.RaiseEvent("processingpool:addeditem", args2);
        freshPoolItem = item;
      }

      var logMessage = string.Format("Item with ID {0} was {1} the MongoDB processing pool.", GetItemIdString(item), operationMessage);
      Log.Debug(logMessage);

      return freshPoolItem;
    }

    private static string GetItemIdString(ProcessingPoolItem item)
    {
      var itemId = string.Empty;

      if (item != null)
      {
        itemId = item.Key.Length == 16 ?
          new Guid(item.Key).ToString() :
          item.Key.ToString();
      }

      return itemId;
    }

    [NotNull]
    private BsonDocument Serialize([NotNull] ProcessingPoolItem item)
    {
      BsonValue properties = BsonUtilities.ToBsonValue(typeof(Dictionary<string, string>), item.Properties);

      var result = new BsonDocument();

      result.Add(Fields.Id, item.Key);
      result.Add(Fields.Scheduled, item.Scheduled);
      result.Add(Fields.Attempts, 0);
      result.Add(Fields.Properties, properties);

      var listRelatedPoolItem = item as ListProcessingPoolItem;

      if (listRelatedPoolItem != null && listRelatedPoolItem.BulkOperationIds.Count > 0)
      {
        BsonValue operationIds = BsonUtilities.ToBsonValue(typeof(Set<ID>), listRelatedPoolItem.BulkOperationIds);
        result.Add(CustomFields.ContactListOperationIds, operationIds);
      }

      return result;
    }

    public override void CheckIn(ProcessingPoolItem item)
    {
      Assert.ArgumentNotNull(item, "item");
      // this.WaitForCollectionReady();
      this.dWaitForCollectionReady();
      IMongoQuery query = Query.EQ("_id", item.Key);
      WriteConcernResult result = this.Pool.Remove(query);

      var args = new RemovedItemFromProcessingPoolArgs(this.Name, item, RemovedItemFromProcessingPoolArgs.ProcessingPoolItemResult.Successful);
      Event.RaiseEvent("processingpool:removeditem", args);

      if (result.Ok && (result.DocumentsAffected == 1L))
      {
        AnalyticsMongoDbCount.DataAggregationProcessingPoolCheckIn.Increment(1L);
      }
    }

    public override ProcessingPoolItem TryCheckOut()
    {
      // this.WaitForCollectionReady();
      this.dWaitForCollectionReady();

      bool exhausted = false;
      ProcessingPoolItem result = null;

      do
      {
        IList<Candidate> candidates = this.GetCandidates();

        if (candidates.Count > 0)
        {
          int index = this.random.Next(candidates.Count);
          Candidate candidate = candidates[index];

          IMongoQuery query = this.GetItemQuery(candidate.Key, candidate.Scheduled);

          TimeSpan delay = TimeSpan.FromSeconds(this.RetryDelay);
          DateTime later = (DateTime.UtcNow + delay);

          UpdateBuilder update = new UpdateBuilder();

          update.Set(Fields.Scheduled, later);
          update.Inc(Fields.Attempts, 1);

          FindAndModifyResult famr = this.Pool.FindAndModify(query, null, update, false, false);

          if ((famr != null) && (famr.ModifiedDocument != null))
          {
            ProcessingPoolItem item = this.Deserialize(famr.ModifiedDocument);

            if (item.Attempts > this.RetryAttempts)
            {
              IMongoQuery removeQuery = this.GetItemQuery(candidate.Key, later);

              var debugMessage = string.Format("Attempts to check an item with ID {0} exceed out the max attempt count.", GetItemIdString(item));
              Log.Debug(debugMessage);
              this.Pool.Remove(removeQuery, RemoveFlags.Single, WriteConcern.Unacknowledged);
              var args = new RemovedItemFromProcessingPoolArgs(this.Name, item, RemovedItemFromProcessingPoolArgs.ProcessingPoolItemResult.MaxAttemptsReached);
              Event.RaiseEvent("processingpool:removeditem", args);
            }
            else
            {
              result = item;
            }
          }
        }
        else
        {
          exhausted = true;
        }
      }
      while ((result == null) && (exhausted == false));

      if (result != null)
      {
        AnalyticsMongoDbCount.DataAggregationProcessingPoolCheckOut.Increment();
      }

      AnalyticsMongoDbCount.DataAggregationProcessingPoolCheckOutCalls.Increment();

      return result;
    }

    private IList<Candidate> GetCandidates()
    {
      // this.WaitForCollectionReady();
      this.dWaitForCollectionReady();

      DateTime utcNow = DateTime.UtcNow;
      IMongoQuery query = Query.LTE("Scheduled", utcNow);
      IMongoSortBy sortBy = SortBy.Ascending(new string[]
      {
        "Scheduled"
      });
      IList<Candidate> list = new List<Candidate>();
      MongoCursor<BsonDocument> cursor = this.Pool.FindAs<BsonDocument>(query);
      if (cursor != null)
      {
        cursor.SetFields(new string[]
        {
          "_id"
        });
        cursor.SetFields(new string[]
        {
          "Scheduled"
        });
        cursor.SetSortOrder(sortBy);
        cursor.SetLimit(0x10);
        foreach (BsonDocument document in cursor)
        {
          byte[] asByteArray = document["_id"].AsByteArray;
          DateTime scheduled = document["Scheduled"].ToUniversalTime();
          Candidate item = new Candidate(asByteArray, scheduled);
          list.Add(item);
        }
      }

      return list;
    }

    private IMongoQuery GetItemQuery(byte[] key, DateTime scheduled)
    {
      IMongoQuery queryKeyMatches = Query.EQ(Fields.Id, key);
      IMongoQuery queryScheduleMatches = Query.EQ(Fields.Scheduled, scheduled);
      return Query.And(queryKeyMatches, queryScheduleMatches);
    }

    [NotNull]
    private ProcessingPoolItem Deserialize([NotNull] BsonDocument document)
    {
      Debug.ArgumentNotNull(document, "document");

      bool hasOpearationIds = document.Contains(CustomFields.ContactListOperationIds);

      // Maybe rework to deserialization pipeline or event
      ProcessingPoolItem result = hasOpearationIds ? new ListProcessingPoolItem() : new ProcessingPoolItem();

      if (document[Fields.Id].IsGuid)
      {
        result.Id = document[Fields.Id].AsGuid;
      }
      else
      {
        result.Key = document[Fields.Id].AsByteArray;
      }

      result.State = ProcessingItemState.New;
      result.LastAccessed = DateTime.UtcNow;

      result.Scheduled = document[Fields.Scheduled].ToUniversalTime();
      result.Attempts = document[Fields.Attempts].ToInt32();

      BsonValue bsonProperties = document[Fields.Properties];

      if (bsonProperties != null)
      {
        Dictionary<string, string> properties = BsonUtilities.FromBsonValue(typeof(Dictionary<string, string>), bsonProperties) as Dictionary<string, string>;

        if (properties != null)
        {
          foreach (var entry in properties)
          {
            result.Properties.Add(entry.Key, entry.Value);
          }
        }
      }

      var clResult = result as ListProcessingPoolItem;

      if (clResult == null)
      {
        return result;
      }

      BsonValue bsonOpearationIds = document[CustomFields.ContactListOperationIds];

      HashSet<ID> ids = BsonUtilities.FromBsonValue(typeof(HashSet<ID>), bsonOpearationIds) as HashSet<ID>;

      if (ids != null)
      {
        foreach (var id in ids)
        {
          clResult.BulkOperationIds.Add(id);
        }
      }

      return clResult;
    }
  }
}
