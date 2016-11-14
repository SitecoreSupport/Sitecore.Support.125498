
namespace Sitecore.Support.ListManager.Contact.Bulk
{
  using System;
  using System.Collections.Concurrent;
  using System.Collections.Generic;
  using System.Reflection;
  using Sitecore.Analytics.Data;
  using Sitecore.Analytics.Data.Bulk;
  using Sitecore.Analytics.Data.Bulk.Contact;
  using Sitecore.Analytics.Data.Bulk.Contact.Pipelines.AfterPersist;
  using Sitecore.Analytics.Data.Bulk.Contact.Pipelines.BeforePersist;
  using Sitecore.Analytics.Data.Bulk.Contact.Pipelines.UpdateFields;
  using Sitecore.Analytics.DataAccess;
  using Sitecore.Analytics.Model;
  using Sitecore.Analytics.Model.Entities;
  using Sitecore.Common;
  using Sitecore.Configuration;
  using Sitecore.Data;
  using Sitecore.Diagnostics;
  using Sitecore.Events;
  using Sitecore.Jobs;

  public class ContactBulkUpdateManager : Sitecore.Analytics.Data.Bulk.Contact.ContactBulkUpdateManager
  {
    private readonly ConcurrentDictionary<ID, string> JobIdsToNames;

    private readonly MethodInfo miGetWorkItems;

    private const string JobName = "CONTACT_BULK_UPDATE";

    public ContactBulkUpdateManager()
    {
      // 1. ContactBulkUpdateManager.JobIdsToNames:
      var fi = typeof(Sitecore.Analytics.Data.Bulk.Contact.ContactBulkUpdateManager).GetField("JobIdsToNames", BindingFlags.NonPublic | BindingFlags.Static);
      Assert.IsNotNull(fi, "Can't find the JobIdsToNames field in ContactBulkUpdateManager");

      // static field can be used in instance field because is marked as readonly.
      this.JobIdsToNames = fi.GetValue(null) as ConcurrentDictionary<ID, string>;

      // 2. BasicKnownContactSet.GetWorkItems()
      var assembly = typeof(Sitecore.Analytics.Data.Bulk.Contact.KnownContactSet).Assembly;
      var typeBasicKnownContactSet = assembly.GetType("Sitecore.Analytics.Data.Bulk.Contact.BasicKnownContactSet");
      Assert.IsNotNull(typeBasicKnownContactSet, string.Format("Can't find the BasicKnownContactSet type in '{0}' assembly", assembly.FullName));

      this.miGetWorkItems = typeBasicKnownContactSet.GetMethod("GetWorkItems", BindingFlags.NonPublic | BindingFlags.Instance);
      Assert.IsNotNull(this.miGetWorkItems, "Can't find the GetWorkItems method in BasicKnownContactSet");
    }

    [UsedImplicitly]
    [NotNull]
    protected List<IContactUpdateResult> ProcessSetWithLists([NotNull] KnownContactSet set, [NotNull] string jobName, [NotNull] string contextData, [NotNull] ID operationId)
    {
      Assert.ArgumentNotNull(set, "set");
      Assert.ArgumentNotNull(jobName, "jobName");
      Assert.ArgumentNotNull(contextData, "contextData");
      Assert.ArgumentNotNull(operationId, "operationId");

      // REPLACED: var workItems = ((BasicKnownContactSet)set).GetWorkItems();
      var workItems = this.miGetWorkItems.Invoke(set, null) as IReadOnlyList<IContactTemplate>;

      var currentJob = Assert.ResultNotNull(JobManager.GetJob(jobName), "Job with name " + jobName + " not found during ProcessSet call");
      currentJob.Status.State = JobState.Running;

      var results = currentJob.Status.Result as List<IContactUpdateResult>;
      Assert.IsNotNull(results, "job should be initialized with a list of results");
      var repository = Factory.CreateObject("tracking/contactRepository", true) as ContactRepositoryBase;
      Assert.IsNotNull(repository, "failed to retrieve tracking/contactRepository");

      var totalQueuedRecords = 0;

      var args1 = new ContactBulkUpdateArgs(operationId, 0);
      Event.RaiseEvent("contactbulkupdate:batchprocessing", args1);

      try
      {
        foreach (var incomingContact in workItems)
        {
          try
          {
            var identifier = incomingContact.Identifiers.Identifier;
            if (string.IsNullOrEmpty(identifier))
            {
              // TODO Warn about skipped contact
              continue;
            }

            Sitecore.Analytics.Tracking.Contact destinationContact;

            var leaseOwner = new LeaseOwner(JobName, LeaseOwnerType.OutOfRequestWorker);

            var loadResult = repository.TryLoadContact(identifier, leaseOwner, TimeSpan.FromMinutes(1));

            switch (loadResult.Status)
            {
              case LockAttemptStatus.NotFound:
                destinationContact = repository.CreateContact(ID.NewID);
                destinationContact.Identifiers.Identifier = identifier;
                break;
              case LockAttemptStatus.Success:
                destinationContact = loadResult.Object;
                break;
              default:
                throw new NotImplementedException("Handling of collection database locking failures is not yet implemented.");
            }

            Assert.IsNotNull(destinationContact, "unexpected null contact after load/create"); // repository.CreateContact and Successful TryLoadContact must not return a null contact.

            BeforePersistArgs beforePersistArgs;
            try
            {
              // var updateFieldsArgs = new UpdateFieldsArgs(destinationContact, incomingContact, contextData);
              var updateFieldsArgs = CreateInstance<UpdateFieldsArgs>(destinationContact, incomingContact, contextData);

              UpdateFieldsPipeline.Run(updateFieldsArgs);

              if (updateFieldsArgs.Aborted)
              {
                throw new NotImplementedException("Handling of aborted pipelines not yet implemented.");
              }

              // beforePersistArgs = new BeforePersistArgs(updateFieldsArgs.Contact, contextData);
              beforePersistArgs = CreateInstance<BeforePersistArgs>(updateFieldsArgs.Contact, contextData);

              BeforePersistPipeline.Run(beforePersistArgs);

              if (beforePersistArgs.Aborted)
              {
                throw new NotImplementedException("Handling of aborted pipelines not yet implemented.");
              }
            }
            catch (Exception)
            {
              if (loadResult.Status == LockAttemptStatus.Success)
              {
                repository.ReleaseContact(destinationContact.ContactId, leaseOwner);
              }

              throw;
            }

            var success = false;

            // It is better to wrap a whole foreach in the context by design. However to decrease load to IDTable the scope has been minimized  
            var context = new ContextListOperationSwitcher(operationId);

            try
            {
              // var success = repository.SaveContact(beforePersistArgs.Contact, leaseOwner, true);
              success = repository.SaveContact(beforePersistArgs.Contact, new ContactSaveOptions(true, leaseOwner, null));
            }
            finally
            {
              var contextListOperation = ContextListOperationSwitcher.CurrentValue;

              if (contextListOperation == null)
              {
                Log.Warn("SUPPORT Context List Operation is null", this);
              }
              else
              {
                totalQueuedRecords += contextListOperation.ProcessedItemsCount;
              }

              context.Dispose();
            }

            if (success)
            {
              var afterPersistArgs = new AfterPersistArgs(beforePersistArgs.Contact, contextData);
              AfterPersistPipeline.Run(afterPersistArgs);

              if (afterPersistArgs.Aborted)
              {
                throw new NotImplementedException("Handling of aborted pipelines not yet implemented.");
              }

              currentJob.Status.Processed++;
              results.Add(new ContactUpdateSuccess(WorkItemStatus.Succeeded, afterPersistArgs.Contact.ContactId.ToID(), identifier));
            }
            else
            {
              throw new NotImplementedException("Handling of collection database saving failures is not yet implemented.");
            }
          }
          catch (Exception ex)
          {
            currentJob.Status.Processed++;
            results.Add(new ContactUpdateFailure(WorkItemStatus.Failed, ex, incomingContact));
          }
        }
      }
      finally
      {
        var args2 = new ContactBulkUpdateArgs(operationId, totalQueuedRecords);
        Event.RaiseEvent("contactbulkupdate:batchprocessed", args2);
      }

      currentJob.Status.State = JobState.Finished;
      var knownContactSetManager = new KnownContactSetManager();
      set.Complete();
      knownContactSetManager.Delete(set);

      set.Dispose();
      set = null;

      return results;
    }

    // Original behavior except replaced ProcessSetWithLists method name in the job definition
    public override IBulkOperation<IContactUpdateResult> SubmitWorkItemSet(KnownContactSet set, string contextData = "")
    {
      Assert.ArgumentNotNull(set, "set");
      Assert.ArgumentNotNull(contextData, "contextData");

      var jobName = MakeJobName(set.Name);

      if (JobExists(jobName))
      {
        throw new InvalidOperationException(string.Format("Job with the name \"{0}\" already exists.", jobName));
      }

      var jobId = ID.NewID;

      var jobOptions = new JobOptions(jobName, "Contact Bulk Update", "system", new ContactBulkUpdateManager(), "ProcessSetWithLists", new object[] { set, jobName, contextData, jobId })
      {
        AfterLife = TimeSpan.FromHours(42)
      };

      var job = new Job(jobOptions);
      job.Status.Result = new List<IContactUpdateResult>();
      JobManager.Start(job);

      this.JobIdsToNames.TryAdd(jobId, jobName);

      return this.RetrieveBulkOperation(jobId);
    }

    protected static T CreateInstance<T>(params object[] p) where T : class
    {
      return Activator.CreateInstance(typeof(T), BindingFlags.Instance | BindingFlags.NonPublic, null, p, null) as T;
    }

    // Original behavior
    private static string MakeJobName(string setName)
    {
      Assert.ArgumentNotNull(setName, "setName");
      return JobName + setName;
    }

    // Original behavior
    private static bool JobExists(string jobName)
    {
      Assert.ArgumentNotNull(jobName, "jobName");
      return JobManager.GetJob(jobName) != null;
    }
  }
}