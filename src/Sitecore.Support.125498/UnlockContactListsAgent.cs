
namespace Sitecore.Support.ListManager
{
  using System.Collections.Generic;
  using System.Globalization;
  using System.Linq;
  using Sitecore.Analytics.Core;
  using Sitecore.Analytics.Data.Bulk;
  using Sitecore.Analytics.Data.Bulk.Contact;
  using Sitecore.Analytics.Model.Entities;
  using Sitecore.Configuration;
  using Sitecore.ContentSearch.Maintenance;
  using Sitecore.ContentSearch.Utilities;
  using Sitecore.Data;
  using Sitecore.Data.Fields;
  using Sitecore.Data.Items;
  using Sitecore.Diagnostics;
  using Sitecore.ListManagement.Configuration;
  using Sitecore.ListManagement.ContentSearch;
  using Sitecore.ListManagement.ContentSearch.Model;
  using Sitecore.SecurityModel;
  using Sitecore.Support.ListManager.Locking;

  public class UnlockContactListsAgent : Agent
  {
    private readonly Sitecore.ListManagement.ListManager<ContactList, ContactData> listManager;

    private readonly BatchIdMapper mapper;

    private readonly BulkOperationManager<IContactTemplate, KnownContactSet, IContactUpdateResult> operationManager;

    protected readonly IListOperationsCountingLock contactListLock;

    static UnlockContactListsAgent()
    {
      EventHub.IndexingStopping += delegate { Disabled = true; };
    }

    public static bool Disabled { get; protected set; }

    public UnlockContactListsAgent(
      [NotNull] Sitecore.ListManagement.ListManager<ContactList, ContactData> listManager,
      [NotNull] BatchIdMapper mapper,
      [NotNull] BulkOperationManager<IContactTemplate, KnownContactSet, IContactUpdateResult> operationManager)
    {
      Assert.ArgumentNotNull(listManager, "listManager");
      Assert.ArgumentNotNull(mapper, "mapper");
      Assert.ArgumentNotNull(operationManager, "operationManager");

      this.listManager = listManager;
      this.mapper = mapper;
      this.operationManager = operationManager;
      this.contactListLock = Factory.CreateObject("contactListManagement/contactListCountingLock", true) as IListOperationsCountingLock;

      Disabled = !Sitecore.Configuration.Settings.Analytics.Enabled;
    }

    public override void Execute()
    {
      if (Disabled)
      {
        return;
      }

      using (new SecurityDisabler())
      {
        foreach (var contactList in this.listManager.GetLockedLists())
        {
          var operationIds = this.mapper.MapItemContentsToBulkOperationsId(contactList).Select(ID.Parse).ToHashSet();
          if (!operationIds.Any())
          {
            continue;
          }

          if (this.operationManager.RetrieveBulkOperations().Any(o => operationIds.Contains(o.Id) && o.Status == BulkOperationStatus.Processing))
          {
            continue;
          }

          /*
          if (this.listManager.GetContacts(contactList).Count() != contactList.Recipients)
          {
            continue;
          } */

          var releasedOperationIds = operationIds.Where(opId => !this.contactListLock.IsRegistered(opId)).ToArray();

          if (releasedOperationIds.Length < operationIds.Count)
          {
            this.RemoveBulkOperationsIds(this.mapper, contactList, releasedOperationIds);
            continue;
          }
         
          var lockContext = this.listManager.GetLock(contactList);
          if (lockContext == null)
          {
            continue;
          }

          this.RemoveBulkOperationsIds(this.mapper, contactList, releasedOperationIds);
          this.UpdateRecipients(contactList);
          this.listManager.Unlock(lockContext);
        }
      }
    }
       
    protected virtual void UpdateRecipients(ContactList contactList)
    {
      Assert.ArgumentNotNull(contactList, "source");

      using (new SecurityDisabler())
      {
        var database = Database.GetDatabase(ListManagementSettings.Database);

        Item item = database.GetItem(contactList.Id);
        if (item == null)
        {
          Log.Warn(string.Format("SUPPORT Can't find a contact list {0}", contactList.Id), this);
          return;
        }

        var contactsCount = this.listManager.GetContacts(contactList).Count();

        using (new EditContext(item))
        {
          item[ItemField.Recipients] = contactsCount.ToString(CultureInfo.InvariantCulture);
        }
      }
    }

    public void RemoveBulkOperationsIds(BatchIdMapper batchIdMapper, ContactList contactList, IEnumerable<ID> operationIds)
    {
      Assert.ArgumentNotNull(batchIdMapper, "batchIdMapper");
      Assert.ArgumentNotNull(operationIds, "operationIds");

      using (new SecurityDisabler())
      {
        Item item = batchIdMapper.Database.GetItem(contactList.Id);

        if (item == null)
        {
          Log.Warn(string.Format("SUPPORT Can't find a contact list {0}", contactList.Id), this);
          return;
        }

        MultilistField field = item.Fields[ItemField.BulkOperationsId];

        if (field == null)
        {
          Log.Warn("SUPPORT Can't find the BulkOperationsId field.", this);
          return;
        }

        using (new EditContext(item))
        {
          foreach (var operationId in operationIds)
          {
            field.Remove(operationId.ToString());
          }
        }
      }
    }
  }
}