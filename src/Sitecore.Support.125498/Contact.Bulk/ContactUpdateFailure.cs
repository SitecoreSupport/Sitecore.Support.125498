
namespace Sitecore.Support.ListManager.Contact.Bulk
{
  using System;
  using Sitecore.Analytics.Data.Bulk;
  using Sitecore.Analytics.Data.Bulk.Contact;
  using Sitecore.Analytics.Model.Entities;
  using Sitecore.Diagnostics;

  // Original implementation 
  public class ContactUpdateFailure : IContactUpdateFailure
  {
    public ContactUpdateFailure(WorkItemStatus status, [NotNull] Exception exception, [NotNull] IContactTemplate contact)
    {
      Assert.ArgumentNotNull(exception, "exception");
      Assert.ArgumentNotNull(contact, "contact");

      this.Contact = contact;
      this.Exception = exception;
      this.Status = status;
    }

    public WorkItemStatus Status { get; private set; }

    public Exception Exception { get; private set; }

    public IContactTemplate Contact { get; private set; }
  }
}
