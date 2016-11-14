
namespace Sitecore.Support.ListManager.Contact.Bulk
{
  using Sitecore.Analytics.Data.Bulk;
  using Sitecore.Analytics.Data.Bulk.Contact;
  using Sitecore.Data;
  using Sitecore.Diagnostics;

  // Original implementation
  public class ContactUpdateSuccess : IContactUpdateSuccess
  {
    public ContactUpdateSuccess(WorkItemStatus status, [NotNull] ID id, [NotNull] string identifier)
    {
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(identifier, "identifier");

      this.Identifier = identifier;
      this.Id = id;
      this.Status = status;
    }

    public WorkItemStatus Status { get; private set; }

    public ID Id { get; private set; }

    public string Identifier { get; private set; }
  }
}
