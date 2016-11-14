
namespace Sitecore.Support.ListManager
{
  using Sitecore.Common;
  using Sitecore.Data;

  public class ContextListOperation
  {
    public ContextListOperation(ID listOperationID)
    {
      this.ListOperationID = listOperationID;
      this.ProcessedItemsCount = 0;
    }

    public ID ListOperationID { get; private set; }

    public int ProcessedItemsCount { get; set; }
  }

  public class ContextListOperationSwitcher : Switcher<ContextListOperation>
  {
    public ContextListOperationSwitcher(ID listOperationID) : base(new ContextListOperation(listOperationID))
    {
    }
  }
}