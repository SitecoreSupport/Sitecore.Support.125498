
namespace Sitecore.Support.ListManager
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using System.Reflection;
  using Sitecore.ContentSearch;
  using Sitecore.Diagnostics;
  using Sitecore.Events;
  using Sitecore.Jobs;

  public class ContactListIndexObserver
  {
    protected static readonly Func<string, string> dGetJobName;

    protected DateTime lastStarted;

    protected DateTime lastCompleteStarted;

    protected DateTime lastCompleteEnded;

    protected readonly object locker = new object();

    protected readonly ISearchIndex index;

    protected readonly IProviderCrawler[] trackedCrawlers;

    public event EventHandler<JobFinishedEventArgs> IndexJobFinished;

    public bool IndexHadUpdate { get; protected set; }

    static ContactListIndexObserver()
    {
      var mi = typeof(Sitecore.ContentSearch.Maintenance.IndexCustodian).GetMethod("GetJobName", BindingFlags.Static | BindingFlags.NonPublic);
      dGetJobName = mi.CreateDelegate(typeof(Func<string, string>)) as Func<string, string>;
    } 

    public ContactListIndexObserver(ISearchIndex index, Func<IProviderCrawler, bool> crawlersFilter = null)
    {
      Assert.ArgumentNotNull(index, "index");

      this.index = index;
      this.IndexHadUpdate = false;

      IEnumerable<IProviderCrawler> crawlers = index.Crawlers;
      if (crawlersFilter != null)
      {
        crawlers = crawlers.Where(crawlersFilter);
      }

      this.trackedCrawlers = crawlers.ToArray();

      this.InitializeSubscribers();
    }

    public void GetTimeOfLastCompleteJob(out DateTime start, out DateTime end)
    {
      lock (this.locker)
      {
        start = this.lastCompleteStarted;
        end = this.lastCompleteEnded;
      }
    }

    public bool IsIndexJobInQueue()
    {
      var jobName = dGetJobName(this.index.Name);
      return JobManager.IsJobRunning(jobName) || JobManager.IsJobQueued(jobName);
    }

    public bool IsIndexUpdating()
    {
      return JobManager.IsJobRunning(dGetJobName(this.index.Name));
    }

    public bool HasDataToProcess()
    {
      return this.trackedCrawlers.Any(crawler => crawler.HasItemsToIndex());
    }

    protected virtual void InitializeSubscribers()
    {
      Event.Subscribe("job:started", this.JobStartedHandler);
      Event.Subscribe("job:ended", this.JobEndedHandler);
    }

    protected void JobEndedHandler(object sender, EventArgs e)
    {
      var args = this.ParseArgs<JobFinishedEventArgs>(e);
      Assert.IsNotNull(args, "args are null");

      var jobName = dGetJobName(this.index.Name);

      if (!string.Equals(args.Job.Name, jobName, StringComparison.InvariantCultureIgnoreCase))
      {
        return;
      }

      lock (this.locker)
      {
        this.lastCompleteEnded = DateTime.UtcNow;
        this.lastCompleteStarted = this.lastStarted;
      }

      this.IndexHadUpdate = true;

      if (this.IndexJobFinished != null)
      {
        this.IndexJobFinished(this, args);
      }
    }

    protected void JobStartedHandler(object sender, EventArgs e)
    {
      var args = this.ParseArgs<JobStartedEventArgs>(e);
      Assert.IsNotNull(args, "args are null");

      var jobName = dGetJobName(this.index.Name);

      if (!string.Equals(args.Job.Name, jobName, StringComparison.InvariantCultureIgnoreCase))
      {
        return;
      }

      lock (this.locker)
      {
        this.lastStarted = DateTime.UtcNow;
      }
    }

    protected T ParseArgs<T>(EventArgs e) where T : System.EventArgs
    {
      if (e is SitecoreEventArgs)
      {
        return Event.ExtractParameter<T>(e, 0);
      }

      return null;
    }
  }
}