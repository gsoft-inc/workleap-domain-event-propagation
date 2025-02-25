using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Workleap.DomainEventPropagation.EventGridClientAdapter;

using EventBundle = Workleap.DomainEventPropagation.EventGridClientAdapter.EventGridClientAdapter.EventBundle;

namespace Workleap.DomainEventPropagation;

internal sealed class EventPullerService : BackgroundService
{
    private readonly List<EventGridSubscriptionEventPuller> _eventGridSubscriptionPullers;

    public EventPullerService(
        IServiceScopeFactory serviceScopeFactory,
        IEnumerable<EventGridClientDescriptor> clientDescriptors,
        IEventGridClientWrapperFactory eventGridClientWrapperFactory,
        IOptionsMonitor<EventPropagationSubscriptionOptions> optionsMonitor,
        ILogger<EventPullerService> logger)
    {
        this._eventGridSubscriptionPullers = clientDescriptors.Select(descriptor =>
                new EventGridSubscriptionEventPuller(
                    new EventGridTopicSubscription(
                        optionsMonitor.Get(descriptor.Name).TopicName,
                        optionsMonitor.Get(descriptor.Name).SubscriptionName,
                        optionsMonitor.Get(descriptor.Name).MaxDegreeOfParallelism,
                        optionsMonitor.Get(descriptor.Name).MaxRetries,
                        optionsMonitor.Get(descriptor.Name).RetryDelays?.ToList(),
                        eventGridClientWrapperFactory.CreateClient(descriptor.Name)),
                    serviceScopeFactory,
                    logger))
            .ToList();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var taskCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var subscriptionTasks = this._eventGridSubscriptionPullers
            .Select(puller => Task.Run(() => puller.StartReceivingEventsAsync(taskCancellationSource.Token), taskCancellationSource.Token))
            .ToList();

        while (subscriptionTasks.Any())
        {
            var task = await Task.WhenAny(subscriptionTasks).ConfigureAwait(false);

            subscriptionTasks.Remove(task);

            try
            {
                // This will throw the exception if there is one, otherwise we'll continue until all tasks are done
                await task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // To prevent unobserved exceptions, we cancel the tasks and wait for them to complete before throwing
                await taskCancellationSource.CancelAsync().ConfigureAwait(false);
                await Task.WhenAll(subscriptionTasks).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

                throw;
            }
        }
    }

    private class EventGridSubscriptionEventPuller
    {
        // At the moment the Release api only supports these delays
        // See https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.eventgrid.namespaces.eventgridclient.releasecloudeventsasync?view=azure-dotnet-preview#azure-messaging-eventgrid-namespaces-eventgridclient-releasecloudeventsasync(system-string-system-string-azure-core-requestcontent-system-nullable((system-int32))-azure-requestcontext)
        private static readonly TimeSpan[] SupportedReleaseDelays = [TimeSpan.FromSeconds(0), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(600), TimeSpan.FromSeconds(3600)];

        private const int OutputChannelSize = 5000;
        private const int MaxEventRequestSize = 100;

        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly ILogger<EventPullerService> _logger;
        private readonly EventGridTopicSubscription _eventGridTopicSubscription;

        private readonly Dictionary<int, TimeSpan>? _retryDelays;
        private readonly TimeSpan? _defaultDelay;

        private readonly Channel<EventBundle> _acknowledgeEventChannel = Channel.CreateBounded<EventBundle>(OutputChannelSize);
        private readonly Channel<EventBundle> _releaseEventChannel = Channel.CreateBounded<EventBundle>(OutputChannelSize);
        private readonly Channel<EventBundle> _rejectEventChannel = Channel.CreateBounded<EventBundle>(OutputChannelSize);

        private int _handlersInProgress;
        private TaskCompletionSource _taskCompletionSource = new();

        public EventGridSubscriptionEventPuller(
            EventGridTopicSubscription eventGridTopicSubscription,
            IServiceScopeFactory serviceScopeFactory,
            ILogger<EventPullerService> logger)
        {
            this._serviceScopeFactory = serviceScopeFactory;
            this._logger = logger;
            this._eventGridTopicSubscription = eventGridTopicSubscription;

            if (eventGridTopicSubscription.RetryDelays is { Count: > 0 })
            {
                this._retryDelays = eventGridTopicSubscription.RetryDelays
                    .Select((x, index) => new { DeliveryCount = index + 1, Delay = x })
                    .ToDictionary(x => x.DeliveryCount, x => x.Delay);

                this._defaultDelay = eventGridTopicSubscription.RetryDelays.Last();
            }
        }

        public async Task StartReceivingEventsAsync(CancellationToken cancellationToken)
        {
            // Use a local cancellation token source to allow stopping the events reception/processing if a callback task crashes
            using var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            // Start the tasks that will process events and handle the completion callback
            Task[] eventProcessingTasks =
            [
                this.ProcessEvents(cancellationTokenSource.Token),
                    this.AcknowledgeEvents(cancellationToken),
                    this.ReleaseEvents(cancellationToken),
                    this.RejectEvents(cancellationToken)
            ];

            await Task.WhenAny(eventProcessingTasks).ConfigureAwait(false);

            // If any task completed, we want to stop receiving/processing events, flush the channels and then throw (if needed)
            await cancellationTokenSource.CancelAsync().ConfigureAwait(false);
            this.CompleteEventChannels();

            // Since the event processing task was cancelled and the callback channels completed, all the tasks should complete shortly
            await Task.WhenAll(eventProcessingTasks).ConfigureAwait(false);
        }

        private Task ProcessEvents(CancellationToken cancellationToken)
        {
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = this._eventGridTopicSubscription.MaxHandlerDop, CancellationToken = cancellationToken };

            return Parallel.ForEachAsync(this.StreamEventGridEvents(cancellationToken), parallelOptions, async (bundle, ctx) =>
            {
                this.SignalHandlerStarting();

                try
                {
#pragma warning disable CA2007 // Consider calling ConfigureAwait on the awaited task
                    await using var scope = this._serviceScopeFactory.CreateAsyncScope();
#pragma warning restore CA2007 // Consider calling ConfigureAwait on the awaited task
                    var cloudEventHandler = scope.ServiceProvider.GetRequiredService<ICloudEventHandler>();

                    await this.HandleBundleAsync(cloudEventHandler, bundle, ctx).ConfigureAwait(false);
                }
                finally
                {
                    this.SignalHandlerCompleted();
                }
            });
        }

        private async IAsyncEnumerable<EventBundle> StreamEventGridEvents([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await this.WaitForAvailableHandler(cancellationToken).ConfigureAwait(false);

                var availableHandlers = this._eventGridTopicSubscription.MaxHandlerDop - this._handlersInProgress;
                if (availableHandlers <= 0)
                {
                    continue;
                }

                var bundles = await this._eventGridTopicSubscription.Client.ReceiveCloudEventsAsync(this._eventGridTopicSubscription.TopicName, this._eventGridTopicSubscription.SubscriptionName, Math.Min(availableHandlers, MaxEventRequestSize), cancellationToken).ConfigureAwait(false);

                foreach (var bundle in bundles)
                {
                    yield return bundle;
                }
            }
        }

        private async Task HandleBundleAsync(ICloudEventHandler cloudEventHandler, EventBundle eventBundle, CancellationToken cancellationToken)
        {
            try
            {
                await cloudEventHandler.HandleCloudEventAsync(eventBundle.Event, cancellationToken).ConfigureAwait(false);
                await this._acknowledgeEventChannel.Writer.WriteAsync(eventBundle, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                switch (ex)
                {
                    case DomainEventTypeNotRegisteredException:
                    case CloudEventSerializationException:
                    case DomainEventHandlerNotRegisteredException:
                        this._logger.EventWillBeRejected(eventBundle.Event.Id, eventBundle.Event.Type, ex);
                        await this._rejectEventChannel.Writer.WriteAsync(eventBundle, cancellationToken).ConfigureAwait(false);
                        break;
                    case not null when eventBundle.DeliveryCount > this._eventGridTopicSubscription.MaxRetriesCount:
                        this._logger.EventWillBeRejectedDueToMaxRetries(eventBundle.Event.Id, eventBundle.Event.Type, ex);
                        await this._rejectEventChannel.Writer.WriteAsync(eventBundle, cancellationToken).ConfigureAwait(false);
                        break;
                    default:
                        this._logger.EventWillBeReleased(eventBundle.Event.Id, eventBundle.Event.Type, ex!);
                        await this._releaseEventChannel.Writer.WriteAsync(eventBundle, cancellationToken).ConfigureAwait(false);
                        break;
                }
            }
        }

        private async Task AcknowledgeEvents(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && !this._acknowledgeEventChannel.Reader.Completion.IsCompleted)
            {
                await this._acknowledgeEventChannel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);

                var lockTokens = ReadCurrentContent(this._acknowledgeEventChannel).Select(x => x.LockToken);
                await this._eventGridTopicSubscription.Client.AcknowledgeCloudEventsAsync(this._eventGridTopicSubscription.TopicName, this._eventGridTopicSubscription.SubscriptionName, lockTokens, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task ReleaseEvents(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && !this._releaseEventChannel.Reader.Completion.IsCompleted)
            {
                await this._releaseEventChannel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);

                var eventBundlesByDelay = ReadCurrentContent(this._releaseEventChannel)
                    .GroupBy(x => this.GetReleaseDelay(x.DeliveryCount));

                foreach (var eventBundle in eventBundlesByDelay)
                {
                    var lockTokens = eventBundle.Select(x => x.LockToken);
                    await this._eventGridTopicSubscription.Client.ReleaseCloudEventsAsync(this._eventGridTopicSubscription.TopicName, this._eventGridTopicSubscription.SubscriptionName, lockTokens, eventBundle.Key, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task RejectEvents(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && !this._rejectEventChannel.Reader.Completion.IsCompleted)
            {
                await this._rejectEventChannel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);

                var lockTokens = ReadCurrentContent(this._rejectEventChannel).Select(x => x.LockToken);
                await this._eventGridTopicSubscription.Client.RejectCloudEventsAsync(this._eventGridTopicSubscription.TopicName, this._eventGridTopicSubscription.SubscriptionName, lockTokens, cancellationToken).ConfigureAwait(false);
            }
        }

        private void CompleteEventChannels()
        {
            this._acknowledgeEventChannel.Writer.TryComplete();
            this._releaseEventChannel.Writer.TryComplete();
            this._rejectEventChannel.Writer.TryComplete();
        }

        private void SignalHandlerStarting()
        {
            Interlocked.Increment(ref this._handlersInProgress);
        }

        private void SignalHandlerCompleted()
        {
            Interlocked.Decrement(ref this._handlersInProgress);
            lock (this._eventGridTopicSubscription)
            {
                this._taskCompletionSource.TrySetResult();
                this._taskCompletionSource = new TaskCompletionSource();
            }
        }

        private ValueTask WaitForAvailableHandler(CancellationToken cancellationToken)
        {
            lock (this._eventGridTopicSubscription)
            {
                if (this._handlersInProgress < this._eventGridTopicSubscription.MaxHandlerDop)
                {
                    return ValueTask.CompletedTask;
                }

                return new ValueTask(this._taskCompletionSource.Task.WaitAsync(cancellationToken));
            }
        }

        private TimeSpan GetReleaseDelay(int deliveryCount)
        {
            var delay = this._retryDelays?.GetValueOrDefault(deliveryCount, this._defaultDelay!.Value) ?? TimeSpan.FromSeconds((int)Math.Min(Math.Pow(2, deliveryCount - 1), int.MaxValue));
            return SupportedReleaseDelays.FirstOrDefault(x => delay <= x, SupportedReleaseDelays.Last());
        }

        private static IEnumerable<EventBundle> ReadCurrentContent(Channel<EventBundle> channel)
        {
            var maxResultCount = Math.Min(channel.Reader.Count, MaxEventRequestSize);
            var resultCounter = 0;

            while (channel.Reader.TryRead(out var result))
            {
                yield return result;

                if (++resultCounter >= maxResultCount)
                {
                    break;
                }
            }
        }
    }

    private record EventGridTopicSubscription(string TopicName, string SubscriptionName, int MaxHandlerDop, int MaxRetriesCount, List<TimeSpan>? RetryDelays, IEventGridClientAdapter Client);
}