using Azure.Messaging;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Workleap.DomainEventPropagation.EventGridClientAdapter;
using Workleap.DomainEventPropagation.Subscription.PullDelivery.Tests.TestExtensions;

using EventBundle = Workleap.DomainEventPropagation.EventGridClientAdapter.EventGridClientAdapter.EventBundle;

namespace Workleap.DomainEventPropagation.Subscription.PullDelivery.Tests;

public class EventPullerServiceTests : IDisposable
{
    private static readonly TimeSpan[] SupportedReleaseDelays = [TimeSpan.FromSeconds(0), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(600), TimeSpan.FromSeconds(3600)];

    private readonly List<EventPullerClient> _clients = [];
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IEventGridClientWrapperFactory _eventGridClientWrapperFactory;
    private readonly IOptionsMonitor<EventPropagationSubscriptionOptions> _optionsMonitor;
    private readonly TaskCompletionSource _eventCompletionSource = new();

    private EventPullerService? _pullerService;
    private int _eventCounter;

    public EventPullerServiceTests()
    {
        this._scopeFactory = A.Fake<IServiceScopeFactory>();
        this._eventGridClientWrapperFactory = A.Fake<IEventGridClientWrapperFactory>();
        this._optionsMonitor = A.Fake<IOptionsMonitor<EventPropagationSubscriptionOptions>>();
    }

    [Fact]
    public async Task GivenClientFailingOnReceiving_WhenErrorOccured_ThenBubbleUpException()
    {
        // Given
        var failingClient = this.GivenClient("client1");
        var functionalClient = this.GivenClient("client2");

        this.GivenClientFailsReceivingEvents(failingClient);
        var events = this.GivenEventsForClient(functionalClient, GenerateEvent());

        // When
        await Assert.ThrowsAsync<LocalTestException>(this.WhenRunningPullerService);

        // Then
        this.ThenClientReceivedEvents(failingClient);
        this.ThenClientHandledEvents(functionalClient, events);
    }

    [Fact]
    public async Task GivenClientFailingOnAcknowledgeCallback_WhenErrorOccured_ThenBubbleUpException()
    {
        // Given
        var client = this.GivenClient();

        this.GivenClientFailsAcknowledgingEvents(client);
        this.GivenEventsForClient(client, GenerateEvent());

        // When
        await Assert.ThrowsAsync<LocalTestException>(this.WhenRunningPullerService);

        // Then
        this.ThenClientReceivedEvents(client);
    }

    [Fact]
    public async Task GivenClientFailingOnReleaseCallback_WhenErrorOccured_ThenBubbleUpException()
    {
        // Given
        var client = this.GivenClient();

        this.GivenClientFailsReleasingEvents(client);
        this.GivenEventsForClient(client, GenerateEvent());
        this.GivenClientFailsHandlingEvents(client);

        // When
        await Assert.ThrowsAsync<LocalTestException>(this.WhenRunningPullerService);

        // Then
        this.ThenClientReceivedEvents(client);
    }

    [Fact]
    public async Task GivenClientFailingOnRejectCallback_WhenErrorOccured_ThenBubbleUpException()
    {
        // Given
        var client = this.GivenClient(options: new EventPropagationSubscriptionOptions { MaxRetries = 2 });

        this.GivenClientFailsRejectingEvents(client);
        this.GivenEventsForClient(client, GenerateEvent(deliveryCount: 4));
        this.GivenClientFailsHandlingEvents(client);

        // When
        await Assert.ThrowsAsync<LocalTestException>(this.WhenRunningPullerService);

        // Then
        this.ThenClientReceivedEvents(client);
    }

    [Fact]
    public async Task GivenTwoEventReceived_WhenHandleSuccessfully_ThenEveryEventsAreHandled()
    {
        // Given
        var client = this.GivenClient();
        var events = this.GivenEventsForClient(client, GenerateEvent(), GenerateEvent());

        // When
        await this.WhenRunningPullerService();

        // Then
        this.ThenClientHandledEvents(client, events);
    }

    [Fact]
    public async Task GivenTwoEventsReceived_WhenHandleSuccessfully_ThenEventsAreAcknowledged()
    {
        // Given
        var client = this.GivenClient();
        var events = this.GivenEventsForClient(client, GenerateEvent(), GenerateEvent());

        // When
        await this.WhenRunningPullerService();

        // Then
        this.ThenClientAcknowledgedEvents(client, events);
    }

    [Fact]
    public async Task GivenTwoEventsReceived_WhenHandleThrowUnhandledException_ThenEventsAreReleased()
    {
        // Given
        var maxRetries = 3;
        var client = this.GivenClient(options: GenerateOptions(
            maxRetries: maxRetries
        ));
        var events = this.GivenEventsForClient(client, GenerateEvent(), GenerateEvent(deliveryCount: maxRetries));
        this.GivenClientFailsHandlingEvents(client);

        // When
        await this.WhenRunningPullerService();

        // Then
        this.ThenClientReleasedEvents(client, events);
    }

    [Fact]
    public async Task GivenMultipleEventsReceivedWithCustomRetryDelays_WhenHandleThrowUnhandledException_ThenEventsAreReleasedWithDelay()
    {
        // Given
        var client = this.GivenClient(options: GenerateOptions(
            maxRetries: 10,
            retryDelays: [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(75)])
        );
        var events = this.GivenEventsForClient(client, GenerateEvent(), GenerateEvent(deliveryCount: 3), GenerateEvent(deliveryCount: 4), GenerateEvent(deliveryCount: 5));
        this.GivenClientFailsHandlingEvents(client);

        // When
        await this.WhenRunningPullerService();

        // Then
        this.ThenClientReleasedEvents(client, events);
    }

    [Theory]
    [MemberData(nameof(RejectingExceptions))]
    public async Task GivenTwoEventsReceived_WhenHandleThrowRejectingException_ThenEventsAreRejected(Exception exception)
    {
        // Given
        var client = this.GivenClient();
        var events = this.GivenEventsForClient(client, GenerateEvent(), GenerateEvent());
        this.GivenClientFailsHandlingEvents(client, exception);

        // When
        await this.WhenRunningPullerService();

        // Then
        this.ThenClientRejectedEvents(client, events);
    }

    [Fact]
    public async Task GivenEventsReceivedWithDeliveryCountHigherThanMaxRetries_WhenHandleThrowException_ThenEventsAreRejected()
    {
        // Given
        var maxRetries = 3;
        var client = this.GivenClient(options: GenerateOptions(
            maxRetries: maxRetries
        ));
        var events = this.GivenEventsForClient(client, GenerateEvent(deliveryCount: maxRetries + 1));
        this.GivenClientFailsHandlingEvents(client, new Exception());

        // When
        await this.WhenRunningPullerService();

        // Then
        this.ThenClientRejectedEvents(client, events);
    }

    public static IEnumerable<object[]> RejectingExceptions()
    {
        yield return [new DomainEventTypeNotRegisteredException("event")];
        yield return [new CloudEventSerializationException("type", new Exception())];
        yield return [new DomainEventHandlerNotRegisteredException("eventName")];
    }

    private EventPullerClient GivenClient(string clientName = "client", EventPropagationSubscriptionOptions? options = null)
    {
        options ??= GenerateOptions(clientName);

        var client = A.Fake<IEventGridClientAdapter>();
        var scope = A.Fake<IServiceScope>();
        var eventHandler = A.Fake<ICloudEventHandler>();
        var eventHandlingResults = new EventHandlingResult([], [], []);

        var eventPullerClient = new EventPullerClient(clientName, client, eventHandler, options, eventHandlingResults);

        this._clients.Add(eventPullerClient);

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddScoped(_ => eventHandler);

        A.CallTo(() => this._eventGridClientWrapperFactory.CreateClient(clientName)).Returns(eventPullerClient.Client);
        A.CallTo(() => this._optionsMonitor.Get(clientName)).Returns(options);
        A.CallTo(() => this._scopeFactory.CreateScope()).Returns(scope);
        A.CallTo(() => scope.ServiceProvider).Returns(serviceCollection.BuildServiceProvider());

        // Special case: the IEnumerable received by these calls needs to be consumed immediately, otherwise the data is never removed from the channel until the end when it is too late
        A.CallTo(() => client.AcknowledgeCloudEventsAsync(A<string>._, A<string>._, A<IEnumerable<string>>._, A<CancellationToken>._))
            .Invokes(x => this.OnEventCompleted(eventHandlingResults.AcknowledgedEvents, x.GetArgument<IEnumerable<string>>(2)!));

        A.CallTo(() => client.ReleaseCloudEventsAsync(A<string>._, A<string>._, A<IEnumerable<string>>._, A<TimeSpan>._, A<CancellationToken>._))
            .Invokes(x => this.OnEventCompleted(eventHandlingResults.ReleasedEvents, x.GetArgument<IEnumerable<string>>(2)!.Select(y => (y, x.GetArgument<TimeSpan>(3)))));

        A.CallTo(() => client.RejectCloudEventsAsync(A<string>._, A<string>._, A<IEnumerable<string>>._, A<CancellationToken>._))
            .Invokes(x => this.OnEventCompleted(eventHandlingResults.RejectedEvents, x.GetArgument<IEnumerable<string>>(2)!));

        return eventPullerClient;
    }

    private void OnEventCompleted<T>(List<T> eventList, IEnumerable<T> eventsData)
    {
        var events = eventsData.ToList();

        lock (this._pullerService!)
        {
            this._eventCounter -= events.Count;

            if (this._eventCounter <= 0)
            {
                this._eventCompletionSource.SetResult();
            }
        }

        eventList.AddRange(events);
    }

    private void GivenClientFailsReceivingEvents(EventPullerClient client)
    {
        A.CallTo(() => client.Client.ReceiveCloudEventsAsync(client.Options.TopicName, client.Options.SubscriptionName, A<int>._, A<CancellationToken>._)).Throws<LocalTestException>();

        // Disable completion on events received to make sure we wait around and see the exception happen
        this._eventCounter += 1;
    }

    private void GivenClientFailsAcknowledgingEvents(EventPullerClient client)
    {
        A.CallTo(() => client.Client.AcknowledgeCloudEventsAsync(client.Options.TopicName, client.Options.SubscriptionName, A<IEnumerable<string>>._, A<CancellationToken>._)).Throws<LocalTestException>();

        // Disable completion on events received to make sure we wait around and see the exception happen
        this._eventCounter += 1;
    }

    private void GivenClientFailsReleasingEvents(EventPullerClient client)
    {
        A.CallTo(() => client.Client.ReleaseCloudEventsAsync(client.Options.TopicName, client.Options.SubscriptionName, A<IEnumerable<string>>._, A<TimeSpan>._, A<CancellationToken>._)).Throws<LocalTestException>();

        // Disable completion on events received to make sure we wait around and see the exception happen
        this._eventCounter += 1;
    }

    private void GivenClientFailsRejectingEvents(EventPullerClient client)
    {
        A.CallTo(() => client.Client.RejectCloudEventsAsync(client.Options.TopicName, client.Options.SubscriptionName, A<IEnumerable<string>>._, A<CancellationToken>._)).Throws<LocalTestException>();

        // Disable completion on events received to make sure we wait around and see the exception happen
        this._eventCounter += 1;
    }

    private EventBundle[] GivenEventsForClient(EventPullerClient client, params EventBundle[] events)
    {
        // The first call will return the events, subsequent calls will return an empty array after a delay
        // The actual implementation of EventGrid will perform similarly if there are only some events to return
        A.CallTo(() => client.Client
                .ReceiveCloudEventsAsync(client.Options.TopicName, client.Options.SubscriptionName, A<int>._, A<CancellationToken>._))
                .Returns(events).Once()
                .Then.ReturnsLazily(async _ =>
                {
                    await Task.Delay(10);
                    return [];
                });

        this._eventCounter += events.Length;

        return events;
    }

    private void GivenClientFailsHandlingEvents(EventPullerClient client, Exception? exception = null)
    {
        exception ??= new Exception("Unhandled exception");
        A.CallTo(() => client.EventHandler.HandleCloudEventAsync(A<CloudEvent>._, A<CancellationToken>._)).Throws(exception);
    }

    private async Task WhenRunningPullerService()
    {
        this._pullerService = new EventPullerService(this._scopeFactory, this._clients.Select(x => new EventGridClientDescriptor(x.Name)), this._eventGridClientWrapperFactory, this._optionsMonitor, new NullLogger<EventPullerService>());

        // We need this to start on the thread pool otherwise it will just block the test
        Task.Run(this.RunPullerService).Forget();

        await this._eventCompletionSource.Task;
        await this._pullerService.StopAsync(CancellationToken.None);
    }

    private async Task RunPullerService()
    {
        try
        {
            // This only starts the background service, the actual task is not awaited
            await this._pullerService!.StartAsync(CancellationToken.None);

            // This is the actual task from the background service, if it crashes it will be rethrown here
            await this._pullerService!.ExecuteTask!.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            this._eventCompletionSource.SetException(e);
            throw;
        }
    }

    private void ThenClientReceivedEvents(EventPullerClient client)
    {
        A.CallTo(() => client.Client.ReceiveCloudEventsAsync(client.Options.TopicName, client.Options.SubscriptionName, A<int>._, A<CancellationToken>._)).MustHaveHappenedOnceOrMore();
    }

    private void ThenClientHandledEvents(EventPullerClient client, params EventBundle[] events)
    {
        foreach (var eventBundle in events)
        {
            A.CallTo(() => client.EventHandler.HandleCloudEventAsync(eventBundle.Event, A<CancellationToken>._)).MustHaveHappenedOnceOrMore();
        }
    }

    private void ThenClientAcknowledgedEvents(EventPullerClient client, params EventBundle[] events)
    {
        Assert.True(events.All(x => client.EventHandlingResult.AcknowledgedEvents.Contains(x.LockToken)));
    }

    private void ThenClientReleasedEvents(EventPullerClient client, params EventBundle[] events)
    {
        foreach (var eventBundle in events)
        {
            if (client.Options.RetryDelays == null)
            {
                var intendedDelay = TimeSpan.FromSeconds((int)Math.Pow(2, eventBundle.DeliveryCount - 1));
                var expectedDelay = SupportedReleaseDelays.FirstOrDefault(x => intendedDelay <= x, SupportedReleaseDelays.Last());
                Assert.Contains(client.EventHandlingResult.ReleasedEvents, x => x.LockToken == eventBundle.LockToken && x.ReleaseDelay == expectedDelay);
            }
            else
            {
                var intendedDelay = eventBundle.DeliveryCount - 1 < client.Options.RetryDelays.Count ? client.Options.RetryDelays.ElementAt(eventBundle.DeliveryCount - 1) : client.Options.RetryDelays.Last();
                var expectedDelay = SupportedReleaseDelays.FirstOrDefault(x => intendedDelay <= x, SupportedReleaseDelays.Last());
                Assert.Contains(client.EventHandlingResult.ReleasedEvents, x => x.LockToken == eventBundle.LockToken && x.ReleaseDelay == expectedDelay);
            }
        }
    }

    private void ThenClientRejectedEvents(EventPullerClient client, params EventBundle[] events)
    {
        Assert.True(events.All(x => client.EventHandlingResult.RejectedEvents.Contains(x.LockToken)));
    }

    private static EventPropagationSubscriptionOptions GenerateOptions(string id = "id", TimeSpan[]? retryDelays = null, int maxRetries = 3)
    {
        return new EventPropagationSubscriptionOptions
        {
            TopicName = $"topic-{id}",
            SubscriptionName = $"subscription-{id}",
            MaxDegreeOfParallelism = 10,
            MaxRetries = maxRetries,
            RetryDelays = retryDelays,
        };
    }

    private static EventBundle GenerateEvent(string? id = null, int deliveryCount = 1)
    {
        id ??= Guid.NewGuid().ToString();
        return new EventBundle(new CloudEvent("source", "type", null), $"lock-{id}", deliveryCount);
    }

    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            this._pullerService?.Dispose();
        }
    }

    internal sealed record EventPullerClient(string Name, IEventGridClientAdapter Client, ICloudEventHandler EventHandler, EventPropagationSubscriptionOptions Options, EventHandlingResult EventHandlingResult);

    internal sealed record EventHandlingResult(List<string> AcknowledgedEvents, List<(string LockToken, TimeSpan ReleaseDelay)> ReleasedEvents, List<string> RejectedEvents);

    internal sealed class LocalTestException : Exception;
}
