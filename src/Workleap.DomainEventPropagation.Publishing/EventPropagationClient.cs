using Azure.Messaging;
using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.Namespaces;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Options;

namespace Workleap.DomainEventPropagation;

/// <summary>
/// https://github.com/Azure/azure-sdk-for-net/blob/master/sdk/eventgrid/Azure.Messaging.EventGrid/README.md
/// </summary>
internal sealed class EventPropagationClient : IEventPropagationClient
{
    private const string DomainEventDefaultVersion = "1.0";

    private readonly EventPropagationPublisherOptions _eventPropagationPublisherOptions;
    private readonly DomainEventsHandlerDelegate _pipeline;
    private readonly EventGridPublisherClient _eventGridPublisherClient;
    private readonly EventGridClient _eventGridNamespaceClient;

    /// <summary>
    /// To support Namespace topic, we need to use the following EventGridClient https://github.com/Azure/azure-sdk-for-net/blob/Azure.Messaging.EventGrid_4.17.0-beta.1/sdk/eventgrid/Azure.Messaging.EventGridV2/src/Generated/EventGridClient.cs
    /// Note that even if this client has the same name, it is different from the deprecated one found here https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.eventgrid.eventgridclient?view=azure-dotnet-legacy
    /// </summary>
    public EventPropagationClient(
        IAzureClientFactory<EventGridPublisherClient> eventGridPublisherClientFactory,
        IAzureClientFactory<EventGridClient> eventGridClientFactory,
        IOptions<EventPropagationPublisherOptions> eventPropagationPublisherOptions,
        IEnumerable<IPublishingDomainEventBehavior> publishingDomainEventBehaviors)
    {
        this._eventPropagationPublisherOptions = eventPropagationPublisherOptions.Value;
        this._pipeline = publishingDomainEventBehaviors.Reverse().Aggregate((DomainEventsHandlerDelegate)this.SendDomainEventsAsync, BuildPipeline);
        this._eventGridPublisherClient = eventGridPublisherClientFactory.CreateClient(EventPropagationPublisherOptions.CustomTopicClientName);
        this._eventGridNamespaceClient = eventGridClientFactory.CreateClient(EventPropagationPublisherOptions.NamespaceTopicClientName);
    }

    private static DomainEventsHandlerDelegate BuildPipeline(DomainEventsHandlerDelegate accumulator, IPublishingDomainEventBehavior next)
    {
        return (events, cancellationToken) => next.HandleAsync(events, accumulator, cancellationToken);
    }

    public Task PublishDomainEventAsync<T>(T domainEvent, CancellationToken cancellationToken)
        where T : IDomainEvent, new()
        => this.PublishDomainEventsAsync(new[] { domainEvent }, cancellationToken);

    public async Task PublishDomainEventsAsync<T>(IEnumerable<T> domainEvents, CancellationToken cancellationToken)
        where T : IDomainEvent, new()
    {
        if (domainEvents == null)
        {
            throw new ArgumentNullException(nameof(domainEvents));
        }

        var domainEventWrappers = DomainEventWrapperCollection.Create(domainEvents);
        if (domainEventWrappers.Count == 0)
        {
            return;
        }

        try
        {
            await this._pipeline(domainEventWrappers, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new EventPropagationPublishingException(domainEventWrappers.DomainEventName, this._eventPropagationPublisherOptions.TopicEndpoint, ex);
        }
    }

    private Task SendDomainEventsAsync(DomainEventWrapperCollection domainEventWrappers, CancellationToken cancellationToken)
    {
        return domainEventWrappers.DomainSchema switch
        {
            EventSchema.EventGridEvent => this.SendEventGridEvents(domainEventWrappers, cancellationToken),
            EventSchema.CloudEvent => this.SendCloudEvents(domainEventWrappers, cancellationToken),
            _ => throw new NotSupportedException($"Event schema {domainEventWrappers.DomainSchema} is not supported"),
        };
    }

    private Task SendEventGridEvents(
        DomainEventWrapperCollection domainEventWrappers,
        CancellationToken cancellationToken)
    {
        var topicType = this._eventPropagationPublisherOptions.TopicType;
        var eventGridEvents = domainEventWrappers.Select(wrapper => new EventGridEvent(
            subject: wrapper.DomainEventName,
            eventType: wrapper.DomainEventName,
            dataVersion: DomainEventDefaultVersion,
            data: new BinaryData(wrapper.Data)));

        return topicType switch
        {
            TopicType.Custom => this._eventGridPublisherClient.SendEventsAsync(eventGridEvents, cancellationToken),
            TopicType.Namespace => throw new NotSupportedException("Cannot send EventGridEvents to a namespace topic"),
            _ => throw new NotSupportedException($"Topic type {topicType} is not supported"),
        };
    }

    private Task SendCloudEvents(
        DomainEventWrapperCollection domainEventWrappers,
        CancellationToken cancellationToken)
    {
        if (this._eventPropagationPublisherOptions.TopicType is not TopicType.Namespace)
        {
            throw new NotSupportedException("Cannot send CloudEvents events to non-namespace topics");
        }

        var topicName = this._eventPropagationPublisherOptions.TopicName;
        var cloudEvents = domainEventWrappers.Select(wrapper => new CloudEvent(
            type: wrapper.DomainEventName,
            source: this._eventPropagationPublisherOptions.TopicEndpoint,
            jsonSerializableData: wrapper.Data));

        return this._eventGridNamespaceClient.PublishCloudEventsAsync(topicName, cloudEvents, cancellationToken);
    }
}