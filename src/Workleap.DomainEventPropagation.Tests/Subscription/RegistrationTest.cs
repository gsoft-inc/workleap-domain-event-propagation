using System.Text.Json;
using Azure.Messaging;
using Azure.Messaging.EventGrid;
using Microsoft.Extensions.DependencyInjection;
using Workleap.DomainEventPropagation.AzureSystemEvents;
using Workleap.DomainEventPropagation.Extensions;
using Workleap.DomainEventPropagation.Tests.Subscription.Models;

namespace Workleap.DomainEventPropagation.Tests.Subscription;

public class RegistrationTest
{
    private const string OrganizationTopicName = "Organization";

    [Fact]
    public async Task GivenDomainEventIsFired_WhenDomainEventHandlerIsRegisteredToMultipleDomainEvents_ThenDomainEventHandlerIsCalled()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITopicProvider, PlatformTopicProvider>();
        var eventProcessingBuilder = services.AddEventPropagationSubscriber(options => { options.SubscribedTopics = new[] { OrganizationTopicName }; });
        eventProcessingBuilder.AddDomainEventHandlersFromAssembly(typeof(DomainEventHandler).Assembly);

        var serviceProvider = services.BuildServiceProvider();
        var domainEventGridWebhookHandler = serviceProvider.GetRequiredService<IDomainEventGridWebhookHandler>();

        try
        {
            var eventGridEvent = new CloudEvent(
                "subject",
                typeof(OneDomainEvent).FullName,
                JsonSerializer.Serialize(new OneDomainEvent { Number = 1, Text = "Hello" }))
            {
                DataSchema = OrganizationTopicName
            };

            await domainEventGridWebhookHandler.HandleEventGridWebhookEventAsync(eventGridEvent, CancellationToken.None);
        }
        catch (Exception e)
        {
            Assert.Equal("HandleDomainEventAsync called for OneDomainEvent", e.Message);
        }

        try
        {
            var eventGridEvent = new CloudEvent(
                "subject2",
                typeof(TwoDomainEvent).FullName,
                JsonSerializer.Serialize(new TwoDomainEvent { Number = 1, Text = "Hello" }))
            {
                DataSchema = OrganizationTopicName
            };

            await domainEventGridWebhookHandler.HandleEventGridWebhookEventAsync(eventGridEvent, CancellationToken.None);
        }
        catch (Exception e)
        {
            Assert.Equal("HandleDomainEventAsync called for TwoDomainEvent", e.Message);
        }
    }

    [Fact(Skip = "Enable when system topics are used")]
    public async Task GivenAzureSystemEventIsFired_WhenAzureSystemEventHandlerIsRegisteredToMultipleAzureSystemEvents_ThenAzureSystemEventHandlerIsCalled()
    {
        var systemTopicName = "SystemTopicName";
        var systemTopicPattern = "SystemTopicPattern";

        var services = new ServiceCollection();
        var eventProcessingBuilder = services.AddEventPropagationSubscriber(options => { options.SubscribedTopics = new[] { systemTopicName }; });
        eventProcessingBuilder.AddAzureSystemEventHandlersFromAssembly(typeof(AzureSystemEventHandler).Assembly);

        var serviceProvider = services.BuildServiceProvider();
        var azureSystemEventGridWebhookHandler = serviceProvider.GetRequiredService<IAzureSystemEventGridWebhookHandler>();

        try
        {
            var eventGridEvent = new CloudEvent(
                "subject",
                SystemEventNames.MediaJobFinished,
                BinaryData.FromString(@"{ ""outputs"": [] }"))
            {
                DataSchema = $"xzxzxzx{systemTopicPattern}xzxzxzx"
            };

            var wasParsedAsSystemEvent = eventGridEvent.TryGetSystemEventData(out var systemEventData);
            if (!wasParsedAsSystemEvent)
            {
                Assert.Fail("Could not deserialize the event data of type 'MediaJobFinishedEventData' as a valid Azure System Event");
            }

            await azureSystemEventGridWebhookHandler.HandleEventGridWebhookEventAsync(eventGridEvent, systemEventData, CancellationToken.None);
        }
        catch (Exception e)
        {
            Assert.Equal("HandleAzureSystemEventAsync called for MediaJobFinishedEventData", e.Message);
        }

        try
        {
            var eventGridEvent = new CloudEvent(
                "subject2",
                SystemEventNames.MediaJobErrored,
                BinaryData.FromString(@"{ ""outputs"": [] }"))
            {
                DataSchema = $"xzxzxzx{systemTopicPattern}xzxzxzx"
            };

            var wasParsedAsSystemEvent = eventGridEvent.TryGetSystemEventData(out var systemEventData);
            if (!wasParsedAsSystemEvent)
            {
                Assert.Fail("Could not deserialize the event data of type 'MediaJobErroredEventData' as a valid Azure System Event");
            }

            await azureSystemEventGridWebhookHandler.HandleEventGridWebhookEventAsync(eventGridEvent, systemEventData, CancellationToken.None);
        }
        catch (Exception e)
        {
            Assert.Equal("HandleAzureSystemEventAsync called for MediaJobErroredEventData", e.Message);
        }
    }
}