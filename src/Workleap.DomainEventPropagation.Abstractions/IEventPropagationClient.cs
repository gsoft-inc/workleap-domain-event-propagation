namespace Workleap.DomainEventPropagation;

public interface IEventPropagationClient
{
    Task PublishDomainEventAsync<T>(string subject, T domainEvent, CancellationToken cancellationToken)
        where T : IDomainEvent;

    Task PublishDomainEventAsync<T>(T domainEvent, CancellationToken cancellationToken)
        where T : IDomainEvent;

    Task PublishDomainEventsAsync<T>(string subject, IEnumerable<T> domainEvents, CancellationToken cancellationToken)
        where T : IDomainEvent;

    Task PublishDomainEventsAsync<T>(IEnumerable<T> domainEvents, CancellationToken cancellationToken)
        where T : IDomainEvent;
}