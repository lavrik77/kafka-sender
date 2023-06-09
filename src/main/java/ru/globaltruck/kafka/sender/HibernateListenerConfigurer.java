package ru.globaltruck.kafka.sender;

import lombok.RequiredArgsConstructor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.internal.SessionFactoryImpl;
import ru.globaltruck.hibernate.listener.CustomPostCommitInsertEventListener;
import ru.globaltruck.hibernate.listener.CustomPostCommitUpdateEventListener;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

@RequiredArgsConstructor
public class HibernateListenerConfigurer {

    @PersistenceUnit
    private EntityManagerFactory emf;

    private final CustomPostCommitUpdateEventListener updateListener;
    private final CustomPostCommitInsertEventListener insertListener;

    @PostConstruct
    protected void init() {
        SessionFactoryImpl sessionFactory = emf.unwrap(SessionFactoryImpl.class);
        EventListenerRegistry registry = sessionFactory.getServiceRegistry().getService(EventListenerRegistry.class);
        registry.getEventListenerGroup(EventType.POST_COMMIT_UPDATE).appendListener(updateListener);
        registry.getEventListenerGroup(EventType.POST_COMMIT_INSERT).appendListener(insertListener);
    }
}