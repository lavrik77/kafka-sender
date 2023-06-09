package ru.globaltruck.hibernate.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.event.spi.PostCommitInsertEventListener;
import org.hibernate.event.spi.PostInsertEvent;
import org.hibernate.persister.entity.EntityPersister;
import org.springframework.stereotype.Component;
import ru.globaltruck.kafka.sender.service.SenderService;

@Slf4j
@Component
@RequiredArgsConstructor
public class CustomPostCommitInsertEventListener implements PostCommitInsertEventListener {

    private final SenderService senderService;

    @Override
    public void onPostInsert(PostInsertEvent event) {
        Object entity = event.getEntity();
        log.info("Event: 'PostCommitInsertEvent', entity: '{}'", entity);
        try {
            if (senderService.getEntitiesMap().get(event.getPersister().getMappedClass().getSimpleName()) != null) {
                senderService.send(entity);
            }
        } catch (Exception e) {
            log.error("Произошла ошибка при попытке отправки в кафку. Отправка не выполнена", e);
        }
    }

    @Override
    public boolean requiresPostCommitHanding(EntityPersister persister) {
        return true;
    }

    @Override
    public void onPostInsertCommitFailed(PostInsertEvent event) {
        // Nothing
    }
}