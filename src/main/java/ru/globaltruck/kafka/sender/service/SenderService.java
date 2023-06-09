package ru.globaltruck.kafka.sender.service;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.persistence.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "config.kafka.producer.target")
public class SenderService {

    @Setter
    @Getter
    private Map<String, Settings> entitiesMap = Collections.emptyMap();
    @Setter
    private boolean insertObjects = false;
    @Setter
    private boolean includeTypeNameHeader = false;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Async("integrationSenderTaskExecutor")
    public void send(Object source) {

        String entitySimpleName = source.getClass().getSimpleName();
        Settings settings = entitiesMap.get(entitySimpleName);
        Map<String, Object> result;
        try {
            result = getObjectMap(source, settings.getFlatObject());
        } catch (ReflectiveOperationException e) {
            log.error(
                    "Ошибка мэппинга '{}'",
                    entitySimpleName, e);
            return;
        }
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(settings.getTopic(), result);
        fillHeaders(producerRecord, settings.getInterfaceId());
        if (includeTypeNameHeader) {
            producerRecord.headers().add("TypeName", entitySimpleName.getBytes());
        }
        kafkaTemplate.send(producerRecord);
    }

    private Map<String, Object> getObjectMap(Object obj, Boolean flatObject) throws IllegalAccessException {
        Map<String, Object> result = new HashMap<>();
        for (Field sourceField : getAllFields(obj.getClass())) {
            sourceField.setAccessible(true);
            if (sourceField.getDeclaredAnnotation(EmbeddedId.class) != null) {
                Object id = sourceField.get(obj);
                for (Field idField : id.getClass().getDeclaredFields()) {
                    idField.setAccessible(true);
                    result.put(idField.getName(), idField.get(id));
                }
                continue;
            }
            result.put(sourceField.getName(), getValue(sourceField.get(obj), sourceField, flatObject));
        }
        return result;
    }

    private Object getValue(Object source, Field field, Boolean flatObject) throws IllegalAccessException {
        if (source == null) {
            return null;
        }
        List<String> annotations = Arrays
                .stream(field.getDeclaredAnnotations())
                .map(annotation -> annotation.annotationType().getSimpleName()).toList();
        if (annotations.contains(ManyToOne.class.getSimpleName()) ||
                annotations.contains(OneToOne.class.getSimpleName())) {
            if (flatObject) {
                return getObjectMap(source, flatObject);
            } else {
                return getId(source);
            }
        } else if (annotations.contains(ManyToMany.class.getSimpleName()) ||
                annotations.contains(OneToMany.class.getSimpleName())) {
            if (Collection.class.isAssignableFrom(source.getClass())) {
                if (flatObject) {
                    return getCollection(source, flatObject);
                } else {
                    return Collections.emptyList();
                }
            }
        }
        if (source instanceof LocalDateTime) {
            return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format((TemporalAccessor) source);
        } else if (source instanceof OffsetDateTime) {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format((TemporalAccessor) source);
        }
        return source;
    }

    @SuppressWarnings({"ConstantConditions", "rawtypes"})
    private List<Map<String, Object>> getCollection(Object obj, Boolean flatObject) throws IllegalAccessException {
        List<Map<String, Object>> list = new ArrayList<>();
        Iterator items = ((Collection) obj).iterator();
        while (items != null && items.hasNext()) {
            Object item = items.next();
            list.add(getObjectMap(item, flatObject));
        }
        return list;
    }

    private Object getId(Object obj) throws IllegalAccessException {
        Method getter;
        try {
            getter = obj.getClass().getMethod("getId");
            return getter.invoke(obj);
        } catch (InvocationTargetException | NoSuchMethodException ignore) {
            for (Field sourceField : getAllFields(obj.getClass())) {
                sourceField.setAccessible(true);
                if (sourceField.getDeclaredAnnotation(Id.class) != null) {
                    return sourceField.get(obj);
                }
            }
        }
        return null;
    }

    private List<Field> getAllFields(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyList();
        }
        List<Field> result = new ArrayList<>(getAllFields(clazz.getSuperclass()));
        List<Field> fields = List.of(clazz.getDeclaredFields());
        result.addAll(fields);
        return result;
    }

    private void fillHeaders(ProducerRecord<String, Object> producerRecord, String interfaceId) {
        producerRecord.headers()
                .add("GT2-Integration-ID", UUID.randomUUID().toString().getBytes())
                .add("GT2-Source-System", "MDM".getBytes())
                .add("GT2-Interface-ID", interfaceId.getBytes())
                .add("GT2-Destination-Channel", new byte[0])
                .add("GT2-Date-Time", OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME).getBytes());
    }

    @Data
    static class Settings {
        private String interfaceId;
        private String topic;
        private Boolean flatObject = true;
    }
}
