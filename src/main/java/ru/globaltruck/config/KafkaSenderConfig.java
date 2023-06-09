package ru.globaltruck.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import ru.globaltruck.hibernate.listener.CustomPostCommitInsertEventListener;
import ru.globaltruck.hibernate.listener.CustomPostCommitUpdateEventListener;
import ru.globaltruck.kafka.sender.HibernateListenerConfigurer;
import ru.globaltruck.kafka.sender.service.SenderService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@EnableConfigurationProperties
public class KafkaSenderConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.auth:false}")
    private boolean auth;
    @Value("${spring.kafka.truststore.location}")
    private String truststoreLocation;
    @Value("${spring.kafka.truststore.password}")
    private String truststorePassword;
    @Value("${spring.kafka.username}")
    private String username;
    @Value("${spring.kafka.password}")
    private String password;
    @Bean
    public KafkaTemplate<String, Object> senderKafkaTemplate() {
        return new KafkaTemplate<>(senderProducerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> senderProducerFactory() {
        Map<String, Object> configProps = getPropsProducer();
        configProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    private Map<String, Object> getPropsProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (auth) {
            String jaasTemplate =
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
            props.put("sasl.jaas.config", String.format(jaasTemplate, username, password));
            props.put("sasl.mechanism", "SCRAM-SHA-512");
            props.put("security.protocol", "SASL_SSL");
            if (truststoreLocation != null && !truststoreLocation.isBlank()) {
                props.put("ssl.truststore.location", truststoreLocation);
                props.put("ssl.truststore.password", truststorePassword);
            }
        }

        return props;
    }

    @Bean
    public Executor integrationSenderTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setThreadNamePrefix("IntegrationSender-");
        executor.initialize();
        return executor;
    }


    @Bean
    public SenderService senderService() {
        return new SenderService(senderKafkaTemplate());
    }

    @Bean
    public CustomPostCommitInsertEventListener insertEventListener() {
        return new CustomPostCommitInsertEventListener(senderService());
    }

    @Bean
    public CustomPostCommitUpdateEventListener updateEventListener() {
        return new CustomPostCommitUpdateEventListener(senderService());
    }

    @Bean
    public HibernateListenerConfigurer hibernateListenerConfigurer() {
        return new HibernateListenerConfigurer(updateEventListener(), insertEventListener());
    }
}
