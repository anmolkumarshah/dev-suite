package org.anmol.devsuite.config;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.beans.factory.annotation.Value;
import com.github.fppt.jedismock.RedisServer;
import org.h2.tools.Server;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class DevInfrastructureConfig {

    private EmbeddedPostgres pgServer;
    private RedisServer redisServer;
    private Server h2WebServer; // Add this variable

    @PostConstruct
    public void startServers() throws Exception {
        // 1. Start Postgres on 5432
        File dataDir = new File("./dev-data/postgres");
        dataDir.mkdirs();

        pgServer = EmbeddedPostgres.builder()
                .setPort(5432)
                .setDataDirectory(dataDir)
                .start();
        System.out.println("✅ Embedded PostgreSQL running on port 5432");

        // 2. Start Redis on 6379 (Corrected syntax here)
        redisServer = RedisServer.newRedisServer(6379);
        redisServer.start();
        System.out.println("✅ Embedded Redis running on port 6379");

        // 3. Start H2 Web Console Natively on Port 8082
        h2WebServer = Server.createWebServer("-web", "-webAllowOthers", "-webPort", "8082").start();
        System.out.println("✅ Database UI running on port 8082");
    }

    @PreDestroy
    public void stopServers() throws Exception {
        if (pgServer != null) pgServer.close();
        if (redisServer != null) redisServer.stop();
        if (h2WebServer != null) h2WebServer.stop(); // Stop H2 cleanly
    }



    // 3. Start Kafka in KRaft Mode (Auto-assigns Port)
    @Bean
    public EmbeddedKafkaKraftBroker embeddedKafkaBroker() {
        EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 2, "default-test-topic");
        broker.brokerProperty("log.dirs", "./dev-data/kafka");

        // Hardcode 9092 directly to guarantee it does not default to 0
        broker.kafkaPorts(9092);

        System.out.println("✅ Embedded Kafka (KRaft) initialized on port 9092");
        return broker;
    }

    @Bean
    @DependsOn("devInfrastructureConfig") // Forces Spring to wait for @PostConstruct to finish
    public DataSource dataSource() {
        return DataSourceBuilder.create()
                .url("jdbc:postgresql://localhost:5432/postgres")
                .username("postgres")
                .password("")
                .driverClassName("org.postgresql.Driver")
                .build();
    }

    @Bean
    public KafkaAdmin kafkaAdmin(org.springframework.kafka.test.EmbeddedKafkaBroker kafkaBroker) {
        Map<String, Object> configs = new HashMap<>();
        // Fetch the dynamic random port that KRaft assigned
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
        return new KafkaAdmin(configs);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory(org.springframework.kafka.test.EmbeddedKafkaBroker kafkaBroker) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dev-suite-ui-tracker");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // FIX 1: Always read from the beginning of the topic so we don't miss test messages
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // FIX 2: Force the consumer to check for newly created wildcard topics every 3 seconds (Default is 5 minutes!)
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 3000);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

}