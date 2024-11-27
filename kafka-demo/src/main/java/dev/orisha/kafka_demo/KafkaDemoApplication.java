package dev.orisha.kafka_demo;

//import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;

import static dev.orisha.kafka_demo.KafkaDemoApplication.PAGE_VIEWS_TOPIC;

@SpringBootApplication
public class KafkaDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

	final static String PAGE_VIEWS_TOPIC = "pv_topic";

}

@Configuration
class RunnerConfiguration {

	private PageView random(String source) {
		var names = "john, jane, joe, jean, jean, jurgen".split(", ");
		var pages = "blog.html, contact.html, about.html, news.html, index.html".split(", ");
		var random = new SecureRandom();
		var page = pages[random.nextInt(pages.length)];
		var name = names[random.nextInt(names.length)];
        return new PageView(page, Math.random() > .5 ? 100 : 1000, name, source);
	}

	void stream(StreamBridge streamBridge) {
		streamBridge.send("pageViews-out-0", random("stream"));
	}

	void kafka(KafkaTemplate<Object, Object> template) {
		var pageView = random("kafka");
		template.send(PAGE_VIEWS_TOPIC, pageView);
	}

	void integration(MessageChannel channel) {
		var message = MessageBuilder.withPayload(random("integration"))
//				.copyHeadersIfAbsent(Map.of(KafkaHeaders.TOPIC, PAGE_VIEWS_TOPIC))
				.build();
		channel.send(message);
	}

	@Bean
	ApplicationListener<ApplicationReadyEvent> runnerListener(
			StreamBridge streamBridge, KafkaTemplate<Object, Object> template, MessageChannel channel) {
		return event -> {
			for (int i = 0; i < 100; i++) {
				kafka(template);
				integration(channel);
				stream(streamBridge);
			}

        };
	}
}

@Configuration
class IntegrationConfiguration {

	@Bean
	IntegrationFlow flow(MessageChannel channel, KafkaTemplate<Object, Object> template) {
		var kafka = Kafka.outboundChannelAdapter(template)
				.topic(PAGE_VIEWS_TOPIC)
				.getObject();
		return IntegrationFlow.from(channel)
				.handle(kafka)
				.get();
	}

	@Bean
	MessageChannel channel() {
		return MessageChannels.direct().getObject();
	}

}

@Configuration
class KafkaConfiguration {

	@KafkaListener(topics = PAGE_VIEWS_TOPIC, groupId = "pv_topic_group")
	public void onNewPageView(Message<PageView> pageView) {
		System.out.println("-------------------------");
		System.out.println("New Page View: " + pageView.getPayload());
		pageView.getHeaders().forEach((key, value) -> System.out.println("Key: " + key + " Value: " + value));
	}

	@Bean
	NewTopic pageViewsTopic() {
		return new NewTopic(PAGE_VIEWS_TOPIC, 1, (short) 1);
	}

	@Bean
	JsonMessageConverter jsonMessageConverter() {
		return new JsonMessageConverter();
	}

	@Bean
	KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
		return new KafkaTemplate<>(producerFactory,
				Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class));
	}

}

record PageView(String page, long duration, String userId, String source) {}
