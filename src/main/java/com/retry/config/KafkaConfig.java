package com.retry.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;



/**
 * @author jiangsai
 * @create 2019-08-20 14:16
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.dlqName}")
    private String dlqName;

    @Value("${kafka.maxAttempts}")
    private int maxAttempts;

    @Value("${kafka.groupName}")
    private String groupName;

    @Value("${kafka.autoReset}")
    private String autoReset;

    @Value("${kafka.retryPeriod}")
    private int retryPeriod;

    @Value("${kafka.currency:1}")
    private int currency;

    private final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    private KafkaTemplate<String, String> kafkaTemplate;


    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoReset);

        return props;
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setStatefulRetry(true);
        factory.setRetryTemplate(retryTemplate());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setErrorHandler(errorHandler(kafkaTemplate()));
        if(currency > 1){
            factory.setConcurrency(currency);
        }
        return factory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate template = new RetryTemplate();

        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(retryPeriod);
        template.setBackOffPolicy(backOffPolicy);

        Map map = new HashMap(){{
            put(Exception.class, true);
            put(RuntimeException.class, true);
        }};
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(maxAttempts, map, true);
        template.setRetryPolicy(retryPolicy);
        return template;
    }

    @Bean
    public ErrorHandler errorHandler(KafkaTemplate kafkaTemplate) {
        ExtendedDeadLetterPublishingRecoverer recoverer = new ExtendedDeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> new TopicPartition(dlqName, r.partition()));
        ExtendedDeadLetterPublishingRecoverer.dlqName = dlqName;
        return new ExtendedSeekToCurrentErrorHandler(recoverer, maxAttempts,
                Collections.singletonMap(Exception.class, true), true);
    }



    private static class ExtendedSeekToCurrentErrorHandler extends SeekToCurrentErrorHandler {


        private final BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer;
        private final BinaryExceptionClassifier retryableClassifier;


        public ExtendedSeekToCurrentErrorHandler(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
                                                 int maxFailures, Map<Class<? extends Throwable>, Boolean> retryableExceptions,
                                                 boolean traverseCauses) {
            super(recoverer, maxFailures);
            this.setCommitRecovered(true);
            this.recoverer = recoverer;
            this.retryableClassifier = new BinaryExceptionClassifier(retryableExceptions, false);
            this.retryableClassifier.setTraverseCauses(traverseCauses);
        }

    }

    private static class ExtendedDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

        public static String dlqName;

        private static final Log LOGGER = LogFactory.getLog(ExtendedDeadLetterPublishingRecoverer.class); // NOSONAR

        private AtomicInteger POOL_SEQ = new AtomicInteger(1);

        private BlockingQueue<ConsumeMetadata> queue = new LinkedBlockingQueue<>();

        public ExtendedDeadLetterPublishingRecoverer(KafkaTemplate<Object, Object> template,
                                                     BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {
            super(template, destinationResolver);
            reStart();
        }

        @Override
        protected void publish(ProducerRecord<Object, Object> outRecord, KafkaOperations<Object, Object> kafkaTemplate) {
            try {
                kafkaTemplate.send(outRecord).addCallback(result -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Successful dead-letter publication: " + result);
                    }
                }, ex -> {
                    LOGGER.error("Dead-letter publication failed for: " + outRecord, ex);
                });
            }
            catch (Exception e) {
                LOGGER.error("Dead-letter publication failed for: " + outRecord, e);
                LOGGER.error("Dead-letter publication 发生故障重启中");

                try {
                    //故障之后sleep10s 避免死循环
                    LOGGER.info("enter sleep "+10+" s");
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e1) {
                    LOGGER.error(e.getMessage(), e);
                }

                ConsumeMetadata metadata = new ConsumeMetadata();
                metadata.setKafkaTemplate(kafkaTemplate);
                metadata.setOutRecord(outRecord);
                //重新放回启动队列
                queue.add(metadata);
                LOGGER.info("将" + metadata + "重新放回队列");
                throw e;
            }
        }

        @Override
        protected ProducerRecord<Object, Object> createProducerRecord(ConsumerRecord<?, ?> record,
                                                                      TopicPartition topicPartition, RecordHeaders headers) {

            return new ProducerRecord<>(topicPartition.topic(),
                    topicPartition.partition() < 0 ? null : topicPartition.partition(),
                    record.key(), dlqName.equals(record.topic()) ? record.value() : record.topic() + "!" + record.partition() + "!" + record.key() + "!" + record.value(), headers);
        }

        private final ExecutorService cachedThreadPool = Executors.newCachedThreadPool(new ThreadFactory(){
            private final String mPrefix = "pool-";
            private final AtomicInteger mThreadNum = new AtomicInteger(1);
            private final ThreadGroup mGroup = (System.getSecurityManager() == null )?Thread.currentThread().getThreadGroup():
                    System.getSecurityManager().getThreadGroup();;
            @Override
            public Thread newThread(Runnable runnable) {
                String name = mPrefix + mThreadNum.getAndIncrement();
                Thread ret = new Thread(mGroup,runnable,name,0);
                ret.setDaemon(false);
                return ret;
            }
        });

        public void reStart() {
            cachedThreadPool.execute(() -> {
                Thread thread = Thread.currentThread();
                thread.setName("QUEUE-Consumer-Thread " + POOL_SEQ.getAndIncrement());
                while (true) {
                    try {
                        ExtendedDeadLetterPublishingRecoverer.ConsumeMetadata take = queue.take();
                        ProducerRecord<Object, Object> outRecord = take.getOutRecord();
                        KafkaOperations<Object, Object> kafkaTemplate = take.getKafkaTemplate();
                        publish(outRecord, kafkaTemplate);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
            });
        }


        public static class ConsumeMetadata {

            private ProducerRecord<Object, Object> outRecord;

            private KafkaOperations<Object, Object> kafkaTemplate;

            public ConsumeMetadata(ProducerRecord<Object, Object> outRecord, KafkaOperations<Object, Object> kafkaTemplate) {
                this.outRecord = outRecord;
                this.kafkaTemplate = kafkaTemplate;
            }

            public ConsumeMetadata() {

            }

            public ProducerRecord<Object, Object> getOutRecord() {
                return outRecord;
            }

            public void setOutRecord(ProducerRecord<Object, Object> outRecord) {
                this.outRecord = outRecord;
            }

            public KafkaOperations<Object, Object> getKafkaTemplate() {
                return kafkaTemplate;
            }

            public void setKafkaTemplate(KafkaOperations<Object, Object> kafkaTemplate) {
                this.kafkaTemplate = kafkaTemplate;
            }

            @Override
            public String toString() {
                return "ConsumeMetadata{" +
                        "outRecord=" + outRecord +
                        ", kafkaTemplate=" + kafkaTemplate +
                        '}';
            }
        }


    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        kafkaTemplate = new KafkaTemplate<>(producerFactory());
        return kafkaTemplate;
    }

    /**
     * 将死信队列的数据重新入队
     * @param letter
     * @param ack
     * @throws Exception
     */
    @KafkaListener(topics = "${kafka.dlqName}")
    public void dlqConsumer(String letter, Acknowledgment ack){

        logger.info("dlqConsumerReceive:" + letter);

        String splitStr = "!";

        /**
         * 死信格式是topic!partition!key!message,这里分隔下
         */
        int topicIdx = letter.indexOf(splitStr);
        String topic = letter.substring(0, topicIdx);
        String remain = letter.substring(topicIdx+1, letter.length());
        int partitionIdx = remain.indexOf(splitStr);
        Integer partition = Integer.parseInt(remain.substring(0, partitionIdx));
        remain = remain.substring(partitionIdx+1, remain.length());
        int keyIdx = remain.indexOf(splitStr);
        String key = remain.substring(0, keyIdx);
        String message = remain.substring(keyIdx+1, remain.length());

        if(partition == 0 && key == null){
            kafkaTemplate.send(topic, message);
        }else{
            kafkaTemplate.send(topic, partition, key, message);
        }

        ack.acknowledge();
    }


}
