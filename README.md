# spring-kafka-retry-like-rabbitmq
使用Spring Kafka的DeadLetterPublishingRecoverer和SeekToCurrentErrorHandler.  
实现以下特性  
1.重试消息达到最大次数,数据丢进死信队列,死信队列会重新发回业务topic  
  如果成功丢进死信队列会自动确认消息,如果不能丢进死信队列,会无限循环重试,一直到丢进死信队列为止.  
  这就像rabbitmq一样.  
2.因为使用了Spring Kafka所以天然支持单客户端多线程消费,1分区对应1线程  

Using Spring Kafka的DeadLetterPublishingRecoverer和SeekToCurrentErrorHandler.
attain to The following features
1. Message will send to dead letter queue after maxAttempts retries and will be ack automatic   
   And dead letter queue will send message to the original topic.  
   If message can not send to dead letter queue,System will blocking to retry infinite until it can be   
   just like rabbitmq  
2. Because using the Spring Kafka, so it supports multithreading.  
