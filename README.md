# spring-kafka-retry-like-rabbitmq

设计目的:使用kafka可以像rabbitmq一样快速重试,不丢消息,也不影响正常数据消费.  

使用Spring Kafka的DeadLetterPublishingRecoverer和SeekToCurrentErrorHandler.  
实现以下特性,使用请先执行/kafka_script/release-1.0.txt  
1.重试消息达到最大次数,数据丢进死信队列,死信队列会重新发回业务topic  
  如果成功丢进死信队列会自动确认消息,如果不能丢进死信队列,会无限循环重试,一直到丢进死信队列为止.  
  这就像rabbitmq一样.  
2.因为使用了Spring Kafka所以天然支持单客户端多线程消费,1分区对应1线程  

design purpose: Using kafka can fast retry, no lost message, and do not influence not error message to consume  

Using Spring Kafka's DeadLetterPublishingRecoverer and SeekToCurrentErrorHandler.
attain to The following features. Before using please run /kafka_script/release-1.0.txt  
1. The message that sending to dead letter queue after MaxAttempts retried a couple of times will be  
   acknowledged automatically And dead letter queue will send message to the original topic.  If message 
   can not send to dead letter queue,System will be blocked to retry infinite until it can be,    
   just like rabbitmq  
2. Because using the Spring Kafka, so it supports multithreading.  
