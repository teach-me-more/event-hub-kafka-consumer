package com.rbasystems.kafka.eventhub.event_hub_kafka_consumer;

import com.rbasystems.kafka.eventhub.Employee;
import java.util.logging.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;


@Service
public class EmployeeConsumer {
    Logger LOGGER = Logger.getLogger("EmployeeConsumer");

   

    @KafkaListener(topics = "${spring.kafka.consumer.topic-name}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenEmployee(Message<Employee> employee) throws Exception{
        LOGGER.info("message recived from the event hub: in employee type ---{}"+ employee.getPayload());
if(employee.getPayload().getSalary()>50000){
LOGGER.info("throwing the error");
throw new Exception("Too much salary");
}
    }
}
