import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.nio.charset.StandardCharsets;
import java.sql.SQLOutput;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class MQTTKafkaAdapter {

    private IMqttClient subscriber;
    private Producer<String, String> kafkaProducer;

    public MQTTKafkaAdapter(String publisherId, String subscriberURI, Producer<String, String> kafkaProducer) {
        try {
            subscriber = new MqttClient(subscriberURI, publisherId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            subscriber.connect(options);
        } catch (MqttException e) {
            e.printStackTrace();
        }

        this.kafkaProducer = kafkaProducer;
    }


    public void handleMessagesFromTopic(String topicName, int numberOfMessages){
        CountDownLatch receivedSignal = new CountDownLatch(numberOfMessages);
        try {
            subscriber.subscribe("esp32/x", (topic, msg) -> {
                byte[] payload = msg.getPayload();
                // ... payload handling omitted
                kafkaProducer.send(new ProducerRecord<String, String>(topicName, null, new String(payload, StandardCharsets.UTF_8)));


                //System.out.println(new String(payload, StandardCharsets.UTF_8));
                receivedSignal.countDown();
            });
            receivedSignal.await(1, TimeUnit.MINUTES);
        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
        kafkaProducer.close();
        System.out.println("finished");

    }
}
