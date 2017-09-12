package de.tud.ecast;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.json.JSONObject;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class GOFLEXAPI {
	private String clientId; 
	private String publishTopic;
	private String subscribeTopic;
	private Channel publisher;
	private Channel subscriber;
	
	public GOFLEXAPI(String host, int port, String user, String password, String publish, String subscribe) {
		this.clientId = UUID.randomUUID().toString();
		this.publishTopic = publish;
		this.subscribeTopic = subscribe + "/" + this.clientId;
		
		try {
			this.publisher = connect(host, port, user, password);
			this.publisher.queueDeclare(this.publishTopic, true, false, false, null);
			this.subscriber = connect(host, port, user, password);
			this.subscriber.queueDeclare(this.subscribeTopic, false, true, false, null);
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}
	}
	
	private Channel connect(String host, int port, String user, String password) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(user);
		factory.setPassword(password);
		factory.setVirtualHost("/");
		factory.setHost(host);
		factory.setPort(port);
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		return(channel);		
	}
	
	public void publish(JSONObject message, int correlation) {
		message.getJSONObject("serviceRequest").getJSONObject("requestor").put("replyTo", subscribeTopic);
		message.getJSONObject("serviceRequest").getJSONObject("requestor").put("clientID", clientId);
		message.getJSONObject("serviceRequest").getJSONObject("requestor").put("correlationID", correlation);
		byte[] jsonMessage = message.toString().getBytes();
		
		BasicProperties props = new BasicProperties().builder().deliveryMode(1).build();
		
		try {
			publisher.basicPublish("", publishTopic, props, jsonMessage);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void receive() {
		Consumer consumer = new DefaultConsumer(subscriber) {
	      @Override
	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
	          throws IOException {
	    	subscriber.basicAck(envelope.getDeliveryTag(), false);
	    	
	        String message = new String(body, "UTF-8");
	        System.out.println(" [x] Received '" + message + "'");
	      }
	    };
	    
	    try {
			subscriber.basicConsume(subscribeTopic, false, consumer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}			
}
