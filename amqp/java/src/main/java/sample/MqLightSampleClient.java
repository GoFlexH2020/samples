/** @author mark_purcell@ie.ibm.com */

package sample;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.mqlight.api.*;


public class MqLightSampleClient {
	protected static final Logger logger = Logger.getLogger(MqLightSampleClient.class.getName());
	protected NonBlockingClient client = null;
	protected int received=0, sent=0;
	protected boolean isClient = true;

	private MqLightSampleClient() { }
	public MqLightSampleClient(boolean client) {
		this.isClient = client;
	}
	public int getSent() { return sent; }
	public int getReceived() { return received; }
	public String getId() {	return client.getId(); }

	public void initialise(String amqp, String user, String pwd) throws Exception {
		ClientOptions clientOpts = ClientOptions.builder().setCredentials(user, pwd).build();
		client = NonBlockingClient.create(amqp, clientOpts, new NonBlockingClientAdapter<Void>() {
			@Override
			public void onStarted(NonBlockingClient client, Void context) {
				logger.log(Level.INFO, "Connected to " + client.getService() + ":" + client.getId());
			}

			public void onRetrying(NonBlockingClient client, Void context, ClientException t) {
				logger.log(Level.INFO, "MQ Light connected failed, retrying...");
				if (t != null)
					logger.log(Level.INFO, t.getMessage());
			}

			@Override
			public void onStopped(NonBlockingClient client, Void context, ClientException t) {
				logger.log(Level.INFO, "MQ Light client stopped");
				if (t != null)
					logger.log(Level.INFO, t.getMessage());
			}
		}, null);

		logger.log(Level.INFO, "MQ Light client created. Current state: " + client.getState());
		Thread.sleep(2500);

		if (client.getState() != ClientState.STARTED)
			throw new Exception("Not started!!!");
	}

	public void stop() {
		client.stop(new CompletionListener<Void>() {
				public void onSuccess(NonBlockingClient client, Void context) { }
				public void onError(NonBlockingClient c, Void ctx, Exception exception) { }
			}, null);
	}

	public void subscribe(String topic) {
		SubscribeOptions subscribeOpts = SubscribeOptions.builder().setShare("PS").build();
		client.subscribe(topic, subscribeOpts, new DestinationAdapter<Void>() {
				public void onMessage(NonBlockingClient client, Void context, Delivery delivery) {
					received++;
					StringDelivery sd = (StringDelivery) delivery;
					logger.log(Level.INFO,"Topic: " + sd.getTopic() + ":" + sd.getData());

					if (!isClient) // If server, ping back the message on the result topic
						publish(sd.getTopic() + "/result", sd.getData());
				}
			},
			new CompletionListener<Void>() {
				public void onSuccess(NonBlockingClient client, Void context) { }
				public void onError(NonBlockingClient c, Void ctx, Exception exception) {
					logger.log(Level.SEVERE, "Subscribe Error! - ", exception);
				}
			}, null);
	}

	public void publish(String topic, String input) {
			SendOptions sendOpts = SendOptions.builder().setQos(QOS.AT_LEAST_ONCE).build();
			client.send(topic, input, null, sendOpts, new CompletionListener<Void>() {
				public void onSuccess(NonBlockingClient client, Void context) {
					sent++;
				}
				public void onError(NonBlockingClient client, Void context, Exception exception) {
					logger.log(Level.SEVERE, "Send Error! - " + exception.toString());
				}
			}, null);
	}
}

