/** @author mark_purcell@ie.ibm.com */

package sample;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import org.junit.*;


public class MqLightSampleClientTest {
	protected static final Logger logger = Logger.getLogger(MqLightSampleClientTest.class.getName());
	protected static MqLightSampleClient server = new MqLightSampleClient(false);
	protected static MqLightSampleClient client = new MqLightSampleClient(true);

	@BeforeClass
	public static void start() throws Exception {
		String amqp = "amqp://127.0.0.1:5672";
		String user = "this_user";
		String pwd = "my_password";

		logger.log(Level.INFO, user+":"+pwd+"@"+amqp);

		try {
			logger.log(Level.INFO,"Creating an MQ Light client...");
			server.initialise(amqp, user, pwd);
			client.initialise(amqp, user, pwd);
		}
		catch(Exception e) {
			logger.log(Level.SEVERE, "Failed to initialise", e);
			throw new RuntimeException(e);
		}

		logger.log(Level.INFO, "Completed initialisation.");
	}

	@AfterClass
	public static void stop() {
		client.stop();
		server.stop();
	}

	@Test
	public void testMq() {
		server.subscribe("the_topic/+");
		client.subscribe("the_topic/me/result");

		try {
			for (int i=0; i<3; i++) {
				client.publish("the_topic/me", "Message"+i);
			}

			Thread.sleep(3000);
			assertEquals(3, server.getReceived());
			assertEquals(3, client.getReceived());
		}
		catch(Exception e) {
			fail(e.getMessage());
		}
	}
}

