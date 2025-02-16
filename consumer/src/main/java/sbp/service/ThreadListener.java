package sbp.service;

import java.util.Properties;

public class ThreadListener extends Thread {
    private final ConsumerService consumerService;
    private final Properties properties;

    public ThreadListener(Properties properties) {
        this.consumerService = new ConsumerService();
        this.properties = properties;
    }

    @Override
    public void run() {
        consumerService.read(properties);
    }
}
