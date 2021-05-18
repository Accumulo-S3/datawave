package datawave.microservice.common.storage;

import org.springframework.messaging.Message;

public interface QueryQueueListener {
    long WAIT_MS = 100L;
    
    /**
     * Get the listener id
     * 
     * @return the listener id
     */
    String getListenerId();
    
    /**
     * Receive a message
     * 
     * @return the message, or null if none available within WAIT_MS
     */
    default Message<byte[]> receive() {
        return receive(WAIT_MS);
    }
    
    /**
     * Receive a message
     * 
     * @param waitMs
     * @return the message, or null if none available within waitMs
     */
    Message<byte[]> receive(long waitMs);
    
    /**
     * Stop the listener, effectively destroying the listener
     */
    void stop();
}
