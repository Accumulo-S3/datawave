package datawave.microservice.common.storage.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryStoragePropertiesTest {
    
    @Test
    public void testQueryStorageProperties() {
        QueryStorageProperties props = new QueryStorageProperties();
        props.setSynchStorage(true);
        assertTrue(props.isSynchStorage());
        props.setSynchStorage(false);
        assertFalse(props.isSynchStorage());
        props.setSendNotifications(true);
        assertTrue(props.isSendNotifications());
        props.setSendNotifications(false);
        assertFalse(props.isSendNotifications());
        props.setBackend(QueryStorageProperties.BACKEND.KAFKA);
        assertEquals(QueryStorageProperties.BACKEND.KAFKA, props.getBackend());
        props.setLockManager(QueryStorageProperties.LOCKMGR.ZOO);
        assertEquals(QueryStorageProperties.LOCKMGR.ZOO, props.getLockManager());
        props.setZookeeperConnectionString("localhost:199999");
        assertEquals("localhost:199999", props.getZookeeperConnectionString());
        props.setRabbitConnectionString("localhost:199998");
        assertEquals("localhost:199998", props.getRabbitConnectionString());
        props.setKafkaConnectionString("localhost:199997");
        assertEquals("localhost:199997", props.getKafkaConnectionString());
    }
}