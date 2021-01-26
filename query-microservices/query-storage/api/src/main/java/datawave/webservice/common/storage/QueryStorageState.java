package datawave.webservice.common.storage;

import java.util.List;
import java.util.UUID;

/**
 * This is the interface to the query storage state service
 */
public interface QueryStorageState {
    
    /**
     * Get the list of queries running.
     */
    List<QueryState> getRunningQueries();
    
    /**
     * Get the list of taskIDs for a query
     */
    List<TaskDescription> getTasks(String queryId);
    
    /**
     * Get the running queries for a query type
     */
    List<QueryState> getRunningQueries(String type);
}
