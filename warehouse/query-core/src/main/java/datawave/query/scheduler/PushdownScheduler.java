package datawave.query.scheduler;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ShardQueryLogic;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.accumulo.inmemory.impl.InMemoryTabletLocator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.TabletLocatorImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import datawave.mr.bulk.RfileResource;
import datawave.query.tables.BatchScannerSession;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.tables.async.event.VisitorFunction;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.configuration.QueryData;

/**
 * Purpose: Pushes down individual queries to the Tservers. Is aware that each server may have a different query, thus bins ranges per tserver and keeps the
 * plan that corresponds with those queries
 */
public class PushdownScheduler extends Scheduler {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(PushdownScheduler.class);
    
    /**
     * Configuration reference.
     */
    protected final ShardQueryConfiguration config;
    /**
     * Scanner factory reference.
     */
    protected final ScannerFactory scannerFactory;
    /**
     * Count for the number of QueryPlans that we have
     */
    protected final AtomicInteger count = new AtomicInteger(0);
    /**
     * BatchScannerSession reference, so that we can close upon completion or error
     */
    protected BatchScannerSession session = null;
    
    protected Iterator<Entry<Key,Value>> currentIterator = null;
    
    protected List<Function<IteratorSetting,IteratorSetting>> customizedFunctionList;
    
    /**
     * Local instance of the table ID
     */
    protected String tableId;
    
    protected MetadataHelper metadataHelper;
    
    public PushdownScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metaFactory) {
        this(config, scannerFactory, metaFactory.createMetadataHelper(config.getConnector(), config.getMetadataTableName(), config.getAuthorizations()));
    }
    
    protected PushdownScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper) {
        this.config = config;
        this.metadataHelper = helper;
        this.scannerFactory = scannerFactory;
        customizedFunctionList = Lists.newArrayList();
        Preconditions.checkNotNull(config.getConnector());
    }
    
    public void addSetting(IteratorSetting customSetting) {
        settings.add(customSetting);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        if (null == this.config) {
            throw new IllegalArgumentException("Null configuration provided");
        }
        
        try {
            
            return concatIterators();
        } catch (AccumuloException | ParseException | TableNotFoundException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    /**
     * @return
     * @throws ParseException
     * @throws TableNotFoundException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     */
    protected Iterator<Entry<Key,Value>> concatIterators() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, ParseException {
        
        String tableName = config.getShardTableName();
        
        Set<Authorizations> auths = config.getAuthorizations();
        
        TabletLocator tl;
        
        Instance instance = config.getConnector().getInstance();
//        if (instance instanceof InMemoryInstance) {
        if (false) {
//            tl = new TabletLocatorImpl();
            tableId = config.getTableName();
        } else {
            Properties properties = new Properties();
            StreamSupport
                .stream(DefaultConfiguration.getInstance().spliterator(), false).forEach(k -> properties.setProperty(k.getKey(), k.getValue()));
            Credentials credentials = new Credentials(config.getConnector().whoami(), new PasswordToken(config.getAccumuloPassword()));
            ClientInfo info = new ClientInfoImpl(properties, credentials.getToken());
            AccumuloConfiguration serverConf = ClientConfConverter.toAccumuloConf(properties);
            ClientContext ctx = new ClientContext(new SingletonReservation(), info, serverConf);

            tableId = Tables.getTableId(ctx, tableName).toString();
//            Credentials credentials = new Credentials(config.getConnector().whoami(), new PasswordToken(config.getAccumuloPassword()));
            tl = TabletLocator.getLocator(ctx, TableId.of(tableId));
        }
        Iterator<List<ScannerChunk>> chunkIter = Iterators.transform(getQueryDataIterator(), new PushdownFunction(tl, config, settings, tableId));
        
        try {
            session = scannerFactory.newQueryScanner(tableName, auths, config.getQuery());
            
            if (config.getBypassAccumulo()) {
                session.setDelegatedInitializer(RfileResource.class);
            }
            
            if (config.getSpeculativeScanning()) {
                session.setSpeculativeScanning(true);
            }
            
            session.addVisitor(new VisitorFunction(config, metadataHelper));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        session.setScanLimit(config.getMaxDocScanTimeout());
        
        if (config.getBackoffEnabled()) {
            session.setBackoffEnabled(true);
        }
        
        session.setChunkIter(chunkIter);
        
        session.setTabletLocator(tl);
        
        session.updateIdentifier(config.getQuery().getId().toString());
        
        return session;
    }
    
    protected Iterator<QueryData> getQueryDataIterator() {
        return config.getQueries();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        if (session != null)
            scannerFactory.close(session);
        
        log.debug("Ran " + count.get() + " queries for a single user query");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see Scheduler#createBatchScanner(ShardQueryConfiguration, datawave.query.tables.ScannerFactory, datawave.webservice.query.configuration.QueryData)
     */
    @Override
    public BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        return ShardQueryLogic.createBatchScanner(config, scannerFactory, qd);
    }
    
    @Override
    public ScanSessionStats getSchedulerStats() {
        
        ScanSessionStats stats = null;
        if (null != session) {
            stats = session.getStatistics();
        }
        return stats;
    }
    
}
