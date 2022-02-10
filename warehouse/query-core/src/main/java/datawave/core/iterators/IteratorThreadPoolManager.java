package datawave.core.iterators;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
//import org.apache.accumulo.core.util.threads.NamedThreadFactory;
//import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class IteratorThreadPoolManager {
    private static final Logger log = Logger.getLogger(IteratorThreadPoolManager.class);
    private static final String IVARATOR_THREAD_PROP = "tserver.datawave.ivarator.threads";
    private static final String IVARATOR_THREAD_NAME = "DATAWAVE Ivarator";
    private static final String EVALUATOR_THREAD_PROP = "tserver.datawave.evaluation.threads";
    private static final String EVALUATOR_THREAD_NAME = "DATAWAVE Evaluation";
    private static final int DEFAULT_THREAD_POOL_SIZE = 100;
    
    private Map<String,ExecutorService> threadPools = new TreeMap<>();
    
    private static final Object instanceSemaphore = new Object();
    private static final String instanceId = Integer.toHexString(instanceSemaphore.hashCode());
    private static volatile IteratorThreadPoolManager instance;
    
    private IteratorThreadPoolManager(IteratorEnvironment env) {
        // create the thread pools
        createExecutorService(IVARATOR_THREAD_PROP, IVARATOR_THREAD_NAME, env);
        createExecutorService(EVALUATOR_THREAD_PROP, EVALUATOR_THREAD_NAME, env);
    }
    
    private ThreadPoolExecutor createExecutorService(final String prop, final String name, IteratorEnvironment env) {
        final AccumuloConfiguration accumuloConfiguration;
        if (env != null) {
            accumuloConfiguration = env.getConfig();
        } else {
            accumuloConfiguration = DefaultConfiguration.getInstance();
        }
        final ThreadPoolExecutor service = createExecutorService(getMaxThreads(prop, accumuloConfiguration), name + " (" + instanceId + ')');
        threadPools.put(name, service);
        SimpleTimer.getInstance(accumuloConfiguration).schedule(() -> {
            try {
                
                int max = getMaxThreads(prop, accumuloConfiguration);
                if (service.getMaximumPoolSize() != max) {
                    log.info("Changing " + prop + " to " + max);
                    service.setCorePoolSize(max);
                    service.setMaximumPoolSize(max);
                }
            } catch (Throwable t) {
                log.error(t, t);
            }
        }, 1000, 10 * 1000);
        return service;
    }
    
    private ThreadPoolExecutor createExecutorService(int maxThreads, String name) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(maxThreads, maxThreads, 5 * 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                        new NamedThreadFactory(name));
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }
    
    private int getMaxThreads(final String prop, AccumuloConfiguration conf) {
        if (conf != null) {
            Map<String,String> properties = new TreeMap<>();
            conf.getProperties(properties, k -> Objects.equals(k, prop));
            if (properties.containsKey(prop)) {
                return Integer.parseInt(properties.get(prop));
            }
        }
        return DEFAULT_THREAD_POOL_SIZE;
    }
    
    private static IteratorThreadPoolManager instance(IteratorEnvironment env) {
        if (instance == null) {
            synchronized (instanceSemaphore) {
                if (instance == null) {
                    instance = new IteratorThreadPoolManager(env);
                }
            }
        }
        return instance;
    }
    
    private Future<?> execute(String name, final Runnable task, final String taskName) {
        return threadPools.get(name).submit(() -> {
            String oldName = Thread.currentThread().getName();
            Thread.currentThread().setName(oldName + " -> " + taskName);
            try {
                task.run();
            } finally {
                Thread.currentThread().setName(oldName);
            }
        });
    }
    
    public static Future<?> executeIvarator(Runnable task, String taskName, IteratorEnvironment env) {
        return instance(env).execute(IVARATOR_THREAD_NAME, task, taskName);
    }
    
    public static Future<?> executeEvaluation(Runnable task, String taskName, IteratorEnvironment env) {
        return instance(env).execute(EVALUATOR_THREAD_NAME, task, taskName);
    }


    public static class SimpleTimer {
        private static final org.slf4j.Logger log = LoggerFactory.getLogger(SimpleTimer.class);

        private static class ExceptionHandler implements Thread.UncaughtExceptionHandler {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.warn("SimpleTimer task failed", e);
            }
        }

        private static int instanceThreadPoolSize = -1;
        private static SimpleTimer instance;
        private ScheduledExecutorService executor;

        private static final int DEFAULT_THREAD_POOL_SIZE = 1;

        /**
         * Gets the timer instance. If an instance has already been created, it will have the number of
         * threads supplied when it was constructed, and the size provided here is ignored.
         *
         * @param threadPoolSize
         *          number of threads
         */
        public static synchronized SimpleTimer getInstance(int threadPoolSize) {
            if (instance == null) {
                instance = new SimpleTimer(threadPoolSize);
                SimpleTimer.instanceThreadPoolSize = threadPoolSize;
            } else {
                if (SimpleTimer.instanceThreadPoolSize != threadPoolSize) {
                    log.warn("Asked to create SimpleTimer with thread pool size " + threadPoolSize
                        + ", existing instance has " + instanceThreadPoolSize);
                }
            }
            return instance;
        }

        /**
         * Gets the timer instance. If an instance has already been created, it will have the number of
         * threads supplied when it was constructed, and the size provided by the configuration here is
         * ignored. If a null configuration is supplied, the number of threads defaults to 1.
         *
         * @param conf
         *          configuration from which to get the number of threads
         * @see Property#GENERAL_SIMPLETIMER_THREADPOOL_SIZE
         */
        public static synchronized SimpleTimer getInstance(AccumuloConfiguration conf) {
            int threadPoolSize;
            if (conf != null) {
                threadPoolSize = conf.getCount(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
            } else {
                threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
            }
            return getInstance(threadPoolSize);
        }

        /**
         * Gets the thread pool size for the timer instance. Use for testing only.
         *
         * @return thread pool size for timer instance, or -1 if not yet constructed
         */
        @VisibleForTesting
        static int getInstanceThreadPoolSize() {
            return instanceThreadPoolSize;
        }

        private SimpleTimer(int threadPoolSize) {
            executor = Executors.newScheduledThreadPool(threadPoolSize,
                new ThreadFactoryBuilder().setNameFormat("SimpleTimer-%d").setDaemon(true)
                    .setUncaughtExceptionHandler(new ExceptionHandler()).build());
        }

        /**
         * Schedules a task to run in the future.
         *
         * @param task
         *          task to run
         * @param delay
         *          number of milliseconds to wait before execution
         * @return future for scheduled task
         */
        public ScheduledFuture<?> schedule(Runnable task, long delay) {
            return executor.schedule(task, delay, TimeUnit.MILLISECONDS);
        }

        /**
         * Schedules a task to run in the future with a fixed delay between repeated executions.
         *
         * @param task
         *          task to run
         * @param delay
         *          number of milliseconds to wait before first execution
         * @param period
         *          number of milliseconds to wait between executions
         * @return future for scheduled task
         */
        public ScheduledFuture<?> schedule(Runnable task, long delay, long period) {
            return executor.scheduleWithFixedDelay(task, delay, period, TimeUnit.MILLISECONDS);
        }
    }

    public static class NamedThreadFactory implements ThreadFactory {

        private static final String FORMAT = "%s-%s-%d";

        private AtomicInteger threadNum = new AtomicInteger(1);
        private String name;
        private OptionalInt priority;

        NamedThreadFactory(String name) {
            this(name, OptionalInt.empty());
        }

        NamedThreadFactory(String name, OptionalInt priority) {
            this.name = name;
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable r) {
            String threadName = null;
            if (r instanceof NamedRunnable) {
                NamedRunnable nr = (NamedRunnable) r;
                threadName = String.format(FORMAT, name, nr.getName(), threadNum.getAndIncrement());
            } else {
                threadName =
                    String.format(FORMAT, name, r.getClass().getSimpleName(), threadNum.getAndIncrement());
            }
            return Threads.createThread(threadName, priority, r);
        }
    }

    class NamedRunnable implements Runnable {

        private final String name;
        private final OptionalInt priority;
        private final Runnable r;

        NamedRunnable(String name, Runnable r) {
            this(name, OptionalInt.empty(), r);
        }

        NamedRunnable(String name, OptionalInt priority, Runnable r) {
            this.name = name;
            this.priority = priority;
            this.r = r;
        }

        public String getName() {
            return name;
        }

        public OptionalInt getPriority() {
            return priority;
        }

        public void run() {
            r.run();
        }

    }
}
