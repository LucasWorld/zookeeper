package lucas;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Lucas on 2017/7/30.
 */
public class Worker implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toHexString((new Random().nextInt()));
    final String PATH = "/workers/worker-" + serverId;

    private volatile boolean connected = false;
    private volatile boolean expired = false;


    private ThreadPoolExecutor executor;


    String name;
    String status;


    public Worker(String hostPort) {
        this.hostPort = hostPort;
        this.executor = new ThreadPoolExecutor(1, 1, 1000L, TimeUnit.MICROSECONDS,
                new ArrayBlockingQueue<Runnable>(200));
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event.toString() + " " + hostPort);
    }

    /**
     * Checks if this client is connected.
     *
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Checks if ZooKeeper session is expired.
     *
     * @return
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * Bootstrapping here is just creating a /assign parent
     * znode to hold the tasks assigned to this worker.
     */
    public void bootstrap() {
        createAssignNode();
    }

    void createAssignNode() {
        zk.create("/assign/worker-" + serverId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                (int rc, String path, Object ctx, String name) -> {
                    switch (Code.get(rc)) {
                        case CONNECTIONLOSS:
                            createAssignNode();
                            break;
                        case OK:
                            LOG.info("Assign node created");
                            break;
                        case NODEEXISTS:
                            LOG.warn("Assign node already registered");
                            break;
                        default:
                            LOG.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
                    }
                }, null);
    }

    void register() {
        //name = "worker-" + serverId;

        zk.create(PATH, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                (int rc, String path, Object ctx, String name) -> {
                    switch (Code.get(rc)) {
                        case OK:
                            System.out.println("registered successfully: " + serverId);
                            break;
                        case CONNECTIONLOSS:
                            register();
                            break;
                        case NODEEXISTS:
                            System.out.println("Already registered: " + serverId);
                        default:
                            System.out.println("Wrong " + KeeperException.create(Code.get(rc), path));
                    }

                }, null);
    }



    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            zk.setData("/workers/" + name, status.getBytes(), -1,
                    (int rc, String path, Object ctx, Stat stat) -> {
                        switch (Code.get(rc)) {
                            case CONNECTIONLOSS:
                                updateStatus((String) ctx);
                                return;
                        }
                    }, status);
        }
    }



    private int executionCount;

    synchronized void changeExecutionCount(int countChange) {
        executionCount += countChange;
        if (executionCount == 0 && countChange < 0) {
            // we have just become idle
            setStatus("Idle");
        }
        if (executionCount == 1 && countChange > 0) {
            // we have just become idle
            setStatus("Working");
        }
    }


    void getTasks() {
        zk.getChildren("/assign/worker-" + serverId, (WatchedEvent event) -> {
            if (event.getType().equals(Event.EventType.NodeChildrenChanged)) {
                getTasks();
            }
        }, (int rc, String path, Object ctx, List<String> children) -> {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (children != null) {
                        executor.execute(new Runnable() {
                            List<String> children;
                            AsyncCallback.DataCallback cb;

                            public Runnable init(List<String> children, AsyncCallback.DataCallback cb) {
                                this.children = children;
                                this.cb = cb;

                                return this;
                            }

                            @Override
                            public void run() {
                                if (children == null) return;

                                LOG.info("Looping into tasks");

                                setStatus("Working");

                                for (String task : children) {
                                    LOG.trace("New task: {}", task);
                                    zk.getData("/assign/worker-" + serverId + "/" + task,
                                            false,
                                            cb,
                                            task);
                                }


                            }
                        }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                    }
                    break;
                default:
                    LOG.error("getChildren failed: " + KeeperException.create(Code.get(rc), path));
            }

        }, null);
    }

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                /*
                 *  Executing a task in this example is simply printing out
                 *  some string representing the task.
                 */
                    executor.execute(new Runnable() {
                        byte[] data;
                        Object ctx;

                        /*
                         * Initializes the variables this anonymous class needs
                         */
                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;

                            return this;
                        }

                        public void run() {
                            LOG.info("Executing your task: " + new String(data));
                            zk.create("/status/" + (String) ctx, "done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT, taskStatusCreateCallback, null);
                            zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
                                    -1, taskVoidCallback, null);
                        }
                    }.init(data, ctx));

                    break;
                default:
                    LOG.error("Failed to get task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.create(path + "/status", "done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                            taskStatusCreateCallback, null);
                    break;
                case OK:
                    LOG.info("Created status znode correctly: " + name);
                    break;
                case NODEEXISTS:
                    LOG.warn("Node exists: " + path);
                    break;
                default:
                    LOG.error("Failed to create task data: ", KeeperException.create(Code.get(rc), path));
            }

        }
    };


    AsyncCallback.VoidCallback taskVoidCallback = (int rc, String path, Object rtx) -> {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    break;
                case OK:
                    LOG.info("Task correctly deleted: " + path);
                    break;
                default:
                    LOG.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            }
        };



    protected ChildrenCache assignedTasksCache = new ChildrenCache();


    public static void main(String args[]) throws Exception {
        Worker w = new Worker(args[0]);
        w.startZK();

        while(!w.isConnected()){
            Thread.sleep(100);
        }
        /*
         * bootstrap() create some necessary znodes.
         */
        w.bootstrap();

        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w.register();

        /*
         * Getting assigned tasks.
         */
        w.getTasks();

        while(!w.isExpired()){
            Thread.sleep(1000);
        }

    }
}
