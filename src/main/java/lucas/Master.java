package lucas;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Created by Lucas on 2017/7/30.
 *
 * @author Lucas Y D
 */
public class Master implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    enum MasterStates {RUNNING, ELECTED, NOTELECTED}

    private volatile MasterStates state = MasterStates.RUNNING;

    MasterStates getState() {
        return state;
    }

    private Random random = new Random(this.hashCode());
    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toHexString((new Random().nextInt()));

    private volatile boolean connedted = false;
    private volatile boolean expired = false;

    protected ChildrenCache tasksCache;
    //The cache that holds the last set of workers we have seen
    protected ChildrenCache workersCache;

    boolean isLeader = false;
    String path = "/master";
    final String WORKERS = "/workers";
    final String TASKS = "/tasks";
    final String ASSIGN = "/assign";
    final String STATUS = "/status";


    public Master(String hostPort) {
        this.hostPort = hostPort;
    }


    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public void stopZK() throws Exception {
        zk.close();
    }

    public void bootstrap() {
        createParent(WORKERS, new byte[0]);
        createParent(ASSIGN, new byte[0]);
        createParent(TASKS, new byte[0]);
        createParent(STATUS, new byte[0]);
    }

    void createParent(String p, byte[] data) {
        zk.create(p, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                (int rc, String path, Object ctx, String name) -> {
                    switch (Code.get(rc)) {
                        case CONNECTIONLOSS:
                            createParent(path, (byte[]) ctx);
                            break;
                        case OK:
                            System.out.println("Created");
                            break;
                        case NODEEXISTS:
                            System.out.println("Parent already registered: " + path);
                            break;
                        default:
                            System.out.println("something went wrong");
                    }
                }, data);
    }


    boolean isConnedted() {
        return connedted;
    }

    boolean isExpired() {
        return expired;
    }


    /*
     * Run for master. To run for master, we try to create the /master znode,
     * with masteCreateCallback being the callback implementation.
     * In the case the create call succeeds, the client becomes the master.
     * If it receives a CONNECTIONLOSS event, then it needs to check if the
     * znode has been created. In the case the znode exists, it needs to watch it and check
     * which server is the master.
     */

    /**
     * Tries to create a /master lock znode to acquire leadership.
     */


    void runForMaster() {
        //(final String path, byte data[], List<ACL> acl, CreateMode createMode,  StringCallback cb, Object ctx)
        zk.create(path, serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                //StringCallback
                (int rc, String path, Object ctx, String name) -> {
                    switch (Code.get(rc)) {
                        /**
                         * In case of connection loss event, the client check if the master node is there as it
                         * doesn't know if it has been able to create it or not.
                         * */
                        case CONNECTIONLOSS:
                            checkMaster();
                            return;
                        case OK:
                            state = MasterStates.ELECTED;
                            takeLeadership();
                            isLeader = true;
                            break;
                        /**
                         * if someone else already create the znode, then the client needs to watch it
                         * */
                        case NODEEXISTS:
                            state = MasterStates.NOTELECTED;
                            masterExists();
                        default:
                            //
                            state = MasterStates.NOTELECTED;
                            LOG.error("Something went wrong when running for master",
                                    KeeperException.create(Code.get(rc)));
                            isLeader = false;
                    }
                }, null);
        isLeader = true;
    }

    void checkMaster() {
        zk.getData(path, false,
                (int rc, String path, Object ctx, byte[] data, Stat stat) -> {
                    switch (Code.get(rc)) {
                        case CONNECTIONLOSS:
                            checkMaster();
                            return;
                        case NONODE:
                            runForMaster();
                            return;
                    }
                }, null);
    }

    void masterExists() {
        //(final String path, Watcher watcher, StatCallback cb, Object ctx)
        //masterExistWatcher
        //set an watch on the /master znode
        zk.exists(path, (WatchedEvent event) -> {
            //if the /master is deleted, then it runs for master again
            if (event.getType() == Event.EventType.NodeDeleted) {
                assert "/master".equals(event.getPath());
                runForMaster();
            }
            //masterExistCallback
        }, (int rc, String path, Object ctx, Stat stat) -> {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    /**
                     * if exists returns OK, run for master in the case the znode is gone.
                     * */
                    if (stat == null) {
                        state = MasterStates.RUNNING;
                        runForMaster();
                    }
                    break;
                default:
                    //if something unexpected happens, check if /master is there by getting its data
                    checkMaster();
                    break;
            }
        }, null);
    }

    void takeLeadership() {
        LOG.info("Going to list of workers");
        getWorkers();


    }

    //obtain the children list and watch for changes
    void getWorkers() {
        //workersChangedWatcher
        zk.getChildren(WORKERS, (WatchedEvent event) -> {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                getWorkers();
            }
            //workersGetChildrenCallback
        }, (int rc, String path, Object ctx, List<String> children) -> {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    //reassigns tasks of dead workers and sets the new list of worker
                    LOG.info("Successfully got a list of worker: " + children.size() + " workers");
                    reassignAndSet(children);
                    break;
                default:
                    LOG.error("getChildren failed", KeeperException.create(Code.get(rc)));
            }
        }, null);
    }

    void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if (workersCache == null) {
            //if this is the first time it is using the cache, then instantiate it
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            //if this is not the first time, we should check if some workers has been removed
            LOG.info("Removing and setting");
            toProcess = workersCache.removedAndSet(children);
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }


    void getAbsentWorkerTasks(String worker) {
        zk.getChildren("/assign/" + worker, false, (int rc, String path, Object ctx, List<String> children) -> {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getAbsentWorkerTasks(path);
                    break;

                case OK:
                    LOG.info("Successfully got a list of assignments: " + children.size() + "tasks");
                    for (String task : children) {
                        getDataReassign(path + "/" + task, task);
                    }
                    break;

                default:
                    LOG.error("getChildren failed", KeeperException.create(Code.get(rc), path));
            }
        }, null);
    }


    void getTasks() {
        zk.getChildren(TASKS, (WatchedEvent event) -> {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert TASKS.equals(event.getPath());
                getTasks();
            }
        }, (int rc, String path, Object ctx, List<String> children) -> {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    List<String> toProcess;
                    if (tasksCache == null) {
                        tasksCache = new ChildrenCache(children);
                        toProcess = children;
                    } else {
                        toProcess = tasksCache.addedAndSet(children);
                    }

                    if (null != toProcess) {
                        assignTasks(toProcess);
                    }

                    break;
                default:
                    LOG.error("getChildren failed.",
                            KeeperException.create(Code.get(rc), path));
            }
        }, null);
    }

    void assignTasks(List<String> tasks) {
        for (String task : tasks) {
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        zk.getData(TASKS + "/" + task, false, (int rc, String path, Object ctx, byte[] data, Stat stat) -> {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTaskData(path);
                    break;
                case OK:
                    //Choose worker at random.
                    List<String> list = workersCache.getList();
                    String designatedWorker = list.get(random.nextInt(list.size()));
                    //Assign task to randomly chosen worker.

                    String assignmentPath = "/assign/" + designatedWorker + "/" + (String) ctx;
                    LOG.info("Assignment path: " + assignmentPath);
                    createAssignment(assignmentPath, data);
                    break;
                default:
                    LOG.error("Error when trying to get task data.",
                            KeeperException.create(Code.get(rc), path));
            }
        }, task);
    }

    void createAssignment(String path, byte[] data){
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                (int rc, String p, Object ctx, String name) -> {
                    switch (Code.get(rc)) {
                        case CONNECTIONLOSS:
                            createAssignment(p, (byte[])ctx);
                            break;
                        case OK:
                            LOG.info("Task assigned correctly: " + name);
                            deleteTask(name.substring( name.lastIndexOf("/") + 1));
                            break;
                        case NODEEXISTS:
                            LOG.warn("Task already assigned");
                            break;
                        default:
                            LOG.error("Error when trying to assign task.",
                                    KeeperException.create(Code.get(rc), path));
                    }
                },
                data);
    }

    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteTask(String name){
        zk.delete("/tasks/" + name, -1, (int rc, String path, Object ctx) -> {
            switch (Code.get(rc)){
                case CONNECTIONLOSS:
                    deleteTask(path);
                    break;
                case OK:
                    LOG.info("Successfully deleted " + path);
                    break;
                case NONODE:
                    LOG.info("Task has been deleted already");
                    break;
                default:
                    LOG.error("Something went wrong here, " +
                            KeeperException.create(Code.get(rc), path));
            }
        }, null);
    }


    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. *
     ************************************************
     */

    /**
     * Context for recreate operation.
     */
    class RecreateTaskCtx {
        String path;
        String task;
        byte[] data;

        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get reassigned task data.
     *
     * @param p    Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String p, String task) {
        zk.getData(path,
                false,
                (int rc, String path, Object ctx, byte[] data, Stat stat) -> {
                    switch (Code.get(rc)) {
                        case CONNECTIONLOSS:
                            getDataReassign(path, (String) ctx);

                            break;
                        case OK:
                            recreateTask(new RecreateTaskCtx(path, (String) ctx, data));

                            break;
                        default:
                            LOG.error("Something went wrong when getting data ",
                                    KeeperException.create(Code.get(rc)));
                    }
                },
                task);
    }


    /**
     * Recreate task znode in /tasks
     *
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                (int rc, String path, Object c, String name) -> {
                    switch (Code.get(rc)) {
                        case CONNECTIONLOSS:
                            recreateTask((RecreateTaskCtx) c);

                            break;
                        case OK:
                            deleteAssignment(((RecreateTaskCtx) c).path);

                            break;
                        case NODEEXISTS:
                            LOG.info("Node exists already, but if it hasn't been deleted, " +
                                    "then it will eventually, so we keep trying: " + path);
                            recreateTask((RecreateTaskCtx) c);

                            break;
                        default:
                            LOG.error("Something wwnt wrong when recreating task",
                                    KeeperException.create(Code.get(rc)));
                    }
                },
                ctx);
    }


    /**
     * Delete assignment of absent worker
     *
     * @param p Path of znode to be deleted
     */
    void deleteAssignment(String p) {
        zk.delete(path, -1, (int rc, String path, Object rtx) -> {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteAssignment(path);
                    break;
                case OK:
                    LOG.info("Task correctly deleted: " + path);
                    break;
                default:
                    LOG.error("Failed to delete task data" +
                            KeeperException.create(Code.get(rc), path));
            }
        }, null);
    }


    public static void main(String args[]) throws Exception {
        Master master = new Master(args[0]);
        master.startZK();
        master.bootstrap();
        master.runForMaster();
        ;

        if (master.isLeader) {
            System.out.println("I'm a leader");
            Thread.sleep(60000);
        } else {
            System.out.println("someone else is the leader");
        }
        //wait for a bit
        Thread.sleep(60000);

        master.stopZK();
    }
}
