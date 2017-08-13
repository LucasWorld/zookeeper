package lucas;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

/**
 * Created by Lucas on 2017/7/30.
 */
public class AdminClient implements Watcher{
    ZooKeeper zk;
    String hostPort;

    public AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    void listState() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        try {
            byte masterData[] = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + "since" + startDate);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Workers.......");

        for (String worker : zk.getChildren("/workers", false)){
            byte data[] = zk.getData("/workers/" + worker, false, null);
            String state = new String(data);
            System.out.println("\t" + worker + ":" + state);
        }

        System.out.println("Tasks......");
        for (String task : zk.getChildren("/assign", false)){
            System.out.println("\t" + task);
        }

    }

    public static void main(String args[]) throws IOException, KeeperException, InterruptedException {
        AdminClient client = new AdminClient(args[0]);

        client.startZK();
        //client.listState();

    }
}
