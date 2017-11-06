package me.jinsui.shennong.util;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * To update bk-layout in zk 2017/10/24.
 */
public class ZKClient {
    public static void main(String[] args) throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);

//        String zkHostPort = "172.18.11.160:2181";
        String zkHostPort = args[0];
//        System.out.print("zk is " + zkHostPort);
        ZooKeeper zkc = new ZooKeeper(zkHostPort, 5000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }
        });
        if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new IOException("Zookeeper took too long to connect");
        }

        if(zkc.exists("/ledgers/LAYOUT",false) != null)
            zkc.delete("/ledgers/LAYOUT",zkc.exists("/ledgers/LAYOUT",false).getVersion());
        //todo which version does bookkeeper use actually?
        zkc.create("/ledgers/LAYOUT", "2\ndlshade.org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory:1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);


    }
}
