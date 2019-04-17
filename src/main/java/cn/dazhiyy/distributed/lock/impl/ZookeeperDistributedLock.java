package cn.dazhiyy.distributed.lock.impl;

import cn.dazhiyy.distributed.lock.DistributedLock;
import cn.dazhiyy.distributed.lock.exception.DistributedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * @author dazhi
 * @projectName distributedlock
 * @packageName cn.dazhiyy.distributed.lock.impl
 * @className ZookeeperDistributedLock
 * @description TODO
 * @date 2019/4/17 21:47
 */
public class ZookeeperDistributedLock implements DistributedLock, Watcher {

    private String path;
    private ZooKeeper zooKeeper;
    private CountDownLatch countDownLatch;
    /**
     * 父节点的路径
     */
    private final String DEFAULT_ROOT_PATH = "/distributed_lock";
    private final String PATH = "/";

    public ZookeeperDistributedLock(ZooKeeper zooKeeper,String path) throws DistributedException{
        if (zooKeeper == null) {
            throw new DistributedException("不能传入一个null的zookeeper对象");
        }
        this.zooKeeper = zooKeeper;
        this.path = path;

        // 新建父节点
        try {
            Stat exists = zooKeeper.exists( DEFAULT_ROOT_PATH, Boolean.TRUE);
            if (exists == null) {
                // 父节点不存在，创建父节点,父节点是临时节点
                zooKeeper.create(DEFAULT_ROOT_PATH,"".getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
        } catch (Exception e) {
            throw new DistributedException(e.getMessage());
        }
    }

    public void process(WatchedEvent event) {
        if (this.countDownLatch != null) {
            this.countDownLatch.countDown();
        }
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void lock() {
        if (!tryLock()){
            waitForLock();
        }
    }

    private void waitForLock() {
        // 等待拿锁
        if (countDownLatch == null ) {
            countDownLatch = new CountDownLatch(1);
        }

        try {
            countDownLatch.wait();
            // 线程被唤醒 重新尝试拿锁
            lock();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void lockInterruptibly() throws InterruptedException {

    }

    public boolean tryLock() {
        // 尝试去拿锁
        try {
            Stat exists = zooKeeper.exists( DEFAULT_ROOT_PATH+PATH+path, Boolean.TRUE);
            if (exists==null) {
                zooKeeper.create(DEFAULT_ROOT_PATH+PATH+path, "".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            }
            // 若抛出异常，说明节点创建失败
            List<String> subNodes = zooKeeper.getChildren(DEFAULT_ROOT_PATH, false);
            Collections.sort(subNodes);

            if (path.equals(subNodes.get(0))){
                return Boolean.TRUE;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return Boolean.FALSE;
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {

        return false;
    }

    public void unlock() {
        // 释放锁
        try {
            zooKeeper.delete(DEFAULT_ROOT_PATH+PATH+path, -1);
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    public Condition newCondition() {
        return null;
    }


    public static void main(String[] args) throws IOException, DistributedException, InterruptedException {
        final ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181",4000,null);
        ZookeeperDistributedLock  lock = new ZookeeperDistributedLock(zooKeeper, "test" );
        lock.lock();
        Thread.sleep(10000L);
//        for (int i = 0; i < 100; i++) {
//            final Integer count = i;
//            new Thread(new Runnable() {
//                public void run() {
//                    try {
//                        ZookeeperDistributedLock  lock = new ZookeeperDistributedLock(zooKeeper, "test" + count);
//                        lock.lock();
//                        System.out.println("test" + count +":拿到了锁");
//                        Thread.sleep(1000);
//                        lock.unlock();
//                    } catch (DistributedException e) {
//
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//
//                }
//            }).start();
//
//        }

    }

}
