package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class DistributedLock implements Lock, Watcher {
    private ZooKeeper zk;
    private String lockName;
    private String ROOT_LOCK = "/locks";
    private String WAIT_LOCK;
    private int sessionTimeOut = 30000;
    private String CURRENT_LOCK; //临时节点
    //计数器
    private CountDownLatch countDownLatch;
    private List<Exception> exceptionList = new ArrayList<Exception>();
    public DistributedLock(String url,String lockName) {
        this.lockName = lockName;
        try {
            zk = new ZooKeeper(url, sessionTimeOut, this);
            Stat stat = zk.exists(ROOT_LOCK, false);
            if (stat==null){
                zk.create(ROOT_LOCK,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    //节点监视器
    public void process(WatchedEvent watchedEvent) {
        if (this.countDownLatch!=null){
            countDownLatch.countDown();
        }
    }

    public void lock() {
        if (exceptionList.size()>0){
            throw new LockException(exceptionList.get(0));
        }
        try {
            if (this.tryLock()){
                System.out.println(Thread.currentThread().getName()+" "+lockName+"获得了锁");
                return;
            }else{
                waitForLock(WAIT_LOCK,sessionTimeOut);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean waitForLock(String preLock, int sessionTimeOut) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(ROOT_LOCK + "/" + preLock, true);
        if (stat!=null){
            System.out.println(Thread.currentThread().getName()+"等待锁"+ROOT_LOCK+"/"+preLock);
            this.countDownLatch = new CountDownLatch(1);
            this.countDownLatch .await(sessionTimeOut,TimeUnit.MILLISECONDS);
            this.countDownLatch=null;
            System.out.println(Thread.currentThread().getName()+"等到了锁");
        }
        return true;
    }

    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }

    public boolean tryLock() {
        String split = "_lock_";
        try {
            CURRENT_LOCK = zk.create(ROOT_LOCK + "/" + lockName + split, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(CURRENT_LOCK+"已经被创建");
            List<String> subNodes = zk.getChildren(ROOT_LOCK, false);
            List<String> locks = new ArrayList<String>();
            for (String node : subNodes) {
                String s = node.split(split)[0];
                if (s.equals(lockName)){
                    locks.add(node);
                }
            }
            Collections.sort(locks);
            System.out.println(Thread.currentThread().getName()+"的锁是"+CURRENT_LOCK);
            if (CURRENT_LOCK.equals(ROOT_LOCK+"/"+locks.get(0))){
                return true;
            }
            String preNode = CURRENT_LOCK.substring(CURRENT_LOCK.lastIndexOf("/") + 1);
            WAIT_LOCK = locks.get(Collections.binarySearch(locks, preNode) - 1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        try {
            if (this.tryLock()){
                return true;
            }
            return waitForLock(WAIT_LOCK,sessionTimeOut);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public void unlock() {
        try {
            System.out.println("释放锁"+CURRENT_LOCK);
            zk.delete(CURRENT_LOCK,-1);
            CURRENT_LOCK=null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public Condition newCondition() {
        return null;
    }

    //自定义lock异常
    public class LockException extends RuntimeException{
        private static final long serialVersionUID = 1L;
        public LockException(String message) {
            super(message);
        }
        public LockException(Exception e) {
            super(e);
        }
    }

}
