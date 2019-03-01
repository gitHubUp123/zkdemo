package zookeeper;

public class Test {
    static int n = 500;
    public static void seckill(){
        System.out.println(--n);
    }
    public static void main(String[] args) {


        Runnable runnable = new Runnable() {
            public void run() {
                DistributedLock lock = null;
                try {
                    lock = new DistributedLock("119.29.28.114:2181","test1");
                    lock.lock();
                    seckill();
                    System.out.println(Thread.currentThread().getName()+"正在运行");
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    if (lock!=null){
                        lock.unlock();
                    }
                }

            }
        };
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(runnable);
            thread.start();
        }
    }
}
