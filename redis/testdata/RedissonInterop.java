import java.util.concurrent.TimeUnit;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

// Run against a disposable Redis instance to inspect real Redisson 3.22.1
// behavior. mode: hold, try, read, write. Port is the second argument.
public class RedissonInterop {
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        int port = Integer.parseInt(args[1]);
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:" + port);
        RedissonClient client = Redisson.create(config);
        try {
            RLock lock = mode.equals("read") ? client.getReadWriteLock("shared-lock").readLock()
                    : mode.equals("write") ? client.getReadWriteLock("shared-lock").writeLock()
                    : client.getLock("shared-lock");
            boolean acquired = lock.tryLock(0, 5, TimeUnit.SECONDS);
            System.out.println("ACQUIRED=" + acquired);
            System.out.flush();
            if (acquired) {
                if (mode.equals("hold")) Thread.sleep(args.length > 2 ? Long.parseLong(args[2]) : 10000);
                lock.unlock();
            }
        } finally {
            client.shutdown();
        }
    }
}
