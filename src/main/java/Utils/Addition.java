package Utils;

import org.cinchapi.concourse.Concourse;

/**
 * Created by akshay.kumar1 on 20/07/16.
 */
public enum Addition {
    INSTANCE;

    public boolean add(int x, int y, Concourse concourse, String key, long record) {
        concourse.set(key, x+y, record);
        return true;
    }

    public int retrive(Concourse concourse, String key, long record) {
        int sum = concourse.get(key, record);
        return sum;
    }
}
