package org.rioslab.spark.core.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class CacheUtil {

    public static JedisPool pool = new JedisPool("localhost", 6379);

    public static boolean set(String key, String value) {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(key, value);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean del(String key) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(key);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static String getString(String key) {
        String ret = "";
        try (Jedis jedis = pool.getResource()) {
            ret = jedis.get(key);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

}