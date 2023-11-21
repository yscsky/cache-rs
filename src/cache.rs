use redis::{Client, Connection, FromRedisValue, RedisResult, ToRedisArgs};

use crate::{config::RedisConfig, ICache};

pub struct Cache {
    con: Connection,
}

impl Cache {
    pub fn new(config: RedisConfig) -> RedisResult<Self> {
        let client = Client::open(config)?;
        let con = client.get_connection()?;
        let ca = Cache { con };
        Ok(ca)
    }
}

impl ICache for Cache {
    fn get<K, V>(&mut self, key: K) -> RedisResult<V>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("GET").arg(key).query(&mut self.con)
    }

    fn set<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("SET").arg(key).arg(value).query(&mut self.con)
    }

    fn del<K>(&mut self, key: K) -> RedisResult<()>
    where
        K: ToRedisArgs,
    {
        redis::cmd("DEL").arg(key).query(&mut self.con)
    }

    fn incr<K>(&mut self, key: K) -> RedisResult<i32>
    where
        K: ToRedisArgs,
    {
        redis::cmd("INCR").arg(key).query(&mut self.con)
    }

    fn exists<K>(&mut self, key: K) -> RedisResult<bool>
    where
        K: ToRedisArgs,
    {
        redis::cmd("EXISTS").arg(key).query(&mut self.con)
    }

    fn expire<K>(&mut self, key: K, sec: i32) -> RedisResult<()>
    where
        K: ToRedisArgs,
    {
        redis::cmd("EXPIRE").arg(key).arg(sec).query(&mut self.con)
    }

    fn sadd<K, V>(&mut self, key: K, value: &[V]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("SADD").arg(key).arg(value).query(&mut self.con)
    }

    fn smembers<K, V>(&mut self, key: K) -> RedisResult<Vec<V>>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("SMEMBERS").arg(key).query(&mut self.con)
    }

    fn srem<K, V>(&mut self, key: K, values: &[V]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("SREM").arg(key).arg(values).query(&mut self.con)
    }

    fn scard<K>(&mut self, key: K) -> RedisResult<usize>
    where
        K: ToRedisArgs,
    {
        redis::cmd("SCARD").arg(key).query(&mut self.con)
    }

    fn sismember<K, V>(&mut self, key: K, value: V) -> RedisResult<bool>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("SISMEMBER")
            .arg(key)
            .arg(value)
            .query(&mut self.con)
    }

    fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg(value)
            .query(&mut self.con)
    }

    fn hget<K, F, V>(&mut self, key: K, field: F) -> RedisResult<V>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("HGET").arg(key).arg(field).query(&mut self.con)
    }

    fn hmset<K, F, V>(&mut self, key: K, values: &[(F, V)]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("HMSET")
            .arg(key)
            .arg(values)
            .query(&mut self.con)
    }

    fn hmget<K, F, V>(&mut self, key: K, fields: &[F]) -> RedisResult<V>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("HMGET")
            .arg(key)
            .arg(fields)
            .query(&mut self.con)
    }

    fn hsetall<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("HMSET").arg(key).arg(value).query(&mut self.con)
    }

    fn hgetall<K, V>(&mut self, key: K) -> RedisResult<V>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("HGETALL").arg(key).query(&mut self.con)
    }

    fn hexists<K, F>(&mut self, key: K, field: F) -> RedisResult<bool>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
    {
        redis::cmd("HEXISTS")
            .arg(key)
            .arg(field)
            .query(&mut self.con)
    }

    fn hdel<K, F>(&mut self, key: K, fields: &[F]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
    {
        redis::cmd("HDEL").arg(key).arg(fields).query(&mut self.con)
    }

    fn zadd<K, S, M>(&mut self, key: K, items: &[(S, M)]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        S: ToRedisArgs,
        M: ToRedisArgs,
    {
        redis::cmd("ZADD").arg(key).arg(items).query(&mut self.con)
    }

    fn zrange_by_score<K, M, V>(&mut self, key: K, min: M, max: M) -> RedisResult<Vec<V>>
    where
        K: ToRedisArgs,
        M: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("ZRANGEBYSCORE")
            .arg(key)
            .arg(min)
            .arg(max)
            .query(&mut self.con)
    }

    fn zrevrange_by_score<K, M, V>(&mut self, key: K, max: M, min: M) -> RedisResult<Vec<V>>
    where
        K: ToRedisArgs,
        M: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("ZREVRANGEBYSCORE")
            .arg(key)
            .arg(max)
            .arg(min)
            .query(&mut self.con)
    }

    fn zrem<K, M>(&mut self, key: K, items: &[M]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        M: ToRedisArgs,
    {
        redis::cmd("ZREM").arg(key).arg(items).query(&mut self.con)
    }
}

#[cfg(test)]
mod tests_cache {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::thread;
    use std::time::Duration;

    const ADDR: &str = "192.168.100.5:6379";
    const DB: u8 = 1;

    #[test]
    fn test_basic() {
        let mut ca = Cache::new(RedisConfig::new(ADDR, DB)).unwrap();
        ca.set("k1", "v1").unwrap();
        if let Ok(v1) = ca.get::<&str, String>("k1") {
            println!("k1: {v1}");
        }
        ca.del("k1").unwrap();

        ca.set("num1", 1).unwrap();
        if let Ok(v1) = ca.get::<&str, String>("num1") {
            println!("num1: {v1}");
        }
        ca.del("num1").unwrap();
        match ca.get::<&str, i32>("num1") {
            Ok(v) => println!("num1: {v}"),
            Err(e) => println!("get err: {}", e),
        }
    }

    #[test]
    fn test_incr() {
        let mut ca = Cache::new(RedisConfig::new(ADDR, DB)).unwrap();
        let count: i32 = ca.incr("count").unwrap();
        println!("count: {count}");
        let count: i32 = ca.incr("count").unwrap();
        println!("count: {count}");
        let count: i32 = ca.incr("count").unwrap();
        println!("count: {count}");
        let count: i32 = ca.incr("count").unwrap();
        println!("count: {count}");
        let count: i32 = ca.incr("count").unwrap();
        println!("count: {count}");

        let ex: bool = ca.exists("count").unwrap();
        println!("exist count: {ex}");

        ca.expire("count", 10).unwrap();

        thread::sleep(Duration::from_secs(5));
        let ex: bool = ca.exists("count").unwrap();
        println!("exist count: {ex}");
        thread::sleep(Duration::from_secs(5));
        let ex: bool = ca.exists("count").unwrap();
        println!("exist count: {ex}");
    }

    #[test]
    fn test_set() {
        let mut ca = Cache::new(RedisConfig::new(ADDR, DB)).unwrap();
        ca.sadd("my_set", &["abc", "def"]).unwrap();
        ca.sadd("my_set", &["abc", "ghi"]).unwrap();

        let set: HashSet<String> = HashSet::from_iter(ca.smembers("my_set").unwrap());
        println!("set: {:?}", set);

        let count = ca.scard("my_set").unwrap();
        assert_eq!(count, set.len());

        assert!(ca.sismember("my_set", "abc").unwrap());
        ca.srem("my_set", &["abc"]).unwrap();
        assert!(!ca.sismember("my_set", "abc").unwrap());
        ca.srem("my_set", &["def", "ghi"]).unwrap();
    }

    #[test]
    fn test_hash() {
        let mut ca = Cache::new(RedisConfig::new(ADDR, DB)).unwrap();
        ca.hset("my_hash", "f1", "v1").unwrap();
        let v1: String = ca.hget("my_hash", "f1").unwrap();
        println!("v1: {v1}");
        ca.hmset("my_hash", &[("f1", "v11"), ("f2", "v2")]).unwrap();
        let list: Vec<String> = ca.hmget("my_hash", &["f1", "f2"]).unwrap();
        println!("list: {list:?}");
        let map: HashMap<String, String> = ca.hgetall("my_hash").unwrap();
        println!("map: {map:?}");
        println!("f3 exist: {}", ca.hexists("my_hash", "f3").unwrap());
        ca.hdel("my_hash", &["f2", "f3"]).unwrap();
        let map: HashMap<String, String> = ca.hgetall("my_hash").unwrap();
        println!("map: {map:?}");
        ca.del("my_hash").unwrap();
    }

    #[test]
    fn test_sorted() {
        let mut ca = Cache::new(RedisConfig::new(ADDR, DB)).unwrap();
        ca.zadd("my_sorted", &[(123, "abc"), (456, "def")]).unwrap();
        let list: Vec<String> = ca.zrange_by_score("my_sorted", 0, 1000).unwrap();
        println!("list: {:?}", list);
        let list: Vec<String> = ca.zrevrange_by_score("my_sorted", 1000, 0).unwrap();
        println!("list: {:?}", list);
        ca.zrem("my_sorted", &["abc", "def"]).unwrap();
        let list: Vec<String> = ca.zrange_by_score("my_sorted", 0, 1000).unwrap();
        println!("list: {:?}", list);
    }
}
