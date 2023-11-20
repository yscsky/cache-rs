use redis::{Client, Connection, RedisResult};

use crate::{config::RedisConfig, ICache};

pub struct Cache {
    conn: Connection,
}

impl Cache {
    pub fn new(config: RedisConfig) -> RedisResult<Self> {
        let client = Client::open(config)?;
        let conn = client.get_connection()?;
        let ca = Cache { conn };
        Ok(ca)
    }
}

impl ICache for Cache {
    fn get<K, V>(&mut self, key: K) -> RedisResult<V>
    where
        K: redis::ToRedisArgs,
        V: redis::FromRedisValue,
    {
        redis::cmd("GET").arg(key).query::<V>(&mut self.conn)
    }

    fn set<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: redis::ToRedisArgs,
        V: redis::ToRedisArgs,
    {
        redis::cmd("SET").arg(key).arg(value).query(&mut self.conn)
    }

    fn del<K>(&mut self, key: K) -> RedisResult<()>
    where
        K: redis::ToRedisArgs,
    {
        redis::cmd("DEL").arg(key).query(&mut self.conn)
    }
    fn incr<K>(&mut self, key: K) -> RedisResult<i32>
    where
        K: redis::ToRedisArgs,
    {
        redis::cmd("INCR").arg(key).query::<i32>(&mut self.conn)
    }

    fn exists<K>(&mut self, key: K) -> RedisResult<bool>
    where
        K: redis::ToRedisArgs,
    {
        redis::cmd("EXISTS").arg(key).query::<bool>(&mut self.conn)
    }

    fn expire<K>(&mut self, key: K, sec: i32) -> RedisResult<()>
    where
        K: redis::ToRedisArgs,
    {
        redis::cmd("EXPIRE").arg(key).arg(sec).query(&mut self.conn)
    }

    fn sadd<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: redis::ToRedisArgs,
        V: redis::ToRedisArgs,
    {
        redis::cmd("SADD").arg(key).arg(value).query(&mut self.conn)
    }

    fn smembers<K, V>(&mut self, key: K) -> RedisResult<Vec<V>>
    where
        K: redis::ToRedisArgs,
        V: redis::FromRedisValue,
    {
        redis::cmd("SMEMBERS").arg(key).query(&mut self.conn)
    }

    fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> RedisResult<()>
    where
        K: redis::ToRedisArgs,
        F: redis::ToRedisArgs,
        V: redis::ToRedisArgs,
    {
        redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg(value)
            .query(&mut self.conn)
    }

    fn hget<K, F, V>(&mut self, key: K, field: F) -> RedisResult<V>
    where
        K: redis::ToRedisArgs,
        F: redis::ToRedisArgs,
        V: redis::FromRedisValue,
    {
        redis::cmd("HGET").arg(key).arg(field).query(&mut self.conn)
    }

    fn hmset<K, F, V>(&mut self, key: K, values: &[(F, V)]) -> RedisResult<()>
    where
        K: redis::ToRedisArgs,
        F: redis::ToRedisArgs,
        V: redis::ToRedisArgs,
    {
        redis::cmd("HMSET")
            .arg(key)
            .arg(values)
            .query(&mut self.conn)
    }

    fn hmget<K, F, V>(&mut self, key: K, fields: &[F]) -> RedisResult<V>
    where
        K: redis::ToRedisArgs,
        F: redis::ToRedisArgs,
        V: redis::FromRedisValue,
    {
        redis::cmd("HMGET")
            .arg(key)
            .arg(fields)
            .query(&mut self.conn)
    }

    fn hsetall<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: redis::ToRedisArgs,
        V: redis::ToRedisArgs,
    {
        redis::cmd("HMSET")
            .arg(key)
            .arg(value)
            .query(&mut self.conn)
    }

    fn hgetall<K, V>(&mut self, key: K) -> RedisResult<V>
    where
        K: redis::ToRedisArgs,
        V: redis::FromRedisValue,
    {
        redis::cmd("HGETALL").arg(key).query(&mut self.conn)
    }

    fn hexists<K, F>(&mut self, key: K, field: F) -> RedisResult<bool>
    where
        K: redis::ToRedisArgs,
        F: redis::ToRedisArgs,
    {
        redis::cmd("HEXISTS")
            .arg(key)
            .arg(field)
            .query::<bool>(&mut self.conn)
    }

    fn hdel<K, F>(&mut self, key: K, fields: &[F]) -> RedisResult<()>
    where
        K: redis::ToRedisArgs,
        F: redis::ToRedisArgs,
    {
        redis::cmd("HDEL")
            .arg(key)
            .arg(fields)
            .query(&mut self.conn)
    }
}

#[cfg(test)]
mod tests_cache {
    use super::*;
    use std::collections::HashSet;
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
        ca.sadd("my_set", "abc").unwrap();
        ca.sadd("my_set", "def").unwrap();

        let set: HashSet<String> = HashSet::from_iter(ca.smembers("my_set").unwrap());
        println!("set: {:?}", set);
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
    }
}
