use redis::{aio::MultiplexedConnection, Client, FromRedisValue, RedisResult, ToRedisArgs};

use crate::RedisConfig;

#[derive(Clone)]
pub struct AsyncCache {
    con: MultiplexedConnection,
}

impl AsyncCache {
    pub async fn new(config: RedisConfig<'_>) -> RedisResult<Self> {
        let client = Client::open(config)?;
        let con = client.get_multiplexed_tokio_connection().await?;
        let ac = AsyncCache { con };
        Ok(ac)
    }
}

impl AsyncCache {
    pub async fn get<K, V>(&mut self, key: K) -> RedisResult<V>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("GET").arg(key).query_async(&mut self.con).await
    }

    pub async fn set<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async(&mut self.con)
            .await
    }

    pub async fn del<K>(&mut self, key: K) -> RedisResult<()>
    where
        K: ToRedisArgs,
    {
        redis::cmd("DEL").arg(key).query_async(&mut self.con).await
    }

    pub async fn incr<K>(&mut self, key: K) -> RedisResult<i32>
    where
        K: ToRedisArgs,
    {
        redis::cmd("INCR").arg(key).query_async(&mut self.con).await
    }

    pub async fn exists<K>(&mut self, key: K) -> RedisResult<bool>
    where
        K: ToRedisArgs,
    {
        redis::cmd("EXISTS")
            .arg(key)
            .query_async(&mut self.con)
            .await
    }

    pub async fn expire<K>(&mut self, key: K, sec: i32) -> RedisResult<()>
    where
        K: ToRedisArgs,
    {
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(sec)
            .query_async(&mut self.con)
            .await
    }

    pub async fn sadd<K, V>(&mut self, key: K, value: &[V]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("SADD")
            .arg(key)
            .arg(value)
            .query_async(&mut self.con)
            .await
    }

    pub async fn smembers<K, V>(&mut self, key: K) -> RedisResult<Vec<V>>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("SMEMBERS")
            .arg(key)
            .query_async(&mut self.con)
            .await
    }

    pub async fn srem<K, V>(&mut self, key: K, values: &[V]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("SREM")
            .arg(key)
            .arg(values)
            .query_async(&mut self.con)
            .await
    }

    pub async fn scard<K>(&mut self, key: K) -> RedisResult<usize>
    where
        K: ToRedisArgs,
    {
        redis::cmd("SCARD")
            .arg(key)
            .query_async(&mut self.con)
            .await
    }

    pub async fn sismember<K, V>(&mut self, key: K, value: V) -> RedisResult<bool>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("SISMEMBER")
            .arg(key)
            .arg(value)
            .query_async(&mut self.con)
            .await
    }

    pub async fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg(value)
            .query_async(&mut self.con)
            .await
    }

    pub async fn hget<K, F, V>(&mut self, key: K, field: F) -> RedisResult<V>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("HGET")
            .arg(key)
            .arg(field)
            .query_async(&mut self.con)
            .await
    }

    pub async fn hmset<K, F, V>(&mut self, key: K, values: &[(F, V)]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("HMSET")
            .arg(key)
            .arg(values)
            .query_async(&mut self.con)
            .await
    }

    pub async fn hmget<K, F, V>(&mut self, key: K, fields: &[F]) -> RedisResult<V>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("HMGET")
            .arg(key)
            .arg(fields)
            .query_async(&mut self.con)
            .await
    }

    pub async fn hsetall<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        redis::cmd("HMSET")
            .arg(key)
            .arg(value)
            .query_async(&mut self.con)
            .await
    }

    pub async fn hgetall<K, V>(&mut self, key: K) -> RedisResult<V>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("HGETALL")
            .arg(key)
            .query_async(&mut self.con)
            .await
    }

    pub async fn hexists<K, F>(&mut self, key: K, field: F) -> RedisResult<bool>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
    {
        redis::cmd("HEXISTS")
            .arg(key)
            .arg(field)
            .query_async(&mut self.con)
            .await
    }

    pub async fn hdel<K, F>(&mut self, key: K, fields: &[F]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
    {
        redis::cmd("HDEL")
            .arg(key)
            .arg(fields)
            .query_async(&mut self.con)
            .await
    }

    pub async fn zadd<K, S, M>(&mut self, key: K, items: &[(S, M)]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        S: ToRedisArgs,
        M: ToRedisArgs,
    {
        redis::cmd("ZADD")
            .arg(key)
            .arg(items)
            .query_async(&mut self.con)
            .await
    }

    pub async fn zrange_by_score<K, M, V>(&mut self, key: K, min: M, max: M) -> RedisResult<Vec<V>>
    where
        K: ToRedisArgs,
        M: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("ZRANGEBYSCORE")
            .arg(key)
            .arg(min)
            .arg(max)
            .query_async(&mut self.con)
            .await
    }

    pub async fn zrevrange_by_score<K, M, V>(
        &mut self,
        key: K,
        max: M,
        min: M,
    ) -> RedisResult<Vec<V>>
    where
        K: ToRedisArgs,
        M: ToRedisArgs,
        V: FromRedisValue,
    {
        redis::cmd("ZREVRANGEBYSCORE")
            .arg(key)
            .arg(max)
            .arg(min)
            .query_async(&mut self.con)
            .await
    }

    pub async fn zrem<K, M>(&mut self, key: K, items: &[M]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        M: ToRedisArgs,
    {
        redis::cmd("ZREM")
            .arg(key)
            .arg(items)
            .query_async(&mut self.con)
            .await
    }
}

#[cfg(test)]
mod tests_async_cache {
    use std::{
        collections::{HashMap, HashSet},
        time::Duration,
    };

    use super::*;

    const ADDR: &str = "192.168.100.5:6379";
    const DB: u8 = 1;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_basic() {
        let mut ca = AsyncCache::new(RedisConfig::new(ADDR, DB)).await.unwrap();
        ca.set("k1", "v1").await.unwrap();
        if let Ok(v1) = ca.get::<&str, String>("k1").await {
            println!("k1: {v1}");
        }
        ca.del("k1").await.unwrap();

        ca.set("num1", 1).await.unwrap();
        if let Ok(v1) = ca.get::<&str, String>("num1").await {
            println!("num1: {v1}");
        }
        ca.del("num1").await.unwrap();
        match ca.get::<&str, i32>("num1").await {
            Ok(v) => println!("num1: {v}"),
            Err(e) => println!("get err: {}", e),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_incr() {
        let mut ca = AsyncCache::new(RedisConfig::new(ADDR, DB)).await.unwrap();
        let count: i32 = ca.incr("count").await.unwrap();
        println!("count: {count}");
        let count: i32 = ca.incr("count").await.unwrap();
        println!("count: {count}");
        let count: i32 = ca.incr("count").await.unwrap();
        println!("count: {count}");
        let count: i32 = ca.incr("count").await.unwrap();
        println!("count: {count}");
        let count: i32 = ca.incr("count").await.unwrap();
        println!("count: {count}");

        let ex: bool = ca.exists("count").await.unwrap();
        println!("exist count: {ex}");

        ca.expire("count", 10).await.unwrap();

        tokio::time::sleep(Duration::from_secs(5)).await;
        let ex: bool = ca.exists("count").await.unwrap();
        println!("exist count: {ex}");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let ex: bool = ca.exists("count").await.unwrap();
        println!("exist count: {ex}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set() {
        let mut ca = AsyncCache::new(RedisConfig::new(ADDR, DB)).await.unwrap();
        ca.sadd("my_set", &["abc", "def"]).await.unwrap();
        ca.sadd("my_set", &["abc", "ghi"]).await.unwrap();

        let set: HashSet<String> = HashSet::from_iter(ca.smembers("my_set").await.unwrap());
        println!("set: {:?}", set);

        let count = ca.scard("my_set").await.unwrap();
        assert_eq!(count, set.len());

        assert!(ca.sismember("my_set", "abc").await.unwrap());
        ca.srem("my_set", &["abc"]).await.unwrap();
        assert!(!ca.sismember("my_set", "abc").await.unwrap());
        ca.srem("my_set", &["def", "ghi"]).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hash() {
        let mut ca = AsyncCache::new(RedisConfig::new(ADDR, DB)).await.unwrap();
        ca.hset("my_hash", "f1", "v1").await.unwrap();
        let v1: String = ca.hget("my_hash", "f1").await.unwrap();
        println!("v1: {v1}");
        ca.hmset("my_hash", &[("f1", "v11"), ("f2", "v2")])
            .await
            .unwrap();
        let list: Vec<String> = ca.hmget("my_hash", &["f1", "f2"]).await.unwrap();
        println!("list: {list:?}");
        let map: HashMap<String, String> = ca.hgetall("my_hash").await.unwrap();
        println!("map: {map:?}");
        println!("f3 exist: {}", ca.hexists("my_hash", "f3").await.unwrap());
        ca.hdel("my_hash", &["f2", "f3"]).await.unwrap();
        let map: HashMap<String, String> = ca.hgetall("my_hash").await.unwrap();
        println!("map: {map:?}");
        ca.del("my_hash").await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sorted() {
        let mut ca = AsyncCache::new(RedisConfig::new(ADDR, DB)).await.unwrap();
        ca.zadd("my_sorted", &[(123, "abc"), (456, "def")])
            .await
            .unwrap();
        let list: Vec<String> = ca.zrange_by_score("my_sorted", 0, 1000).await.unwrap();
        println!("list: {:?}", list);
        let list: Vec<String> = ca.zrevrange_by_score("my_sorted", 1000, 0).await.unwrap();
        println!("list: {:?}", list);
        ca.zrem("my_sorted", &["abc", "def"]).await.unwrap();
        let list: Vec<String> = ca.zrange_by_score("my_sorted", 0, 1000).await.unwrap();
        println!("list: {:?}", list);
    }
}
