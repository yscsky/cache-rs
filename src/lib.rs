use redis::{FromRedisValue, RedisResult, ToRedisArgs};

pub mod cache;
pub mod config;

pub use cache::Cache;
pub use config::RedisConfig;

pub trait ICache {
    fn get<K, V>(&mut self, key: K) -> RedisResult<V>
    where
        K: ToRedisArgs,
        V: FromRedisValue;
    fn set<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs;
    fn del<K>(&mut self, key: K) -> RedisResult<()>
    where
        K: ToRedisArgs;
    fn incr<K>(&mut self, key: K) -> RedisResult<i32>
    where
        K: ToRedisArgs;
    fn exists<K>(&mut self, key: K) -> RedisResult<bool>
    where
        K: ToRedisArgs;
    fn expire<K>(&mut self, key: K, sec: i32) -> RedisResult<()>
    where
        K: ToRedisArgs;
    fn sadd<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs;
    fn smembers<K, V>(&mut self, key: K) -> RedisResult<Vec<V>>
    where
        K: ToRedisArgs,
        V: FromRedisValue;
    fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs;
    fn hget<K, F, V>(&mut self, key: K, field: F) -> RedisResult<V>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: FromRedisValue;
    fn hmset<K, F, V>(&mut self, key: K, values: &[(F, V)]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs;
    fn hmget<K, F, V>(&mut self, key: K, fields: &[F]) -> RedisResult<V>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: FromRedisValue;
    fn hsetall<K, V>(&mut self, key: K, value: V) -> RedisResult<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs;
    fn hgetall<K, V>(&mut self, key: K) -> RedisResult<V>
    where
        K: ToRedisArgs,
        V: FromRedisValue;
    fn hexists<K, F>(&mut self, key: K, field: F) -> RedisResult<bool>
    where
        K: ToRedisArgs,
        F: ToRedisArgs;
    fn hdel<K, F>(&mut self, key: K, fields: &[F]) -> RedisResult<()>
    where
        K: ToRedisArgs,
        F: ToRedisArgs;
}
