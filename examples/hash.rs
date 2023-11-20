use cache_ru::{Cache, ICache, RedisConfig};
use redis::{ErrorKind, FromRedisValue, ToRedisArgs, Value};

fn main() {
    let mut ca = Cache::new(RedisConfig::new("192.168.100.5:6379", 1)).unwrap();
    ca.hsetall(
        UserKey,
        User {
            id: "123".into(),
            name: "abc".into(),
            age: 321,
        },
    )
    .unwrap();
    let u: User = ca.hgetall(UserKey).unwrap();
    println!("user: {:?}", u);
}

struct UserKey;

impl ToRedisArgs for UserKey {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg(b"UserKey")
    }
}

#[derive(Default, Debug)]
struct User {
    id: String,
    name: String,
    age: i32,
}

impl ToRedisArgs for User {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg(b"id");
        out.write_arg(self.id.as_bytes());
        out.write_arg(b"name");
        out.write_arg(self.name.as_bytes());
        out.write_arg(b"age");
        out.write_arg(&self.age.to_ne_bytes());
    }
}

impl FromRedisValue for User {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let map_iter = v.as_map_iter().ok_or((ErrorKind::TypeError, ""))?;
        let mut u = User::default();
        for (k, v) in map_iter {
            if let Value::Data(key) = k {
                match key.as_slice() {
                    b"id" => {
                        if let Value::Data(val) = v {
                            u.id = String::from_utf8(val.to_owned()).unwrap();
                        }
                    }
                    b"name" => {
                        if let Value::Data(val) = v {
                            u.name = String::from_utf8(val.to_owned()).unwrap();
                        }
                    }
                    b"age" => {
                        if let Value::Data(val) = v {
                            u.age = i32::from_ne_bytes(val[..].try_into().unwrap());
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(u)
    }
}
