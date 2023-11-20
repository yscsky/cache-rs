use redis::{ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisResult};

pub struct RedisConfig<'a> {
    pub address: &'a str,
    pub username: &'a str,
    pub password: &'a str,
    pub db: u8,
}

impl<'a> RedisConfig<'a> {
    pub fn new(address: &'a str, db: u8) -> Self {
        RedisConfig {
            address,
            username: "",
            password: "",
            db,
        }
    }
    pub fn set_username(&mut self, username: &'a str) {
        self.username = username;
    }
    pub fn set_password(&mut self, password: &'a str) {
        self.password = password;
    }
}

impl<'a> IntoConnectionInfo for RedisConfig<'a> {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        let mut input = String::from("redis://");
        if !self.username.is_empty() {
            input.push_str(self.username);
            if !self.password.is_empty() {
                input.push(':');
                input.push_str(self.password);
            }
            input.push('@');
        }
        input.push_str(self.address);
        input.push('/');
        input.push_str(&self.db.to_string());
        match redis::parse_redis_url(&input) {
            Some(u) => u.into_connection_info(),
            None => Err((ErrorKind::InvalidClientConfig, "Redis URL did not parse").into()),
        }
    }
}

#[test]
fn test_config_into_connection_info() {
    let mut config = RedisConfig::new("127.0.0.1:6379", 5);
    config.set_username("username");
    config.set_password("password");
    if let Ok(conn_info) = config.into_connection_info() {
        println!("{} {:?}", conn_info.addr, conn_info.redis);
    }
}
