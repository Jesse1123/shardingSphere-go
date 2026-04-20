# ShardingSphere-Go

ShardingSphere-Go 是一个用 Go 语言实现的 MySQL 数据库分库分表代理服务器，参考 Apache ShardingSphere-Proxy 的设计理念。

## 功能特性

### 已实现功能

1. **MySQL 协议兼容**
   - 完整的 MySQL 握手认证流程
   - 支持 TEXT 和 BINARY 协议
   - Prepared Statement 支持
   - 多种 MySQL 命令支持 (COM_QUIT, COM_PING, COM_QUERY, COM_STMT_PREPARE, 等)

2. **分库分表支持**
   - 自动解析 `actualDataNodes` 配置
   - 支持 `ds_${0..1}.table_${0..256}` 格式的范围解析
   - 分片键提取和路由
   - SQL 改写 (表名替换)

3. **分片算法**
   - INLINE 算法 (支持模运算)
   - MOD 算法
   - 可扩展架构

4. **Key Generator**
   - Snowflake 算法

5. **跨库查询**
   - 多数据源并行查询
   - 结果集合并

6. **事务支持**
   - 单数据源事务
   - 分布式事务管理框架
   - 自动回滚

7. **连接池**
   - 自动连接池管理
   - 配置化连接参数
   - 连接池统计

## 配置说明

### config.yaml 完整配置示例

```yaml
# 逻辑数据库名
databaseName: zmdb

# 数据源配置
dataSources:
  ds_0:
    url: mysql://192.168.77.81:3306/zmdb?serverTimezone=UTC&useSSL=false
    username: root
    password: 123456
    connectionTimeoutMilliseconds: 30000
    idleTimeoutMilliseconds: 60000
    maxLifetimeMilliseconds: 1800000
    maxPoolSize: 50
  ds_1:
    url: mysql://192.168.77.53:3306/zmdb?serverTimezone=UTC&useSSL=false
    username: zmdb
    password: 123456
    connectionTimeoutMilliseconds: 30000
    idleTimeoutMilliseconds: 60000
    maxLifetimeMilliseconds: 1800000
    maxPoolSize: 50

# 分片规则
rules:
  - !SHARDING
    tables:
      # 分库分表表
      core_coin_logs:
        actualDataNodes: ds_${0..1}.core_coin_logs_${0..256}
        databaseStrategy:
          standard:
            shardingColumn: co_us_id
            shardingAlgorithmName: database_inline
        tableStrategy:
          standard:
            shardingColumn: co_us_id
            shardingAlgorithmName: core_coin_inline
      
      # 只分库表
      core_users:
        actualDataNodes: ds_${0..1}.core_users
        databaseStrategy:
          standard:
            shardingColumn: co_us_id
            shardingAlgorithmName: database_inline
      
      # 绑定到固定数据源
      ad_users:
        actualDataNodes: ds_0.ad_users
    
    shardingAlgorithms:
      database_inline:
        type: INLINE
        props:
          algorithm-expression: ds_${co_us_id % 2}
      
      core_coin_inline:
        type: INLINE
        props:
          algorithm-expression: core_coin_logs_${co_us_id % 256}
    
    keyGenerators:
      snowflake:
        type: SNOWFLAKE
```

## 使用方法

### 编译运行

```bash
# 编译
go build -o sharding-proxy .

# 运行 (默认监听 3307 端口)
./sharding-proxy
```

### 客户端连接

```bash
mysql -h 127.0.0.1 -P 3307 -u user -p
```

## SQL 路由说明

### 自动路由

当 SQL 包含分片键时，系统会自动路由到正确的数据节点：

```sql
-- 假设 co_us_id = 100，系统会自动路由到 ds_0
SELECT * FROM core_users WHERE co_us_id = 100;
```

### Hint 强制路由

可以使用注释 Hint 强制指定路由目标：

```sql
/* ds=ds_0 */ SELECT * FROM core_users WHERE id = 1;
/* tb=core_users_100 */ SELECT * FROM core_users WHERE id = 1;
```

## 项目结构

```
shardingSphere-go/
├── main.go              # 入口文件
├── config/
│   ├── config.go        # 配置解析
│   └── config.yaml      # 配置文件
├── protocol/
│   └── handshake.go     # MySQL 协议处理
├── server/
│   └── server.go        # TCP 服务器
└── sqlrouter/
    ├── router.go        # SQL 路由主逻辑
    ├── sharding.go      # 分片算法
    ├── parser.go        # SQL 解析
    └── transaction.go   # 事务管理
```

## 未来计划

- [ ] 支持更多分片算法 (Hash, Range, Cosid)
- [ ] 数据库连接验证
- [ ] SQL 日志和审计
- [ ] 配置热更新
- [ ] 读写分离支持
- [ ] XA 分布式事务
- [ ] 性能监控指标

## 参考项目

- [Apache ShardingSphere](https://shardingsphere.apache.org/)
- [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)

## License

MIT License
