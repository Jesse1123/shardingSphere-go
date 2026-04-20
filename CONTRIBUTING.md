# 贡献指南

感谢您对 ShardingSphere-Go 项目的关注！我们欢迎并感谢所有形式的贡献。

## 目录

- [行为准则](#行为准则)
- [如何贡献](#如何贡献)
  - [报告问题](#报告问题)
  - [提交功能建议](#提交功能建议)
  - [提交代码](#提交代码)
- [开发指南](#开发指南)
  - [环境准备](#环境准备)
  - [项目结构](#项目结构)
  - [代码规范](#代码规范)
  - [测试要求](#测试要求)
- [提交规范](#提交规范)
- [发布流程](#发布流程)

## 行为准则

参与本项目即表示您同意遵守以下原则：

- 尊重所有参与者，保持友善和专业的交流
- 接受建设性的批评，以礼相待
- 关注对社区最有利的事情
- 对其他社区成员表示同理心

## 如何贡献

### 报告问题

如果您发现了 bug，请通过 [GitHub Issues](https://github.com/yourusername/shardingSphere-go/issues) 提交，并包含以下信息：

1. **问题描述**：清晰简洁地描述问题
2. **复现步骤**：详细说明如何复现问题
3. **期望行为**：描述您期望发生的行为
4. **实际行为**：描述实际发生的行为
5. **环境信息**：
   - Go 版本 (`go version`)
   - 操作系统
   - 相关配置
6. **附加信息**：日志、截图等

### 提交功能建议

我们欢迎新功能建议！请通过 GitHub Issues 提交，并包含：

1. **功能描述**：清晰描述您想要的功能
2. **使用场景**：说明这个功能将如何解决实际问题
3. **可能的实现方案**（可选）：如果您有实现思路，欢迎分享
4. **是否愿意贡献**：您是否愿意自己实现这个功能

### 提交代码

1. **Fork 仓库**：点击右上角的 Fork 按钮
2. **克隆仓库**：
   ```bash
   git clone https://github.com/yourusername/shardingSphere-go.git
   cd shardingSphere-go
   ```
3. **创建分支**：
   ```bash
   git checkout -b feature/your-feature-name
   # 或
   git checkout -b fix/your-bug-fix
   ```
4. **提交更改**：
   ```bash
   git add .
   git commit -m "feat: 添加新功能"
   ```
5. **推送到远程**：
   ```bash
   git push origin feature/your-feature-name
   ```
6. **创建 Pull Request**：在 GitHub 上创建 PR，描述您的更改

## 开发指南

### 环境准备

**前置要求：**

- Go 1.23.5 或更高版本
- MySQL 5.7+（用于测试）
- Git

**安装步骤：**

```bash
# 克隆仓库
git clone https://github.com/yourusername/shardingSphere-go.git
cd shardingSphere-go

# 安装依赖
go mod download

# 验证安装
go build -o sharding-proxy .
```

### 项目结构

```
shardingSphere-go/
├── main.go              # 入口文件
├── config/              # 配置管理
│   ├── config.go        # 配置解析逻辑
│   └── config.yaml      # 配置文件示例
├── protocol/            # MySQL 协议实现
│   └── handshake.go     # 握手认证、协议处理
├── server/              # TCP 服务器
│   └── server.go        # 服务器启动和连接管理
├── sqlrouter/           # SQL 路由核心
│   ├── router.go        # 路由主逻辑
│   ├── sharding.go      # 分片算法
│   ├── parser.go        # SQL 解析
│   └── transaction.go   # 事务管理
└── ncaos/               # 配置中心相关
```

### 代码规范

我们遵循标准的 Go 代码规范：

1. **格式化**：使用 `gofmt` 或 `goimports` 格式化代码
   ```bash
   gofmt -w .
   goimports -w .
   ```

2. **命名规范**：
   - 包名：小写，简短，无下划线（如 `sqlrouter`）
   - 函数/变量：驼峰命名（如 `parseSQL`）
   - 常量：驼峰或全大写下划线（如 `MaxConnections` 或 `MAX_CONNECTIONS`）
   - 接口名：以 `er` 结尾（如 `Reader`, `Writer`）

3. **注释规范**：
   - 所有导出（大写）的函数、类型、变量必须有注释
   - 注释以被注释对象的名称开头
   - 使用完整句子，以句号结尾

   ```go
   // Router handles SQL routing logic for sharded databases.
   type Router struct {
       // rules contains all sharding rules.
       rules map[string]*ShardingRule
   }

   // Route parses the SQL and returns the target data nodes.
   func (r *Router) Route(sql string, params []interface{}) (*RouteResult, error) {
       // ...
   }
   ```

4. **错误处理**：
   - 错误字符串小写开头，不包含标点结尾
   - 使用 `fmt.Errorf` 包装错误时添加上下文
   - 优先返回错误而不是 panic

   ```go
   if err != nil {
       return fmt.Errorf("failed to parse config: %w", err)
   }
   ```

5. **代码组织**：
   - 一个文件不超过 500 行
   - 一个函数不超过 50 行
   - 函数职责单一，避免过度嵌套

### 测试要求

1. **单元测试**：
   - 所有新功能必须包含单元测试
   - 测试文件以 `_test.go` 结尾
   - 使用表格驱动测试

   ```go
   func TestParseShardingRule(t *testing.T) {
       tests := []struct {
           name     string
           input    string
           expected *ShardingRule
           wantErr  bool
       }{
           {
               name:  "valid rule",
               input: "ds_${0..1}.table_${0..256}",
               expected: &ShardingRule{
                   // ...
               },
           },
           // ...
       }
       
       for _, tt := range tests {
           t.Run(tt.name, func(t *testing.T) {
               result, err := ParseShardingRule(tt.input)
               if (err != nil) != tt.wantErr {
                   t.Errorf("ParseShardingRule() error = %v, wantErr %v", err, tt.wantErr)
                   return
               }
               if !reflect.DeepEqual(result, tt.expected) {
                   t.Errorf("ParseShardingRule() = %v, want %v", result, tt.expected)
               }
           })
       }
   }
   ```

2. **运行测试**：
   ```bash
   # 运行所有测试
   go test ./...

   # 运行特定包的测试
   go test ./sqlrouter/...

   # 带覆盖率
   go test -cover ./...

   # 生成覆盖率报告
   go test -coverprofile=coverage.out ./...
   go tool cover -html=coverage.out
   ```

3. **集成测试**：
   - 涉及数据库操作的修改需要验证与 MySQL 的兼容性
   - 测试不同版本的 MySQL（5.7, 8.0）

## 提交规范

我们使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>(<scope>): <subject>

<body>

<footer>
```

**类型（type）：**

- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档更新
- `style`: 代码格式调整（不影响功能）
- `refactor`: 代码重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建/工具相关

**示例：**

```
feat(sqlrouter): 支持 Range 分片算法

- 实现 RangeShardingAlgorithm 结构体
- 添加范围解析逻辑
- 支持配置中的范围表达式

Closes #123
```

```
fix(protocol): 修复 prepared statement 内存泄漏

在处理 COM_STMT_CLOSE 时未正确释放资源，
导致长时间运行后内存持续增长。

Fixes #456
```

## 发布流程

1. 维护者会定期审查和合并 PR
2. 版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)（MAJOR.MINOR.PATCH）
3. 发布时会创建 GitHub Release 并附带更新日志

## 获取帮助

如果您在贡献过程中遇到问题：

1. 查看 [README.md](./README.md) 了解项目基本信息
2. 搜索 [Issues](https://github.com/yourusername/shardingSphere-go/issues) 查看是否已有类似问题
3. 创建新的 Issue 描述您的问题

---

再次感谢您的贡献！🎉
