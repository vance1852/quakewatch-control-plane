# QuakeWatch Control Plane

QuakeWatch Control Plane 是面向地震监测站网的生产级 Go 后端服务。系统管理台站和传感器生命周期，接收波形批次，协调事件候选复核与发布，并以持久化 worker 执行波形处理和告警投递。所有关键流程使用真实关系数据库、事务、乐观并发控制、幂等记录和审计事件。

## 主要能力

- 台站、传感器、维护窗口和采集资格管理。
- 波形批次接收、幂等校验、持久化作业和重启恢复。
- 事件候选、震相拾取、复核租约、确认、驳回与发布状态流。
- 告警规则匹配、投递去重、租约、退避重试和永久失败记录。
- 登录、可撤销会话、退出、过期清理，以及 analyst、operator、admin 三种业务角色。
- 请求 ID、统一错误响应、结构化日志、审计查询、健康与就绪检查。

## 环境要求

- Go 1.22.x
- Docker 24+（可选）

服务默认使用 SQLite 数据库，驱动为纯 Go 实现，不要求本机安装 C 编译器。生产数据目录需要可写并持久挂载。

## 本地运行

```bash
cp .env.example .env
go mod download
go run ./cmd/server
```

配置均使用 `QUAKEWATCH_` 前缀环境变量。首次启动会执行版本化 migration；当配置了 bootstrap 邮箱与密码时，系统以幂等方式创建初始管理员。

## 测试与静态检查

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
```

测试使用临时真实 SQLite 数据库，覆盖 migration、事务回滚、并发版本冲突、会话撤销与过期、HTTP 契约、worker 重试和重启恢复。

## Docker

```bash
docker build --platform linux/amd64 -t quakewatch-control-plane:amd64 .
docker run --rm -p 8080:8080 -v quakewatch-data:/data quakewatch-control-plane:amd64
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

镜像默认入口是 `/app/quakewatch-server`，监听 `:8080`，数据库位于 `/data/quakewatch.db`。容器以非 root 用户运行。
