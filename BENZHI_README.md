# BENZHI_README

这是一个使用 Go 与 SQLite 构建的地震监测站网控制后端，用于管理台站采集、事件复核、告警投递和持久化后台作业。

## 环境要求

- Go 1.22.5，`go.mod` 语言版本为 1.22.0。
- 本地运行需要可写的数据目录；默认数据库为 `./data/quakewatch.db`。
- 可通过 `QUAKEWATCH_HTTP_ADDR` 和 `QUAKEWATCH_DATABASE_PATH` 调整监听地址与数据库路径。

## 标准构建、运行和测试命令

进入容器后执行：

```bash
# 编译
cd '/app' && GOTOOLCHAIN=local go build ./...

# 启动
cd '/app' && GOTOOLCHAIN=local go run ./cmd/server

# 测试
cd '/app' && GOTOOLCHAIN=local go test ./... -count=1
cd '/app' && GOTOOLCHAIN=local go test -race ./... -count=1
cd '/app' && GOTOOLCHAIN=local go vet ./...
```

服务默认监听 `:8080`，存活与就绪接口分别为 `/healthz` 和 `/readyz`。如需登录业务接口，请设置成对出现的 `QUAKEWATCH_BOOTSTRAP_EMAIL` 与 `QUAKEWATCH_BOOTSTRAP_PASSWORD` 后启动服务。

## Docker 构建和进入容器

```bash
chmod +x build_benzhi_docker.sh
./build_benzhi_docker.sh quakewatch-benzhi-amd64 linux/amd64
./build_benzhi_docker.sh quakewatch-benzhi-arm64 linux/arm64
docker run -it quakewatch-benzhi-amd64:latest
docker run -it --platform linux/arm64 quakewatch-benzhi-arm64:latest
```
