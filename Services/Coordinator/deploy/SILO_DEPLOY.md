# Silo (社区维护版 MinIO) 部署指南

LetsDeepRock Coordinator 的 Checkpoint blob 存储使用 **Silo** —— MinIO 官方归档(2026-04)后由 [Pigsty](https://pigsty.io) 社区维护的 fork(`pgsty/minio`)。

- 与官方 MinIO **API/环境变量/CLI 全兼容**,`minio-go` 客户端无需任何改动
- 版本号沿用 `RELEASE.YYYY-MM-DD` 格式
- 仓库:https://github.com/pgsty/minio · 文档:https://silo.pigsty.cc
- 许可:GNU AGPLv3(与上游一致)

## 为什么用 Silo 而不是官方 MinIO

| | 官方 MinIO | Silo |
|---|---|---|
| 维护状态 | 2026-04 归档只读 | 社区活跃维护 |
| 二进制 | 不再发布 | 持续发布 |
| S3 API | 标准 | 完全兼容 |
| 兼容迁移 | — | 直接替换,零改动 |

## Linux 生产部署(systemd)

### 1. 下载二进制

```bash
# 从 pgsty/minio release 下载 Linux amd64 二进制
# 见 https://github.com/pgsty/minio/releases (RELEASE.* 标签)
curl -L -o /usr/local/bin/silo <release-url>/silo-linux-amd64
chmod +x /usr/local/bin/silo
```

### 2. 创建数据目录与用户

```bash
sudo useradd -r -s /usr/sbin/nologin letsdeeprock
sudo mkdir -p /var/lib/silo/data
sudo chown -R letsdeeprock:letsdeeprock /var/lib/silo
```

### 3. systemd 服务单元

`/etc/systemd/system/silo.service`:

```ini
[Unit]
Description=Silo (community MinIO) object storage for LetsDeepRock
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=letsdeeprock
Group=letsdeeprock
EnvironmentFile=/etc/letsdeeprock/silo.env
ExecStart=/usr/local/bin/silo server /var/lib/silo/data --address :9000 --console-address :9001
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

# 只监听内网/本机,不直接暴露公网
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/silo

[Install]
WantedBy=multi-user.target
```

`/etc/letsdeeprock/silo.env`:

```bash
MINIO_ROOT_USER=letsdeeprock
MINIO_ROOT_PASSWORD=<强随机密码,与 coordinator.env 的 MINIO_SECRET_KEY 一致>
```

### 4. 启动与验证

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now silo
curl -s http://127.0.0.1:9000/minio/health/live   # 返回 200
```

## Windows 本地开发

下载 Windows amd64 二进制(见 releases),放入 `MinIO/` 目录(已 gitignore):

```bash
# 开发用固定凭证(与 .env 一致)
MINIO_ROOT_USER=letsdeeprock MINIO_ROOT_PASSWORD=letsdeeprock123 \
  ./minio.exe server ./MinIO/data --address :9000 --console-address :9001
```

## 客户端验证(S3 兼容)

```bash
# mcli(MinIO 官方客户端,兼容 silo)
mcli alias set local http://127.0.0.1:9000 letsdeeprock <password>
mcli mb local/checkpoints
mcli ls local
```

## 与 Coordinator 的对接

Coordinator 通过 `minio-go/v7` 连接,配置在 `.env` / `coordinator.env`:

```bash
MINIO_ENDPOINT=127.0.0.1:9000
MINIO_BUCKET=checkpoints
MINIO_ACCESS_KEY=letsdeeprock
MINIO_SECRET_KEY=<与 silo.env 一致>
```

启动时 Coordinator 自动创建 `checkpoints` bucket(`EnsureBucket`)。

## Checkpoint 回收策略

- 每个 match 保留最近 **5** 个 checkpoint(commit 时自动删最旧)
- 所有成员离开(`match_peers` 空)后**立即删除**该 match 全部 checkpoint
- **24h** 兜底:无论成员是否离开,`expires_at` 过期后由后台循环每小时强制清理
