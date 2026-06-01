#!/bin/bash
# 100万行速度测试启动脚本
# 使用方式: bash scripts/run_1m_test.sh
# 查看进度: bash scripts/run_1m_test.sh status

LOCKFILE="/tmp/ghostsync_1m.pid"

if [ "$1" = "status" ]; then
    if [ -f "$LOCKFILE" ]; then
        PID=$(cat "$LOCKFILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "GhostSync 正在运行 (PID=$PID)"
            echo ""
            echo "=== 当前进度 ==="
            mysql -h 127.0.0.1 -P 3306 -u root -p123456789 ghostsync_target -e "SELECT COUNT(*) AS target_rows FROM sync_test_users;"
            echo ""
            echo "=== 日志末尾 ==="
            tail -5 /tmp/ghostsync_1m.log
            exit 0
        else
            echo "进程已退出 (PID=$PID)"
            rm -f "$LOCKFILE"
            echo ""
            echo "=== 最终行数 ==="
            mysql -h 127.0.0.1 -P 3306 -u root -p123456789 ghostsync_target -e "SELECT COUNT(*) AS target_rows FROM sync_test_users;"
            echo ""
            echo "=== 日志末尾 ==="
            tail -10 /tmp/ghostsync_1m.log
            exit 0
        fi
    else
        echo "没有找到运行中的进程"
        exit 1
    fi
fi

if [ "$1" = "stop" ]; then
    if [ -f "$LOCKFILE" ]; then
        PID=$(cat "$LOCKFILE")
        kill "$PID" 2>/dev/null && echo "已停止 PID=$PID" || echo "停止失败"
        rm -f "$LOCKFILE"
    fi
    exit 0
fi

# 清理目标表
echo "=== 清空目标表 ==="
mysql -h 127.0.0.1 -P 3306 -u root -p123456789 ghostsync_target -e "TRUNCATE sync_test_users;"

echo "=== 启动 100 万行同步测试 ==="
cd /Users/dongfangyuechu/www/ghostSync || exit 1

# macOS 下正确守护进程
nohup env GHOSTSYNC_LOG=debug ./target/release/ghostSync run config.test.yaml --task mysql-speed-test > /tmp/ghostsync_1m.log 2>&1 &
PID=$!
echo "$PID" > "$LOCKFILE"

# macOS disown
disown "$PID" 2>/dev/null

echo "GhostSync 已启动 (PID=$PID)"
echo ""
echo "监控命令:"
echo "  bash scripts/run_1m_test.sh status    # 查看进度"
echo "  bash scripts/run_1m_test.sh stop       # 停止"
echo "  tail -f /tmp/ghostsync_1m.log         # 实时日志"
