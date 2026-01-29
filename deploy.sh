#!/bin/bash
# 로컬에서 실행: 코드 push → EC2 반영 → 서비스 재시작

set -e
KEY="$HOME/.ssh/nba-monitor-key.pem"
HOST="ec2-user@13.125.87.204"
REMOTE_DIR="/home/ec2-user/NBA_Monitoring"

echo "[1/3] git push..."
git push

echo "[2/3] EC2에 파일 전송..."
scp -i "$KEY" snapshot.py report.py schema.sql CLAUDE.md ws_client.py anomaly_detector.py hi_res_capture.py hi_res_analysis.py "$HOST:$REMOTE_DIR/"

echo "[3/3] 서비스 재시작..."
ssh -i "$KEY" "$HOST" "sudo systemctl restart nba-monitor && sudo systemctl status nba-monitor --no-pager"

echo ""
echo "배포 완료!"
