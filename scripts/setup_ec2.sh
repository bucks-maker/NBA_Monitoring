#!/bin/bash
set -euo pipefail

PROJECT_DIR="/home/ec2-user/NBA_Monitoring"
VENV_DIR="${PROJECT_DIR}/venv"
SYSTEMD_SRC="${PROJECT_DIR}/scripts/systemd"

echo "=== Step 1: 시스템 패키지 설치 ==="
sudo dnf update -y
sudo dnf install -y docker git python3.11 python3.11-pip

# Docker 서비스 시작 + 부팅 시 자동 시작
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

# Docker Compose plugin 설치
COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep '"tag_name"' | cut -d'"' -f4)
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-$(uname -m)" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

echo ""
echo "=== Step 2: 프로젝트 클론/업데이트 ==="
cd /home/ec2-user
if [ -d "NBA_Monitoring" ]; then
  cd NBA_Monitoring
  git pull origin main
else
  git clone https://github.com/bucks-maker/NBA_Monitoring.git
  cd NBA_Monitoring
fi

mkdir -p data

# .env 파일 생성 (없을 때만)
if [ ! -f .env ]; then
  cp .env.example .env
  echo ">>> .env 파일이 생성되었습니다. 값을 채워주세요:"
  echo "    vi ${PROJECT_DIR}/.env"
fi

echo ""
echo "=== Step 3: 구버전 systemd 정리 ==="

# 구버전 서비스 중지 및 제거 (monitor/snapshot.py 기반)
OLD_SERVICES=("nba-monitor" "polymarket-monitor" "snapshot-monitor")
for svc in "${OLD_SERVICES[@]}"; do
  if systemctl list-unit-files | grep -q "${svc}.service"; then
    echo "  구버전 서비스 발견: ${svc} → 중지 및 제거"
    sudo systemctl stop "${svc}" 2>/dev/null || true
    sudo systemctl disable "${svc}" 2>/dev/null || true
    sudo rm -f "/etc/systemd/system/${svc}.service"
  fi
done

# 구버전 monitor/data symlink 제거
if [ -L "${PROJECT_DIR}/monitor/data" ]; then
  echo "  monitor/data symlink 제거"
  rm -f "${PROJECT_DIR}/monitor/data"
fi

# monitor/ 디렉토리 잔여물 제거
if [ -d "${PROJECT_DIR}/monitor" ]; then
  echo "  monitor/ 디렉토리 잔여물 제거"
  rm -rf "${PROJECT_DIR}/monitor"
fi

sudo systemctl daemon-reload

echo ""
echo "=== Step 4: Python venv 설정 ==="
if [ ! -d "${VENV_DIR}" ]; then
  python3.11 -m venv "${VENV_DIR}"
fi
"${VENV_DIR}/bin/pip" install --upgrade pip
"${VENV_DIR}/bin/pip" install -r "${PROJECT_DIR}/requirements.txt"

echo ""
echo "=== Step 5: 신규 systemd 서비스 설치 ==="
sudo cp "${SYSTEMD_SRC}/nba-lag-monitor.service" /etc/systemd/system/
sudo cp "${SYSTEMD_SRC}/nba-rebalance-monitor.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable nba-lag-monitor nba-rebalance-monitor

echo ""
echo "=== 설치 완료 ==="
echo ""
echo "※ docker 그룹 적용을 위해 재로그인 필요: exit 후 다시 SSH 접속"
echo ""
echo "실행 방법 (택 1):"
echo ""
echo "  [Option A] Docker (권장):"
echo "    cd ${PROJECT_DIR}"
echo "    docker compose up -d --build"
echo ""
echo "  [Option B] systemd:"
echo "    sudo systemctl start nba-lag-monitor"
echo "    sudo systemctl start nba-rebalance-monitor"
echo "    journalctl -u nba-lag-monitor -f     # 로그 확인"
echo ""
echo "  ※ Docker와 systemd를 동시에 실행하지 마세요 (DB 충돌 위험)"
