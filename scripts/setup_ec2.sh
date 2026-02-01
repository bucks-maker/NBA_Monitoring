#!/bin/bash
set -euo pipefail

echo "=== Docker 설치 (Amazon Linux 2023) ==="

# Docker 설치
sudo dnf update -y
sudo dnf install -y docker git

# Docker 서비스 시작 + 부팅 시 자동 시작
sudo systemctl start docker
sudo systemctl enable docker

# ec2-user를 docker 그룹에 추가 (sudo 없이 docker 사용)
sudo usermod -aG docker ec2-user

# Docker Compose plugin 설치
COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep '"tag_name"' | cut -d'"' -f4)
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-$(uname -m)" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

echo ""
echo "=== 프로젝트 설정 ==="

# 레포 클론 (이미 있으면 pull)
cd /home/ec2-user
if [ -d "NBA_Monitoring" ]; then
  cd NBA_Monitoring
  git pull origin main
else
  git clone https://github.com/bucks-maker/NBA_Monitoring.git
  cd NBA_Monitoring
fi

# data 디렉토리 생성
mkdir -p data

# .env 파일 생성 (없을 때만)
if [ ! -f .env ]; then
  cp .env.example .env
  echo ""
  echo ">>> .env 파일이 생성되었습니다. 아래 값을 채워주세요:"
  echo "    vi /home/ec2-user/NBA_Monitoring/.env"
  echo ""
fi

echo "=== 설치 완료 ==="
echo ""
echo "※ docker 그룹 적용을 위해 재로그인 필요:"
echo "    exit 후 다시 SSH 접속"
echo ""
echo "재로그인 후 실행:"
echo "    cd /home/ec2-user/NBA_Monitoring"
echo "    docker compose up -d --build"
