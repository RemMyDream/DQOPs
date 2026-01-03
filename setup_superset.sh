#!/bin/bash

# Dừng script nếu có lỗi (trừ những chỗ cho phép lỗi)
# set -e 

echo "=========================================="
echo "🚀 AUTO SETUP APACHE SUPERSET FOR DEMO"
echo "=========================================="

# 1. Start Superset (chỉ chạy container này và postgres để tiết kiệm resource)
echo "[1/6] Starting Superset container..."
docker-compose up -d superset postgres

echo "⏳ Waiting 30s for container to warm up..."
sleep 30

# 2. CÀI ĐẶT DRIVER TỰ ĐỘNG (Phần quan trọng nhất)
echo "[2/6] Installing PostgreSQL Drivers (Fixing 'No module named psycopg2')..."
# Cài pip và driver vào thẳng môi trường của Superset
docker exec -u 0 superset bash -c "apt-get update && apt-get install -y python3-pip && pip install psycopg2-binary" || echo "⚠️ Driver install returned code $?, continuing anyway..."

# 3. Restart để nhận Driver
echo "[3/6] Restarting Superset to apply drivers..."
docker restart superset
echo "⏳ Waiting 20s for restart..."
sleep 20

# 4. Tạo Admin User (Sử dụng '|| true' để không báo lỗi nếu user đã tồn tại)
echo "[4/6] Creating admin user..."
docker exec superset superset fab create-admin \
              --username admin \
              --firstname Admin \
              --lastname User \
              --email admin@fab.org \
              --password admin || echo "✅ Admin user might already exist. Skipping..."

# 5. Nâng cấp Database (Fix lỗi migrate state)
echo "[5/6] Upgrading internal database & Initializing..."
docker exec superset superset db upgrade
docker exec superset superset init

# 6. Thông báo hoàn tất
echo "=========================================="
echo "✅ SUPERSET IS READY!"
echo "👉 Access here: http://localhost:8088"
echo "🔑 Login: admin / admin"
echo "💡 Tip: Nếu gặp lỗi 'migrate query editor state', hãy dùng Tab Ẩn Danh (Incognito)."
echo "=========================================="