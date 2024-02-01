
SERVICE_NAME=$1

sudo systemctl stop $SERVICE_NAME
sudo systemctl disable $SERVICE_NAME
sudo rm -f "/lib/systemd/system/$SERVICE_NAME.service"
sudo systemctl daemon-reload
