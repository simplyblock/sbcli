
SERVICE_NAME=$1
SERVICE_UNIT_FILE=$2


systemctl is-active --quiet $SERVICE_NAME
if [ $? -eq 0 ]
then
    echo "$SERVICE_NAME is installed and running, nothing to do."
    exit 0
fi

if [ ! -f "/lib/systemd/system/$SERVICE_NAME.service" ]; then
    echo "Creating service unit files..."
    sudo cp $SERVICE_UNIT_FILE "/lib/systemd/system/$SERVICE_NAME.service"
    sudo systemctl daemon-reload
    sudo systemctl enable $SERVICE_NAME
    sudo systemctl start $SERVICE_NAME
else
    echo "$SERVICE_NAME stopped, trying to start it..."
    sudo systemctl enable $SERVICE_NAME
    sudo systemctl start $SERVICE_NAME
    sudo systemctl --no-pager status $SERVICE_NAME
fi

systemctl is-active --quiet $SERVICE_NAME
if [ $? -eq 0 ]
then
    echo "$SERVICE_NAME is active"
    exit 0
else
    echo "$SERVICE_NAME is not active"
    exit 1
fi
