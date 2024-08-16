#!/bin/bash

while true; do
    echo "$(date +'%Y-%m-%d %H:%M:%S')" >> watch_df.log
    sudo du -h --max-depth=1 / | sort -hr >> watch_df.log
    echo "---------------------------------------" >> watch_df.log
    sleep 10
    sudo df -h >> watch_df.log
    echo "---------------------------------------" >> watch_df.log
    sleep 10
done
