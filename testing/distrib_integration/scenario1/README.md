# Distrib Integration scenario I
## Description
This repo has `create_scenario1` script which implement distib bdev on top of five alcmel bdevs.  
The alcmel bdev is on top of splited nvme bdev  

## Components
- `create_scenario1`: python script for auutomating distrib bdev creating from a config file for scenario 1  
- `CSX.X.json`: configuration files to be passed the `create_scenario1`  
- `config.txt`: Configuration file containing a list of input configurations (start-index end-index operation for testing  
- [test_block_device.py](../../test_block_device.py): python script for running R/W testing over bdev

## How to create distrib bdev
On a storage node where you have spdk running:
- To run with page size 4M, run cases with CSX.1.json, for example:  
`sudo python create_scenario1 -c CS4.1.json -ip <storage_node_ip>`
- To run with page size 512K, run cases with CSX.2.json, for example:  
`sudo python create_scenario1 -c CS4.2.json -ip <storage_node_ip>`

## How to test R/W on distrib bdev
On a client node where you connect to the distrib bdev:
- to run test with block size of 4k:  
`sudo python test_block_device.py -b <storage_device> -c config.txt  -s 4k`
- To run test with block size of 512:  
`sudo python test_block_device.py -b <storage_device> -c config.txt  -s 512`

