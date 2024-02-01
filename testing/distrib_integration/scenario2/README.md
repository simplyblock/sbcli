# Distrib Integration scenario II
## Description
This repo has `create_scenario2` script which implements distib bdev on top of two local alcmel bdevs and 4 remote alcmel bdevs.  
The alcmel bdev is on top of splited nvme bdev  
This script must be run on 3 storage nodes on three seteps:  
- The first step to create the local alcmel bdevs on the three nodes  
- The second step to create the remote alcmel bdevs and distrib map on the three nodes  
- The third step to create the distrib bdevs on the three nodes  
Also the repo has a `generate_config.py` which creates a configuration json file contains required alcmel and distrib parameters

## Components
- `generate_config`: python script for creating a config json file contains alcmel and distrib parameters to be passed to `create_scenario2`
- `create_scenario2`: python script for automating distrib bdev creating from config files for scenario 2  
- `CSX.X.json`: configuration files to be passed the `create_scenario1`  
- `config.txt`: Configuration file containing a list of input configurations (start-index end-index operation for testing  
- [test_block_device.py](../../test_block_device.py): python script for running R/W testing over bdev

## How to create distrib bdevs
On any node run:  
`sudo generate_config.py -ip1 <storage1_ip> -ip2 <storage2_ip> -ip3 <storage3_ip>`  
This will create `config1.json`, `config2.json`, `config3.json`
- On first storage node run:  
`sudo python create_scenario2 -c CS4.1.json -n  config1.json -vuid 1`  
- On second storage node run:  
`sudo python create_scenario2 -c CS4.1.json -n  config2.json -vuid 2` 
- On third storage node run:  
`sudo python create_scenario2 -c CS4.1.json -n  config3.json -vuid 3` 
> **_NOTE:_** Be careful to finish the first step on the three nodes firstly and then back to press enter to continue to step 2 and after finishing it on the three nodes press enter to continue to step3  
> **_NOTE:_** To run with page size 4M, run cases with CSX.1.json  
> **_NOTE:_** To run with page size 512K, run cases with CSX.2.json  

After running the above scripts finish, it creates on each node a command how to connect to that ditrib bdev
```commandline
sudo nvme connect --transport=tcp --traddr=172.31.5.249 --trsvcid=4420 --nqn=nqn.2023-02.io.simlpyblock:subsystem-172-31-5-249
sudo nvme connect --transport=tcp --traddr=172.31.4.135 --trsvcid=4420 --nqn=nqn.2023-02.io.simlpyblock:subsystem-172-31-4-135
sudo nvme connect --transport=tcp --traddr=172.31.9.62 --trsvcid=4420 --nqn=nqn.2023-02.io.simlpyblock:subsystem-172-31-9-62
```
## How to test R/W on distrib bdev
On a client node where you connect to the three distrib bdev:
- to run test with block size of 4k:  
`sudo python test_block_device.py -b <storage_device> -c config.txt  -s 4k`
- To run test with block size of 512:  
`sudo python test_block_device.py -b <storage_device> -c config.txt  -s 512`

