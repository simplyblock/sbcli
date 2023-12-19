#set -e
PATH=$PATH:/usr/local/bin

SPDK_DIR=$1
if [ -d $SPDK_DIR ]
then
    echo "Directory $SPDK_DIR exists."
    echo "Aborting SPDK installation"
    exit
fi

sudo yum update -y
sudo yum install git -y

sudo dnf install https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm -y

sudo rpm -i --noverify https://dl.rockylinux.org/pub/rocky/9/devel/x86_64/os/Packages/y/yasm-1.3.0-15.el9.x86_64.rpm
sudo rpm -i --noverify https://dl.rockylinux.org/pub/rocky/9/CRB/x86_64/os/Packages/n/nasm-2.15.03-7.el9.x86_64.rpm
sudo yum --enablerepo="codeready-builder-for-rhel-9-rhui-rpms" install CUnit-devel git wget yasm nasm cmake unzip -y

git clone https://github.com/spdk/spdk $SPDK_DIR
cd $SPDK_DIR
git submodule update --init

sudo  /bin/bash scripts/pkgdep.sh --rdma --pmem
mkdir /tmp/pmem

/bin/bash ./configure --with-crypto --with-vbdev-compress --with-rdma --disable-tests --disable-examples --enable-debug
echo "Building SPDK ..."
make
echo "Building SPDK complete"

cd $SPDK_DIR
curl -L -o ultra.zip https://www.dropbox.com/s/q40gk7r0f8k3t22/ultra.zip?dl=1
unzip ultra.zip
mv simplyblock-io-ultra-* ultra


echo "Building ultra21 ..."
cd $SPDK_DIR/ultra
git clone https://github.com/SysMan-One/utility_routines.git ./3rdparty/utility_routines/

cmake CMakeLists.txt -DCMAKE_BUILD_TYPE=Debug -B ./build -DSPDK_ROOT=/home/ec2-user/spdk
cd ./build
make
echo "Building ultra21 complete"

echo "Building distr ..."
cd $SPDK_DIR/ultra/DISTR_v2/
cmake CMakeLists.txt -DCMAKE_BUILD_TYPE=Debug -B ./build -DSPDK_ROOT=$SPDK_DIR
cd ./build
make

echo "build complete"

export HUGEMEM=4096
sudo /bin/bash $SPDK_DIR/scripts/setup.sh
sudo modprobe nvme-tcp
