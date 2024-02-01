import logging
import os

logger = logging.getLogger()


def bind_spdk_driver(pci_addr):
    logger.info("Binding %s > uio_pci_generic", pci_addr)
    os.system("echo -n \"%s\" > /sys/bus/pci/drivers/nvme/unbind" % pci_addr)
    os.system("echo -n \"%s\" > /sys/bus/pci/drivers/uio_pci_generic/bind" % pci_addr)


def bind_nvme_driver(pci_addr):
    logger.info("Binding %s > nvme", pci_addr)
    os.system("echo -n \"%s\" > /sys/bus/pci/drivers/uio_pci_generic/unbind" % pci_addr)
    os.system("echo -n \"%s\" > /sys/bus/pci/drivers/nvme/bind" % pci_addr)


def get_nvme_devices():
    # Returns a list of nvme pci address and vendor IDs,
    # each list item is a tuple [("PCI_ADDRESS", "VENDOR_ID:DEVICE_ID")]
    stream = os.popen("lspci -Dnn | grep -i nvme")
    ret = stream.readlines()
    devs = []
    for line in ret:
        line_array = line.split()
        devs.append((line_array[0], line_array[-1][1:-1]))
    return devs
