# How to create encrypted LVol

Simplyblock logical volumes supports encryption at rest by leveraging [crypro bdev](https://spdk.io/doc/bdev.html) module using SPDK Software Accel framework.

The framework internally uses AES_XTS cipher which is used to sequence arbitary length of block data. This type of cipher is standardly used in backup software.

AES_XTS cipher requires 2 keys of length either 16 bytes of 32 bytes. The hex value of the key can be generated using the command.

for example to generate a 32 byte key
```
openssl rand -hex 32
```

After the keys are generated, an encrypted lvol can created by passing the generated keys to `crypto-key1` and `crypto-key2` cli flags

```
sbcli lvol add --distr-ndcs 1 \
    --distr-npcs 1 \
    --distr-bs 4096 \
    --distr-chunk-bs 4096 \
    --ha-type single \
    --encrypt \
    --crypto-key1 72477a786a63425872777174627a6678537463674e52336f75364a4646306378a \
    --crypto-key2 485a4539323173634a684c4c37737267364a454d655979375a684b514e714939a \
    lvolx3 25G testing1
```
