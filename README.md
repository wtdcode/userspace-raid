# Userspace RAID Device (URD)

Userspace RAID Device (URD) is an experimental implementation (course assignment, actually) in userspace to simulate the common RAID setups.

The code is strictly _not appropriate_ for any production usage.

## Setup

URD relies on [NBD](https://docs.kernel.org/admin-guide/blockdev/nbd.html) to mount block devices. Therefore, only Linux is supported so far.

Most modern Linux distributions shall have `NBD` shipped out-of-box, just enable it by:

```bash
modprobe nbd
```

No other software is needed.

### Backends

URD supports a variety of backends including:

- Memory
- File
- Block device
- Remote NBD export

and mixing these backends freely.

### Build

Obtain rust toolchain and:

```
cargo build --release
```

### Quick Setup

Server side:

```
truncate -s 2048m /tmp/file1
truncate -s 2048m /tmp/file2
./target/release/urd server --level 0 --raid stripe=1024 --device /tmp/file1 --device /tmp/file2
```

This spins up an NBD server providing read/write access.

Client side:

```
sudo ./target/release/urd mount --device /dev/nbd2
```

This mount the server to `/dev/nbd2`. Then `/dev/nbd2` can act like a common block device.

```
sudo fdisk -l /dev/nbd2 # This shall show 4gb
sudo mkfs.xfs /dev/nbd2 # Create a filesystem
mkdir /tmp/test
sudo mount /dev/nbd2 /tmp/test
dd if=/dev/zero of=./test bs=2048k status=progress count=1024
```

### Arguments 

### Device configuration

The `--device /tmp/file2` of the server arguments is a shorthand of `--device type=file,path=/tmp/file2`.

The available shorthand includes:

- `--device <IP>:<PORT>`, connects to a remote NBD export
- `--device /path/to/unix.socket`, connects to a local NBD export via UNIX socket
- `--device /dev/sda`, connects to a local block device
- `--device 2048m`, create a in-memory device with 2048m available space

The full configuration is `--device type=[file|f|block|b|remote|r|memory|m][,property=...]`, with the properties:

- `file|f`: `path`
- `block|b`: `path`
- `remote`: `address`
- `memory`: `size`

### RAID Configuration

The RAID configuration has the same syntax like `--raid property=...[,property=...]` and properties includes:

- RAID0: `stripe`

