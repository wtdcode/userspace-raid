use color_eyre::Result;
use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::net::{SocketAddrV4, SocketAddrV6};
use std::os::fd::AsRawFd;
use std::os::linux::fs::MetadataExt;
use std::pin::Pin;
use std::{path::PathBuf, str::FromStr};
use tracing::{info, warn};

use color_eyre::eyre::{eyre, OptionExt};
use nix::ioctl_read;
use nix::libc::{S_IFBLK, S_IFMT, S_IFREG, S_IFSOCK};
use regex::Regex;
use tokio::net::TcpStream;

use crate::nbd::client::Client;
use crate::nbd::server::{Blocks, MemBlocks};
use crate::raid::cli_configurations;

// http://syhpoon.ca/posts/how-to-get-block-device-size-on-linux-with-rust/
const BLKGETSIZE64_CODE: u8 = 0x12; // Defined in linux/fs.h
const BLKGETSIZE64_SEQ: u8 = 114;
ioctl_read!(ioctl_blkgetsize64, BLKGETSIZE64_CODE, BLKGETSIZE64_SEQ, u64);

fn get_device_size(file: &std::fs::File) -> u64 {
    let fd = file.as_raw_fd();

    let mut cap = 0u64;
    let cap_ptr = &mut cap as *mut u64;

    unsafe {
        ioctl_blkgetsize64(fd, cap_ptr).unwrap();
    }

    return cap;
}

pub fn parse_size(s: &str) -> Result<usize> {
    // note lvm convention, g 1024, G 1000
    let units: HashMap<&str, usize> = HashMap::from([
        ("k", 1024),
        ("K", 1000),
        ("m", 1024 * 1024),
        ("M", 1000 * 1000),
        ("g", 1024 * 1024 * 1024),
        ("G", 1000 * 1000 * 1000),
    ]);
    let re = Regex::new(r"(\d+)([k|K|m|M|g|G]?)")?;
    if !re.is_match(s) {
        return Err(eyre!("Unrecognized size {} specified", s));
    }

    let caps = re.captures(s).ok_or_eyre(eyre!("no captures??"))?;
    let size = usize::from_str(caps.get(0).map(|t| t.as_str()).unwrap())?;
    let unit = *units.get(caps.get(1).map(|t| t.as_str()).unwrap()).unwrap();

    return Ok(size * unit);
}

pub fn connectable(s: &str) -> bool {
    s.parse::<SocketAddrV4>().is_ok()
        || s.parse::<SocketAddrV6>().is_ok()
        || (std::fs::metadata(s)
            .map(|t| t.st_mode() & S_IFMT == S_IFSOCK)
            .is_ok_and(|t| t))
}

#[derive(Debug)]
pub enum DeviceConfiguration {
    Memory(MemBlocks),
    File(File, u64),
    BlockDevice(File, u64),
    NetworkBlockDevice(Client<TcpStream>),
}

impl Blocks for DeviceConfiguration {
    fn flush(&self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                Self::File(fp, _) => fp.flush().await,
                Self::BlockDevice(fp, _) => fp.flush().await,
                Self::NetworkBlockDevice(cl) => {
                    cl.flush().await.map_err(|e| std::io::Error::other(e))
                }
                Self::Memory(m) => m.flush().await,
            }
        })
    }

    fn read_at<'a>(
        &'a self,
        buf: &'a mut [u8],
        off: u64,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                Self::File(fp, _) => fp.read_at(buf, off).await,
                Self::BlockDevice(fp, _) => fp.read_at(buf, off).await,
                Self::NetworkBlockDevice(cl) => cl
                    .read_at(off, buf)
                    .await
                    .map_err(|e| std::io::Error::other(e)),
                Self::Memory(v) => v.read_at(buf, off).await,
            }
        })
    }

    fn write_at<'a>(
        &'a self,
        buf: &'a [u8],
        off: u64,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                Self::File(fp, _) => fp.write_at(buf, off).await,
                Self::BlockDevice(fp, _) => fp.write_at(buf, off).await,
                Self::NetworkBlockDevice(cl) => cl
                    .write(off, buf)
                    .await
                    .map_err(|e| std::io::Error::other(e)),
                Self::Memory(v) => v.write_at(buf, off).await,
            }
        })
    }

    fn size(&self) -> Pin<Box<dyn Future<Output = std::io::Result<u64>> + Send + '_>> {
        Box::pin(async move {
            match self {
                Self::File(fp, _) => fp.size().await,
                Self::BlockDevice(fp, _) => fp.size().await,
                Self::NetworkBlockDevice(cl) => Ok(cl.size()),
                Self::Memory(v) => v.size().await,
            }
        })
    }
}

impl DeviceConfiguration {
    pub fn memory(size: usize) -> Self {
        Self::Memory(MemBlocks::new(vec![0; size]))
    }

    pub fn file(path: PathBuf) -> Result<Self> {
        info!(path = path.to_string_lossy().to_string(), "Adding file backend deivce");
        let st = std::fs::metadata(&path)?;
        // File backend
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)?;
        Ok(Self::File(file.into(), st.len()))
    }

    pub fn block(path: PathBuf) -> Result<Self> {
        info!(path = path.to_string_lossy().to_string(), "Adding block backend deivce");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)?;
        let sz = get_device_size(&file);
        Ok(Self::BlockDevice(file.into(), sz))
    }

    pub async fn remote(addr: &str) -> Result<Self> {
        info!(addr, "Adding remote backend deivce");
        let tcp = TcpStream::connect(addr).await?;
        let client = Client::new(tcp).await?;
        Ok(Self::NetworkBlockDevice(client))
    }

    pub async fn from_confs(mut confs: HashMap<String, String>) -> Result<Self> {
        let mut arr: Vec<(String, String)> = confs.clone().into_iter().collect();

        // Check shorthands
        if arr.len() == 1 && arr[0].1.len() == 0 {
            // First check if it is a valid path as shorthand
            let s = arr.pop().unwrap().0;

            let path = PathBuf::from(&s);
            if path.exists() {
                // Okay, file or blockdevice, let's see
                let st = std::fs::metadata(&path)?;

                return match st.st_mode() & S_IFMT {
                    S_IFBLK => {
                        info!("Inferred as local block device backend, path = {}", s);
                        Self::block(path)
                    }
                    S_IFREG => {
                        info!("Inferred as local block file backend, path = {}", s);
                        Self::file(path)
                    }
                    S_IFSOCK => {
                        info!(
                            "Inferred as a remote NBD backend in UNIX socket, path = {}",
                            s
                        );
                        Self::remote(&s).await
                    }
                    _ => Err(eyre!(
                        "Not supported file: {}",
                        path.to_string_lossy().to_string()
                    )),
                };
            } else {
                // Check if memory shorthand,
                if let Ok(sz) = parse_size(&s) {
                    info!("Inferred as memory backend, size = {}", sz);
                    return Ok(Self::memory(sz));
                }

                if connectable(&s) {
                    info!("Inferred as remote NBD backend, address: {}", s.to_string());
                    return Self::remote(&s).await;
                }
            }
        }

        // All shorthands gone, configuration mode, firstly look for type
        let device_type = if confs.contains_key("type") {
            confs.remove("type").unwrap()
        } else {
            // inference
            let mut typ = None;
            for (k, v) in confs.iter() {
                if v.len() == 0 {
                    typ = Some(k.clone());
                }
            }

            // more inference
            if confs.contains_key("size") {
                typ = Some("memory".to_string());
            }

            if let Some(typ) = typ {
                typ
            } else {
                return Err(eyre!("Unknown device type, you forget type=...?"));
            }
        };
        let ret = match device_type.to_ascii_lowercase().as_str() {
            "memory" | "mem" | "m" => {
                if let Some(sz) = confs.remove("size") {
                    let sz = parse_size(&sz)?;
                    Ok(Self::memory(sz))
                } else {
                    Err(eyre!("No size given for memory backend"))
                }
            }
            "file" | "f" => {
                if let Some(path) = confs.remove("path") {
                    Self::file(PathBuf::from(path))
                } else {
                    Err(eyre!("No path given for file backend"))
                }
            }
            "remote" | "r" | "nbd" | "n" => {
                if let Some(remote) = confs.remove("address") {
                    Self::remote(&remote).await
                } else {
                    Err(eyre!("No remote given for remote NBD backend"))
                }
            }
            "block" | "b" => {
                if let Some(path) = confs.remove("path") {
                    Self::block(PathBuf::from(path))
                } else {
                    Err(eyre!("No path given for block backend"))
                }
            }
            _ => Err(eyre!("Unkown device type of {}", device_type)),
        };

        for (k, v) in confs.into_iter() {
            warn!(key = k, value = v, "Not used configurations");
        }

        return ret;
    }
}
