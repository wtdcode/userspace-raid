use crate::{device::DeviceConfiguration, nbd::server::Blocks};
use color_eyre::{eyre::eyre, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use std::{collections::HashMap, future::Future, pin::Pin, str::FromStr, sync::Arc};
use tokio::task::{JoinSet, LocalSet};
use tracing::{debug, warn};

pub fn cli_configurations(s: &str) -> Result<HashMap<String, String>> {
    let mut ret = HashMap::new();
    for tk in s.split(",") {
        if tk.contains("=") {
            let mut conf: Vec<String> = tk.split("=").map(|t| t.to_string()).collect();

            if conf.len() == 2 {
                let v = conf.pop().unwrap();
                let k = conf.pop().unwrap();
                ret.insert(k.to_ascii_lowercase(), v);
            } else {
                return Err(eyre!("Not recoginized configuration: {}", tk));
            }
        } else {
            ret.insert(tk.to_string().to_ascii_lowercase(), String::new());
        }
    }
    Ok(ret)
}

#[derive(Debug, Clone)]
pub enum RaidConfiguration {
    RAID0 { stripe: usize },
    RAID1,
    RAID6 { data: usize, parities: usize },
}

impl RaidConfiguration {
    pub fn from_level_and_string(
        level: usize,
        mut configs: HashMap<String, String>,
    ) -> Result<Self> {
        let ret = match level {
            0 => {
                let stripe =
                    usize::from_str(&configs.remove("stripe").unwrap_or("1024".to_string()))?;
                // let jbod = configs
                //     .remove("stripe")
                //     .map(|t| {
                //         if t.len() == 0 {
                //             Ok(false)
                //         } else {
                //             bool::from_str(&t)
                //         }
                //     })
                //     .unwrap_or(Ok(false))?;

                Ok(RaidConfiguration::RAID0 {
                    stripe: stripe,
                })
            }
            1 => Ok(RaidConfiguration::RAID1),
            6 => {
                let data = usize::from_str(&configs.remove("data").unwrap_or("6".to_string()))?;
                let stripes =
                    usize::from_str(&configs.remove("stripes").unwrap_or("2".to_string()))?;
                Ok(RaidConfiguration::RAID6 {
                    data: data,
                    parities: stripes,
                })
            }
            _ => Err(eyre!("RAID-{} not implemented yet", level)),
        };

        for (k, v) in configs {
            warn!(key = k, value = v, "Unused Raid Configuration Value");
        }

        ret
    }
}

#[derive(Debug)]
pub struct RAID {
    pub devices: Vec<Arc<DeviceConfiguration>>,
    pub config: RaidConfiguration,
    pub size: usize,
}

impl RAID {
    async fn determine_size(
        devices: &Vec<DeviceConfiguration>,
        raid: &RaidConfiguration,
    ) -> Result<usize> {
        let mut sizes = vec![];

        for dev in devices.iter() {
            let sz = dev.size().await?;
            sizes.push(sz as usize);
        }

        let min = *sizes.iter().min().unwrap();

        for sz in sizes {
            if sz != min {
                return Err(eyre!("Devices of different sizes not supported yet"));
            }
        }

        let sz = match raid {
            RaidConfiguration::RAID0 { stripe: _ } => devices.len() * min,
            _ => {
                return Err(eyre!("Not implemented size inference"));
            }
        };

        Ok(sz)
    }

    pub async fn new(devices: Vec<DeviceConfiguration>, raid: RaidConfiguration) -> Result<Self> {
        let sz = Self::determine_size(&devices, &raid).await?;
        Ok(Self {
            devices: devices.into_iter().map(|t| Arc::new(t)).collect(),
            config: raid,
            size: sz,
        })
    }

    // RAID0
    fn off_to_device_off(&self, off: usize, stripe: usize) -> usize {
        let devices = self.devices.len();
        let off_in_page = off % stripe;
        let off_in_dev = (off / stripe / devices) * stripe + off_in_page;

        off_in_dev
    }

    fn raid0_segments(
        &self,
        off: usize,
        len: usize,
        stripe: usize,
    ) -> std::io::Result<Vec<(usize, usize, usize, usize, usize)>> {
        if off + len > self.size {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }

        let mut lhs = off as usize;
        let devices = self.devices.len();
        let mut dev = (off as usize / stripe) % devices;
        let mut request = vec![];
        while lhs < off + len {
            let rhs = (((lhs / stripe) + 1) * stripe).min(len + off as usize);

            let lhs_in_device = self.off_to_device_off(lhs, stripe);
            let rhs_in_device = (lhs_in_device / stripe) * stripe + stripe.min(off + len - lhs);

            if rhs_in_device - lhs_in_device != rhs - lhs {
                warn!(
                    lhs,
                    rhs, lhs_in_device, rhs_in_device, "Inconsistency in RAID0 detected"
                );
                return Err(std::io::ErrorKind::ConnectionReset.into());
            }

            request.push((lhs, rhs, lhs_in_device, rhs_in_device, dev));

            dev = (dev + 1) % devices;
            lhs = rhs;
        }

        Ok(request)
    }

    pub async fn raid0_read_at(
        &self,
        buf: &mut [u8],
        off: u64,
        stripe: usize,
    ) -> std::io::Result<()> {
        let off = off as usize;
        let request = self.raid0_segments(off, buf.len(), stripe)?;
        let mut js = JoinSet::new();

        for (lhs, rhs, lhs_indevice, rhs_in_device, dev_idx) in request {
            let dev = self.devices[dev_idx].clone();
            js.spawn(async move {
                let mut buf = vec![0u8; rhs_in_device - lhs_indevice];
                debug!(lhs_indevice, size=buf.len(), dev_idx, "RAID0 read request");
                dev.read_at(&mut buf, lhs_indevice as u64).await?;
                Ok::<_, std::io::Error>((lhs, rhs, buf))
            });
        }

        while let Some(ret) = js.join_next().await {
            let (lhs, rhs, seg_buf) = ret??;
            
            buf[lhs - off..rhs - off].copy_from_slice(&seg_buf);
        }

        Ok(())
    }

    async fn raid0_write_at<'a>(&self, buf: &[u8], off: u64, stripe: usize) -> std::io::Result<()> {
        let off = off as usize;
        let request = self.raid0_segments(off, buf.len(), stripe)?;
        let mut js = FuturesUnordered::new();
        for (lhs, rhs, lhs_indevice, rhs_in_device, dev_idx) in request {
            let dev = self.devices[dev_idx].clone();
            js.push(async move {
                let buf = &buf[lhs - off..rhs - off];
                debug!(lhs_indevice, size=buf.len(), dev_idx, "RAID0 write request");
                dev.write_at(buf, lhs_indevice as u64).await?;
                Ok::<_, std::io::Error>(())
            });
        }

        while let Some(r) = js.next().await {
            let _ = r?;
        }
        Ok(())
    }
}

impl Blocks for RAID {
    fn flush(&self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            match &self.config {
                RaidConfiguration::RAID0 { stripe } => {
                    // Flush all
                    let mut js = JoinSet::new();

                    for dev in self.devices.clone().into_iter() {
                        js.spawn(async move { dev.flush().await });
                    }

                    while let Some(t) = js.join_next().await {
                        let _ = t??;
                    }

                    Ok(())
                }
                _ => Err(std::io::Error::other("Not implemented yet")),
            }
        })
    }

    fn read_at<'a>(
        &'a self,
        buf: &'a mut [u8],
        off: u64,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            match &self.config {
                RaidConfiguration::RAID0 { stripe } => {
                    self.raid0_read_at(buf, off, *stripe).await
                }
                _ => Err(std::io::Error::other("Not implemented yet")),
            }
        })
    }

    fn write_at<'a>(
        &'a self,
        buf: &'a [u8],
        off: u64,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            match &self.config {
                RaidConfiguration::RAID0 { stripe } => {
                    self.raid0_write_at(buf, off, *stripe).await
                }
                _ => Err(std::io::Error::other("Not implemented yet")),
            }
        })
    }

    fn size(&self) -> Pin<Box<dyn Future<Output = std::io::Result<u64>> + Send + '_>> {
        Box::pin(std::future::ready(Ok(self.size as u64)))
    }
}
