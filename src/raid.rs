use crate::{
    device::DeviceConfiguration,
    nbd::server::Blocks,
    parity::{gl_div_two, gl_mul_two, GN},
};
use color_eyre::{eyre::eyre, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{Debug, Display},
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    sync::RwLock,
    task::{JoinSet, LocalSet},
};
use tracing::{debug, info, trace, warn};

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
    RAID6 { stripe: usize },
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

                Ok(RaidConfiguration::RAID0 { stripe: stripe })
            }
            1 => Ok(RaidConfiguration::RAID1),
            6 => {
                let stripe =
                    usize::from_str(&configs.remove("stripe").unwrap_or("1024".to_string()))?;
                Ok(RaidConfiguration::RAID6 { stripe: stripe })
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
    pub failures: RwLock<HashSet<usize>>,
    pub config: RaidConfiguration,
    pub size: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct RAID6Device {
    pub device: usize,
    pub group: usize,
    pub first_parity: usize,
    pub second_parity: usize,
    pub first_data: usize,
    pub last_data: usize,
}

impl Display for RAID6Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Debug>::fmt(&self, f)
    }
}

#[derive(Error, Debug)]
pub struct RAIDError {
    pub device: usize,
    pub err: std::io::Error,
}

impl Display for RAIDError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.err, f)
    }
}
#[derive(Debug, Clone, Copy)]
struct RAID6Reqeust {
    pub offset: Option<(usize, usize)>,
    pub lhs_in_device: usize,
    pub rhs_in_device: usize,
    pub device: RAID6Device,
}

impl RAID {
    async fn determine_size(
        devices: &Vec<DeviceConfiguration>,
        raid: &RaidConfiguration,
    ) -> Result<usize> {
        let mut sizes = vec![];

        for dev in devices.iter() {
            match dev.size().await {
                Ok(sz) => {
                    sizes.push(sz as usize);
                }
                Err(e) => {
                    warn!("Device not having a size, skipped, error = {}", e);
                }
            }
        }

        let min = *sizes.iter().min().unwrap();

        for sz in sizes {
            if sz != min {
                return Err(eyre!("Devices of different sizes not supported yet"));
            }
        }

        let sz = match raid {
            RaidConfiguration::RAID0 { stripe: _ } => devices.len() * min,
            RaidConfiguration::RAID6 { stripe: _ } => {
                if devices.len() <= 2 {
                    return Err(eyre!("Too less devices for RAID6 to work!"));
                } else {
                    (devices.len() - 2) * min
                }
            }
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
            failures: RwLock::new(HashSet::new()),
            size: sz,
        })
    }

    // RAID0
    fn off_to_device_off(off: usize, stripe: usize, devices: usize) -> usize {
        let off_in_page = off % stripe;
        let off_in_dev = (off / stripe / devices) * stripe + off_in_page;

        off_in_dev
    }

    fn striped_off_to_device(
        off: usize,
        bytes_left: usize,
        stripe: usize,
        devices: usize,
    ) -> (usize, usize) {
        trace!(off, bytes_left, stripe, devices, "striped_off_to_device");
        let lhs_in_device = Self::off_to_device_off(off, stripe, devices);
        let rhs_in_device = (lhs_in_device / stripe) * stripe + stripe.min(bytes_left);

        (lhs_in_device, rhs_in_device)
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

            let (lhs_in_device, rhs_in_device) =
                Self::striped_off_to_device(off, len + off - lhs, stripe, devices);

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

    async fn raid0_read_at(&self, buf: &mut [u8], off: u64, stripe: usize) -> std::io::Result<()> {
        let off = off as usize;
        let request = self.raid0_segments(off, buf.len(), stripe)?;
        let mut js = JoinSet::new();

        for (lhs, rhs, lhs_indevice, rhs_in_device, dev_idx) in request {
            let dev = self.devices[dev_idx].clone();
            js.spawn(async move {
                let mut buf = vec![0u8; rhs_in_device - lhs_indevice];
                debug!(
                    lhs_indevice,
                    size = buf.len(),
                    dev_idx,
                    "RAID0 read request"
                );
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
                debug!(
                    lhs_indevice,
                    size = buf.len(),
                    dev_idx,
                    "RAID0 write request"
                );
                dev.write_at(buf, lhs_indevice as u64).await?;
                Ok::<_, std::io::Error>(())
            });
        }

        while let Some(r) = js.next().await {
            let _ = r?;
        }
        Ok(())
    }

    // RAID6
    fn raid6_off_to_device(off: usize, stripe: usize, devices: usize) -> RAID6Device {
        // 4 + 2 we have stripes like:
        // 0 1 2 3 (4 5), 0 1 2 (3 4) 5, 0 1 (2 3) 4 5, 0 (1 2) 3 4 5, (0 1) 2 3 4 5, (0) 1 2 3 4 (5)
        // assume lhs / stripe = 10, then groups = 2, dev = (6 - 2) % 6 = 2
        //        lhs / stripe = 6, then group = 1, dev = (2 + 2 - 1) % 6 = 4
        let data_devices = devices - 2;
        let stripe_idx = off / stripe;
        let group = (stripe_idx / data_devices) % devices;
        let first_parity = (data_devices + devices - group) % devices;
        let second_parity = (first_parity + 1) % devices;
        let data_idx = stripe_idx % data_devices;
        if first_parity == 0 {
            RAID6Device {
                device: 2 + data_idx,
                group: group,
                first_parity: first_parity,
                second_parity: second_parity,
                first_data: 2,
                last_data: 1 + data_devices,
            }
        } else if second_parity == 0 {
            RAID6Device {
                device: 1 + data_idx,
                group: group,
                first_parity: first_parity,
                second_parity: second_parity,
                first_data: 1,
                last_data: data_devices,
            }
        } else if second_parity == devices - 1 {
            RAID6Device {
                device: data_idx,
                group: group,
                first_parity: first_parity,
                second_parity: second_parity,
                first_data: 0,
                last_data: data_devices - 1,
            }
        } else if data_idx < (data_devices - group) {
            RAID6Device {
                device: data_idx,
                group: group,
                first_parity: first_parity,
                second_parity: second_parity,
                first_data: 0,
                last_data: devices - 1,
            }
        } else {
            RAID6Device {
                device: data_idx + 2,
                group: group,
                first_parity: first_parity,
                second_parity: second_parity,
                first_data: 0,
                last_data: devices - 1,
            }
        }
    }

    fn raid6_segments_extend(
        &self,
        stripe: usize,
        request: Vec<RAID6Reqeust>,
    ) -> Vec<RAID6Reqeust> {
        let mut request: VecDeque<RAID6Reqeust> = request.into_iter().collect();

        // Extend the request to a full group
        if request.len() > 0 {
            let front = *request.front().unwrap();
            let back = *request.back().unwrap();
            // first could be last, that's okay
            if front.device.device != front.device.first_data {
                let mut cur = front.device.device - 1;
                while cur >= front.device.first_data {
                    if cur == front.device.first_parity || cur == front.device.second_parity {
                        if cur == 0 {
                            break;
                        }
                        cur = cur - 1;
                        continue;
                    }
                    let mut new = front;
                    new.device.device = cur;
                    new.offset = None;
                    new.lhs_in_device = (front.lhs_in_device / stripe) * stripe;
                    new.rhs_in_device = new.lhs_in_device + stripe;
                    request.push_front(new);
                    if cur == 0 {
                        break;
                    }
                    cur = cur - 1;
                }
            }

            if back.device.device != back.device.last_data {
                let mut cur = back.device.device + 1;
                while cur <= back.device.last_data {
                    if cur == back.device.first_parity || cur == back.device.second_parity {
                        cur = cur + 1;
                        continue;
                    }
                    let mut new = back;
                    new.device.device = cur;
                    new.offset = None;
                    new.lhs_in_device = (back.lhs_in_device / stripe) * stripe;
                    new.rhs_in_device = new.lhs_in_device + stripe;
                    request.push_back(new);
                    cur = cur + 1;
                }
            }
        }

        request.into_iter().collect()
    }

    fn raid6_segments(
        &self,
        off: usize,
        len: usize,
        stripe: usize,
    ) -> std::io::Result<Vec<RAID6Reqeust>> {
        if off + len > self.size {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let mut request = Vec::new();

        let mut lhs = off as usize;
        let data_devces = self.devices.len() - 2; // data drives
        let devices = self.devices.len();

        while lhs < off + len {
            let rhs = (((lhs / stripe) + 1) * stripe).min(len + off as usize);
            let (lhs_in_device, rhs_in_device) =
                Self::striped_off_to_device(lhs, len + off - lhs, stripe, data_devces);

            if rhs_in_device - lhs_in_device != rhs - lhs {
                warn!(
                    lhs,
                    rhs, lhs_in_device, rhs_in_device, "Inconsistency in RAID6 detected"
                );
                return Err(std::io::ErrorKind::ConnectionReset.into());
            }
            request.push(RAID6Reqeust {
                offset: Some((lhs, rhs)),
                lhs_in_device,
                rhs_in_device,
                device: Self::raid6_off_to_device(lhs, stripe, devices),
            });

            lhs = rhs;
        }

        Ok(request)
    }

    fn raid6_split_groups(request: &Vec<RAID6Reqeust>) -> Vec<Vec<(usize, RAID6Reqeust)>> {
        let mut groups = vec![];
        let mut group: Vec<(usize, RAID6Reqeust)> = vec![];
        for (idx, req) in request.iter().enumerate() {
            if let Some(last) = group.last() {
                if last.1.device.group == req.device.group {
                    group.push((idx, *req));
                } else {
                    groups.push(std::mem::take(&mut group));
                    group.push((idx, *req));
                }
            } else {
                group.push((idx, *req));
            }
        }

        return groups;
    }

    async fn raid6_read_at(&self, buf: &mut [u8], off: u64, stripe: usize) -> std::io::Result<()> {
        let off = off as usize;
        let request = self.raid6_segments(off, buf.len(), stripe)?;
        let mut js = JoinSet::new();

        for (req_idx, req) in request.iter().enumerate() {
            if let Some((lhs, rhs)) = req.offset {
                let dev = self.devices[req.device.device].clone();
                let lhs_in_device = req.lhs_in_device;
                let rhs_in_device = req.rhs_in_device;
                let req = req.clone();
                js.spawn(async move {
                    let mut buf = vec![0u8; rhs_in_device - lhs_in_device];
                    debug!(
                        req_idx,
                        lhs_in_device,
                        size = buf.len(),
                        "RAID6 read request, request = {:?}",
                        req
                    );
                    dev.read_at(&mut buf, lhs_in_device as u64)
                        .await
                        .map_err(|e| RAIDError {
                            device: req.device.device,
                            err: e,
                        })?;
                    Ok::<_, RAIDError>((req_idx, lhs, rhs, buf))
                });
            }
        }

        let mut bufs = vec![None; request.len()];
        let mut failed_devices = HashSet::new();
        while let Some(ret) = js.join_next().await {
            match ret? {
                Ok((req_idx, lhs, rhs, seg_buf)) => {
                    bufs[req_idx] = Some((lhs, rhs, seg_buf));
                }
                Err(e) => {
                    info!(e.device, "Degration detected during RAID6 raid");
                    self.failures.write().await.insert(e.device);
                    failed_devices.insert(e.device);
                }
            }
        }

        if failed_devices.len() == 0 {
            // happy path
        } else {
            if failed_devices.len() > 2 {
                warn!(
                    "Only support 1 failed disk at this moment, we have {}",
                    failed_devices.len()
                );
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            let groups = Self::raid6_split_groups(&request);
            let mut all_recoverd = vec![];
            if failed_devices.len() != 0 {
                for group in groups {
                    let mut recover_bufs = vec![None; self.devices.len()];

                    let mut failed_request = vec![];
                    for (req_idx, req) in group.iter() {
                        if bufs[*req_idx].is_none() {
                            failed_request.push((*req_idx, *req));
                        }
                    }

                    if failed_request.len() == 0 {
                        continue;
                    }

                    let lhs_in_device = (group[0].1.lhs_in_device / stripe) * stripe;
                    let first_parity = group[0].1.device.first_parity;
                    let second_parity = group[0].1.device.second_parity;
                    debug!(
                        first_parity,
                        second_parity, lhs_in_device, "Failed request = {:?}", failed_request
                    );
                    // Try to get known buffers
                    for (other_idx, other_req) in bufs.iter().enumerate() {
                        let successful_req = request[other_idx];
                        if successful_req.lhs_in_device == lhs_in_device {
                            if let Some(other_req) = other_req {
                                debug!(
                                    "Filled device {} from known buffers",
                                    successful_req.device.device
                                );
                                recover_bufs[successful_req.device.device] =
                                    Some(other_req.2.clone());
                            }
                        }
                    }

                    // get all
                    for (device_idx, recover_buf) in recover_bufs.iter_mut().enumerate() {
                        if recover_buf.is_none() {
                            let mut buf = vec![0u8; stripe];
                            match self.devices[device_idx]
                                .read_at(&mut buf, lhs_in_device as u64)
                                .await
                            {
                                Ok(_) => {
                                    debug!("Filled device {} by reading", device_idx);
                                    *recover_buf = Some(buf);
                                }
                                Err(e) => {
                                    debug!("Reading device {} failed due to {}", device_idx, e);
                                }
                            }
                        }
                    }

                    let mut failed_bufs = recover_bufs
                        .iter()
                        .enumerate()
                        .filter(|t| t.1.is_none())
                        .collect_vec();

                    if failed_bufs.len() > 2 {
                        warn!("More failure during rebuilding reading!")
                    } else if failed_bufs.len() == 1 {
                        let (failed_idx, failed_req) = failed_request.pop().unwrap();
                        if recover_bufs[first_parity].is_some() {
                            let recovered = recover_bufs
                                .into_iter()
                                .enumerate()
                                .filter(|t| t.0 != second_parity)
                                .fold(vec![0u8; stripe], |acc, x| {
                                    if let Some(x) = x.1 {
                                        acc.into_iter()
                                            .zip(x)
                                            .into_iter()
                                            .map(|(lhs, rhs)| lhs ^ rhs)
                                            .collect()
                                    } else {
                                        acc
                                    }
                                });
                            all_recoverd.push((
                                failed_idx,
                                Some((
                                    failed_req.offset.unwrap().0,
                                    failed_req.offset.unwrap().1,
                                    recovered,
                                )),
                            ));
                        } else {
                            warn!("Control flow shouldn't go here");
                            return Err(std::io::ErrorKind::InvalidInput.into());
                        }
                    } else {
                        let (x, _) = failed_bufs.pop().unwrap();
                        let (y, _) = failed_bufs.pop().unwrap();

                        let (x, y) = if x > y { (y, x) } else { (x, y) };

                        if recover_bufs[first_parity].is_some()
                            && recover_bufs[second_parity].is_some()
                        {
                            // two data drives fails
                            let p = recover_bufs[first_parity].clone().unwrap();
                            let q = recover_bufs[second_parity].clone().unwrap();

                            let pxy = recover_bufs
                                .iter()
                                .enumerate()
                                .filter(|t| t.0 != first_parity && t.0 != second_parity)
                                .fold(vec![0u8; stripe], |acc, x| {
                                    if let Some(x) = x.1 {
                                        acc.into_iter().zip(x).map(|(a, b)| a ^ b).collect_vec()
                                    } else {
                                        acc
                                    }
                                });

                            let mut qxy = vec![0u8; stripe];
                            for (n, data) in recover_bufs.into_iter().enumerate() {
                                if n == second_parity || n == first_parity {
                                    continue;
                                }
                                let gn = GN[n];
                                let data = if let Some(data) = data {
                                    data
                                } else {
                                    vec![0u8; stripe]
                                };
                                let data =
                                    data.into_iter().map(|t| gl_mul_two(t, gn)).collect_vec();
                                qxy = qxy
                                    .into_iter()
                                    .zip(data)
                                    .map(|(lhs, rhs)| lhs ^ rhs)
                                    .collect_vec();
                            }

                            let g_y_x = GN[y - x];
                            let a = gl_div_two(g_y_x, g_y_x ^ 0x1);
                            let b = gl_div_two(GN[255 - x], g_y_x ^ 0x1);

                            let p_p_xy = p
                                .into_iter()
                                .zip(pxy.into_iter())
                                .map(|(l, r)| l ^ r)
                                .collect_vec();
                            let dx = p_p_xy
                                .clone()
                                .into_iter()
                                .map(|t| gl_mul_two(a, t))
                                .zip(q.into_iter().zip(qxy).map(|(l, r)| gl_mul_two(b, l ^ r)))
                                .map(|(l, r)| l ^ r)
                                .collect_vec();

                            let dy = p_p_xy
                                .into_iter()
                                .zip(dx.clone().into_iter())
                                .map(|(l, r)| l ^ r)
                                .collect_vec();

                            for (req_idx, failed_req) in failed_request {
                                if failed_req.device.device == x {
                                    all_recoverd.push((
                                        req_idx,
                                        Some((
                                            failed_req.offset.unwrap().0,
                                            failed_req.offset.unwrap().1,
                                            dx.clone(),
                                        )),
                                    ));
                                }

                                if failed_req.device.device == y {
                                    all_recoverd.push((
                                        req_idx,
                                        Some((
                                            failed_req.offset.unwrap().0,
                                            failed_req.offset.unwrap().1,
                                            dy.clone(),
                                        )),
                                    ));
                                }
                            }
                        } else if recover_bufs[second_parity].is_none() {
                            // This is easy, simply recalculate the corrupted data disk
                            let recovered = recover_bufs
                                .into_iter()
                                .enumerate()
                                .filter(|t| t.0 != second_parity)
                                .fold(vec![0u8; stripe], |acc, x| {
                                    if let Some(x) = x.1 {
                                        acc.into_iter()
                                            .zip(x)
                                            .into_iter()
                                            .map(|(lhs, rhs)| lhs ^ rhs)
                                            .collect()
                                    } else {
                                        acc
                                    }
                                });
                            for (req_idx, failed_req) in failed_request {
                                if failed_req.device.device == x && x != second_parity {
                                    all_recoverd.push((
                                        req_idx,
                                        Some((
                                            failed_req.offset.unwrap().0,
                                            failed_req.offset.unwrap().1,
                                            recovered.clone(),
                                        )),
                                    ));
                                }

                                if failed_req.device.device == y && y != second_parity {
                                    all_recoverd.push((
                                        req_idx,
                                        Some((
                                            failed_req.offset.unwrap().0,
                                            failed_req.offset.unwrap().1,
                                            recovered.clone(),
                                        )),
                                    ));
                                }
                            }
                        } else if recover_bufs[first_parity].is_none() {
                            let x = if x == first_parity { y } else { x };

                            let mut q = recover_bufs[second_parity].as_ref().unwrap().clone();
                            let mut qxy = vec![0u8; stripe];
                            for (n, data) in recover_bufs.into_iter().enumerate() {
                                if n == second_parity || n == first_parity {
                                    continue;
                                }
                                let gn = GN[n];
                                let data = if let Some(data) = data {
                                    data
                                } else {
                                    vec![0u8; stripe]
                                };
                                let data =
                                    data.into_iter().map(|t| gl_mul_two(t, gn)).collect_vec();
                                qxy = qxy
                                    .into_iter()
                                    .zip(data)
                                    .map(|(lhs, rhs)| lhs ^ rhs)
                                    .collect_vec();
                            }

                            let g_x = GN[255 - x];
                            let dx = q
                                .into_iter()
                                .zip(qxy.into_iter())
                                .map(|(l, r)| gl_mul_two(g_x, l ^ r))
                                .collect_vec();
                            for (req_idx, failed_req) in failed_request {
                                if failed_req.device.device == x {
                                    all_recoverd.push((
                                        req_idx,
                                        Some((
                                            failed_req.offset.unwrap().0,
                                            failed_req.offset.unwrap().1,
                                            dx,
                                        )),
                                    ));
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            for (idx, seg) in all_recoverd {
                bufs[idx] = seg;
            }
        }

        for seg_buf in bufs {
            let (lhs, rhs, seg_buf) = seg_buf.unwrap();
            buf[lhs - off..rhs - off].copy_from_slice(&seg_buf);
        }

        Ok(())
    }

    async fn compute_parities(
        &self,
        buf: &[u8],
        off: usize,
        stripe: usize,
        request: Vec<RAID6Reqeust>,
    ) -> std::io::Result<()> {
        let request = self.raid6_segments_extend(stripe, request);
        let mut js = FuturesUnordered::new();
        // Now compute parities
        if request.len() % (self.devices.len() - 2) != 0 {
            warn!("Inconsistency detected in RAID6, requests = {:?}", request);
            return Err(std::io::ErrorKind::InvalidInput.into());
        }

        for chunk in request
            .into_iter()
            .chunks(self.devices.len() - 2)
            .into_iter()
            .map(|t| t.collect_vec())
        {
            // sanity
            let target = chunk[0];
            for r in chunk.iter() {
                if r.device.group != target.device.group {
                    warn!(
                        "Inconsistency grouping detected in RAID6, requests = {:?}",
                        chunk
                    );
                    return Err(std::io::ErrorKind::InvalidInput.into());
                }
            }
            let devs = self.devices.clone();

            js.push(async move {
                let mut data = vec![];

                for group in chunk {
                    if let Some((lhs, rhs)) = group.offset {
                        if rhs - lhs == stripe {
                            data.push(Vec::from_iter(
                                buf[lhs - off..rhs - off].into_iter().copied(),
                            ));
                            continue;
                        }
                    }
                    // Issue a read request
                    let lhs_in_device = (group.lhs_in_device / stripe) * stripe;
                    let mut buf = vec![0u8; stripe];
                    let dev = &devs[group.device.device];
                    dev.read_at(&mut buf, group.lhs_in_device as u64).await?;
                    data.push(buf);
                }

                // first parity
                let xored = data.iter().fold(vec![0u8; stripe], |acc, data| {
                    acc.into_iter()
                        .zip(data)
                        .into_iter()
                        .map(|(lhs, rhs)| lhs ^ rhs)
                        .collect()
                });

                // second parity
                let mut gl = vec![0u8; stripe];
                for (n, data) in data.into_iter().enumerate() {
                    let gn = GN[n];
                    let data = data.into_iter().map(|t| gl_mul_two(t, gn)).collect_vec();
                    gl = gl
                        .into_iter()
                        .zip(data)
                        .map(|(lhs, rhs)| lhs ^ rhs)
                        .collect_vec();
                }

                // Write parities
                let lhs_in_parity = ((target.lhs_in_device / stripe) * stripe) as u64;
                debug!(
                    target.device.first_parity,
                    target.device.second_parity, lhs_in_parity, "Parity writting..."
                );
                let (r1, r2) = tokio::join!(
                    devs[target.device.first_parity].write_at(&xored, lhs_in_parity),
                    devs[target.device.second_parity].write_at(&gl, lhs_in_parity)
                );

                let _ = r1?;
                let _ = r2?;

                Ok::<_, std::io::Error>(())
            });
        }
        while let Some(r) = js.next().await {
            let _ = r?;
        }

        Ok(())
    }

    async fn raid6_write_at(&self, buf: &[u8], off: u64, stripe: usize) -> std::io::Result<()> {
        let off = off as usize;
        let request = self.raid6_segments(off, buf.len(), stripe)?;
        let mut js = FuturesUnordered::new();
        for req in request.iter() {
            if let Some((lhs, rhs)) = req.offset {
                let dev = self.devices[req.device.device].clone();
                let lhs_in_device = req.lhs_in_device;
                js.push(async move {
                    let buf = &buf[lhs - off..rhs - off];
                    debug!(
                        lhs_in_device,
                        size = buf.len(),
                        "RAID6 write request, device = {:?}",
                        req
                    );
                    dev.write_at(buf, lhs_in_device as u64).await?;
                    Ok::<_, std::io::Error>(())
                });
            }
        }

        while let Some(r) = js.next().await {
            let _ = r?;
        }
        drop(js);
        self.compute_parities(buf, off, stripe, request.into_iter().collect())
            .await?;

        Ok(())
    }
}

impl Blocks for RAID {
    fn flush(&self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            // Flush all
            let mut js = JoinSet::new();

            for dev in self.devices.clone().into_iter() {
                js.spawn(async move { dev.flush().await });
            }

            while let Some(t) = js.join_next().await {
                let _ = t?; // ignore errors from flushing
            }

            Ok(())
        })
    }

    fn read_at<'a>(
        &'a self,
        buf: &'a mut [u8],
        off: u64,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            match &self.config {
                RaidConfiguration::RAID0 { stripe } => self.raid0_read_at(buf, off, *stripe).await,
                RaidConfiguration::RAID6 { stripe } => self.raid6_read_at(buf, off, *stripe).await,
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
                RaidConfiguration::RAID0 { stripe } => self.raid0_write_at(buf, off, *stripe).await,
                RaidConfiguration::RAID6 { stripe } => self.raid6_write_at(buf, off, *stripe).await,
                _ => Err(std::io::Error::other("Not implemented yet")),
            }
        })
    }

    fn size(&self) -> Pin<Box<dyn Future<Output = std::io::Result<u64>> + Send + '_>> {
        Box::pin(std::future::ready(Ok(self.size as u64)))
    }
}

#[cfg(test)]
mod test {
    use super::RAID;

    #[test]
    fn test_raid6_devices() {
        let stripe = 1usize;
        let devices = 5usize;
        let out: Vec<usize> = (0..15)
            .into_iter()
            .map(|t| RAID::raid6_off_to_device(t, stripe, devices))
            .map(|t| t.device)
            .collect();
        let expected = vec![0, 1, 2, 0, 1, 4, 0, 3, 4, 2, 3, 4, 1, 2, 3];

        assert_eq!(out, expected);
    }
}
