/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

//! # Phoenix Network Coordinate System
//!
//! This module provides a framework for estimating network latencies between
//! nodes in a distributed system. By embedding nodes in a virtual coordinate
//! space, Phoenix allows for efficient estimation of network distance (latency)
//! without the need to directly measure the latency between every pair of nodes.
//!
//! ## Overview
//!
//! The Phoenix system works by assigning each node in the network a set of
//! coordinates that correspond to its position in a virtual space. The distance
//! between any two nodes in this space is indicative of the expected network
//! latency between them. This method reduces the overhead and scale issues
//! associated with all-to-all latency measurements.
//!
//! The system is designed to be both self-organizing and adaptive, meaning that
//! it can handle nodes joining, leaving, and changing latencies over time.
//! Phoenix periodically updates the coordinates of each node based on a subset
//! of latency measurements to reflect the current state of the network.

use std::{collections::HashMap, net::SocketAddr, ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;
use dashmap::DashMap;

use crate::{
    config::{self, IcaoCode},
    time::DurationNanos,
};

/// The number of consecutive ping failures after which we will inform that this is a bad node
const BAD_NODE_THRESHOLD: u64 = 10;

pub fn spawn(
    listener: crate::net::TcpListener,
    datacenters: config::Watch<config::DatacenterMap>,
    phoenix: Phoenix<crate::codec::qcmp::QcmpTransceiver>,
    mut shutdown_rx: crate::signal::ShutdownRx,
) -> crate::Result<crate::service::Finalizer> {
    use eyre::WrapErr as _;

    phoenix.add_nodes_from_config(&datacenters);

    let mut dc_watcher = datacenters.watch();

    let ph_thread = std::thread::Builder::new()
        .name("phoenix-http".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_name_fn(|| {
                    static ATOMIC_ID: std::sync::atomic::AtomicUsize =
                        std::sync::atomic::AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    format!("phoenix-http-{id}")
                })
                .build()
                .unwrap();
            let res = runtime.block_on({
                let mut phoenix_watcher = phoenix.update_watcher();
                let datacenters = datacenters.clone();

                async move {
                    let json = Arc::new(arc_swap::ArcSwap::new(Arc::new(serde_json::Map::default())));

                    tokio::spawn({
                        let phoenix = phoenix.clone();
                        async move { phoenix.background_update_task().await }
                    });

                    tracing::info!(addr=%listener.local_addr(), "starting phoenix HTTP service");
                    let (stx, mut srx) = tokio::sync::oneshot::channel::<()>();
                    let handler_json = json.clone();

                    let http_task: tokio::task::JoinHandle<eyre::Result<()>> = {
                        let phoenix = phoenix.clone();
                        tokio::spawn(async move {
                            let router = http_router(phoenix, handler_json);
                            tokio::select! {
                                result = async move { axum::serve(listener.into_tokio()?, router).await } => result.map_err(From::from),
                                _ = &mut srx => {
                                    crate::metrics::phoenix_task_closed().set(true as _);
                                    Ok(())
                                }
                            }
                        })
                    };

                    let res = loop {
                        use eyre::WrapErr as _;

                        tokio::select! {
                            _ = shutdown_rx.changed() => break Ok::<_, eyre::Error>(()),
                            result = dc_watcher.changed() => if let Err(err) = result {
                                break Err(err).context("config watcher sender dropped");
                            },
                            result = phoenix_watcher.changed() => if let Err(err) = result {
                                break Err(err).context("phoenix watcher sender dropped");
                            },
                        }

                        tracing::trace!("change detected, updating phoenix");
                        phoenix.add_nodes_from_config(&datacenters);
                        let nodes = phoenix.ordered_nodes_by_latency();
                        let mut new_json = serde_json::Map::default();

                        for (identifier, latency) in nodes {
                            new_json.insert(identifier.to_string(), latency.into());
                        }

                        json.store(new_json.into());
                    };

                    if stx.send(()).is_err() {
                        tracing::error!("phoenix HTTP service task has already exited");
                    }

                    // This should happen quickly, abort if it takes too long
                    let max_wait =
                        std::time::Instant::now() + std::time::Duration::from_millis(100);
                    let mut interval = tokio::time::interval(std::time::Duration::from_millis(1));

                    loop {
                        if http_task.is_finished() || std::time::Instant::now() > max_wait {
                            break;
                        }

                        interval.tick().await;
                    }

                    if !http_task.is_finished() {
                        http_task.abort();
                    }

                    if let Err(err) = http_task.await {
                        if let Ok(panic) = err.try_into_panic() {
                            let message = panic
                                .downcast_ref::<String>()
                                .map(String::as_str)
                                .or_else(|| panic.downcast_ref::<&str>().copied())
                                .unwrap_or("<unknown non-string panic>");

                            tracing::error!(panic = message, "phoenix HTTP task panicked");
                        }
                    }

                    res
                }
            });

            if let Err(err) = res {
                tracing::error!(err = %err, "phoenix thread failed with an error");
            }
        })
        .context("failed to spawn phoenix-http thread")?;

    let finalizer = Box::new(move || {
        let start = std::time::Instant::now();
        if ph_thread.join().is_err() {
            tracing::error!("error joining phoenix thread");
        }
        tracing::debug!(elapsed = ?start.elapsed(), "phoenix thread shutdown");
    });

    Ok(finalizer)
}

fn http_router(
    phoenix: Phoenix<crate::codec::qcmp::QcmpTransceiver>,
    hj: Arc<arc_swap::ArcSwap<serde_json::Map<String, serde_json::Value>>>,
) -> axum::Router {
    axum::Router::new()
        .route(
            "/",
            axum::routing::get(|| async move {
                crate::metrics::phoenix_requests().inc();
                tracing::trace!("serving phoenix request");
                axum::response::Json(hj)
            }),
        )
        .route(
            "/network-coordinates",
            axum::routing::get(|| async move {
                crate::metrics::phoenix_requests().inc();
                tracing::trace!("serving phoenix request");
                axum::response::Json(phoenix.coordinate_map())
            }),
        )
}

#[derive(Copy, Clone)]
#[cfg_attr(test, derive(Debug))]
pub struct DistanceMeasure {
    /// Latency for a packet travelling to a given target (incoming for the target)
    pub incoming: DurationNanos,
    /// Latency for a packet arriving from a given target (outgoing for the target)
    pub outgoing: DurationNanos,
}

impl Default for DistanceMeasure {
    fn default() -> Self {
        Self::from((0, 0))
    }
}

impl From<(i64, i64)> for DistanceMeasure {
    fn from(value: (i64, i64)) -> Self {
        Self {
            incoming: DurationNanos::from_nanos(value.0),
            outgoing: DurationNanos::from_nanos(value.1),
        }
    }
}

impl DistanceMeasure {
    #[inline]
    pub fn total_nanos(self) -> i64 {
        self.incoming.nanos() + self.outgoing.nanos()
    }

    #[inline]
    pub fn total(self) -> std::time::Duration {
        self.incoming.duration() + self.outgoing.duration()
    }
}

/// An implementation of measuring the network difference between two nodes.
#[async_trait]
pub trait Measurement {
    /// Gets the difference between this node and `address`, returning the
    /// latency in nanoseconds on success.
    async fn measure_distance(&self, address: SocketAddr) -> eyre::Result<DistanceMeasure>;
}

/// A `Phoenix` instance maintains a virtual coordinate space for nodes in a
/// distributed system to estimate their network latencies. It uses the provided
/// `Measurement` trait to periodically measure and update each node's
/// coordinates, allowing for latency estimation between any two nodes.
#[derive(Debug)]
pub struct Phoenix<M> {
    inner: Arc<Inner<M>>,
}

impl<M> Clone for Phoenix<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug)]
pub struct Inner<M> {
    nodes: DashMap<SocketAddr, Node>,
    measurement: M,
    stability_threshold: Duration,
    adjustment_duration: Duration,
    interval_range: Range<Duration>,
    subset_percentage: f64,
    update_watcher: (
        tokio::sync::watch::Sender<()>,
        tokio::sync::watch::Receiver<()>,
    ),
    bad_node_informer: Option<crate::config::BadNodeInformer>,
}

impl<M> Phoenix<M> {
    fn update_watcher(&self) -> tokio::sync::watch::Receiver<()> {
        self.update_watcher.1.clone()
    }

    #[allow(dead_code)]
    fn all_nodes(&self) -> Vec<SocketAddr> {
        self.nodes
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>()
    }

    /// Returns a set of node addresses to probe.
    ///
    /// - Always returns at least 1 node unless the list of nodes is empty
    /// - Always returns all of the nodes that have not been mapped yet
    /// - Returns a randomly selected subset of nodes that have been mapped
    fn select_nodes_to_probe(&self) -> Vec<SocketAddr> {
        use rand::seq::SliceRandom;

        let (unmapped, mut mapped): (Vec<_>, Vec<_>) = self
            .nodes
            .iter()
            .partition(|entry| entry.coordinates.is_none());

        mapped.shuffle(&mut rand::rng());

        // Select a subset of the already mapped nodes, but always at least one node
        let subset_size = (mapped.len() as f64 * self.subset_percentage)
            .abs()
            .max(1.0) as usize;

        mapped
            .iter()
            .map(|entry| *entry.key())
            .take(subset_size)
            .chain(unmapped.iter().map(|entry| *entry.key())) // Always include all unmapped nodes
            .collect()
    }

    pub fn get_coordinates(&self, address: &SocketAddr) -> Option<Coordinates> {
        self.nodes.get(address).and_then(|node| node.coordinates)
    }

    pub fn ordered_nodes_by_latency(&self) -> Vec<(IcaoCode, f64)> {
        use std::collections::hash_map::Entry;

        let origin = Coordinates::ORIGIN;
        let mut icao_map = HashMap::new();

        for entry in self.nodes.iter() {
            let Some(coordinates) = entry.value().coordinates else {
                continue;
            };
            let distance = origin.distance_to(&coordinates);
            let icao = entry.value().icao_code;

            match icao_map.entry(icao) {
                Entry::Vacant(entry) => {
                    entry.insert(distance);
                }
                Entry::Occupied(entry) => {
                    let old_distance = entry.into_mut();
                    if *old_distance > distance {
                        *old_distance = distance;
                    }
                }
            }
        }

        let mut vec = icao_map.into_iter().collect::<Vec<_>>();
        vec.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        vec
    }

    pub fn coordinate_map(&self) -> HashMap<IcaoCode, Coordinates> {
        let mut icao_map = HashMap::new();

        for entry in self.nodes.iter() {
            let Some(coordinates) = entry.value().coordinates else {
                continue;
            };
            let icao = entry.value().icao_code;
            icao_map.insert(icao, coordinates);
        }

        icao_map
    }

    pub fn add_node(&self, address: SocketAddr, icao_code: IcaoCode) {
        self.nodes.insert(address, Node::new(icao_code));
    }

    pub fn add_node_if_not_exists(&self, address: SocketAddr, icao_code: IcaoCode) {
        self.nodes
            .entry(address)
            .or_insert_with(|| Node::new(icao_code));
    }

    pub fn add_nodes_from_config(&self, datacenters: &config::Watch<config::DatacenterMap>) {
        let dcs = datacenters.write();

        for removed in dcs.removed() {
            self.nodes.remove(&removed);
        }

        for entry in dcs.iter() {
            let addr = (*entry.key(), entry.value().qcmp_port).into();
            self.add_node_if_not_exists(addr, entry.value().icao_code);
        }
    }
}

impl<M: Measurement + 'static> Phoenix<M> {
    pub fn new(measurement: M) -> Self {
        Builder::new(measurement).build()
    }

    pub fn builder(measurement: M) -> Builder<M> {
        Builder::new(measurement)
    }

    async fn update(&self, mut current_interval: std::time::Duration) -> std::time::Duration {
        let nodes = self.select_nodes_to_probe();

        let (count, total_difference) = self.measure_nodes(nodes).await;

        if count > 0 {
            let avg_difference_ns = total_difference / count;

            // Adjust the interval based on the avg_difference
            if Duration::from_nanos(avg_difference_ns as u64) < self.stability_threshold {
                current_interval += self.adjustment_duration;
            } else {
                current_interval -= self.adjustment_duration;
            }

            // Ensure current_interval remains within bounds
            current_interval =
                current_interval.clamp(self.interval_range.start, self.interval_range.end);
        }

        let _ = self.update_watcher.0.send(());
        current_interval
    }

    /// Starts the background update task to continously sample from nodes
    /// and update their coordinates.
    pub async fn background_update_task(&self) {
        let mut current_interval = self.interval_range.start;

        loop {
            current_interval = self.update(current_interval).await;

            tokio::time::sleep(current_interval).await;
        }
    }

    async fn measure_nodes(&self, nodes: Vec<SocketAddr>) -> (i64, i64) {
        let mut total_difference = 0;
        let mut count = 0;
        for address in nodes {
            let Some(mut node) = self.nodes.get_mut(&address) else {
                tracing::debug!(%address, "node removed between selection and measurement");
                continue;
            };

            match self.measurement.measure_distance(address).await {
                Ok(distance) => {
                    node.adjust_coordinates(distance);
                    total_difference += distance.total_nanos();
                    count += 1;
                }
                Err(error) => {
                    node.increase_error_estimate();
                    let consecutive_errors = node.consecutive_errors();
                    if consecutive_errors > 3 {
                        tracing::warn!(%address, %error, %consecutive_errors, "error measuring distance");
                        if consecutive_errors > BAD_NODE_THRESHOLD {
                            if let Some(bad_node_informer) = self.bad_node_informer.as_ref() {
                                if let Err(error) = bad_node_informer.send(address) {
                                    tracing::warn!(%address, %error, %consecutive_errors, "failed to inform about bad node");
                                }
                            }
                        }
                    } else {
                        tracing::debug!(%address, %error, "error measuring distance");
                    }
                }
            }
        }
        (count, total_difference)
    }

    #[cfg(test)]
    async fn measure_all_nodes(&self) {
        let nodes = self
            .nodes
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        let _ = self.measure_nodes(nodes).await;
    }
}

impl<M> std::ops::Deref for Phoenix<M> {
    type Target = Inner<M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct Builder<M> {
    measurement: M,
    stability_threshold: Option<Duration>,
    adjustment_duration: Option<Duration>,
    interval_range: Option<Range<Duration>>,
    subset_percentage: Option<f64>,
    bad_node_informer: Option<crate::config::BadNodeInformer>,
}

impl<M: Measurement> Builder<M> {
    const DEFAULT_STABILITY_THRESHOLD: Duration = Duration::from_millis(50);
    const DEFAULT_ADJUSTMENT_DURATION: Duration = Duration::from_millis(5);
    const DEFAULT_INTERVAL_RANGE: Range<Duration> =
        Duration::from_secs(60)..Duration::from_secs(10 * 60);
    const DEFAULT_SUBSET: f64 = 0.5;

    /// Constructs a new [`Phoenix`] builder.
    pub fn new(measurement: M) -> Self {
        Builder {
            measurement,
            stability_threshold: None,
            adjustment_duration: None,
            interval_range: None,
            subset_percentage: None,
            bad_node_informer: None,
        }
    }

    /// The amount of time the check will change by depending on network stability.
    pub fn adjustment_duration(mut self, adjustment: Duration) -> Self {
        self.adjustment_duration = Some(adjustment);
        self
    }

    /// The threshold at which the path to a node is consider unstable.
    pub fn stability_threshold(mut self, threshold: Duration) -> Self {
        self.stability_threshold = Some(threshold);
        self
    }

    /// The range at which continually update the nodes measurements. This
    /// a range as the time will increase/decrease in response to
    /// network stability.
    ///
    /// # Panics
    /// If the start of the range is greater than end of the range.
    pub fn interval_range(mut self, range: Range<Duration>) -> Self {
        assert!(range.start < range.end);
        self.interval_range = Some(range);
        self
    }

    /// Sets the percentage of nodes to regularly measure at random.
    ///
    /// # Panics
    /// If the percentage is greater than 1.0 or lower or equal to 0.0.
    pub fn subset_percentage(mut self, percentage: f64) -> Self {
        assert!(percentage > 0.0 && percentage <= 1.0);
        self.subset_percentage = Some(percentage);
        self
    }

    /// Inform about bad nodes that don't respond to pings.
    pub fn inform_bad_nodes(mut self, bad_node_informer: crate::config::BadNodeInformer) -> Self {
        self.bad_node_informer = Some(bad_node_informer);
        self
    }

    pub fn build(self) -> Phoenix<M> {
        Phoenix {
            inner: Arc::new(Inner {
                nodes: DashMap::new(),
                measurement: self.measurement,
                stability_threshold: self
                    .stability_threshold
                    .unwrap_or(Self::DEFAULT_STABILITY_THRESHOLD),
                adjustment_duration: self
                    .adjustment_duration
                    .unwrap_or(Self::DEFAULT_ADJUSTMENT_DURATION),
                interval_range: self.interval_range.unwrap_or(Self::DEFAULT_INTERVAL_RANGE),
                subset_percentage: self.subset_percentage.unwrap_or(Self::DEFAULT_SUBSET),
                update_watcher: tokio::sync::watch::channel(()),
                bad_node_informer: self.bad_node_informer,
            }),
        }
    }
}

/// The network coordinates of a node in the phoenix system.
#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize)]
pub struct Coordinates {
    incoming: f64,
    outgoing: f64,
}

impl Coordinates {
    const ORIGIN: Self = Self {
        incoming: 0.0,
        outgoing: 0.0,
    };

    fn distance_to(&self, other: &Coordinates) -> f64 {
        let x_diff = self.incoming - other.incoming;
        let y_diff = self.outgoing - other.outgoing;
        #[allow(clippy::imprecise_flops)]
        (x_diff.powi(2) + y_diff.powi(2)).sqrt()
    }
}

/// A node in Phoenix system, contains its location, and an estimate of how
/// imprecise the location may be due to errors.
#[derive(Debug, Clone)]
struct Node {
    coordinates: Option<Coordinates>,
    icao_code: IcaoCode,
    error_estimate: f64,
    consecutive_errors: u64,
    alpha: f64,
}

impl Node {
    fn new(icao_code: IcaoCode) -> Self {
        crate::metrics::phoenix_distance_error_estimate(icao_code).set(1.0);
        crate::metrics::phoenix_coordinates_alpha(icao_code).set(1.0);
        Node {
            coordinates: None,
            icao_code,
            error_estimate: 1.0,
            consecutive_errors: 0,
            alpha: 1.0,
        }
    }

    fn consecutive_errors(&self) -> u64 {
        self.consecutive_errors
    }

    fn increase_error_estimate(&mut self) {
        self.error_estimate += 0.1;
        self.consecutive_errors += 1;
        crate::metrics::phoenix_measurement_errors(self.icao_code).inc();
        crate::metrics::phoenix_distance_error_estimate(self.icao_code).set(self.error_estimate);
        self.alpha = (self.alpha - 0.1).clamp(0.2, 1.0);
        crate::metrics::phoenix_coordinates_alpha(self.icao_code).set(self.alpha);
    }

    fn adjust_coordinates(&mut self, distance: DistanceMeasure) {
        self.consecutive_errors = 0;
        let incoming = distance.incoming.nanos() as f64;
        let outgoing = distance.outgoing.nanos() as f64;

        crate::metrics::phoenix_measurement_seconds(self.icao_code, "incoming")
            .observe(distance.incoming.duration().as_secs_f64());
        crate::metrics::phoenix_measurement_seconds(self.icao_code, "outgoing")
            .observe(distance.outgoing.duration().as_secs_f64());

        let Some(coordinates) = &mut self.coordinates else {
            let coordinates = Coordinates { incoming, outgoing };
            crate::metrics::phoenix_coordinates(self.icao_code, "x").set(coordinates.incoming);
            crate::metrics::phoenix_coordinates(self.icao_code, "y").set(coordinates.outgoing);
            crate::metrics::phoenix_distance(self.icao_code)
                .set(Coordinates::ORIGIN.distance_to(&coordinates));
            self.coordinates = Some(coordinates);
            return;
        };

        // Exponentially weighted moving average
        coordinates.incoming = self.alpha * incoming + (1.0 - self.alpha) * coordinates.incoming;
        coordinates.outgoing = self.alpha * outgoing + (1.0 - self.alpha) * coordinates.outgoing;
        self.alpha = (self.alpha + 0.05).clamp(0.2, 1.0);
        crate::metrics::phoenix_coordinates_alpha(self.icao_code).set(self.alpha);

        crate::metrics::phoenix_coordinates(self.icao_code, "x").set(coordinates.incoming);
        crate::metrics::phoenix_coordinates(self.icao_code, "y").set(coordinates.outgoing);
        crate::metrics::phoenix_distance(self.icao_code)
            .set(Coordinates::ORIGIN.distance_to(coordinates));
    }
}

#[cfg(test)]
mod tests {
    use crate::net::raw_socket_with_reuse;

    use super::*;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    #[allow(dead_code)]
    struct LoggingMockMeasurement {
        latencies: HashMap<SocketAddr, DistanceMeasure>,
        probed_addresses: Arc<Mutex<HashSet<SocketAddr>>>,
    }

    #[async_trait]
    impl Measurement for LoggingMockMeasurement {
        async fn measure_distance(&self, address: SocketAddr) -> eyre::Result<DistanceMeasure> {
            self.probed_addresses.lock().await.insert(address);
            Ok(*self
                .latencies
                .get(&address)
                .unwrap_or(&DistanceMeasure::default()))
        }
    }

    struct MockMeasurement {
        latencies: HashMap<SocketAddr, DistanceMeasure>,
    }

    #[async_trait]
    impl Measurement for MockMeasurement {
        async fn measure_distance(&self, address: SocketAddr) -> eyre::Result<DistanceMeasure> {
            Ok(*self
                .latencies
                .get(&address)
                .unwrap_or(&DistanceMeasure::default()))
        }
    }

    #[derive(Debug)]
    struct FailedAddressesMock {
        latencies: HashMap<SocketAddr, DistanceMeasure>,
        failed_addresses: Arc<Mutex<HashSet<SocketAddr>>>,
    }

    #[async_trait]
    impl Measurement for FailedAddressesMock {
        async fn measure_distance(&self, address: SocketAddr) -> eyre::Result<DistanceMeasure> {
            let failed_addresses = self.failed_addresses.lock().await;
            if failed_addresses.contains(&address) {
                Err(eyre::eyre!("Measurement timed out"))
            } else {
                Ok(*self
                    .latencies
                    .get(&address)
                    .unwrap_or(&DistanceMeasure::default()))
            }
        }
    }

    fn abcd() -> IcaoCode {
        "ABCD".parse().unwrap()
    }

    fn efgh() -> IcaoCode {
        "EFGH".parse().unwrap()
    }

    fn ijkl() -> IcaoCode {
        "IJKL".parse().unwrap()
    }

    #[test]
    fn default_builder() {
        let _phoenix = Phoenix::new(MockMeasurement {
            latencies: <_>::default(),
        });
    }

    #[test]
    fn zero_nodes_return_empty_subset() {
        let phoenix = Phoenix::new(MockMeasurement {
            latencies: <_>::default(),
        });

        assert_eq!(phoenix.select_nodes_to_probe(), vec![]);
    }

    #[tokio::test]
    async fn one_node_returns_single_node_subset() {
        let phoenix = Phoenix::new(MockMeasurement {
            latencies: <_>::default(),
        });

        let socket_addr = "127.0.0.1:8080".parse().unwrap();
        phoenix.add_node(socket_addr, abcd());

        // First time it will be returned as part of "unmapped_nodes"
        assert_eq!(phoenix.select_nodes_to_probe(), vec![socket_addr]);
        phoenix.measure_all_nodes().await;
        // After it has been measured it should still be returned so we don't get stuck without
        // ever making additional measurements
        assert_eq!(phoenix.select_nodes_to_probe(), vec![socket_addr]);
    }

    #[tokio::test]
    async fn select_nodes_to_probe() {
        let latencies = HashMap::from([
            ("127.0.0.1:8080".parse().unwrap(), (100, 100).into()),
            ("127.0.0.1:8081".parse().unwrap(), (200, 200).into()),
            ("127.0.0.1:8082".parse().unwrap(), (200, 200).into()),
            ("127.0.0.1:8083".parse().unwrap(), (200, 200).into()),
            ("127.0.0.1:8084".parse().unwrap(), (200, 200).into()),
        ]);
        let failed_address = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        let failed_addresses = Arc::new(Mutex::new(HashSet::from([failed_address])));
        let phoenix = Phoenix::builder(FailedAddressesMock {
            latencies,
            failed_addresses,
        })
        .subset_percentage(0.25)
        .build();

        phoenix.add_node("127.0.0.1:8080".parse().unwrap(), abcd());
        phoenix.add_node("127.0.0.1:8081".parse().unwrap(), efgh());
        phoenix.add_node("127.0.0.1:8082".parse().unwrap(), efgh());
        phoenix.add_node("127.0.0.1:8083".parse().unwrap(), efgh());
        phoenix.add_node("127.0.0.1:8084".parse().unwrap(), efgh());

        let mut nodes_to_probe = phoenix.select_nodes_to_probe();
        nodes_to_probe.sort();
        let expected_nodes_to_probe = vec![
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
            "127.0.0.1:8082".parse().unwrap(),
            "127.0.0.1:8083".parse().unwrap(),
            "127.0.0.1:8084".parse().unwrap(),
        ];
        assert_eq!(nodes_to_probe, expected_nodes_to_probe);

        phoenix.measure_all_nodes().await;

        // Ensure that we always get the node that has not been mapped yet, as well as 1 out of the
        // 4 mapped nodes due to the 25% subset percentage
        for _ in 0..10 {
            let nodes_to_probe = phoenix.select_nodes_to_probe();
            assert_eq!(nodes_to_probe.len(), 2);
            assert!(nodes_to_probe.contains(&failed_address));
        }
    }

    #[tokio::test]
    async fn coordinates_adjustment() {
        let mut mock_latencies = HashMap::new();
        mock_latencies.insert("127.0.0.1:8081".parse().unwrap(), (25, 25).into());
        let phoenix = Phoenix::new(MockMeasurement {
            latencies: mock_latencies,
        });

        phoenix.add_node("127.0.0.1:8080".parse().unwrap(), abcd());
        phoenix.add_node("127.0.0.1:8081".parse().unwrap(), efgh());
        phoenix.measure_all_nodes().await;

        let coords = phoenix
            .get_coordinates(&"127.0.0.1:8081".parse().unwrap())
            .unwrap();
        assert!(
            coords.incoming != 0.0 || coords.outgoing != 0.0,
            "Coordinates were not adjusted."
        );
    }

    #[tokio::test]
    async fn ordered_nodes_by_latency() {
        let mut mock_latencies = HashMap::new();
        mock_latencies.insert("127.0.0.1:8080".parse().unwrap(), (10, 10).into());
        mock_latencies.insert("127.0.0.1:8081".parse().unwrap(), (50, 50).into());
        mock_latencies.insert("127.0.0.1:8082".parse().unwrap(), (30, 30).into());

        let phoenix = Phoenix::new(MockMeasurement {
            latencies: mock_latencies,
        });

        phoenix.add_node("127.0.0.1:8080".parse().unwrap(), abcd());
        phoenix.add_node("127.0.0.1:8081".parse().unwrap(), efgh());
        phoenix.add_node("127.0.0.1:8082".parse().unwrap(), ijkl());

        phoenix.measure_all_nodes().await;

        let ordered_nodes = phoenix.ordered_nodes_by_latency();

        assert_eq!(ordered_nodes[0].0, abcd());
        assert_eq!(ordered_nodes[1].0, ijkl());
        assert_eq!(ordered_nodes[2].0, efgh());
    }

    #[test]
    fn invalid_interval_range() {
        let measurement = MockMeasurement {
            latencies: HashMap::new(),
        };

        let result = std::panic::catch_unwind(|| {
            Builder::new(measurement)
                .interval_range(Duration::from_secs(10)..Duration::from_secs(5))
                .build()
        });

        assert!(
            result.is_err(),
            "Builder should panic when given an invalid interval range."
        );
    }

    #[test]
    fn node_not_added() {
        let mock_latencies = HashMap::new();
        let phoenix = Phoenix::new(MockMeasurement {
            latencies: mock_latencies,
        });
        let result = phoenix.get_coordinates(&"127.0.0.1:8080".parse().unwrap());

        assert!(
            result.is_none(),
            "Should not get coordinates for a node that was not added."
        );
    }

    #[test]
    fn invalid_subset_percentage() {
        let measurement = MockMeasurement {
            latencies: HashMap::new(),
        };

        let result =
            std::panic::catch_unwind(|| Builder::new(measurement).subset_percentage(1.5).build());

        assert!(
            result.is_err(),
            "Builder should panic when given an invalid subset percentage."
        );
    }

    #[tokio::test]
    async fn successful_measurements() {
        let latencies = HashMap::from([
            ("127.0.0.1:8080".parse().unwrap(), (100, 100).into()),
            ("127.0.0.1:8081".parse().unwrap(), (200, 200).into()),
        ]);
        let failed_addresses = Arc::new(Mutex::new(HashSet::new()));
        let measurement = FailedAddressesMock {
            latencies,
            failed_addresses,
        };

        let phoenix = Phoenix::new(measurement);

        phoenix.add_node("127.0.0.1:8080".parse().unwrap(), abcd());
        phoenix.add_node("127.0.0.1:8081".parse().unwrap(), efgh());

        phoenix.measure_all_nodes().await;

        let ordered_nodes = phoenix.ordered_nodes_by_latency();
        assert_eq!(ordered_nodes.len(), 2);
        assert_eq!(ordered_nodes[0].0, abcd());
        assert!(ordered_nodes[0].1 >= 100.);
        assert_eq!(ordered_nodes[1].0, efgh());
        assert!(ordered_nodes[1].1 >= 200.);
    }

    #[tokio::test]
    async fn failed_measurements_excluded() {
        let latencies = HashMap::from([
            ("127.0.0.1:8080".parse().unwrap(), (100, 100).into()),
            ("127.0.0.1:8081".parse().unwrap(), (200, 200).into()),
        ]);
        let failed_addresses = Arc::new(Mutex::new(HashSet::from(["127.0.0.1:8081"
            .parse()
            .unwrap()])));
        let measurement = FailedAddressesMock {
            latencies,
            failed_addresses,
        };

        let phoenix = Phoenix::new(measurement);

        phoenix.add_node("127.0.0.1:8080".parse().unwrap(), abcd());
        phoenix.add_node("127.0.0.1:8081".parse().unwrap(), efgh());

        phoenix.measure_all_nodes().await;

        let ordered_nodes = phoenix.ordered_nodes_by_latency();
        assert_eq!(ordered_nodes.len(), 1);
        assert_eq!(ordered_nodes[0].0, abcd());
        assert!(ordered_nodes[0].1 >= 100.);
    }

    #[tokio::test]
    async fn bad_nodes_reported() {
        let ok_node = "127.0.0.1:8080".parse().unwrap();
        let bad_node = "127.0.0.1:8081".parse().unwrap();
        let latencies =
            HashMap::from([(ok_node, (100, 100).into()), (bad_node, (200, 200).into())]);
        let failed_addresses = Arc::new(Mutex::new(HashSet::from([bad_node])));
        let measurement = FailedAddressesMock {
            latencies,
            failed_addresses,
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let phoenix = Phoenix::builder(measurement).inform_bad_nodes(tx).build();

        phoenix.add_node(ok_node, abcd());
        phoenix.add_node(bad_node, efgh());

        for _ in 0..(BAD_NODE_THRESHOLD) {
            phoenix.measure_all_nodes().await;
        }

        // Check that we haven't informed yet
        assert!(rx.try_recv().is_err());

        // But one more measurement brings it over the threshold
        phoenix.measure_all_nodes().await;

        let result = rx.try_recv();
        assert!(result.is_ok());
        let node = result.unwrap();
        assert_eq!(node, bad_node);
    }

    #[tokio::test]
    #[cfg_attr(target_os = "macos", ignore)]
    async fn http_server() {
        let qcmp_listener = crate::net::TcpListener::bind(None).expect("failed to bind listener");
        let qcmp_port = qcmp_listener.port();

        let icao_code = "ABCD".parse().unwrap();

        let datacenters =
            crate::config::Watch::<crate::config::DatacenterMap>::new(Default::default());

        datacenters.write().insert(
            std::net::Ipv4Addr::LOCALHOST.into(),
            crate::config::Datacenter {
                qcmp_port,
                icao_code,
            },
        );

        let (tx, rx) = crate::signal::channel();
        let socket = raw_socket_with_reuse(qcmp_port).unwrap();
        let pc = crate::codec::qcmp::port_channel();
        crate::codec::qcmp::spawn_task(socket, pc.subscribe(), rx.clone()).unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        let measurement =
            crate::codec::qcmp::QcmpTransceiver::with_artificial_delay(Duration::from_millis(50))
                .unwrap();

        let phoenix = Phoenix::builder(measurement)
            .interval_range(Duration::from_millis(10)..Duration::from_millis(15))
            .build();

        let end = super::spawn(qcmp_listener, datacenters, phoenix, rx).unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build_http::<http_body_util::Empty<bytes::Bytes>>();
        use http_body_util::BodyExt;
        for _ in 0..10 {
            let resp = tokio::time::timeout(
                Duration::from_millis(100),
                client
                    .get(format!("http://localhost:{qcmp_port}/").parse().unwrap())
                    .await
                    .unwrap()
                    .into_body()
                    .collect(),
            )
            .await
            .unwrap()
            .unwrap()
            .to_bytes();

            let map = serde_json::from_slice::<serde_json::Map<_, _>>(&resp).unwrap();

            let coords = Coordinates {
                incoming: std::time::Duration::from_millis(50).as_nanos() as f64 / 2.0,
                outgoing: std::time::Duration::from_millis(1).as_nanos() as f64 / 2.0,
            };

            let min = Coordinates::ORIGIN.distance_to(&coords);
            let max = min * 3.0;
            let distance = map[icao_code.as_ref()].as_f64().unwrap();

            assert!(
                distance > min && distance < max,
                "expected distance {distance} to be > {min} and < {max}",
            );
        }

        let _ = tx.send(());
        end();
    }

    #[tokio::test]
    #[cfg_attr(target_os = "macos", ignore)]
    async fn get_network_coordinates() {
        let qcmp_listener = crate::net::TcpListener::bind(None).expect("failed to bind listener");
        let qcmp_port = qcmp_listener.port();

        let icao_code = "ABCD".parse().unwrap();

        let datacenters =
            crate::config::Watch::<crate::config::DatacenterMap>::new(Default::default());

        datacenters.write().insert(
            std::net::Ipv4Addr::LOCALHOST.into(),
            crate::config::Datacenter {
                qcmp_port,
                icao_code,
            },
        );

        let (tx, rx) = crate::signal::channel();
        let socket = raw_socket_with_reuse(qcmp_port).unwrap();
        let pc = crate::codec::qcmp::port_channel();
        crate::codec::qcmp::spawn_task(socket, pc.subscribe(), rx.clone()).unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        let measurement =
            crate::codec::qcmp::QcmpTransceiver::with_artificial_delay(Duration::from_millis(50))
                .unwrap();

        let phoenix = Phoenix::builder(measurement)
            .interval_range(Duration::from_millis(10)..Duration::from_millis(15))
            .build();

        let end = super::spawn(qcmp_listener, datacenters, phoenix, rx).unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build_http::<http_body_util::Empty<bytes::Bytes>>();
        use http_body_util::BodyExt;
        for _ in 0..10 {
            let resp = tokio::time::timeout(
                Duration::from_millis(100),
                client
                    .get(
                        format!("http://localhost:{qcmp_port}/network-coordinates")
                            .parse()
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .into_body()
                    .collect(),
            )
            .await
            .unwrap()
            .unwrap()
            .to_bytes();

            let map = serde_json::from_slice::<HashMap<IcaoCode, Coordinates>>(&resp).unwrap();
            assert!(map.contains_key(&icao_code));
        }

        let _ = tx.send(());
        end();
    }
}
