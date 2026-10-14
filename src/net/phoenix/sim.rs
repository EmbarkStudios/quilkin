//! Replays a recorded latency matrix through the coordinate solver.

use std::{collections::HashMap, net::SocketAddr};

use async_trait::async_trait;
use rand::{SeedableRng, seq::SliceRandom};

use super::{DistanceMeasure, Measurement, Phoenix, accuracy};
use crate::config::IcaoCode;

const TEST_MATRIX: &str = include_str!("../../../tests/data/phoenix/median.csv");

/// A fixed latency matrix, indexed by position.
pub struct Matrix {
    rows: Vec<String>,
    columns: Vec<String>,
    /// Round trip in nanoseconds, `None` where the pair was never measured.
    cells: Vec<Vec<Option<f64>>>,
}

impl Matrix {
    /// Header of column labels, then one row per measuring site.
    ///
    /// An empty cell is an unmeasured pair, stored as `None`. Zero would be a
    /// zero-latency path: the solve would fit it and the scoring would count it
    /// correct.
    ///
    /// # Panics
    ///
    /// Panics if a row has a different width to the header, or a cell is
    /// present but unparseable.
    pub fn from_csv(text: &str) -> Self {
        let mut lines = text.lines().filter(|line| !line.trim().is_empty());
        let header = lines.next().expect("matrix has no header row");
        let columns: Vec<String> = header.split(',').skip(1).map(str::to_owned).collect();

        let mut rows = Vec::new();
        let mut cells = Vec::new();

        for line in lines {
            let mut fields = line.split(',');
            let label = fields.next().expect("row has no label").to_owned();
            let values: Vec<Option<f64>> = fields
                .map(|field| {
                    let field = field.trim();
                    (!field.is_empty()).then(|| field.parse().expect("cell is not a number"))
                })
                .collect();

            assert_eq!(
                values.len(),
                columns.len(),
                "row {label} has {} cells, header has {}",
                values.len(),
                columns.len(),
            );

            rows.push(label);
            cells.push(values);
        }

        Self {
            rows,
            columns,
            cells,
        }
    }

    pub fn test() -> Self {
        Self::from_csv(TEST_MATRIX)
    }

    fn get(&self, row: usize, column: usize) -> Option<f64> {
        self.cells[row][column]
    }

    /// Sites appearing as both a row and a column, reduced to the largest set
    /// whose every ordered pair is populated, as `(row, column)` indices.
    ///
    /// Triangle inequality analysis needs this: the minimum is taken over
    /// intermediate sites, so every `i -> k` and `k -> j` has to exist.
    fn square_block(&self) -> Vec<(usize, usize)> {
        let mut sites: Vec<(usize, usize)> = self
            .rows
            .iter()
            .enumerate()
            .filter_map(|(row, label)| {
                self.columns
                    .iter()
                    .position(|column| column == label)
                    .map(|column| (row, column))
            })
            .collect();

        let holes = |site: (usize, usize), members: &[(usize, usize)]| {
            members
                .iter()
                .filter(|other| **other != site)
                .filter(|other| {
                    self.get(site.0, other.1).is_none() || self.get(other.0, site.1).is_none()
                })
                .count()
        };

        // Drop whichever site leaves the most holes until the block is full.
        while let Some(worst) = sites
            .iter()
            .copied()
            .max_by_key(|site| holes(*site, &sites))
        {
            if holes(worst, &sites) == 0 {
                break;
            }
            sites.retain(|site| *site != worst);
        }

        sites
    }
}

/// Supplies measurements for one row of the matrix.
///
/// Reference columns resolve; every other address errors, which the solver
/// cannot tell from a host it did not probe. Held-out columns must not be
/// visible to the solve.
struct Row {
    measurements: HashMap<SocketAddr, DistanceMeasure>,
}

#[async_trait]
impl Measurement for Row {
    async fn measure_distance(&self, address: SocketAddr) -> eyre::Result<DistanceMeasure> {
        self.measurements
            .get(&address)
            .copied()
            .ok_or_else(|| eyre::eyre!("{address} is not a reference host"))
    }
}

/// Splits a round trip into the two directed distances the model takes.
///
/// Round trips cancel clock offset between the endpoints; one-way measurements
/// carry it. Asymmetry cannot be recovered from a single value, so the halves
/// are equal.
fn split(round_trip_nanos: f64) -> DistanceMeasure {
    let half = (round_trip_nanos / 2.0) as i64;
    DistanceMeasure::from((half, half))
}

/// Loopback address standing for a column: the solver keys nodes by
/// [`SocketAddr`]. Nothing is bound, no packet sent.
fn address(column: usize) -> SocketAddr {
    format!("127.0.0.1:{}", 10_000 + column)
        .parse()
        .expect("column address")
}

/// One scored cell: the distance the coordinates imply, against the recorded
/// one.
pub struct Prediction {
    pub predicted: f64,
    pub measured: f64,
}

impl Prediction {
    fn relative_error(&self) -> f64 {
        accuracy::relative_error(self.predicted, self.measured)
    }
}

/// The output of one round, pooled across every host in the run.
#[derive(Default)]
pub struct RoundResult {
    /// Columns the host did not measure. Accuracy is scored on these.
    pub held_out: Vec<Prediction>,
    /// Columns the host measured directly. A model can score well here and
    /// still predict poorly.
    pub reference: Vec<Prediction>,
    /// Held-out cells with a recorded value but no prediction. Kept out of
    /// [`Self::held_out`] so a missing prediction is not averaged with an
    /// inaccurate one.
    pub unpredicted: usize,
    /// Each host's solved outgoing coordinate, in input order.
    pub self_outgoing: Vec<f64>,
}

impl RoundResult {
    /// 90th percentile relative error over columns the host did not measure.
    pub fn held_out_npre(&self) -> f64 {
        accuracy::npre(&Self::errors(&self.held_out))
    }

    /// 90th percentile relative error over columns the host measured directly.
    pub fn reference_npre(&self) -> f64 {
        accuracy::npre(&Self::errors(&self.reference))
    }

    fn errors(predictions: &[Prediction]) -> Vec<f64> {
        predictions.iter().map(Prediction::relative_error).collect()
    }
}

/// A configured run: the matrix, how much of each row the solver sees, and the
/// seed fixing which part.
pub struct Simulation {
    matrix: Matrix,
    /// Columns acting as reference hosts. Trades equations per solve against
    /// the size of the scored set.
    reference_columns: usize,
    seed: u64,
}

impl Simulation {
    pub fn new(matrix: Matrix, reference_columns: usize, seed: u64) -> Self {
        assert!(
            reference_columns > 0 && reference_columns < matrix.columns.len(),
            "reference_columns must leave at least one column held out",
        );

        Self {
            matrix,
            reference_columns,
            seed,
        }
    }

    /// Reference host columns for this run, from the seed alone so runs
    /// reproduce and configurations compare with the selection held fixed.
    pub fn references(&self) -> Vec<usize> {
        let mut columns: Vec<usize> = (0..self.matrix.columns.len()).collect();
        columns.shuffle(&mut rand::rngs::StdRng::seed_from_u64(self.seed));
        columns.truncate(self.reference_columns);
        columns.sort_unstable();
        columns
    }

    /// Rows dropped for missing a reference column: they would solve against
    /// fewer equations than the rest.
    pub fn skipped_rows(&self) -> Vec<&str> {
        let references = self.references();
        (0..self.matrix.rows.len())
            .filter(|&row| {
                references
                    .iter()
                    .any(|&column| self.matrix.get(row, column).is_none())
            })
            .map(|row| self.matrix.rows[row].as_str())
            .collect()
    }

    /// One solver per eligible row, stepped `rounds` times.
    ///
    /// Hosts persist across rounds, so the sequence shows convergence rather
    /// than repeated first rounds.
    pub async fn run(&self, rounds: usize) -> Vec<RoundResult> {
        let references = self.references();
        let addresses: Vec<SocketAddr> = (0..self.matrix.columns.len()).map(address).collect();

        let mut hosts = Vec::new();
        for row in 0..self.matrix.rows.len() {
            let measurements: HashMap<_, _> = references
                .iter()
                .filter_map(|&column| {
                    self.matrix
                        .get(row, column)
                        .map(|rtt| (addresses[column], split(rtt)))
                })
                .collect();

            if measurements.len() < references.len() {
                continue;
            }

            let phoenix = Phoenix::new(Row { measurements });
            for (column, address) in addresses.iter().enumerate() {
                let icao: IcaoCode = self.matrix.columns[column]
                    .parse()
                    .expect("column label is not an ICAO code");
                phoenix.add_node(*address, icao);
            }

            hosts.push((row, phoenix));
        }

        let mut results = Vec::with_capacity(rounds);

        for _ in 0..rounds {
            let mut result = RoundResult::default();

            for (row, phoenix) in &hosts {
                phoenix.measure_all_nodes().await;

                let self_outgoing = *phoenix.self_outgoing.read();
                let self_incoming = *phoenix.self_incoming.read();
                result.self_outgoing.push(self_outgoing);

                for (column, address) in addresses.iter().enumerate() {
                    let Some(measured) = self.matrix.get(*row, column) else {
                        continue;
                    };
                    let is_reference = references.contains(&column);

                    match phoenix.get_coordinates(address) {
                        // Round trip is the two directed dot products summed.
                        Some(peer) => {
                            let prediction = Prediction {
                                predicted: self_outgoing * peer.incoming
                                    + peer.outgoing * self_incoming,
                                measured,
                            };

                            if is_reference {
                                result.reference.push(prediction);
                            } else {
                                result.held_out.push(prediction);
                            }
                        }
                        None if !is_reference => result.unpredicted += 1,
                        None => {}
                    }
                }
            }

            results.push(result);
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let matrix = Matrix::test();

        assert_eq!(matrix.rows.len(), 21);
        assert_eq!(matrix.columns.len(), 15);

        let filled: usize = matrix
            .cells
            .iter()
            .map(|row| row.iter().filter(|cell| cell.is_some()).count())
            .sum();
        assert_eq!(filled, 283, "harvested coverage changed");

        // Both ends in the same metro, so around a millisecond.
        let ymml = matrix.columns.iter().position(|c| c == "YMML").unwrap();
        let row = matrix.rows.iter().position(|r| r == "YMML").unwrap();
        let diagonal = matrix.get(row, ymml).unwrap();
        assert!(
            (1.0e6..3.0e6).contains(&diagonal),
            "co-located round trip was {diagonal}ns"
        );
    }

    /// A matrix with no triangle inequality violations would make the whole
    /// approach untestable: Euclidean embedding is optimal on one, so an
    /// incorrect factorization would score as well as a correct one.
    #[test]
    fn matrix_contains_triangle_inequality_violations() {
        let matrix = Matrix::test();
        let block = matrix.square_block();
        assert_eq!(block.len(), 10, "square block changed");

        let distance = |from: (usize, usize), to: (usize, usize)| {
            matrix
                .get(from.0, to.1)
                .expect("square block is fully populated")
        };

        let mut pairs = 0;
        let mut violations = 0;

        for i in &block {
            for j in &block {
                if i == j {
                    continue;
                }

                // Shortest two-hop detour against the direct path: below 1.0,
                // some route through a third site beats going straight there.
                let shortest = block
                    .iter()
                    .filter(|k| *k != i && *k != j)
                    .map(|k| distance(*i, *k) + distance(*k, *j))
                    .fold(f64::INFINITY, f64::min);

                pairs += 1;
                if shortest < distance(*i, *j) {
                    violations += 1;
                }
            }
        }

        let rate = violations as f64 / pairs as f64;
        assert!(
            (0.05..0.30).contains(&rate),
            "{violations} of {pairs} pairs violate the triangle inequality",
        );
    }

    #[tokio::test]
    async fn run_is_deterministic() {
        let run = || async {
            Simulation::new(Matrix::test(), 5, 7)
                .run(3)
                .await
                .last()
                .map(|round| (round.self_outgoing.clone(), round.unpredicted))
                .unwrap()
        };

        assert_eq!(run().await, run().await);
    }

    /// More than one, so a solve that converges over several rounds has.
    const BASELINE_ROUNDS: usize = 5;
    const BASELINE_REFERENCES: usize = 5;
    const BASELINE_SEED: u64 = 7;
    const BASELINE_PATH: &str = "tests/data/phoenix/baseline.json";

    async fn baseline_round() -> RoundResult {
        Simulation::new(Matrix::test(), BASELINE_REFERENCES, BASELINE_SEED)
            .run(BASELINE_ROUNDS)
            .await
            .pop()
            .expect("at least one round")
    }

    fn baseline_summary(round: &RoundResult) -> serde_json::Value {
        let mean_self_outgoing =
            round.self_outgoing.iter().sum::<f64>() / round.self_outgoing.len() as f64;

        serde_json::json!({
            "matrix": "median.csv",
            "reference_columns": BASELINE_REFERENCES,
            "seed": BASELINE_SEED,
            "rounds": BASELINE_ROUNDS,
            "hosts": round.self_outgoing.len(),
            "reference_npre": round.reference_npre(),
            "reference_median_error": accuracy::median_relative_error(
                &RoundResult::errors(&round.reference),
            ),
            "reference_scored": round.reference.len(),
            "held_out_scored": round.held_out.len(),
            "held_out_unpredicted": round.unpredicted,
            "mean_self_outgoing": mean_self_outgoing,
        })
    }

    /// Regenerates the recorded baseline. Never runs in CI:
    /// `cargo test --lib regenerate_baseline -- --ignored`
    #[tokio::test]
    #[ignore]
    async fn regenerate_baseline() {
        let summary = baseline_summary(&baseline_round().await);
        let rendered = serde_json::to_string_pretty(&summary).unwrap();
        std::fs::write(BASELINE_PATH, format!("{rendered}\n")).unwrap();
        println!("{summary:#}");
    }

    /// Pins the baseline and the two properties behind it: coordinates come
    /// only from measuring, so held-out columns get no prediction; and the solve
    /// fits `self_outgoing * leg = round trip`, converging on a dimensionless
    /// 2.0.
    #[tokio::test]
    async fn baseline_matches_recorded() {
        let recorded: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(BASELINE_PATH).unwrap()).unwrap();
        let round = baseline_round().await;
        let current = baseline_summary(&round);

        assert_eq!(current, recorded, "run regenerate_baseline to update");

        // Nothing outside the reference set is predicted.
        assert_eq!(round.held_out.len(), 0);
        assert!(round.unpredicted > 0);
        assert!(round.held_out_npre().is_nan());

        // Predictions on measured nodes come out at exactly twice the recorded
        // distance, hence a relative error of 1.0.
        assert!((round.reference_npre() - 1.0).abs() < 1e-6);
        for self_outgoing in &round.self_outgoing {
            assert!((self_outgoing - 2.0).abs() < 1e-6);
        }

        // EDDF reports only 2 of 15 columns, too few to cover the reference set.
        assert_eq!(
            Simulation::new(Matrix::test(), BASELINE_REFERENCES, BASELINE_SEED).skipped_rows(),
            vec!["EDDF"],
        );
    }

    #[tokio::test]
    async fn reference_selection_is_seeded() {
        let references = |seed| Simulation::new(Matrix::test(), 5, seed).references();

        assert_eq!(references(7), references(7));
        assert_ne!(references(7), references(8));
        assert_eq!(references(7).len(), 5);
    }
}
