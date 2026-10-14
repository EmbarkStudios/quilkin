//! Per-reference weighting for the coordinate solve.
//!
//! A reference host whose coordinate mispredicts its own measured distance is
//! trusted less, which keeps one bad coordinate from propagating into every
//! host that solves against it.
//!
//! Chen et al., "Phoenix: A Weight-Based Network Coordinate System Using Matrix
//! Factorization" (IEEE TNSM 8(4), 2011), equations 8 to 14.

use ndarray::{Array1, Array2};

use super::{DIMENSION, accuracy, dot};

/// Error at which a reference loses all weight, as a multiple of the median.
const CUTOFF: f64 = 10.0;

/// Weight per reference, from the absolute error of the prediction that used it.
///
/// Full trust at or below the median error, none at [`CUTOFF`] times it, the
/// squared ratio between. At least half of any set keeps full trust, since at
/// most half can exceed its own median.
fn weights(errors: &[f64]) -> Vec<f64> {
    let median = accuracy::median(errors);

    errors
        .iter()
        .map(|error| {
            if *error <= median {
                1.0
            } else if *error < CUTOFF * median {
                (median / error).powi(2)
            } else {
                0.0
            }
        })
        .collect()
}

/// The weighted least-squares system for one direction.
///
/// `references` are the coefficient rows and `distances` the measured distance
/// for each. `current` is the vector being solved for, as last solved: it is
/// what predicts `distances` from `references`, so it is what their error is
/// measured against.
///
/// Regularization rows are appended by the solver and are left unweighted.
///
/// # Panics
///
/// Panics unless `references` and `distances` have the same length.
fn weighted_system(
    references: &[[f64; DIMENSION]],
    distances: &[f64],
    current: &[f64; DIMENSION],
) -> (Array2<f64>, Array1<f64>) {
    assert_eq!(
        references.len(),
        distances.len(),
        "{} references against {} distances",
        references.len(),
        distances.len(),
    );

    let errors: Vec<f64> = references
        .iter()
        .zip(distances)
        .map(|(reference, distance)| (dot(current, reference) - distance).abs())
        .collect();

    let weighting = weights(&errors);

    let mut rows = Array2::zeros((references.len(), DIMENSION));
    let mut targets = Array1::zeros(references.len());

    for (index, (reference, distance)) in references.iter().zip(distances).enumerate() {
        let scale = weighting[index].sqrt();

        for element in 0..DIMENSION {
            rows[[index, element]] = scale * reference[element];
        }

        targets[index] = scale * distance;
    }

    (rows, targets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bands_are_split_at_the_median_and_the_cutoff() {
        // Seven errors, so the median is the fourth: 2.0, putting the cutoff
        // at 20.0.
        let weights = weights(&[1.0, 1.0, 1.0, 2.0, 8.0, 20.0, 100.0]);

        assert_eq!(weights[2], 1.0, "below the median");
        assert_eq!(weights[3], 1.0, "at the median");
        assert_eq!(weights[4], 0.0625, "(2/8)^2");
        assert_eq!(weights[5], 0.0, "at the cutoff");
        assert_eq!(weights[6], 0.0, "beyond the cutoff");
    }

    #[test]
    fn a_zero_median_trusts_only_exact_predictions() {
        // The conditional band is empty, so anything inaccurate drops straight
        // out rather than dividing by zero.
        let weights = weights(&[0.0, 0.0, 0.0, 7.0]);

        assert_eq!(weights, vec![1.0, 1.0, 1.0, 0.0]);
    }

    #[test]
    fn half_of_any_set_keeps_full_trust() {
        // Load-bearing: it means no set of references can ever be weighted to
        // all zeroes, which would leave the solver with nothing to fit.
        for count in 1u32..32 {
            let errors: Vec<f64> = (0..count).map(|index| f64::from(index).exp()).collect();
            let full = weights(&errors).iter().filter(|w| **w == 1.0).count();

            assert!(full >= count.div_ceil(2) as usize, "{count} references");
        }
    }

    #[test]
    fn rows_and_targets_scale_by_the_square_root_of_the_weight() {
        let references = [[2.0], [2.0], [2.0]];
        // The first two predict exactly, the third is far enough out to be cut.
        let distances = [2.0, 2.0, 100.0];
        let (rows, targets) = weighted_system(&references, &distances, &[1.0]);

        assert_eq!(rows[[0, 0]], 2.0);
        assert_eq!(targets[0], 2.0);
        assert_eq!(rows[[2, 0]], 0.0, "a cut reference contributes nothing");
        assert_eq!(targets[2], 0.0);
    }

    #[test]
    fn weighting_pulls_the_solution_away_from_a_bad_reference() {
        // Four references agreeing on a scale of 3.0, and one claiming 30.0.
        let references = [[1.0], [2.0], [3.0], [4.0], [1.0]];
        let distances = [3.0, 6.0, 9.0, 12.0, 30.0];
        let current = [3.0];

        let (rows, targets) = weighted_system(&references, &distances, &current);
        let (weighted, _) = crate::net::phoenix::nnls::solve_regularized(&rows, &targets, 0.0);

        let unweighted_rows =
            Array2::from_shape_vec((5, 1), references.iter().map(|r| r[0]).collect()).unwrap();
        let (unweighted, _) = crate::net::phoenix::nnls::solve_regularized(
            &unweighted_rows,
            &Array1::from_vec(distances.to_vec()),
            0.0,
        );

        assert!(
            (weighted[0] - 3.0).abs() < (unweighted[0] - 3.0).abs(),
            "weighted {} should sit closer to 3.0 than unweighted {}",
            weighted[0],
            unweighted[0],
        );
    }
}
