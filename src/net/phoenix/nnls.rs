//! Wrappers around the [`nnls`] crate for Phoenix coordinate computation.
//!
//! Phoenix represents each node as two d-dimensional vectors: outgoing and
//! incoming. To compute a node's outgoing vector, we collect RTT measurements
//! to reference hosts and solve for the vector whose dot products with those
//! hosts' incoming vectors best approximate the measured RTTs — subject to the
//! constraint that every element is non-negative (guaranteeing predicted
//! distances are never negative).
//!
//! On top of the plain NNLS solver we add regularized solving
//! ([`solve_regularized`]) which penalizes large coordinate values to prevent
//! drift across successive update rounds (Phoenix paper Section III-C).

use ndarray::{Array1, Array2};

/// Solve for a node's coordinate vector given reference host vectors and
/// measured RTTs.
///
/// - `coefficients`: each row is a reference host's incoming coordinate vector
///   (e.g. 32 hosts × 8 dimensions).
/// - `targets`: the measured RTTs to each reference host (e.g. 32 elements).
///
/// Returns `(solution, residual_norm)` where `solution` is the node's
/// coordinate vector (all elements >= 0) and `residual_norm` indicates fit
/// quality (lower is better).
///
/// # Panics
///
/// Panics if `coefficients.nrows() != targets.len()`.
pub fn solve(coefficients: &Array2<f64>, targets: &Array1<f64>) -> (Array1<f64>, f64) {
    assert_eq!(
        coefficients.nrows(),
        targets.len(),
        "number of rows in coefficients ({}) must match length of targets ({})",
        coefficients.nrows(),
        targets.len(),
    );

    if coefficients.ncols() == 0 {
        return (Array1::zeros(0), targets.dot(targets).sqrt());
    }

    nnls::nnls(coefficients.view(), targets.view())
}

/// Like [`solve`] but with Tikhonov regularization that penalizes large
/// coordinate values. The Phoenix paper claims this helps provide
/// "numerical stability".
///
/// `strength` controls how aggressively values are pulled toward zero
/// (Phoenix paper recommends 2.0). Implemented by appending
/// `sqrt(strength) * I` rows to the coefficient matrix before solving.
///
/// # Panics
///
/// Panics if `strength` is negative.
pub fn solve_regularized(
    coefficients: &Array2<f64>,
    targets: &Array1<f64>,
    strength: f64,
) -> (Array1<f64>, f64) {
    assert!(
        strength >= 0.0,
        "regularization strength must be non-negative, got {strength}"
    );

    if strength == 0.0 {
        return solve(coefficients, targets);
    }

    let (num_equations, num_unknowns) = (coefficients.nrows(), coefficients.ncols());
    let sqrt_strength = strength.sqrt();

    let total_rows = num_equations + num_unknowns;
    let mut augmented_coefficients = Array2::zeros((total_rows, num_unknowns));
    augmented_coefficients
        .slice_mut(ndarray::s![..num_equations, ..])
        .assign(coefficients);
    for col in 0..num_unknowns {
        augmented_coefficients[[num_equations + col, col]] = sqrt_strength;
    }

    let mut augmented_targets = Array1::zeros(total_rows);
    augmented_targets
        .slice_mut(ndarray::s![..num_equations])
        .assign(targets);

    solve(&augmented_coefficients, &augmented_targets)
}

/// Predict one-way latency between two nodes: `outgoing · incoming`.
///
/// The dot product is always >= 0 when both vectors come from NNLS, which
/// is why Phoenix enforces non-negativity.
#[inline]
// TODO: Change phoenix to use this function.
#[allow(unused)]
pub fn predict_distance(outgoing: &Array1<f64>, incoming: &Array1<f64>) -> f64 {
    outgoing.dot(incoming)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::{Array2, array};

    // =====================================================================
    // Basic correctness
    // =====================================================================

    #[test]
    fn identity_with_positive_targets_returns_targets() {
        let coefficients = Array2::eye(3);
        let targets = array![1.0, 2.0, 3.0];
        let (solution, _residual) = solve(&coefficients, &targets);

        assert!((solution[0] - 1.0).abs() < 1e-10);
        assert!((solution[1] - 2.0).abs() < 1e-10);
        assert!((solution[2] - 3.0).abs() < 1e-10);
    }

    #[test]
    fn negative_targets_are_clamped_to_zero() {
        let coefficients = Array2::eye(3);
        let targets = array![-1.0, -2.0, -3.0];
        let (solution, _) = solve(&coefficients, &targets);

        assert!(solution[0].abs() < 1e-10);
        assert!(solution[1].abs() < 1e-10);
        assert!(solution[2].abs() < 1e-10);
    }

    #[test]
    fn mixed_positive_and_negative_targets() {
        let coefficients = Array2::eye(3);
        let targets = array![5.0, -3.0, 2.0];
        let (solution, _) = solve(&coefficients, &targets);

        assert!((solution[0] - 5.0).abs() < 1e-10);
        assert!(solution[1].abs() < 1e-10);
        assert!((solution[2] - 2.0).abs() < 1e-10);
    }

    #[test]
    fn overdetermined_system_gives_reasonable_fit() {
        let coefficients = array![[1.0, 0.0], [0.0, 1.0], [1.0, 1.0], [2.0, 1.0]];
        let targets = array![1.0, 2.0, 3.5, 4.0];
        let (solution, _) = solve(&coefficients, &targets);

        assert!(solution[0] >= 0.0);
        assert!(solution[1] >= 0.0);

        let residual = coefficients.dot(&solution) - &targets;
        let residual_norm = residual.dot(&residual).sqrt();
        assert!(residual_norm < 2.0, "residual too large: {residual_norm}");
    }

    #[test]
    fn zero_unknowns_returns_empty_vector() {
        let coefficients = Array2::zeros((3, 0));
        let targets = array![1.0, 2.0, 3.0];
        let (solution, _) = solve(&coefficients, &targets);
        assert_eq!(solution.len(), 0);
    }

    #[test]
    fn all_solution_elements_are_non_negative() {
        let coefficients = array![
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0],
            [1.0, 0.0, 1.0]
        ];
        let targets = array![10.0, 20.0, 30.0, 5.0];
        let (solution, _) = solve(&coefficients, &targets);

        for idx in 0..solution.len() {
            assert!(
                solution[idx] >= -1e-15,
                "element {idx} is negative: {}",
                solution[idx]
            );
        }
    }

    // =====================================================================
    // Regularization
    // =====================================================================

    #[test]
    fn regularization_shrinks_solution_magnitude() {
        let coefficients = Array2::eye(3);
        let targets = array![10.0, 20.0, 30.0];

        let (unregularized, _) = solve(&coefficients, &targets);
        let (regularized, _) = solve_regularized(&coefficients, &targets, 2.0);

        let unreg_norm = unregularized.dot(&unregularized).sqrt();
        let reg_norm = regularized.dot(&regularized).sqrt();
        assert!(reg_norm < unreg_norm);

        for idx in 0..regularized.len() {
            assert!(regularized[idx] >= 0.0);
        }
    }

    #[test]
    fn regularization_with_zero_strength_matches_unregularized() {
        let coefficients = array![[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]];
        let targets = array![1.0, 2.0, 3.0];

        let (unregularized, _) = solve(&coefficients, &targets);
        let (zero_strength, _) = solve_regularized(&coefficients, &targets, 0.0);

        let diff = &unregularized - &zero_strength;
        let diff_norm = diff.dot(&diff).sqrt();
        assert!(diff_norm < 1e-10);
    }

    #[test]
    fn increasing_regularization_monotonically_shrinks_norm() {
        let coefficients = Array2::eye(3);
        let targets = array![10.0, 20.0, 30.0];

        let norm = |strength: f64| -> f64 {
            let (s, _) = solve_regularized(&coefficients, &targets, strength);
            s.dot(&s).sqrt()
        };

        let norm_at_0 = norm(0.0);
        let norm_at_1 = norm(1.0);
        let norm_at_10 = norm(10.0);
        let norm_at_100 = norm(100.0);

        assert!(
            norm_at_0 >= norm_at_1,
            "norm should decrease: {norm_at_0} vs {norm_at_1}"
        );
        assert!(
            norm_at_1 >= norm_at_10,
            "norm should decrease: {norm_at_1} vs {norm_at_10}"
        );
        assert!(
            norm_at_10 >= norm_at_100,
            "norm should decrease: {norm_at_10} vs {norm_at_100}"
        );
    }

    #[test]
    fn regularization_with_large_strength_pushes_toward_zero() {
        let coefficients = Array2::eye(3);
        let targets = array![10.0, 20.0, 30.0];
        let (solution, _) = solve_regularized(&coefficients, &targets, 1e6);

        let norm = solution.dot(&solution).sqrt();
        assert!(norm < 1.0, "solution too large: {norm}");
        for idx in 0..solution.len() {
            assert!(solution[idx] >= 0.0);
        }
    }

    // =====================================================================
    // Predict distance
    // =====================================================================

    #[test]
    fn dot_product_distance_computed_correctly() {
        let outgoing = array![1.0, 2.0, 3.0];
        let incoming = array![4.0, 5.0, 6.0];

        let distance = predict_distance(&outgoing, &incoming);
        assert!((distance - 32.0).abs() < 1e-10);
    }

    #[test]
    fn non_negative_vectors_produce_non_negative_distance() {
        let outgoing = array![1.0, 0.0, 3.0, 0.5];
        let incoming = array![0.0, 2.0, 1.0, 4.0];

        let distance = predict_distance(&outgoing, &incoming);
        assert!(distance >= 0.0);
    }

    // =====================================================================
    // Phoenix scenario
    // =====================================================================

    #[test]
    fn phoenix_scenario_with_reference_hosts() {
        // 4 reference hosts with 2-dimensional incoming vectors;
        // solve for the proxy's outgoing vector.
        let reference_host_vectors = array![[3.0, 1.0], [1.0, 4.0], [2.0, 2.0], [0.5, 3.0]];
        let measured_distances = array![7.0, 9.0, 8.0, 6.5];

        let (proxy_outgoing, _) = solve(&reference_host_vectors, &measured_distances);

        assert!(proxy_outgoing[0] >= 0.0);
        assert!(proxy_outgoing[1] >= 0.0);

        for host_idx in 0..4 {
            let host_incoming = reference_host_vectors.row(host_idx).to_owned();
            let predicted = predict_distance(&proxy_outgoing, &host_incoming);
            let error = (predicted - measured_distances[host_idx]).abs();
            assert!(
                error < 2.0,
                "prediction error too large for host {host_idx}: \
                 predicted={predicted}, actual={}, error={error}",
                measured_distances[host_idx]
            );
        }
    }

    // =====================================================================
    // Edge cases and bad inputs
    // =====================================================================

    #[test]
    #[should_panic(expected = "number of rows in coefficients")]
    fn panics_on_mismatched_dimensions() {
        let coefficients = Array2::eye(3);
        let targets = array![1.0, 2.0];
        solve(&coefficients, &targets);
    }

    #[test]
    #[should_panic(expected = "regularization strength must be non-negative")]
    fn panics_on_negative_regularization_strength() {
        let coefficients = Array2::eye(2);
        let targets = array![1.0, 2.0];
        solve_regularized(&coefficients, &targets, -1.0);
    }

    #[test]
    fn zero_targets_produce_zero_solution() {
        let coefficients = Array2::eye(3);
        let targets = array![0.0, 0.0, 0.0];
        let (solution, _) = solve(&coefficients, &targets);

        for idx in 0..solution.len() {
            assert!(solution[idx].abs() < 1e-10, "expected zero at index {idx}");
        }
    }

    #[test]
    fn all_zero_coefficients_produce_zero_solution() {
        let coefficients = Array2::<f64>::zeros((3, 2));
        let targets = array![1.0, 2.0, 3.0];
        let (solution, _) = solve(&coefficients, &targets);

        for idx in 0..solution.len() {
            assert!(
                solution[idx].abs() < 1e-10,
                "expected zero at index {idx}, got {}",
                solution[idx]
            );
        }
    }

    #[test]
    fn single_equation_single_unknown() {
        let coefficients = array![[2.0]];
        let targets = array![6.0];
        let (solution, _) = solve(&coefficients, &targets);

        assert!((solution[0] - 3.0).abs() < 1e-10);
    }

    #[test]
    fn single_equation_single_unknown_negative_clamped() {
        let coefficients = array![[2.0]];
        let targets = array![-6.0];
        let (solution, _) = solve(&coefficients, &targets);

        assert!(solution[0].abs() < 1e-10);
    }

    #[test]
    fn very_large_values_handled() {
        let coefficients = Array2::eye(2);
        let targets = array![1e15, 1e15];
        let (solution, _) = solve(&coefficients, &targets);

        assert!((solution[0] - 1e15).abs() / 1e15 < 1e-6);
        assert!((solution[1] - 1e15).abs() / 1e15 < 1e-6);
    }

    #[test]
    fn very_small_positive_values_handled() {
        let coefficients = Array2::eye(2);
        let targets = array![1e-12, 1e-12];
        let (solution, _) = solve(&coefficients, &targets);

        assert!(solution[0] >= 0.0);
        assert!(solution[1] >= 0.0);
    }

    #[test]
    fn rank_deficient_coefficients_do_not_panic() {
        let coefficients = array![[1.0, 1.0], [1.0, 1.0], [1.0, 1.0]];
        let targets = array![2.0, 2.0, 2.0];
        let (solution, _) = solve(&coefficients, &targets);

        for idx in 0..solution.len() {
            assert!(solution[idx] >= 0.0);
        }

        let predicted = coefficients.dot(&solution);
        for idx in 0..targets.len() {
            assert!(
                (predicted[idx] - targets[idx]).abs() < 1.0,
                "poor fit at row {idx}: predicted={}, target={}",
                predicted[idx],
                targets[idx]
            );
        }
    }

    #[test]
    fn underdetermined_system_is_non_negative() {
        let coefficients = array![[1.0, 0.0, 1.0], [0.0, 1.0, 1.0]];
        let targets = array![3.0, 4.0];
        let (solution, _) = solve(&coefficients, &targets);

        for idx in 0..solution.len() {
            assert!(solution[idx] >= 0.0);
        }

        let residual = coefficients.dot(&solution) - &targets;
        let residual_norm = residual.dot(&residual).sqrt();
        assert!(residual_norm < 1.0, "residual too large: {}", residual_norm);
    }

    #[test]
    fn residual_norm_is_returned() {
        let coefficients = Array2::eye(3);
        let targets = array![1.0, 2.0, 3.0];
        let (_solution, residual_norm) = solve(&coefficients, &targets);

        assert!(
            residual_norm < 1e-10,
            "expected near-zero residual, got {residual_norm}"
        );
    }

    #[test]
    fn residual_norm_nonzero_for_clamped_solution() {
        let coefficients = Array2::eye(2);
        let targets = array![-3.0, -4.0];
        let (_solution, residual_norm) = solve(&coefficients, &targets);

        // ||0 - [-3, -4]|| = 5
        assert!(
            (residual_norm - 5.0).abs() < 1e-10,
            "expected residual 5.0, got {residual_norm}"
        );
    }
}
