//! Scoring predicted network distances against measured ones.


/// Error between a predicted and a measured distance, over the smaller of the
/// two.
pub fn relative_error(predicted: f64, measured: f64) -> f64 {
    debug_assert!(
        predicted >= 0.0 && measured >= 0.0,
        "distances are non-negative, got predicted={predicted} measured={measured}"
    );

    let denominator = predicted.min(measured);

    if denominator == 0.0 {
        if predicted == measured {
            return 0.0;
        }
        return f64::INFINITY;
    }

    (predicted - measured).abs() / denominator
}

/// Ninetieth percentile relative error (NPRE)
pub fn npre(errors: &[f64]) -> f64 {
    percentile(errors, 0.90)
}

/// Relative error at the 50th percentile.
pub fn median_relative_error(errors: &[f64]) -> f64 {
    percentile(errors, 0.50)
}

/// Median of `|predicted - measured|`, in the unit of the inputs.
///
/// # Panics
///
/// Panics if the slices differ in length.
pub fn median_absolute_error(predicted: &[f64], measured: &[f64]) -> f64 {
    assert_eq!(
        predicted.len(),
        measured.len(),
        "predicted ({}) and measured ({}) must be the same length",
        predicted.len(),
        measured.len(),
    );

    let errors: Vec<f64> = predicted
        .iter()
        .zip(measured)
        .map(|(p, m)| (p - m).abs())
        .collect();

    percentile(&errors, 0.50)
}

/// Smallest element at or above which `quantile` of the data falls.
///
/// Nearest rank, no interpolation: every result occurs in `values`, and an even
/// set gives the lower middle element. Stated because the alternatives differ
/// on small samples.
///
/// Ordered by [`f64::total_cmp`]: infinities rank normally, NaN sorts last.
/// NaN for an empty slice.
fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);

    let rank = (quantile * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_error_is_symmetric() {
        // Over- and under-predicting by the same factor give the same result.
        assert_eq!(relative_error(10.0, 5.0), 1.0);
        assert_eq!(relative_error(5.0, 10.0), 1.0);
        assert_eq!(relative_error(7.5, 7.5), 0.0);
    }

    #[test]
    fn relative_error_handles_zero() {
        assert_eq!(relative_error(0.0, 0.0), 0.0);
        assert_eq!(relative_error(5.0, 0.0), f64::INFINITY);
        assert_eq!(relative_error(0.0, 5.0), f64::INFINITY);
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        // The 9th element, not an interpolation between the 9th and 10th; the
        // median is the lower middle.
        let values: Vec<f64> = (1..=10).map(f64::from).collect();
        assert_eq!(npre(&values), 9.0);
        assert_eq!(median_relative_error(&values), 5.0);

        assert_eq!(npre(&[42.0]), 42.0);
        assert!(npre(&[]).is_nan());
    }

    #[test]
    fn median_absolute_error_over_pairs() {
        let predicted = [10.0, 20.0, 30.0, 40.0];
        let measured = [12.0, 25.0, 30.0, 49.0];
        // errors 2, 5, 0, 9 -> sorted 0, 2, 5, 9 -> lower median 2
        assert_eq!(median_absolute_error(&predicted, &measured), 2.0);
    }
}
