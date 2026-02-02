pub fn mean<T1, T2: Into<f64> + Copy>(vals: &[(T1, T2)]) -> f64 {
    if vals.is_empty() {
        return 0.0;
    }

    let sum: f64 = vals.iter().map(|(_, y)| (*y).into()).sum();

    sum / vals.len() as f64
}
