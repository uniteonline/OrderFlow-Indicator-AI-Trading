pub fn score_from_z(z: Option<f64>) -> f64 {
    let v = z.unwrap_or(0.0).abs();
    (v / 3.0).min(1.0)
}

pub fn confidence_from_ratio(ratio: f64) -> f64 {
    ratio.clamp(0.0, 1.0)
}
