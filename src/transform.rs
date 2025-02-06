use pyo3::{pyclass, pymethods};

#[pyclass]
pub struct TransformPipeline;

#[pymethods]
impl TransformPipeline {
    #[new]
    fn new() -> Self {
        TransformPipeline {}
    }

    fn transform(&self, input: Vec<f64>) -> Vec<f64> {
        input.iter().map(|x| x * 2.0).collect()
    }
}
