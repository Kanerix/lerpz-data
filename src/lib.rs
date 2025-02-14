use pyo3::prelude::*;

#[pymodule]
fn rust(_: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
