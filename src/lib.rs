mod correction;
mod invoice;
mod transform;
mod visualize;

use correction::Correction;
use invoice::Invoice;
use pyo3::prelude::*;
use transform::TransformPipeline;
use visualize::Visualize;

#[pymodule]
fn lerpz_invoice(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TransformPipeline>()?;
    m.add_class::<Invoice>()?;
    m.add_class::<Visualize>()?;
    m.add_class::<Correction>()?;
    Ok(())
}
