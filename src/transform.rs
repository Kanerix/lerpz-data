use pyo3::{pyclass, pymethods, types::PyFunction, Py, PyAny, PyRef, PyRefMut, PyResult};

#[pyclass]
pub struct TransformBuilder {
    data: Py<PyAny>,
    rules: Vec<Py<PyFunction>>,
}

pub struct Transform {
    pub data: Py<PyAny>,
    pub rules: Vec<Py<PyFunction>>,
}

#[pymethods]
impl TransformBuilder {
    #[new]
    pub fn new(data: Py<PyAny>) -> PyResult<Self> {
        Ok(Self {
            data,
            rules: Vec::new(),
        })
    }

    pub fn add_rule<'a>(mut slf: PyRefMut<'a, Self>, f: Py<PyFunction>) -> PyResult<PyRefMut<'a, Self>> {
        slf.rules.push(f);
        Ok(slf)
    }

    pub fn build<'a>(slf: PyRef<'a, Self>) -> PyResult<()> {
        Ok(())
    }
}
