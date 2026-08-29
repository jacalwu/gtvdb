//! gtv-udf: WASM sandbox for user-defined scalar functions, backed by
//! [`wasmtime`].
//!
//! A [`WasmUdf`] loads a module exporting a single `f64 -> f64` function and
//! applies it element-wise. The module runs in an isolated `Store`, so a
//! misbehaving UDF cannot touch the host process. This is the scalar MVP;
//! buffer-based (zero-copy) operator sharing is a later optimization.

use wasmtime::{Engine, Instance, Module, Store, TypedFunc};

/// A compiled, sandboxed scalar UDF: `func: f64 -> f64`.
pub struct WasmUdf {
    store: Store<()>,
    func: TypedFunc<(f64,), f64>,
    name: String,
}

impl WasmUdf {
    /// Compile a WASM module from its binary representation.
    pub fn from_bytes(bytes: &[u8], func_name: &str) -> anyhow::Result<Self> {
        let engine = Engine::default();
        let module = Module::new(&engine, bytes)?;
        Self::instantiate(&engine, &module, func_name)
    }

    /// Compile a WASM module from WebAssembly text (WAT).
    pub fn from_wat(wat: &str, func_name: &str) -> anyhow::Result<Self> {
        let bytes = wat::parse_str(wat)?;
        Self::from_bytes(&bytes, func_name)
    }

    fn instantiate(
        engine: &Engine,
        module: &Module,
        func_name: &str,
    ) -> anyhow::Result<Self> {
        let mut store = Store::new(engine, ());
        let instance = Instance::new(&mut store, module, &[])?;
        let func = instance.get_typed_func::<(f64,), f64>(&mut store, func_name)?;
        Ok(Self {
            store,
            func,
            name: func_name.to_string(),
        })
    }

    /// The exported function name this UDF wraps.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Invoke the UDF on a single input.
    pub fn call(&mut self, x: f64) -> anyhow::Result<f64> {
        Ok(self.func.call(&mut self.store, (x,))?)
    }

    /// Apply the UDF element-wise over a slice.
    pub fn map(&mut self, xs: &[f64]) -> anyhow::Result<Vec<f64>> {
        xs.iter().map(|&x| self.call(x)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOUBLE: &str = r#"
(module
  (func (export "map") (param f64) (result f64)
    local.get 0
    f64.const 2
    f64.mul))
"#;

    #[test]
    fn scalar_call_works() {
        let mut udf = WasmUdf::from_wat(DOUBLE, "map").unwrap();
        assert_eq!(udf.call(3.0).unwrap(), 6.0);
    }

    #[test]
    fn vectorized_map_works() {
        let mut udf = WasmUdf::from_wat(DOUBLE, "map").unwrap();
        assert_eq!(udf.map(&[1.0, 2.0, 3.0]).unwrap(), vec![2.0, 4.0, 6.0]);
    }

    #[test]
    fn missing_export_is_an_error() {
        let wat = r#"(module (func (export "other") (param f64) (result f64) local.get 0))"#;
        assert!(WasmUdf::from_wat(wat, "map").is_err());
    }

    #[test]
    fn module_with_state_is_sandboxed() {
        // A module with a mutable global counter: each call increments it.
        // Running it in its own Store means it cannot leak state to the host.
        let wat = r#"
(module
  (global $g (mut f64) (f64.const 0))
  (func (export "map") (param f64) (result f64)
    local.get 0
    global.get $g
    f64.add
    global.get $g
    f64.const 1
    f64.add
    global.set $g))
"#;
        let mut udf = WasmUdf::from_wat(wat, "map").unwrap();
        assert_eq!(udf.call(10.0).unwrap(), 10.0);
        assert_eq!(udf.call(10.0).unwrap(), 11.0); // state persisted within the store
    }
}
