use crate::error::Result;
use crate::logicalplan::ScalarValue;
use crate::variable::VarProvider;

pub struct UserDefinedVar {}

impl UserDefinedVar {
    pub fn new() -> Self {
        Self {}
    }
}

impl VarProvider for UserDefinedVar {
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        let s = format!("{}-{}", "test".to_string(), var_names.concat());
        Ok(ScalarValue::Utf8(s))
    }
}
