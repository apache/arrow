use crate::error::Result;
use crate::logicalplan::ScalarValue;

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum VarType {
    /// System variable, like @@version
    System,
    /// User defined variable, like @name
    UserDefined,
}

pub trait VarProvider: Send + Sync {
    /// Get variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue>;
}

pub mod system;
pub mod user_defined;
