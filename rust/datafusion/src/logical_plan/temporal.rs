use core::fmt;

#[derive(Clone, Copy, Debug, PartialEq)]
/// Date part
pub enum DatePart {
    /// Hour part of date / time
    Hour,
}

impl fmt::Display for DatePart {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DatePart::Hour => {
                write!(f, "HOUR")
            }
        }
    }
}
