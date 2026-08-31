#![doc = include_str!("../README.md")]

mod decode;
mod encode;
mod error;

#[cfg(test)]
mod tests;

pub use decode::DecodeFormat;
pub use encode::{
    EncodeFormat,
    EncodedStream,
};
pub use error::{
    FormatError,
    PayloadLimitExceeded,
};
