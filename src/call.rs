//! Calls the application makes over the client's connection and awaits
//! (Block 13): their requests, results and errors.

/// What a call asks the server. `#[non_exhaustive]`: later blocks add
/// methods.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CallRequest {
    /// An RPC (`client.proto`, `RPCRequest`).
    Rpc {
        /// Routes the call; empty for none.
        method: String,
        /// Raw JSON with the JSON protocol, any bytes with Protobuf.
        data: Vec<u8>,
    },
}

impl std::fmt::Debug for CallRequest {
    // Application data is shown by its length only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc { method, data } => f
                .debug_struct("Rpc")
                .field("method", method)
                .field("data", &format_args!("<{} bytes>", data.len()))
                .finish(),
        }
    }
}

/// What a call returned. `#[non_exhaustive]`: later blocks add methods.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CallReply {
    /// The RPC's result data (`client.proto`, `RPCResult.data`): raw JSON
    /// with the JSON protocol, any bytes with Protobuf; empty when the
    /// server sent none.
    Rpc(Vec<u8>),
}

impl std::fmt::Debug for CallReply {
    // Application data is shown by its length only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc(data) => f
                .debug_tuple("Rpc")
                .field(&format_args!("<{} bytes>", data.len()))
                .finish(),
        }
    }
}

/// Why a call failed: an error the server answered with, keeping its code,
/// message and `temporary` flag (`client.proto`, `Error`), or a client-side
/// one with a code of [`crate::codes::error`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CallError {
    /// The server's error code (from 100), or a client-side code below 100.
    pub code: u32,
    /// What went wrong.
    pub message: String,
    /// The server marked the error temporary: the same call may succeed
    /// later.
    pub temporary: bool,
}

impl CallError {
    /// An error with this code and message, not temporary.
    #[must_use]
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            temporary: false,
        }
    }

    /// The same error, marked temporary or not.
    #[must_use]
    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {}", self.message, self.code)?;
        if self.temporary {
            f.write_str(", temporary")?;
        }
        f.write_str(")")
    }
}

impl std::error::Error for CallError {}
