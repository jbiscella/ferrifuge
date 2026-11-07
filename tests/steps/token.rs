//! Steps of Block 1 about connection tokens.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cucumber::{given, when};
use ferrifuge::{Client, ClientBuilder, ClientEvent, TokenError};

use super::{ClientWorld, expect_event, test_config};
use crate::support::token::connection_token;

/// User the scenario's tokens are issued to.
const USER: &str = "token-user";
/// Short enough for the refresh to happen within the scenario.
const TOKEN_LIFETIME: Duration = Duration::from_secs(3);

#[given("a connected client using a connection token with expiration")]
async fn a_connected_client_with_expiring_token(world: &mut ClientWorld) {
    let builder = with_expiring_token(world, Client::builder(world.server_endpoint()));
    world.build_client(builder);
    world.client().connect();
    expect_event(world, "connecting").await;
    let ClientEvent::Connected { client_id, .. } = expect_event(world, "connected").await else {
        unreachable!("expect_event checked the name");
    };
    world.client_id = Some(client_id);
}

/// `builder` with a connection token that expires within the scenario, and
/// a callback refreshing it until [`ClientWorld::token_denied`] is set.
pub(super) fn with_expiring_token(world: &ClientWorld, builder: ClientBuilder) -> ClientBuilder {
    let denied = Arc::clone(&world.token_denied);
    builder
        .config(test_config().token(connection_token(USER, TOKEN_LIFETIME)))
        .get_token(move || {
            let denied = denied.load(Ordering::SeqCst);
            async move {
                if denied {
                    Err(TokenError::Unauthorized)
                } else {
                    Ok(connection_token(USER, TOKEN_LIFETIME))
                }
            }
        })
}

#[when(r#"the token callback signals "unauthorized" during refresh"#)]
fn token_callback_signals_unauthorized(world: &mut ClientWorld) {
    world.token_denied.store(true, Ordering::SeqCst);
}
