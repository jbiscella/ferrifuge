//! Steps of Block 1 about connection tokens.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cucumber::{given, when};
use ferrifuge::{Client, ClientEvent, Config, TokenError};

use super::{ClientWorld, expect_event, test_config};
use crate::support::token::connection_token;

/// User the scenario's tokens are issued to.
const USER: &str = "token-user";
/// Short enough for the refresh to happen within the scenario.
const TOKEN_LIFETIME: Duration = Duration::from_secs(3);

#[given("a connected client using a connection token with expiration")]
async fn a_connected_client_with_expiring_token(world: &mut ClientWorld) {
    let denied = Arc::clone(&world.token_denied);
    let builder = Client::builder(world.server_endpoint())
        .config(Config {
            token: connection_token(USER, TOKEN_LIFETIME),
            ..test_config()
        })
        .get_token(move || {
            let denied = denied.load(Ordering::SeqCst);
            async move {
                if denied {
                    Err(TokenError::Unauthorized)
                } else {
                    Ok(connection_token(USER, TOKEN_LIFETIME))
                }
            }
        });
    world.build_client(builder);
    world.client().connect();
    expect_event(world, "connecting").await;
    let ClientEvent::Connected { client_id, .. } = expect_event(world, "connected").await else {
        unreachable!("expect_event checked the name");
    };
    world.client_id = Some(client_id);
}

#[when(r#"the token callback signals "unauthorized" during refresh"#)]
fn token_callback_signals_unauthorized(world: &mut ClientWorld) {
    world.token_denied.store(true, Ordering::SeqCst);
}
