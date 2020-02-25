use crate::BankEvent::FundsWithdrawn;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
struct FundsWithdrawnPayload {
    account_id: String,
    amount: usize,
    correlation: Uuid,
}

#[derive(Deserialize, Serialize)]
struct FundsDepositedPayload {
    account_id: String,
    amount: usize,
    correlation: Uuid,
}

enum BankEvent {
    FundsWithdrawn(FundsWithdrawnPayload),
    FundsDeposited(FundsDepositedPayload),
}

impl BankEvent {
    fn into_eventstore_event(self) -> eventstore::EventData {
        match self {
            BankEvent::FundsWithdrawn(payload) => {
                eventstore::EventData::json("funds-withdrawn", payload)
                    .expect("Relax our json is valid!")
            }

            BankEvent::FundsDeposited(payload) => {
                eventstore::EventData::json("funds-deposited", payload)
                    .expect("Relax our json is valid!")
            }
        }
    }

    fn from_eventstore_event(event: eventstore::ResolvedEvent) -> Self {
        let event = event.get_original_event();

        match event.event_type.as_str() {
            "funds-withdrawn" => {
                let payload = event.as_json().expect("Trust me, I'm an engineer");

                BankEvent::FundsWithdrawn(payload)
            }

            "funds-deposited" => {
                let payload = event.as_json().expect("Trust me, I'm an engineer");

                BankEvent::FundsDeposited(payload)
            }

            _ => unreachable!(),
        }
    }
}

struct Command {
    correlation: Uuid,
    tpe: BankCommand,
}

enum BankCommand {
    WithdrawFunds(String, usize),
    DepositFunds(String, usize),
}

#[derive(Debug, Clone)]
struct AccountData {
    account_id: String,
    balance: i64,
    generation: usize,
}

impl AccountData {
    fn new(id: String) -> Self {
        AccountData {
            account_id: id,
            balance: 0,
            generation: 0,
        }
    }
}

struct AccountError {
    correlation: Uuid,
    tpe: AccountErrorType,
}

enum AccountErrorType {
    NotEnoughFunds,
}

impl AccountData {
    fn apply_bank_event(mut self, event: BankEvent) -> Self {
        match event {
            BankEvent::FundsWithdrawn(payload) => {
                self.balance -= payload.amount as i64;
                self.generation += 1;

                self
            }

            BankEvent::FundsDeposited(payload) => {
                self.balance += payload.amount as i64;
                self.generation += 1;

                self
            }
        }
    }

    fn handle_bank_command(&self, cmd: Command) -> Result<BankEvent, AccountError> {
        match cmd.tpe {
            BankCommand::DepositFunds(account_id, amount) => {
                let payload = FundsDepositedPayload {
                    account_id,
                    amount,
                    correlation: cmd.correlation,
                };

                Ok(BankEvent::FundsDeposited(payload))
            }

            BankCommand::WithdrawFunds(account_id, amount) => {
                if self.balance - (amount as i64) < 0 {
                    let err = AccountError {
                        correlation: Uuid::new_v4(),
                        tpe: AccountErrorType::NotEnoughFunds,
                    };

                    Err(err)
                } else {
                    let payload = FundsWithdrawnPayload {
                        account_id,
                        amount,
                        correlation: cmd.correlation,
                    };

                    Ok(BankEvent::FundsWithdrawn(payload))
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt;

    let account_id = "12345-6789-00000".to_string();
    let account_stream_name = format!("account-{}", account_id);
    let addr = "127.0.0.1:1113".parse()?;
    let connection = eventstore::Connection::builder()
        .single_node_connection(addr)
        .await;

    let events = connection
        .read_stream(account_stream_name.as_str())
        .iterate_over();

    let cmd1 = Command {
        correlation: Uuid::new_v4(),
        tpe: BankCommand::DepositFunds(account_id.clone(), 3_000),
    };

    let cmd2 = Command {
        correlation: Uuid::new_v4(),
        tpe: BankCommand::WithdrawFunds(account_id.clone(), 500),
    };

    let seed = AccountData::new(account_id);

    // We don't really project a stream state asynchronously besides fetching events from the
    // database but you get the idea.
    let account = events
        .fold(seed, |state, item| async {
            // For simplicity sake, we presume everything went fine when fetching events from the
            // database.
            let event = item.expect("Trust me, everything will be fine!");

            state.apply_bank_event(BankEvent::from_eventstore_event(event))
        })
        .await;

    let commands = futures::stream::iter(vec![cmd1, cmd2]);

    // Same as when we load an account aggregate, we don't really handle a command asynchronously.
    // One could call an external service there.
    let events = commands.filter_map(|cmd| async {
        match account.handle_bank_command(cmd) {
            Err(error) => {
                // Considering we have a correlation id in an 'AccountError', we might notify
                // a different application that a banking operation went wrong.
                None
            }

            Ok(event) => Some(event.into_eventstore_event()),
        }
    });

    // We decide to persist events to the database. Note that even if we know that we don't have that
    // many events in our example, that snippet shows that we could batch those events before saving, for
    // better performance.
    events
        .chunks(1_000)
        .for_each(|batch| async {
            connection
                .write_events(account_stream_name.clone())
                .append_events(batch)
                .execute()
                .await
                .expect("In the database, we trust!");
        })
        .await;

    // At that point, everything is saved in EventStore!
    Ok(())
}
