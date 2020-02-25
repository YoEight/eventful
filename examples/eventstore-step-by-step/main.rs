/// Example inspired by F# version from https://medium.com/@dzoukr/event-sourcing-step-by-step-in-f-be808aa0ca18

#[macro_use]
extern crate serde_json;

use chrono::{Utc, DateTime};
use futures::{StreamExt, TryStreamExt};

mod cmd_args {
    use chrono::prelude::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize, Serialize)]
    pub struct AddTask {
        pub id: usize,
        pub name: String,
        pub due_date: Option<DateTime<Utc>>,
    }

    #[derive(Deserialize, Serialize)]
    pub struct RemoveTask {
        pub id: usize,
    }

    #[derive(Deserialize, Serialize)]
    pub struct CompleteTask {
        pub id: usize,
    }

    #[derive(Deserialize, Serialize)]
    pub struct ChangeTaskDueDate {
        pub id: usize,
        pub due_date: Option<DateTime<Utc>>,
    }
}

enum Command {
    AddTask(self::cmd_args::AddTask),
    RemoveTask(self::cmd_args::RemoveTask),
    ClearAllTasks,
    CompleteTask(self::cmd_args::CompleteTask),
    ChangeTaskDueDate(self::cmd_args::ChangeTaskDueDate),
}

enum Event {
    TaskAdded(self::cmd_args::AddTask),
    TaskRemoved(self::cmd_args::RemoveTask),
    AllTasksCleared,
    TaskCompleted(self::cmd_args::CompleteTask),
    TaskDueDateChanged(self::cmd_args::ChangeTaskDueDate),
}

impl Event {
    fn into_eventstore_event(self) -> eventstore::EventData {
        match self {
            Event::TaskAdded(args) => {
                eventstore::EventData::json("task-added", args)
                    .unwrap()
            }

            Event::TaskRemoved(args) => {
                eventstore::EventData::json("task-removed", args)
                    .unwrap()
            }

            Event::TaskCompleted(args) => {
                eventstore::EventData::json("task-completed", args)
                    .unwrap()
            }

            Event::AllTasksCleared => {
                eventstore::EventData::json("all-tasks-cleared", json!({}))
                    .unwrap()
            }

            Event::TaskDueDateChanged(args) => {
                eventstore::EventData::json("task-due-date-changed", args)
                    .unwrap()
            }
        }
    }

    fn from_eventstore_event(event: eventstore::ResolvedEvent) -> Self {
        let event = event.get_original_event();

        match event.event_type.as_str() {
            "task-added" => Event::TaskAdded(event.as_json().unwrap()),
            "task-removed" => Event::TaskRemoved(event.as_json().unwrap()),
            "task-completed" => Event::TaskCompleted(event.as_json().unwrap()),
            "all-tasks-cleared" => Event::AllTasksCleared,
            "task-due-date-changed" => Event::TaskDueDateChanged(event.as_json().unwrap()),
            _ => unreachable!(),
        }
    }
}

struct Task {
    id: usize,
    name: String,
    due_date: Option<DateTime<Utc>>,
    is_complete: bool,
}

struct State {
    tasks: Vec<Task>,
}

#[derive(Debug)]
enum DomainError {
    TaskAlreadyExists,
    TaskDoesNotExist,
    TaskAlreadyFinished,
}

impl State {
    fn default() -> Self {
        State {
            tasks: vec![],
        }
    }

    fn only_if_task_does_not_already_exist(&self, id: usize) -> bool {
        for task in self.tasks.iter() {
            if task.id == id {
                return false;
            }
        }

        true
    }

    fn get_task(&self, id: usize) -> Option<&Task> {
        self.tasks.iter().find(|task| task.id == id)
    }

    fn execute(&self, cmd: Command) -> Result<Vec<Event>, DomainError> {
        let event = match cmd {
            Command::AddTask(args) => {
                if self.only_if_task_does_not_already_exist(args.id) {
                    Ok(Event::TaskAdded(args))
                } else {
                    Err(DomainError::TaskAlreadyExists)
                }
            }

            Command::RemoveTask(args) => {
                if self.get_task(args.id).is_some() {
                    Ok(Event::TaskRemoved(args))
                } else {
                    Err(DomainError::TaskDoesNotExist)
                }
            }

            Command::ClearAllTasks => Ok(Event::AllTasksCleared),

            Command::CompleteTask(args) => {
                if self.get_task(args.id).is_some() {
                    Ok(Event::TaskCompleted(args))
                } else {
                    Err(DomainError::TaskDoesNotExist)
                }
            }

            Command::ChangeTaskDueDate(args) => {
                if let Some(task) = self.get_task(args.id) {
                    if task.is_complete {
                        Err(DomainError::TaskAlreadyFinished)
                    } else {
                        Ok(Event::TaskDueDateChanged(args))
                    }
                } else {
                    Err(DomainError::TaskDoesNotExist)
                }
            }
        }?;

        Ok(vec![event])
    }

    fn apply(&mut self, event: Event) {
        match event {
            Event::TaskAdded(args) => {
                let new_task = Task {
                    id: args.id,
                    name: args.name,
                    due_date: args.due_date,
                    is_complete: false,
                };

                self.tasks.push(new_task);
            }

            Event::TaskRemoved(args) => {
                self.tasks.retain(move|task| task.id != args.id)
            }

            Event::AllTasksCleared => {
                self.tasks.clear()
            }

            Event::TaskCompleted(args) => {
                for task in self.tasks.iter_mut() {
                    if task.id == args.id {
                        task.is_complete = true;
                        break;
                    }
                }
            }

            Event::TaskDueDateChanged(args) => {
                for task in self.tasks.iter_mut() {
                    if task.id == args.id {
                        task.due_date = args.due_date;
                        break;
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use futures::TryStreamExt;

    let addr = "127.0.0.1:1113".parse()?;
    let connection = eventstore::Connection::builder()
        .single_node_connection(addr)
        .await;

    // Get current state from the `tasks` stream.
    let state = connection.read_stream("tasks")
        .iterate_over()
        .try_fold(State::default(), | mut state, event| async {
            state.apply(Event::from_eventstore_event(event));

            Ok(state)
        }).await?;

    let cmds = vec![
        Command::AddTask(self::cmd_args::AddTask{
            id: 42,
            name: "Find the meaning of life".to_string(),
            due_date: None,
        }),

        Command::CompleteTask(self::cmd_args::CompleteTask {
            id: 42,
        }),
    ];

    for cmd in cmds.into_iter() {
        match state.execute(cmd) {
            Err(e) => {
                println!("Command error: {:?}", e);
            }

            Ok(events) => {
                let events = events.into_iter()
                    .map(|evt| evt.into_eventstore_event());

                let _ = connection.write_events("tasks")
                    .append_events(events)
                    .execute()
                    .await?;
            }
        }
    }

    Ok(())
}