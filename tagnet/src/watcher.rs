use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub use notify;
use notify::{
    Event, EventKind, RecommendedWatcher, Watcher, event::{CreateKind, ModifyKind, RemoveKind, RenameMode}
};

#[derive(Clone, Debug)]
pub enum DebouncedEventKind {
    /// Creating of a *file*.
    Create { file_name: PathBuf },
    /// Move of a a *file or directory*.
    Move {
        from: Option<PathBuf>,
        to: Option<PathBuf>,
    },
    /// Modification of a *file*.
    Modify { file_name: PathBuf },
    /// Removal of a *file*.
    Remove { file_name: PathBuf },
}

impl DebouncedEventKind {
    pub fn is_create(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Create { file_name } = self
            && file_name == path.as_ref()
        {
            true
        } else {
            false
        }
    }

    pub fn is_modify(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Modify { file_name } = self
            && file_name == path.as_ref()
        {
            true
        } else {
            false
        }
    }

    pub fn is_move_from_to(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Move { from, to } = self
            && from.is_some()
            && to.as_ref().is_some_and(|to| to == path.as_ref())
        {
            true
        } else {
            false
        }
    }

    pub fn is_move_from(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Move { from, to } = self
            && from.as_ref().is_some_and(|from| from == path.as_ref())
            && to.is_none()
        {
            true
        } else {
            false
        }
    }

    pub fn is_move_to(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Move { from, to } = self
            && from.is_none()
            && to.as_ref().is_some_and(|to| to == path.as_ref())
        {
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Debug)]
pub struct DebouncedEvent {
    pub kind: DebouncedEventKind,
    pub timestamp: Instant,
}

#[derive(Default)]
struct Debouncer {
    queued: Vec<DebouncedEvent>,
}

impl Debouncer {
    pub fn push_raw(&mut self, mut event: Event) {
        let mut split_new_events = Vec::new();
        let timestamp = Instant::now();

        // println!("RAW: {:?}", event);

        // NOTE: This code assumes that events will not be bundled. E.g. two create events being
        // combined into a single event with multiple paths.
        match event.kind {
            EventKind::Create(create_kind) => {
                // We don't track the creation/deletion of directories. Directories are only
                // tracked implicitely through the files that are contained in them.
                if create_kind == CreateKind::File {
                    assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                    let file_name = event.paths.remove(0);
                    split_new_events.push(DebouncedEventKind::Create { file_name });
                }
            }
            EventKind::Modify(modify_kind) => {
                match modify_kind {
                    ModifyKind::Data(_) => {
                        assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                        let file_name = event.paths.remove(0);
                        split_new_events.push(DebouncedEventKind::Modify { file_name });
                    }
                    ModifyKind::Name(rename_mode) => match rename_mode {
                        RenameMode::To => {
                            assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                            let to = event.paths.remove(0);
                            split_new_events.push(DebouncedEventKind::Move {
                                from: None,
                                to: Some(to),
                            });
                        }
                        RenameMode::From => {
                            assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                            let from = event.paths.remove(0);
                            split_new_events.push(DebouncedEventKind::Move {
                                from: Some(from),
                                to: None,
                            });
                        }
                        RenameMode::Both => {
                            assert_eq!(event.paths.len(), 2, "Wrong number of paths");

                            let from = event.paths.remove(0);
                            let to = event.paths.remove(0);

                            split_new_events.push(DebouncedEventKind::Move {
                                from: Some(from),
                                to: Some(to),
                            });
                        }
                        RenameMode::Any | RenameMode::Other => {}
                    },
                    // For now we also ignore metadata changes.
                    ModifyKind::Any | ModifyKind::Metadata(_) | ModifyKind::Other => {}
                }
            }
            EventKind::Remove(remove_kind) => {
                // We don't track the creation/deletion of directories. Directories are only
                // tracked implicitely through the files that are contained in them.
                if remove_kind == RemoveKind::File {
                    assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                    let file_name = event.paths.remove(0);
                    split_new_events.push(DebouncedEventKind::Remove { file_name });
                }
            }
            // Not used, skip adding it.
            EventKind::Any | EventKind::Access(_) | EventKind::Other => {}
        }

        'insert: for new_event in split_new_events {
            // Merge modify + delete events.
            // This results in the modify events being removed.
            if let DebouncedEventKind::Remove { file_name, .. } = &new_event {
                for index in (0..self.queued.len()).rev() {
                    let event = &self.queued[index];

                    // If we find the creation event we stop.
                    if event.kind.is_create(file_name) {
                        break;
                    }

                    if event.kind.is_modify(file_name) {
                        self.queued.remove(index);
                    }
                }
            }

            // Merge create + delete events.
            // This results in them canceling out.
            if let DebouncedEventKind::Remove { file_name, .. } = &new_event
                && let Some(index_from_back) = self
                    .queued
                    .iter()
                    .rev()
                    .position(|event| event.kind.is_create(file_name))
            {
                let index = self.queued.len() - index_from_back - 1;
                self.queued.remove(index);
                // Skip insertion of the remove event.
                continue 'insert;
            }

            // Try to find the pattern that Vim/Neovim create when editing files.
            // The editor will rename the original file with a suffix, create a new file with
            // the new content, and delete the original file. For us, this should just be a
            // `Modify`.
            //
            // TODO: Maybe this matching here is too eager and might cause issues?
            if let DebouncedEventKind::Remove { file_name, .. } = &new_event
                && let Some(rename_index_from_back) = self
                    .queued
                    .iter()
                    .rev()
                    .position(|event| event.kind.is_move_from_to(file_name))
            {
                let rename_index = self.queued.len() - rename_index_from_back - 1;

                let DebouncedEventKind::Move { from, .. } = self.queued[rename_index].kind.clone()
                else {
                    unreachable!();
                };

                if let Some(from) = from
                    && let Some(create_index_from_back) = self
                        .queued
                        .iter()
                        .rev()
                        .position(|event| event.kind.is_create(&from))
                {
                    let create_index = self.queued.len() - create_index_from_back - 1;

                    // Sanity check: can likely be removed in the future.
                    assert!(
                        rename_index < create_index,
                        "Wound Vim/Neovim edit pattern but the order is wrong"
                    );

                    self.queued[create_index].kind = DebouncedEventKind::Modify { file_name: from };
                    self.queued.remove(rename_index);

                    // Skip insertion of the remove event.
                    continue 'insert;
                }
            }

            // Merge multiple moves.
            // This happens when renaming a file withing the synced directory.
            //
            // NOTE: This code relies on the fact that the `Move` with `from` and `to` is emitted
            // after the single `from` and `to` events. It also assumes that there are no events in-between and that `from` is sent before `to`.
            if let DebouncedEventKind::Move { from, to } = &new_event
                && let Some(from) = from
                && let Some(to) = to
            {
                let to_index = self.queued.len().saturating_sub(1);
                let from_index = to_index.saturating_sub(1);

                if let Some(potential_from) = self.queued.get(from_index)
                    && let Some(potential_to) = self.queued.get(to_index)
                    && potential_from.kind.is_move_from(from)
                    && potential_to.kind.is_move_to(to)
                {
                    self.queued.remove(to_index);
                    self.queued.remove(from_index);
                }
            }

            // Merge create + rename.
            // This happens when creating a symlink for example.
            if let DebouncedEventKind::Move { from, to } = &new_event
                && let Some(from) = from
                && let Some(to) = to
            {
                for event in self.queued.iter_mut().rev() {
                    if let DebouncedEventKind::Create { file_name } = &mut event.kind
                        && file_name == from
                    {
                        *file_name = to.clone();
                        // Skip insertion of the rename event.
                        continue 'insert;
                    }
                }
            }

            // Merge create + modify.
            // This happens when piping into a non-existen file for example.
            if let DebouncedEventKind::Modify { file_name } = &new_event {
                for event in self.queued.iter_mut().rev() {
                    if let DebouncedEventKind::Create {
                        file_name: create_file_name,
                    } = &event.kind
                        && create_file_name == file_name
                    {
                        // Skip insertion of the modify event.
                        continue 'insert;
                    }
                }
            }

            // Merge multiple modifies.
            // This will happen all the time due to the fact that both content and metadata
            // modifications create this event.
            if let DebouncedEventKind::Modify { file_name } = &new_event {
                for event in self.queued.iter_mut().rev() {
                    if let DebouncedEventKind::Modify {
                        file_name: modify_file_name,
                    } = &event.kind
                        && modify_file_name == file_name
                    {
                        // Skip insertion of the modify event.
                        continue 'insert;
                    }
                }
            }

            self.queued.push(DebouncedEvent {
                kind: new_event,
                timestamp,
            });
        }
    }

    fn extract_finalized(&mut self) -> Vec<DebouncedEvent> {
        let mut debounced_events = Vec::new();

        self.queued.retain(|event| {
            if event.timestamp.elapsed() > Duration::from_millis(500) {
                // TODO: Optimize to not clone.
                debounced_events.push(event.clone());
                return false;
            }

            true
        });

        debounced_events
    }
}

pub struct WatchDispatcher {
    stop: Arc<AtomicBool>,
    watcher: RecommendedWatcher,
    _task: tokio::task::JoinHandle<()>,
}

impl WatchDispatcher {
    pub fn watcher(&mut self) -> &mut RecommendedWatcher {
        &mut self.watcher
    }
}

impl Drop for WatchDispatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

pub async fn create_dispatcher() -> Result<
    (
        WatchDispatcher,
        tokio::sync::mpsc::UnboundedReceiver<DebouncedEvent>,
    ),
    notify::Error,
> {
    let debouncer = Arc::new(Mutex::new(Debouncer::default()));

    let stop = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let data_clone = debouncer.clone();
    let stop_clone = stop.clone();
    let task = tokio::spawn(async move {
        loop {
            if stop_clone.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;

            let mut debouncer = data_clone.lock().unwrap();
            for event in debouncer.extract_finalized() {
                let _ = sender.send(event);
            }
        }
    });

    let watcher = RecommendedWatcher::new(
        move |result: Result<Event, notify::Error>| {
            if let Ok(event) = result {
                debouncer.lock().unwrap().push_raw(event);
            }
        },
        notify::Config::default(),
    )?;

    Ok((
        WatchDispatcher {
            stop,
            watcher,
            _task: task,
        },
        receiver,
    ))
}
