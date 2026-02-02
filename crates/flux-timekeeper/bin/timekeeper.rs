use std::{
    env,
    io::stdout,
    panic,
    sync::atomic::{AtomicBool, Ordering},
};

use crossterm::{
    ExecutableCommand,
    cursor::Show,
    event::{
        self, DisableMouseCapture, EnableMouseCapture, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers, MouseEventKind,
    },
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use flux::{
    timing::{Duration, Nanos},
    utils::vsync,
};
use flux_timekeeper::{
    timekeeper::TimeKeeper,
    tui::{PlotSettings, XAxisSpec},
};
use ratatui::{Frame, Terminal, prelude::CrosstermBackend};
use signal_hook::{consts::SIGTERM, low_level};

#[derive(Debug, Clone)]
struct TimekeeperTUI {
    timekeeper: TimeKeeper,
    app_name: String,
    plot_settings: PlotSettings,
    prev_row: u16,
    prev_column: u16,
}

impl TimekeeperTUI {
    pub fn new(app_name: String) -> Self {
        Self {
            app_name,
            timekeeper: Default::default(),
            plot_settings: PlotSettings::new(
                Nanos::now() - Nanos::from_secs(12),
                Nanos::now(),
                128,
                XAxisSpec::UtcTime,
            ),
            prev_row: 0,
            prev_column: 0,
        }
    }
    pub fn update(&mut self) {
        self.timekeeper.update(&self.app_name)
    }
    pub fn render(&mut self, frame: &mut Frame) {
        self.plot_settings.set_range_to_now();
        self.timekeeper.render(frame.area(), frame, &self.plot_settings);
    }

    pub fn handle_key_events(&mut self, code: KeyCode, _modifiers: KeyModifiers) {
        match code {
            KeyCode::Char('z') => self.plot_settings.zoom_in(),
            KeyCode::Char('Z') => self.plot_settings.zoom_out(),
            KeyCode::Char(',') => self.plot_settings.pan_left(1.0),
            KeyCode::Char('.') => self.plot_settings.pan_right(1.0),
            c => self.timekeeper.handle_key_events(c),
        }
    }
    fn handle_mouse_event(&mut self, event: event::MouseEvent) {
        match (event.kind, event.modifiers) {
            (MouseEventKind::ScrollUp, _) => {
                self.plot_settings.zoom_in();
            }
            (MouseEventKind::ScrollDown, _) => {
                self.plot_settings.zoom_out();
            }
            (MouseEventKind::Drag(event::MouseButton::Left), _)
                if event.column > self.prev_column =>
            {
                let factor = event.column.saturating_sub(self.prev_column) as f64;
                self.plot_settings.pan_left(factor);
            }
            (MouseEventKind::Drag(event::MouseButton::Left), _)
                if event.column < self.prev_column =>
            {
                let factor: f64 = self.prev_column.saturating_sub(event.column) as f64;
                self.plot_settings.pan_right(factor);
            }
            _ => {}
        }
        self.prev_row = event.row;
        self.prev_column = event.column;
    }
}

static TERMINAL_INITIALIZED: AtomicBool = AtomicBool::new(false);

fn cleanup_terminal() {
    if TERMINAL_INITIALIZED.load(Ordering::Relaxed) {
        let _ = stdout().execute(Show); // Show cursor
        let _ = stdout().execute(DisableMouseCapture);
        let _ = stdout().execute(LeaveAlternateScreen);
        let _ = disable_raw_mode();
        TERMINAL_INITIALIZED.store(false, Ordering::Relaxed);
    }
}

fn setup_panic_hook() {
    let original_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        cleanup_terminal();
        original_hook(panic_info);
    }));
}

fn setup_signal_handler() {
    unsafe {
        low_level::register(SIGTERM, || {
            cleanup_terminal();
            std::process::exit(1);
        })
        .expect("Failed to register SIGTERM handler");
    }
}

fn main() {
    setup_panic_hook();
    setup_signal_handler();

    let mut stdout_val = stdout();
    stdout_val.execute(EnterAlternateScreen).unwrap();
    stdout_val.execute(EnableMouseCapture).unwrap();
    enable_raw_mode().unwrap();

    TERMINAL_INITIALIZED.store(true, Ordering::Relaxed);

    let mut terminal = Terminal::new(CrosstermBackend::new(stdout())).unwrap();
    let _ = terminal.clear();

    let app_name = get_arg(&["--app-name"]).unwrap();
    let mut timekeeper_tui = TimekeeperTUI::new(app_name);

    loop {
        if !vsync(Some(Duration::from_millis(16)), || {
            timekeeper_tui.update();
            if let Err(e) = terminal.draw(|frame| {
                timekeeper_tui.render(frame);
            }) {
                println!("issue drawing terminal {e}")
            }

            let mut event = None;
            while event::poll(std::time::Duration::from_micros(0)).is_ok_and(|t| t) {
                event = event::read().ok();
            }

            match event {
                Some(event::Event::Mouse(event)) => timekeeper_tui.handle_mouse_event(event),
                Some(event::Event::Key(KeyEvent {
                    kind: KeyEventKind::Press,
                    code,
                    modifiers,
                    ..
                })) => {
                    if let KeyCode::Char('Q') = &code {
                        return false;
                    }
                    timekeeper_tui.handle_key_events(code, modifiers);
                }
                _ => {}
            }
            true
        }) {
            break;
        };
    }

    cleanup_terminal();
}

fn get_arg(flags: &[&str]) -> Option<String> {
    env::args()
        .enumerate()
        .find_map(|(i, arg)| flags.contains(&arg.as_str()).then_some(i))
        .and_then(|idx| env::args().nth(idx + 1))
}
