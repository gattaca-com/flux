pub mod app;
pub mod render;

use std::io::stdout;
use std::path::Path;

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    ExecutableCommand,
};
use ratatui::prelude::*;

use app::View;

/// Process a key event, returning `true` if the application should quit.
fn handle_key(app: &mut app::App, key: KeyEvent) -> bool {
    if key.kind != KeyEventKind::Press {
        return false;
    }

    if app.show_help {
        app.show_help = false;
    }

    let confirming = match &app.view {
        View::List => app.confirm_cleanup || app.confirm_cleanup_all,
        View::Detail(d) => d.confirm_cleanup,
    };
    if confirming {
        match key.code {
            KeyCode::Enter => {
                if app.confirm_cleanup_all {
                    app.request_cleanup_all();
                } else {
                    app.request_cleanup();
                }
            }
            _ => app.cancel_cleanup(),
        }
        return false;
    }

    match &app.view {
        View::List => match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return true,
            KeyCode::Char('?') => app.toggle_help(),
            KeyCode::Up | KeyCode::Char('k') => app.previous(),
            KeyCode::Down | KeyCode::Char('j') => app.next(),
            KeyCode::Enter => app.enter(),
            KeyCode::Char('d') => app.request_cleanup(),
            KeyCode::Char('D') => app.request_cleanup_all(),
            KeyCode::Char('r') => app.refresh(),
            _ => {}
        },
        View::Detail(_) => match key.code {
            KeyCode::Char('q') => return true,
            KeyCode::Esc | KeyCode::Backspace => app.back(),
            KeyCode::Char('?') => app.toggle_help(),
            KeyCode::Char('d') => app.request_cleanup(),
            KeyCode::Char('D') => app.request_cleanup_all(),
            KeyCode::Char('r') => app.refresh(),
            KeyCode::Up | KeyCode::Char('k') => app.previous(),
            KeyCode::Down | KeyCode::Char('j') => app.next(),
            _ => {}
        },
    }

    false
}

pub fn run(base_dir: &Path, app_filter: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let mut app = app::App::new(base_dir, app_filter);

    loop {
        terminal.draw(|frame| render::render(frame, &mut app))?;

        if event::poll(std::time::Duration::from_millis(250))?
            && let Event::Key(key) = event::read()?
            && handle_key(&mut app, key)
        {
            break;
        }
        app.tick();
    }

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
