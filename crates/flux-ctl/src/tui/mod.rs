pub mod app;
pub mod render;

use std::io::stdout;
use std::path::Path;
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    ExecutableCommand,
};
use ratatui::prelude::*;

pub fn run(base_dir: &Path, app_filter: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let mut app = app::App::new(base_dir, app_filter);

    loop {
        terminal.draw(|frame| render::render(frame, &mut app))?;

        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    if app.show_help {
                        // Any key dismisses help
                        app.show_help = false;
                    } else {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => break,
                            KeyCode::Char('?') => app.toggle_help(),
                            KeyCode::Up | KeyCode::Char('k') => app.previous(),
                            KeyCode::Down | KeyCode::Char('j') => app.next(),
                            KeyCode::Enter => app.toggle_expand(),
                            KeyCode::Char('r') => app.refresh(),
                            _ => {}
                        }
                    }
                }
            }
        }
        app.tick();
    }

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
