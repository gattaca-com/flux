pub mod app;
pub mod render;

use std::{io::stdout, path::Path};

use crossterm::{
    ExecutableCommand,
    event::{self, Event},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::prelude::*;

pub fn run(base_dir: &Path, app_filter: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let mut app = app::App::new(base_dir, app_filter);

    loop {
        terminal.draw(|frame| render::render(frame, &mut app))?;

        // crossterm::event::poll requires std::time::Duration.
        if event::poll(std::time::Duration::from_millis(250))? &&
            let Event::Key(key) = event::read()? &&
            app.handle_key(key)
        {
            break;
        }
        app.tick();
    }

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
