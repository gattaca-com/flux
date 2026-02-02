use std::ops::{Deref, DerefMut};

use ratatui::{
    Frame,
    layout::Rect,
    style::{Style, Stylize},
    widgets::{List, ListItem, ListState},
};

#[derive(Debug, Clone)]
pub struct CyclingListState {
    state: ListState,
    padding: usize,
    n: usize,
}

impl Default for CyclingListState {
    fn default() -> Self {
        Self { state: Default::default(), padding: 2, n: 0 }
    }
}

impl CyclingListState {
    pub fn select_next(&mut self) {
        if let Some(selected) = self.state.selected() &&
            selected == self.n.saturating_sub(1)
        {
            self.state.select(Some(0));
            return;
        }
        self.state.select_next();
    }

    pub fn select_previous(&mut self) {
        if let Some(selected) = self.state.selected() &&
            selected == 0
        {
            self.state.select(Some(self.n));
            return;
        }
        self.state.select_previous();
    }

    pub fn render<'a, T>(&mut self, items: T, frame: &mut Frame, area: Rect)
    where
        T: IntoIterator,
        T::Item: Into<ListItem<'a>>,
    {
        if self.selected().is_none() {
            self.select_first();
        }
        let selected_style = Style::default().bold();
        let list = List::new(items)
            .highlight_style(selected_style)
            .highlight_symbol(">")
            .highlight_spacing(ratatui::widgets::HighlightSpacing::Always)
            .scroll_padding(self.padding);
        self.n = list.len();
        frame.render_stateful_widget(list, area, &mut self.state)
    }
}

impl Deref for CyclingListState {
    type Target = ListState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for CyclingListState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
