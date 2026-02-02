use std::ops::{Deref, DerefMut};

use crossterm::event::KeyModifiers;
use ratatui::{
    Frame,
    layout::Rect,
    style::Style,
    text::Text,
    widgets::{List, ListItem, ListState},
};

#[derive(Debug, Clone, Default)]
pub struct KeyedCyclingListState<K> {
    pub state: ListState,
    keys: Vec<K>,
    pub width: u16,
    next_page_jump: usize,
}

impl<K: Clone + PartialEq + std::fmt::Debug> KeyedCyclingListState<K> {
    pub fn select_next(&mut self, m: KeyModifiers) {
        if let Some(mut selected) = self.state.selected() {
            selected += self.next_page_jump * (m == KeyModifiers::SHIFT) as usize + 1;
            if selected >= self.keys.len() {
                self.state.select(Some(0));
                return;
            } else {
                self.state.select(Some(selected));
                return;
            }
        }
        self.state.select_next();
    }

    pub fn select_previous(&mut self, m: KeyModifiers) {
        if let Some(mut selected) = self.state.selected() {
            if selected == 0 && !self.keys.is_empty() {
                self.state.select(Some(self.keys.len() - 1));
                return;
            } else {
                selected = selected
                    .saturating_sub(self.next_page_jump * (m == KeyModifiers::SHIFT) as usize + 1);
                self.state.select(Some(selected));
                return;
            }
        }

        self.state.select_previous();
    }

    pub fn selected(&self) -> Option<K> {
        self.state.selected().and_then(|i| self.keys.get(i).cloned())
    }

    pub fn render<'a, 'b>(
        &mut self,
        rows: impl Iterator<Item = (K, Text<'a>)>,
        frame: &mut Frame,
        area: Rect,
    ) {
        self.keys.clear();
        let mut drawable_rows = vec![];
        for (key, row) in rows.into_iter() {
            self.keys.push(key.clone());
            self.width = self.width.max(row.width() as u16 + 2);
            drawable_rows.push(ListItem::new(row));
        }

        if self.state.selected().is_none() && !self.keys.is_empty() {
            self.select_first();
        }

        let table = List::new(drawable_rows).style(Style::new()).highlight_symbol(">");
        self.next_page_jump = self.keys.len().min((area.height as usize).saturating_sub(5));
        frame.render_stateful_widget(table, area, &mut self.state)
    }

    pub fn is_last_selected(&self) -> bool {
        self.state.selected().is_some_and(|id| id == self.keys.len() - 1)
    }

    pub fn is_first_selected(&self) -> bool {
        self.state.selected().is_some_and(|id| id == 0)
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn select(&mut self, key: K) {
        let Some(id) = self.keys.iter().enumerate().find_map(|(i, k)| (*k == key).then_some(i))
        else {
            return;
        };
        self.state.select(Some(id));
    }
}

impl<K> Deref for KeyedCyclingListState<K> {
    type Target = ListState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl<K> DerefMut for KeyedCyclingListState<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
