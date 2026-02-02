use std::{
    fmt::Write,
    ops::{Deref, DerefMut},
};

use crossterm::event::KeyModifiers;
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Color, Modifier, Style, Styled, Stylize},
    text::{Line, Text},
    widgets::{Block, Borders, Cell, Row, Table, TableState},
};

#[derive(Debug, Clone, Default)]
pub struct CyclingTableState<K> {
    state: TableState,
    keys: Vec<K>,
    pub colwidths: Vec<u16>,
    prev_selected: Option<K>,
    next_page_jump: usize,
}

impl<K: Clone + PartialEq> CyclingTableState<K> {
    pub fn with_prev_slected(mut self, prev_selected: K) -> Self {
        self.prev_selected = Some(prev_selected);
        self
    }

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
            if selected == 0 {
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

    pub fn render<'a, 'b, S: Into<Text<'b>>>(
        &mut self,
        title: Option<String>,
        header: impl Iterator<Item = S>,
        rows: impl Iterator<Item = (K, Vec<Text<'a>>)>,
        frame: &mut Frame,
        area_: Rect,
    ) {
        self.keys.clear();
        let mut b = Block::default().borders(Borders::ALL);
        if let Some(title) = title {
            b = b.title(title);
        }
        let area = b.inner(area_);
        frame.render_widget(b, area_);

        let mut drawable_header = vec![];
        let mut header_height = 0;
        for (ih, h) in header.enumerate() {
            let mut s = String::new();
            let h = h.into();
            let mut height = 0;
            for line in &h.lines {
                let _ = writeln!(&mut s, "▏ {line} ");
                height += 1;
                if ih >= self.colwidths.len() {
                    self.colwidths.push(0);
                }
                self.colwidths[ih] = self.colwidths[ih].max(3 + line.width() as u16);
            }
            header_height = header_height.max(height);
            drawable_header.push(Cell::from(Text::from(s)));
        }
        let n_cols = self.colwidths.len() + 1;
        let drawable_header = Row::new(drawable_header).height(header_height).underlined();
        let mut drawable_rows = vec![];
        for (i, (key, row)) in rows.into_iter().enumerate() {
            self.keys.push(key.clone());
            let mut row_height = row.iter().map(|l| l.lines.len()).max().unwrap();
            let mut cells = vec![];
            for (ic, column) in row.into_iter().enumerate() {
                assert!(n_cols > ic, "rows are not the same length");
                let mut height = 0;
                let mut s = Text::default();
                for line in column.lines {
                    s.push_line(Line::from(format!("▏ {line} ")));
                    height += 1;
                    if ic != n_cols - 1 {
                        self.colwidths[ic] = self.colwidths[ic].max(3 + line.width() as u16);
                    }
                }
                for _ in 0..row_height - height {
                    s.push_line(Line::from("▏"));
                }
                row_height = row_height.max(height);

                cells.push(Cell::from(s.set_style(column.style)));
            }
            let mut row = Row::new(cells).height(row_height as u16);
            if self.prev_selected.as_ref().is_some_and(|k| key == *k) {
                row = row.style(Style::default().underlined().underline_color(Color::Red).bold());
                if self.state.selected().is_none() {
                    self.state.select(Some(i))
                }
            }
            drawable_rows.push(row);
        }

        if self.state.selected().is_none() && !self.keys.is_empty() {
            self.select_first();
        }
        let n_rows = drawable_rows.len();

        let mut column_widths =
            self.colwidths.iter().map(|i| Constraint::Length(*i)).collect::<Vec<_>>();
        *column_widths.last_mut().unwrap() = Constraint::Fill(1);
        let table = Table::new(drawable_rows, column_widths)
            .header(drawable_header)
            .style(Style::new())
            .footer(Row::new(vec![Cell::new(format!("{n_rows} rows"))]))
            .column_spacing(0)
            .row_highlight_style(
                Style::default().add_modifier(Modifier::REVERSED).fg(Color::DarkGray),
            );

        self.next_page_jump = (area_.height as usize).saturating_sub(5).min(self.keys.len());
        frame.render_stateful_widget(table, area, &mut self.state)
    }
    pub fn select(&mut self, key: K) {
        let Some(id) = self.keys.iter().enumerate().find_map(|(i, k)| (*k == key).then_some(i))
        else {
            return;
        };
        self.state.select(Some(id));
    }
    pub fn width(&self) -> u16 {
        self.colwidths.iter().sum::<u16>() + 2
    }
}
impl<K: PartialOrd> CyclingTableState<K> {
    pub fn select_closest_larger(&mut self, key: K) {
        let Some(id) = self.keys.iter().enumerate().find_map(|(i, k)| (*k >= key).then_some(i))
        else {
            return;
        };
        self.state.select(Some(id));
    }
    pub fn select_closest_smaller(&mut self, key: K) {
        let Some(id) =
            self.keys.iter().enumerate().rev().find_map(|(i, k)| (*k <= key).then_some(i))
        else {
            return;
        };
        self.state.select(Some(id));
    }
}

impl<K> Deref for CyclingTableState<K> {
    type Target = TableState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl<K> DerefMut for CyclingTableState<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
