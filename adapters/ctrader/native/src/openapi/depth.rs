// SPDX-License-Identifier: LGPL-3.0-only

//! Exact, bounded Open API depth quote state, scoped to one connection epoch.

use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Bid,
    Ask,
}

/// Price is in 1/100000 and size in 1/100 units, as supplied by Open API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Quote {
    pub id: u64,
    pub side: Side,
    pub price: u64,
    pub size: u64,
}

pub(crate) struct DepthBook {
    symbol_id: u64,
    max_entries: usize,
    entries: HashMap<u64, Quote>,
    bids: Vec<(u64, u64)>,
    asks: Vec<(u64, u64)>,
    invalid: bool,
    observed: bool,
}

impl DepthBook {
    pub(crate) fn new(symbol_id: u64, max_entries: usize) -> Result<Self, &'static str> {
        if symbol_id == 0 || !(1..=100_000).contains(&max_entries) {
            return Err("invalid depth symbol or capacity");
        }
        Ok(Self {
            symbol_id,
            max_entries,
            entries: HashMap::new(),
            bids: Vec::new(),
            asks: Vec::new(),
            invalid: false,
            observed: false,
        })
    }

    pub(crate) fn apply(
        &mut self,
        symbol_id: u64,
        new_quotes: &[Quote],
        deleted: &[u64],
    ) -> Result<(), &'static str> {
        if let Err(error) = self.apply_inner(symbol_id, new_quotes, deleted) {
            self.invalidate();
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn invalidate(&mut self) {
        self.entries.clear();
        self.bids.clear();
        self.asks.clear();
        self.invalid = true;
    }

    pub(crate) fn capacity(&self) -> usize {
        self.max_entries
    }

    fn apply_inner(
        &mut self,
        symbol_id: u64,
        new_quotes: &[Quote],
        deleted: &[u64],
    ) -> Result<(), &'static str> {
        if self.invalid
            || symbol_id != self.symbol_id
            || new_quotes.len() > self.max_entries
            || deleted.len() > self.max_entries
        {
            return Err("depth symbol or event capacity mismatch");
        }
        let mut seen = HashSet::new();
        for id in deleted {
            if *id == 0 || !seen.insert(*id) {
                return Err("invalid depth deletion");
            }
        }
        let mut added = 0usize;
        for quote in new_quotes {
            if quote.id == 0 || quote.price == 0 || quote.size == 0 || !seen.insert(quote.id) {
                return Err("invalid or duplicate depth quote");
            }
            if !self.entries.contains_key(&quote.id) {
                added += 1;
            }
        }
        // The broker sends changes, not a certified complete snapshot. A
        // repeated deletion or a quote never observed in this epoch is a no-op
        // on our partial view. It must not free nonexistent capacity.
        let removed = deleted
            .iter()
            .filter(|id| self.entries.contains_key(id))
            .count();
        if self.entries.len() - removed + added > self.max_entries {
            return Err("depth capacity exceeded");
        }
        let mut bids = BTreeMap::<u64, u64>::new();
        let mut asks = BTreeMap::<u64, u64>::new();
        for quote in self
            .entries
            .values()
            .filter(|q| !seen.contains(&q.id))
            .chain(new_quotes)
        {
            let levels = match quote.side {
                Side::Bid => &mut bids,
                Side::Ask => &mut asks,
            };
            let quantity = levels.entry(quote.price).or_default();
            *quantity = quantity
                .checked_add(quote.size)
                .ok_or("depth size overflow")?;
        }
        if let (Some(bid), Some(ask)) = (bids.last_key_value(), asks.first_key_value()) {
            if bid.0 > ask.0 {
                return Err("crossed depth book");
            }
        }
        for id in deleted {
            self.entries.remove(id);
        }
        for quote in new_quotes {
            self.entries.insert(quote.id, *quote);
        }
        self.bids = bids.into_iter().rev().collect();
        self.asks = asks.into_iter().collect();
        self.observed = true;
        Ok(())
    }

    pub(crate) fn levels(&self) -> Result<(&[(u64, u64)], &[(u64, u64)]), &'static str> {
        if self.invalid || !self.observed {
            return Err("depth state unavailable");
        }
        Ok((&self.bids, &self.asks))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_updates_and_deletions_in_price_order_without_float_conversion() {
        let mut book = DepthBook::new(7, 8).unwrap();
        book.apply(
            7,
            &[
                Quote {
                    id: 1,
                    side: Side::Bid,
                    price: 123_456,
                    size: 100,
                },
                Quote {
                    id: 2,
                    side: Side::Bid,
                    price: 123_456,
                    size: 50,
                },
                Quote {
                    id: 3,
                    side: Side::Ask,
                    price: 123_500,
                    size: 200,
                },
            ],
            &[],
        )
        .unwrap();
        assert_eq!(book.levels().unwrap().0, vec![(123_456, 150)]);
        assert_eq!(book.levels().unwrap().1, vec![(123_500, 200)]);
        book.apply(7, &[], &[1]).unwrap();
        assert_eq!(book.levels().unwrap().0, vec![(123_456, 50)]);
    }

    #[test]
    fn malformed_or_over_limit_event_clears_state() {
        let mut book = DepthBook::new(7, 1).unwrap();
        assert!(book.levels().is_err());
        let quote = Quote {
            id: 1,
            side: Side::Bid,
            price: 123,
            size: 1,
        };
        book.apply(7, &[quote], &[]).unwrap();
        assert!(book.apply(8, &[], &[]).is_err());
        assert!(book.levels().is_err());
        assert!(book.apply(7, &[quote], &[]).is_err());
        let mut book = DepthBook::new(7, 1).unwrap();
        book.apply(7, &[quote], &[]).unwrap();
        assert!(book.apply(7, &[Quote { id: 2, ..quote }], &[]).is_err());
        assert!(book.levels().is_err());
        assert!(book.apply(7, &[], &[999]).is_err());
    }

    #[test]
    fn rejects_duplicate_ids_crossed_books_and_overflow() {
        let mut book = DepthBook::new(7, 8).unwrap();
        let a = Quote {
            id: 1,
            side: Side::Bid,
            price: 100,
            size: u64::MAX,
        };
        assert!(book.apply(7, &[a, a], &[]).is_err());
        assert!(
            book.apply(
                7,
                &[
                    a,
                    Quote {
                        id: 2,
                        size: 1,
                        ..a
                    }
                ],
                &[]
            )
            .is_err()
        );
        assert!(
            book.apply(
                7,
                &[
                    Quote { size: 1, ..a },
                    Quote {
                        id: 2,
                        side: Side::Ask,
                        price: 99,
                        size: 1
                    }
                ],
                &[]
            )
            .is_err()
        );
    }

    #[test]
    fn replacement_and_deletion_at_capacity_apply_as_one_event() {
        let mut book = DepthBook::new(7, 2).unwrap();
        let a = Quote {
            id: 1,
            side: Side::Bid,
            price: 100,
            size: 1,
        };
        let b = Quote {
            id: 2,
            side: Side::Ask,
            price: 102,
            size: 2,
        };
        book.apply(7, &[a, b], &[]).unwrap();
        book.apply(
            7,
            &[Quote {
                id: 3,
                side: Side::Bid,
                price: 101,
                size: 3,
            }],
            &[1],
        )
        .unwrap();
        assert_eq!(book.levels().unwrap().0, &[(101, 3)]);
        book.apply(
            7,
            &[Quote {
                id: 3,
                size: 4,
                ..a
            }],
            &[],
        )
        .unwrap();
        assert_eq!(book.levels().unwrap().0, &[(100, 4)]);
    }

    #[test]
    fn repeated_and_unobserved_deletions_are_idempotent_partial_book_changes() {
        let mut book = DepthBook::new(7, 2).unwrap();
        let a = Quote {
            id: 1,
            side: Side::Bid,
            price: 100,
            size: 20,
        };
        let b = Quote {
            id: 2,
            side: Side::Ask,
            price: 102,
            size: 30,
        };
        book.apply(7, &[a, b], &[]).unwrap();
        book.apply(7, &[], &[99]).unwrap();
        assert_eq!(book.levels().unwrap().0, &[(100, 20)]);
        book.apply(7, &[], &[1]).unwrap();
        book.apply(7, &[], &[1]).unwrap();
        assert!(book.levels().unwrap().0.is_empty());
        assert_eq!(book.levels().unwrap().1, &[(102, 30)]);
    }

    #[test]
    fn unknown_deletion_does_not_free_capacity_or_underflow_observed_count() {
        let mut book = DepthBook::new(7, 1).unwrap();
        let a = Quote {
            id: 1,
            side: Side::Bid,
            price: 100,
            size: 20,
        };
        book.apply(7, &[a], &[]).unwrap();
        assert!(book.apply(7, &[Quote { id: 2, ..a }], &[99]).is_err());
        assert!(book.levels().is_err());
        let mut empty = DepthBook::new(7, 1).unwrap();
        empty.apply(7, &[], &[99]).unwrap();
        assert!(empty.levels().unwrap().0.is_empty());
    }
}
