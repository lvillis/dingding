use std::collections::{BTreeSet, VecDeque};

use super::StreamFrame;
use crate::bot::ConversationScope;

/// Ordering of accepted business frames on one Stream connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StreamOrdering {
    /// Runs one handler at a time in arrival order, across all conversations.
    Serial,
    /// Runs handlers concurrently without preserving conversation order.
    Concurrent,
    /// Preserves order within each bot conversation while different conversations
    /// run concurrently, up to `max_concurrent_handlers`.
    ///
    /// Bot messages require a known scope and conversation id. Other frames and
    /// messages without that identity share one conservative serial lane. This
    /// ordering is local to a single runner, not distributed across replicas.
    #[default]
    PerConversation,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ConversationKey {
    Unknown,
    Group(String),
    Private(String),
}

pub(super) struct QueuedFrame {
    pub id: u64,
    pub frame: StreamFrame,
    key: Option<ConversationKey>,
}

pub(super) struct FrameSlot {
    pub id: u64,
    key: Option<ConversationKey>,
}

impl QueuedFrame {
    pub fn new(id: u64, frame: StreamFrame, ordering: StreamOrdering) -> Self {
        let key = match ordering {
            StreamOrdering::Concurrent => None,
            StreamOrdering::Serial => Some(ConversationKey::Unknown),
            StreamOrdering::PerConversation => Some(conversation_key(&frame)),
        };
        Self { id, frame, key }
    }
}

fn conversation_key(frame: &StreamFrame) -> ConversationKey {
    let Ok(Some(event)) = frame.bot_event() else {
        return ConversationKey::Unknown;
    };
    let Some(id) = event
        .open_conversation_id
        .filter(|id| !id.trim().is_empty())
    else {
        return ConversationKey::Unknown;
    };
    match event.conversation_scope {
        ConversationScope::Group => ConversationKey::Group(id),
        ConversationScope::Private => ConversationKey::Private(id),
        _ => ConversationKey::Unknown,
    }
}

/// Waiting work does not consume execution slots. Search the bounded queue for
/// the earliest free lane so a busy conversation cannot block every other one.
#[derive(Default)]
pub(super) struct FrameQueue {
    waiting: VecDeque<QueuedFrame>,
    active: BTreeSet<ConversationKey>,
}

impl FrameQueue {
    pub fn len(&self) -> usize {
        self.waiting.len()
    }

    pub fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }

    pub fn ready(&self, frame: &QueuedFrame) -> bool {
        frame
            .key
            .as_ref()
            .is_none_or(|key| !self.active.contains(key))
    }

    pub fn push(&mut self, frame: QueuedFrame) {
        self.waiting.push_back(frame);
    }

    pub fn pop_ready(&mut self) -> Option<QueuedFrame> {
        let index = self.waiting.iter().position(|frame| self.ready(frame))?;
        self.waiting.remove(index)
    }

    pub fn start(&mut self, frame: QueuedFrame) -> (FrameSlot, StreamFrame) {
        if let Some(key) = &frame.key {
            self.active.insert(key.clone());
        }
        (
            FrameSlot {
                id: frame.id,
                key: frame.key,
            },
            frame.frame,
        )
    }

    pub fn finish(&mut self, frame: &FrameSlot) {
        if let Some(key) = &frame.key {
            self.active.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::BOT_MESSAGE_TOPIC;

    fn work(
        id: u64,
        scope: &str,
        conversation: Option<&str>,
        ordering: StreamOrdering,
    ) -> QueuedFrame {
        let frame = StreamFrame::from_text(&serde_json::json!({
            "type": "CALLBACK", "headers": { "topic": BOT_MESSAGE_TOPIC, "messageId": id.to_string() },
            "data": { "conversationType": scope, "conversationId": conversation, "msgtype": "text", "text": { "content": "/run" } }
        }).to_string()).expect("frame");
        QueuedFrame::new(id, frame, ordering)
    }

    #[test]
    fn busy_lane_does_not_hide_other_conversations_and_preserves_its_order() {
        let ordering = StreamOrdering::PerConversation;
        let mut queue = FrameQueue::default();
        let (active, _) = queue.start(work(0, "2", Some("a"), ordering));
        queue.push(work(1, "2", Some("a"), ordering));
        queue.push(work(2, "2", Some("a"), ordering));
        queue.push(work(3, "2", Some("b"), ordering));
        queue.push(work(4, "1", Some("a"), ordering));
        let next = queue.pop_ready().expect("different group");
        assert_eq!(next.id, 3);
        let (other, _) = queue.start(next);
        let next = queue.pop_ready().expect("private conversation");
        assert_eq!(next.id, 4);
        let (private, _) = queue.start(next);
        assert!(queue.pop_ready().is_none());
        queue.finish(&active);
        let next = queue.pop_ready().expect("same group resumes");
        assert_eq!(next.id, 1);
        let (first, _) = queue.start(next);
        assert!(queue.pop_ready().is_none());
        queue.finish(&first);
        assert_eq!(queue.pop_ready().expect("last group message").id, 2);
        queue.finish(&other);
        queue.finish(&private);
        assert!(queue.is_empty());
    }

    #[test]
    fn absent_or_unknown_identities_share_a_serial_lane() {
        let ordering = StreamOrdering::PerConversation;
        let mut queue = FrameQueue::default();
        let (active, _) = queue.start(work(0, "2", None, ordering));
        for (scope, id) in [("1", None), ("2", Some("  ")), ("future", Some("a"))] {
            assert!(!queue.ready(&work(1, scope, id, ordering)));
        }
        let generic = StreamFrame::from_text(
            r#"{"type":"EVENT","headers":{"topic":"test","messageId":"test"},"data":{}}"#,
        )
        .expect("frame");
        assert!(!queue.ready(&QueuedFrame::new(2, generic, ordering)));
        let malformed = StreamFrame::from_text(&serde_json::json!({
            "type":"CALLBACK", "headers":{"topic":BOT_MESSAGE_TOPIC,"messageId":"bad"}, "data":"not json"
        }).to_string()).expect("frame");
        assert!(!queue.ready(&QueuedFrame::new(3, malformed, ordering)));
        assert!(queue.ready(&work(4, "2", Some("known"), ordering)));
        queue.finish(&active);
        assert!(queue.ready(&work(5, "2", None, ordering)));
    }

    #[test]
    fn explicit_ordering_modes_do_not_depend_on_identity() {
        for ordering in [StreamOrdering::Serial, StreamOrdering::Concurrent] {
            let mut queue = FrameQueue::default();
            let (active, _) = queue.start(work(0, "2", Some("a"), ordering));
            for id in [Some("a"), Some("b"), None] {
                assert_eq!(
                    queue.ready(&work(1, "2", id, ordering)),
                    ordering == StreamOrdering::Concurrent
                );
            }
            queue.finish(&active);
            assert!(queue.ready(&work(1, "2", Some("a"), ordering)));
        }
    }
}
