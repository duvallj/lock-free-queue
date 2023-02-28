use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::sync::atomic::AtomicPtr;
use core::sync::atomic::Ordering;
use std::marker::PhantomPinned;

#[repr(C)]
pub struct QueueNode<T> {
    next: AtomicPtr<QueueNode<T>>,
    data: T,
    _marker: PhantomPinned,
}

pub struct Queue<T> {
    // INVARIANT: head_ptr is always a valid pointer
    // INVARANT: if head_ptr == tail_ptr, then either (1) tail is "behind" or (2) the queue is
    // empty
    head_ptr: AtomicPtr<QueueNode<T>>,
    // INVARIANT: in an up-to-date tail, we are always pointing at something with a NULL next
    // pointer.
    // INVARIANT: tail_ptr is always a valid pointer
    tail_ptr: AtomicPtr<QueueNode<T>>,
    // dummy node, only for initialization purposes
    dummy: AtomicPtr<QueueNode<T>>,
    _marker: PhantomPinned,
}

impl<T> QueueNode<T> {
    pub fn new(data: T) -> Self {
        Self {
            next: Default::default(),
            data,
            _marker: PhantomPinned,
        }
    }
}

impl<T> Queue<T> {
    // SAFETY: caller must guarantee that (1) struct is never moved once created (2) .init is
    // called after creation
    pub unsafe fn new() -> Self {
        Self {
            head_ptr: Default::default(),
            tail_ptr: Default::default(),
            dummy: Default::default(),
            _marker: PhantomPinned,
        }
    }

    // SAFETY: this must only be called once, right after calling ::new()
    // SAFETY: this must be called before any other thread has the opportunity to call .push or
    // .pop
    pub unsafe fn init(self: Pin<&mut Self>) {
        let pself = self.get_unchecked_mut();
        // SAFETY: only the first argument of QueueNode<T> is used for the dummy node, which is
        // exactly AtomicPtr<QueueNode<T>
        let ptr = &mut pself.dummy as *mut AtomicPtr<QueueNode<T>> as *mut QueueNode<T>;
        // SAFETY: these writes don't move any data
        pself.head_ptr.store(ptr, Ordering::Release);
        pself.tail_ptr.store(ptr, Ordering::Release);
    }

    pub fn push_node(self: Pin<&Self>, new_node: Pin<&mut QueueNode<T>>) {
        debug_assert!(new_node.next.load(Ordering::Acquire) == core::ptr::null_mut());
        // SAFETY: we do not move data
        let new_ptr = unsafe { new_node.get_unchecked_mut() } as *mut _;

        // Attempt to place new_node as the tail.next, returning the pointer to the old tail when we do
        let raw_tail_ptr = loop {
            let raw_tail_ptr = self.tail_ptr.load(Ordering::Acquire);
            // SAFETY: tail_ptr always valid, and first field of the struct is the next pointer
            let tail_next_ptr = raw_tail_ptr.cast::<AtomicPtr<QueueNode<T>>>();
            // SAFETY: self.tail_ptr always valid
            let tail_next = unsafe { &*tail_next_ptr };
            let raw_tail_next_ptr = tail_next.load(Ordering::Acquire);
            if raw_tail_next_ptr == core::ptr::null_mut() {
                // We are at the true end, attempt to swap ourselves in
                if let Ok(_) = tail_next.compare_exchange(
                    core::ptr::null_mut(),
                    new_ptr,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    break raw_tail_ptr;
                }
            } else {
                // We are not at the end, "help" the tail along
                let _ = self.tail_ptr.compare_exchange(
                    raw_tail_ptr,
                    raw_tail_next_ptr,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
            }
        };

        // We have successfully placed new_node onto the queue!
        // Attempt to set self.tail to the new node. If we don't succeed, no problem, that
        // just means someone else has pushed after us (and will succeed in setting the
        // tail). We cannot get popped because the tail needs to be set before this node
        // can even be accessed
        let _ = self.tail_ptr.compare_exchange(
            raw_tail_ptr,
            new_ptr,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    pub fn pop(self: Pin<&Self>) -> Option<T> {
        loop {
            let raw_head_ptr = self.head_ptr.load(Ordering::Acquire);
            let raw_tail_ptr = self.tail_ptr.load(Ordering::Acquire);
            // SAFETY: self.head_ptr is valid, and the layout of the struct guarantees that the
            // first field is the next_ptr
            let head_next_ptr = raw_head_ptr.cast::<AtomicPtr<QueueNode<T>>>();
            // SAFETY: self.head_ptr is always valid
            let head_next = unsafe { &*head_next_ptr };
            let raw_head_next_ptr = head_next.load(Ordering::Acquire);

            if raw_head_ptr as usize == raw_tail_ptr as usize {
                // If we've reached the end of the queue, we are empty
                if raw_head_next_ptr == core::ptr::null_mut() {
                    return None;
                }

                // Otherwise, tail has fallen behind, help it along
                let _ = self.tail_ptr.compare_exchange(
                    raw_tail_ptr,
                    raw_head_next_ptr,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
            } else {
                // Value needs to be read (and moved out of the node) before
                // attempting to dequeue. But, we don't want to run the destructor if this isn't
                // actually the value to be popped and we don't return to move the value out
                // SAFETY: raw_head_next_ptr != NULL because head_ptr != tail_ptr
                // SAFETY: we only *really* take ownership if we pass the compare_exchange, so this
                // is ok
                let data = ManuallyDrop::new(unsafe { core::ptr::read(raw_head_next_ptr) }.data);
                if let Ok(_) = self.head_ptr.compare_exchange(
                    raw_head_ptr,
                    raw_head_next_ptr,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    return Some(ManuallyDrop::into_inner(data));
                }
            }
        }
    }
}

#[cfg(not(feature = "no_std"))]
mod with_std {
    use super::*;
    impl<T> Queue<T> {
        pub fn new_pinned() -> Pin<Box<Self>> {
            // SAFETY: contracts of ::new() and .init() are upheld
            unsafe {
                let mut q = Box::pin(Self::new());
                q.as_mut().init();
                q
            }
        }

        pub fn push(self: Pin<&Self>, node: &mut Pin<Box<QueueNode<T>>>) {
            self.push_node(node.as_mut());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_single_threaded() {
        let q = Queue::<i32>::new_pinned();
        let q = q.as_ref();

        macro_rules! push {
            ($i:expr) => {
                let mut node = Box::pin(QueueNode::new($i));
                q.push(&mut node);
            };
        }

        assert_eq!(q.pop(), None, "initial queue should return None");
        push!(1);
        push!(2);
        push!(3);
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        push!(4);
        assert_eq!(q.pop(), Some(3));
        assert_eq!(q.pop(), Some(4));
        assert_eq!(q.pop(), None, "final drained queue should return None");
    }
}
