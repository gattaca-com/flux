#[macro_export]
macro_rules! safe_panic {
    ($($arg:tt)*) => (if cfg!(debug_assertions) { panic!($($arg)*); } else {tracing::error!($($arg)*)})
}

/// In debug builds, panics on failure just like `debug_assert!`.
/// In release builds, logs an error via `tracing::error!`
/// prefixed with "ASSERT FAILED: " if the condition is false.
#[macro_export]
macro_rules! safe_assert {
    ($cond:expr $(,)?) => {
        #[cfg(debug_assertions)]
        {
            debug_assert!($cond);
        }
        #[cfg(not(debug_assertions))]
        {
            if !$cond {
                tracing::error!("ASSERT FAILED: {}", stringify!($cond));
            }
        }
    };

    ($cond:expr, $($arg:tt)+) => {
        #[cfg(debug_assertions)]
        {
            debug_assert!($cond, $($arg)+);
        }
        #[cfg(not(debug_assertions))]
        {
            if !$cond {
                let msg = format!($($arg)+);
                tracing::error!("ASSERT FAILED: {}", msg);
            }
        }
    };
}

/// In debug builds, panics on failure just like `debug_assert_eq!`.
/// In release builds, logs an error via `tracing::error!`
/// prefixed with "ASSERT FAILED: " if the values aren’t equal.
#[macro_export]
macro_rules! safe_assert_eq {
    // No custom message
    ($left:expr, $right:expr $(,)?) => {
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!($left, $right);
        }
        #[cfg(not(debug_assertions))]
        {
            if !($left == $right) {
                tracing::error!(
                    "ASSERT FAILED: {} (left: `{:?}`, right: `{:?}`)",
                    stringify!($left == $right),
                    &$left,
                    &$right
                );
            }
        }
    };

    ($left:expr, $right:expr, $($arg:tt)+) => {
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!($left, $right, $($arg)+);
        }
        #[cfg(not(debug_assertions))]
        {
            if !($left == $right) {
                let msg = format!($($arg)+);
                tracing::error!("ASSERT FAILED: {}", msg);
            }
        }
    };
}

/// In debug builds, panics on failure just like `debug_assert_ne!`.
/// In release builds, logs an error via `tracing::error!`
/// prefixed with "ASSERT FAILED: " if the values are equal.
#[macro_export]
macro_rules! safe_assert_ne {
    ($left:expr, $right:expr $(,)?) => {
        #[cfg(debug_assertions)]
        {
            debug_assert_ne!($left, $right);
        }
        #[cfg(not(debug_assertions))]
        {
            if !($left != $right) {
                tracing::error!(
                    "ASSERT FAILED: {} (left: `{:?}`, right: `{:?}`)",
                    stringify!($left != $right),
                    &$left,
                    &$right
                );
            }
        }
    };

    ($left:expr, $right:expr, $($arg:tt)+) => {
        #[cfg(debug_assertions)]
        {
            debug_assert_ne!($left, $right, $($arg)+);
        }
        #[cfg(not(debug_assertions))]
        {
            if !($left != $right) {
                let msg = format!($($arg)+);
                tracing::error!("ASSERT FAILED: {}", msg);
            }
        }
    };
}
