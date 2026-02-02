use serde::{Deserialize, Serialize};

bitflags::bitflags! {
    #[repr(transparent)]
    #[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
    pub struct RenderFlags: u32 {
        const ShowMin = 1 << 0;
        const ShowMax = 1 << 1;
        const ShowMedian = 1 << 2;
        const ShowAverages = 1 << 3;
    }
}
impl Default for RenderFlags {
    fn default() -> Self {
        RenderFlags::ShowMedian
    }
}
