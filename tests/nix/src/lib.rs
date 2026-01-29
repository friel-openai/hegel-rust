/// A simple rectangle for testing hegel generators
#[derive(Debug, Clone)]
pub struct Rectangle {
    pub width: u32,
    pub height: u32,
}

impl Rectangle {
    pub fn new(width: u32, height: u32) -> Self {
        Self { width, height }
    }

    pub fn area(&self) -> u64 {
        self.width as u64 * self.height as u64
    }

    pub fn perimeter(&self) -> u64 {
        2 * (self.width as u64 + self.height as u64)
    }

    pub fn is_square(&self) -> bool {
        self.width == self.height
    }
}
