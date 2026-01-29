use hegel::gen::{self, Generate};
use nix_test::Rectangle;

fn rectangles() -> impl Generate<Rectangle> {
    gen::tuples(gen::integers::<u32>(), gen::integers::<u32>())
        .map(|(w, h)| Rectangle::new(w, h))
}

#[test]
fn test_nix_integration_marker() {
    // CI verifies this marker appears in build logs to confirm tests actually ran
    println!("NIX_INTEGRATION_TEST_EXECUTED");
}

#[test]
fn test_area_is_product_of_sides() {
    hegel::hegel(|| {
        let rect = rectangles().generate();
        assert_eq!(rect.area(), rect.width as u64 * rect.height as u64);
    });
}

#[test]
fn test_perimeter_is_twice_sum_of_sides() {
    hegel::hegel(|| {
        let rect = rectangles().generate();
        assert_eq!(rect.perimeter(), 2 * (rect.width as u64 + rect.height as u64));
    });
}

#[test]
fn test_square_has_equal_sides() {
    hegel::hegel(|| {
        let side = gen::integers::<u32>().generate();
        let square = Rectangle::new(side, side);
        assert!(square.is_square());
    });
}

#[test]
fn test_area_fits_in_u64() {
    hegel::hegel(|| {
        let rect = rectangles().generate();
        // This should never overflow since we're using u64 for area
        let _ = rect.area();
    });
}
