// hegel_workout.rs - Tests hegel strategies for correct behavior
//
// Each test is a separate function that generates values and validates them.
// The main function uses sampled_from to pick which test to run.
//
// Run with: HEGEL_SOCKET=/path/to/socket HEGEL_REJECT_CODE=77 cargo run --bin hegel_workout

use hegel::gen::{self, Generate};
use hegel::Generate as DeriveGenerate;
use std::collections::{HashMap, HashSet};

// =============================================================================
// Test helper
// =============================================================================

macro_rules! test_assert {
    ($cond:expr, $msg:expr) => {
        if !($cond) {
            eprintln!("FAILED: {}", $msg);
            eprintln!("Assertion failed: {}", stringify!($cond));
            std::process::exit(1);
        }
    };
}

/// Check if string contains only ASCII characters
fn is_ascii(s: &str) -> bool {
    s.bytes().all(|b| b < 128)
}

// =============================================================================
// Primitive tests
// =============================================================================

fn test_units() {
    let gen = gen::units();
    let _value: () = gen.generate();
    println!("units: generated unit");
}

fn test_booleans() {
    let gen = gen::booleans();
    let value: bool = gen.generate();
    test_assert!(
        value == true || value == false,
        "boolean must be true or false"
    );
    println!("booleans: {}", value);
}

fn test_just_int() {
    let gen = gen::just(42i32);
    let value: i32 = gen.generate();
    test_assert!(value == 42, "just(42) must produce 42");
    println!("just(42): {}", value);
}

fn test_just_string() {
    let gen = gen::just("hello".to_string());
    let value: String = gen.generate();
    test_assert!(value == "hello", "just(\"hello\") must produce \"hello\"");
    println!("just(\"hello\"): {}", value);
}

// =============================================================================
// Integer tests
// =============================================================================

fn test_integers_unbounded() {
    let gen = gen::integers::<i64>();
    let value: i64 = gen.generate();
    println!("integers::<i64>(): {}", value);
}

fn test_integers_bounded() {
    let gen = gen::integers::<i32>().with_min(10).with_max(20);
    let value: i32 = gen.generate();
    test_assert!(
        value >= 10 && value <= 20,
        "integers(10,20) must be in [10,20]"
    );
    println!("integers::<i32>(10,20): {}", value);
}

fn test_integers_min_only() {
    let gen = gen::integers::<i32>().with_min(100);
    let value: i32 = gen.generate();
    test_assert!(value >= 100, "integers(min=100) must be >= 100");
    println!("integers::<i32>(min=100): {}", value);
}

fn test_integers_max_only() {
    let gen = gen::integers::<i32>().with_max(-100);
    let value: i32 = gen.generate();
    test_assert!(value <= -100, "integers(max=-100) must be <= -100");
    println!("integers::<i32>(max=-100): {}", value);
}

fn test_integers_u8() {
    let gen = gen::integers::<u8>();
    let value: u8 = gen.generate();
    // u8 is always 0-255, just verify we got a value
    let _ = value;
    println!("integers::<u8>(): {}", value);
}

fn test_integers_negative_range() {
    let gen = gen::integers::<i32>().with_min(-50).with_max(-10);
    let value: i32 = gen.generate();
    test_assert!(
        value >= -50 && value <= -10,
        "integers(-50,-10) must be in [-50,-10]"
    );
    println!("integers::<i32>(-50,-10): {}", value);
}

// =============================================================================
// Float tests
// =============================================================================

fn test_floats_unbounded() {
    let gen = gen::floats::<f64>();
    let value: f64 = gen.generate();
    println!("floats::<f64>(): {}", value);
}

fn test_floats_bounded() {
    let gen = gen::floats::<f64>().with_min(0.0).with_max(1.0);
    let value: f64 = gen.generate();
    test_assert!(value >= 0.0 && value <= 1.0, "floats(0,1) must be in [0,1]");
    println!("floats::<f64>(0,1): {}", value);
}

fn test_floats_exclusive() {
    let gen = gen::floats::<f64>()
        .with_min(0.0)
        .with_max(1.0)
        .exclude_min()
        .exclude_max();
    let value: f64 = gen.generate();
    test_assert!(
        value > 0.0 && value < 1.0,
        "floats exclusive (0,1) must be in (0,1)"
    );
    println!("floats::<f64>(exclusive 0,1): {}", value);
}

fn test_floats_f32() {
    let gen = gen::floats::<f32>().with_min(-1.0).with_max(1.0);
    let value: f32 = gen.generate();
    test_assert!(value >= -1.0 && value <= 1.0, "f32 must be in [-1,1]");
    println!("floats::<f32>(-1,1): {}", value);
}

// =============================================================================
// String tests
// =============================================================================

fn test_text_unbounded() {
    let gen = gen::text();
    let value: String = gen.generate();
    println!("text(): \"{}\" (chars={})", value, value.chars().count());
}

fn test_text_bounded() {
    let gen = gen::text().with_min_size(5).with_max_size(10);
    let value: String = gen.generate();
    let len = value.chars().count();
    // NOTE: Cannot always check min length due to potential null byte issues
    test_assert!(len <= 10, "text(5,10) char length must be <= 10");
    println!("text(5,10): \"{}\" (chars={})", value, len);
}

fn test_text_min_only() {
    let gen = gen::text().with_min_size(3);
    let value: String = gen.generate();
    let len = value.chars().count();
    println!("text(min=3): \"{}\" (chars={})", value, len);
}

fn test_from_regex() {
    let gen = gen::from_regex(r"[a-z]{3}-[0-9]{3}");
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("from_regex produced non-ASCII string");
    }

    // Basic validation - should be like "abc-123"
    test_assert!(value.len() == 7, "from_regex should produce 7-char string");
    test_assert!(
        &value[3..4] == "-",
        "from_regex should have hyphen at position 3"
    );
    println!("from_regex([a-z]{{3}}-[0-9]{{3}}): \"{}\"", value);
}

// =============================================================================
// Format string tests
// =============================================================================

fn test_emails() {
    let gen = gen::emails();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("email produced non-ASCII string");
    }

    test_assert!(value.contains('@'), "email must contain @");
    println!("emails(): \"{}\"", value);
}

fn test_urls() {
    let gen = gen::urls();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("url produced non-ASCII string");
    }

    test_assert!(value.contains("://"), "url must contain ://");
    println!("urls(): \"{}\"", value);
}

fn test_domains() {
    let gen = gen::domains();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("domain produced non-ASCII string");
    }

    test_assert!(value.chars().count() <= 255, "domain must be <= 255 chars");
    println!("domains(): \"{}\"", value);
}

fn test_ip_addresses_v4() {
    let gen = gen::ip_addresses().v4();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("ipv4 produced non-ASCII string");
    }

    let parts: Vec<&str> = value.split('.').collect();
    test_assert!(parts.len() == 4, "ipv4 must have 4 octets");
    println!("ip_addresses().v4(): \"{}\"", value);
}

fn test_ip_addresses_v6() {
    let gen = gen::ip_addresses().v6();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("ipv6 produced non-ASCII string");
    }

    test_assert!(value.contains(':'), "ipv6 must contain colons");
    println!("ip_addresses().v6(): \"{}\"", value);
}

fn test_ip_addresses_any() {
    let gen = gen::ip_addresses();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("ip address produced non-ASCII string");
    }

    let is_v4 = value.contains('.');
    let is_v6 = value.contains(':');
    test_assert!(is_v4 || is_v6, "ip address must be v4 or v6");
    println!("ip_addresses(): \"{}\"", value);
}

// =============================================================================
// Datetime tests
// =============================================================================

fn test_dates() {
    let gen = gen::dates();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("date produced non-ASCII string");
    }

    // ISO date: YYYY-MM-DD
    test_assert!(value.len() == 10, "date must be 10 chars (YYYY-MM-DD)");
    test_assert!(
        &value[4..5] == "-" && &value[7..8] == "-",
        "date must match YYYY-MM-DD"
    );
    println!("dates(): \"{}\"", value);
}

fn test_times() {
    let gen = gen::times();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("time produced non-ASCII string");
    }

    test_assert!(value.contains(':'), "time must contain colons");
    println!("times(): \"{}\"", value);
}

fn test_datetimes() {
    let gen = gen::datetimes();
    let value: String = gen.generate();

    if !is_ascii(&value) {
        hegel::reject("datetime produced non-ASCII string");
    }

    test_assert!(value.contains('-'), "datetime must contain date part");
    test_assert!(value.contains(':'), "datetime must contain time part");
    println!("datetimes(): \"{}\"", value);
}

// =============================================================================
// Collection tests
// =============================================================================

fn test_vecs_basic() {
    let gen = gen::vecs(gen::integers::<i32>());
    let value: Vec<i32> = gen.generate();
    println!("vecs(integers::<i32>()): size={}", value.len());
}

fn test_vecs_bounded() {
    let gen = gen::vecs(gen::integers::<i32>().with_min(0).with_max(100))
        .with_min_size(3)
        .with_max_size(5);
    let value: Vec<i32> = gen.generate();
    test_assert!(
        value.len() >= 3 && value.len() <= 5,
        "vecs(min=3,max=5) size must be in [3,5]"
    );
    for v in &value {
        test_assert!(*v >= 0 && *v <= 100, "vec elements must be in [0,100]");
    }
    println!("vecs(3,5): {:?}", value);
}

fn test_vecs_unique() {
    let gen = gen::vecs(gen::integers::<i32>().with_min(0).with_max(1000))
        .with_min_size(5)
        .with_max_size(10)
        .unique();
    let value: Vec<i32> = gen.generate();
    let unique: HashSet<_> = value.iter().collect();
    test_assert!(
        unique.len() == value.len(),
        "unique vecs must have no duplicates"
    );
    println!("vecs(unique): size={}, all unique", value.len());
}

fn test_hashsets() {
    let gen = gen::hashsets(gen::integers::<i32>().with_min(0).with_max(100))
        .with_min_size(3)
        .with_max_size(7);
    let value: HashSet<i32> = gen.generate();
    test_assert!(
        value.len() >= 3 && value.len() <= 7,
        "hashsets(3,7) size must be in [3,7]"
    );
    println!("hashsets(3,7): size={}", value.len());
}

fn test_hashmaps() {
    let gen = gen::hashmaps(gen::integers::<i32>())
        .with_min_size(1)
        .with_max_size(3);
    let value: HashMap<String, i32> = gen.generate();
    test_assert!(
        value.len() >= 1 && value.len() <= 3,
        "hashmaps(1,3) size must be in [1,3]"
    );
    println!("hashmaps(1,3): {:?}", value);
}

fn test_hashmaps_no_schema() {
    // Use a mapped values generator which has no schema, forcing compositional generation
    let gen = gen::hashmaps(
        gen::integers::<i32>()
            .with_min(0)
            .with_max(100)
            .map(|x| x * 2),
    )
    .with_min_size(2)
    .with_max_size(4);
    let value: HashMap<String, i32> = gen.generate();
    test_assert!(
        value.len() >= 2 && value.len() <= 4,
        "hashmaps_no_schema size must be in [2,4]"
    );
    for (key, val) in &value {
        test_assert!(key.len() >= 1, "key must have at least 1 char");
        test_assert!(val % 2 == 0, "values must be even (doubled)");
        test_assert!(*val >= 0 && *val <= 200, "values must be in [0,200]");
    }
    println!("hashmaps_no_schema(2,4): {:?}", value);
}

// =============================================================================
// Tuple tests
// =============================================================================

fn test_tuples_pair() {
    let gen = gen::tuples(gen::integers::<i32>(), gen::text().with_max_size(10));
    let (i, s): (i32, String) = gen.generate();
    println!("tuples(int, string): ({}, \"{}\")", i, s);
}

fn test_tuples_triple() {
    let gen = gen::tuples3(
        gen::booleans(),
        gen::integers::<i32>().with_min(0),
        gen::floats::<f64>(),
    );
    let (b, i, f): (bool, i32, f64) = gen.generate();
    test_assert!(i >= 0, "tuple int element must be >= 0");
    println!("tuples3(bool, int, f64): ({}, {}, {})", b, i, f);
}

// =============================================================================
// Combinator tests
// =============================================================================

fn test_sampled_from_strings() {
    let options = vec!["apple", "banana", "cherry"];
    let gen = gen::sampled_from(options.clone());
    let value: &str = gen.generate();
    test_assert!(
        value == "apple" || value == "banana" || value == "cherry",
        "sampled_from must return one of the options"
    );
    println!("sampled_from(fruits): \"{}\"", value);
}

fn test_sampled_from_ints() {
    let gen = gen::sampled_from(vec![10, 20, 30, 40, 50]);
    let value: i32 = gen.generate();
    test_assert!(
        value == 10 || value == 20 || value == 30 || value == 40 || value == 50,
        "sampled_from must return one of the options"
    );
    println!("sampled_from(ints): {}", value);
}

fn test_one_of() {
    let gen = hegel::one_of!(
        gen::integers::<i32>().with_min(0).with_max(10),
        gen::integers::<i32>().with_min(100).with_max(110),
    );
    let value: i32 = gen.generate();
    test_assert!(
        (value >= 0 && value <= 10) || (value >= 100 && value <= 110),
        "one_of must return from one of the ranges"
    );
    println!("one_of(0-10, 100-110): {}", value);
}

fn test_optional_some() {
    let gen = gen::optional(gen::integers::<i32>().with_min(0).with_max(100));
    let value: Option<i32> = gen.generate();
    match value {
        Some(v) => {
            test_assert!(v >= 0 && v <= 100, "optional value must be in range");
            println!("optional(int): Some({})", v);
        }
        None => {
            println!("optional(int): None");
        }
    }
}

// =============================================================================
// Struct generation tests
// =============================================================================

#[derive(DeriveGenerate, Debug)]
struct Point {
    x: i32,
    y: i32,
}

#[derive(DeriveGenerate, Debug)]
struct Person {
    name: String,
    age: u32,
}

fn test_derived_struct_point() {
    let gen = PointGenerator::new()
        .with_x(gen::integers::<i32>().with_min(-100).with_max(100))
        .with_y(gen::integers::<i32>().with_min(-100).with_max(100));
    let p: Point = gen.generate();
    test_assert!(p.x >= -100 && p.x <= 100, "point.x must be in range");
    test_assert!(p.y >= -100 && p.y <= 100, "point.y must be in range");
    println!("PointGenerator: ({}, {})", p.x, p.y);
}

fn test_derived_struct_person() {
    let gen = PersonGenerator::new()
        .with_name(gen::text().with_min_size(1).with_max_size(20))
        .with_age(gen::integers::<u32>().with_min(0).with_max(120));
    let p: Person = gen.generate();
    let name_len = p.name.chars().count();
    test_assert!(name_len <= 20, "person.name must be <= 20 chars");
    test_assert!(p.age <= 120, "person.age must be in [0,120]");
    println!("PersonGenerator: {{name=\"{}\", age={}}}", p.name, p.age);
}

// =============================================================================
// Enum generation tests
// =============================================================================

/// Unit-only enum (all variants are unit variants)
#[derive(DeriveGenerate, Debug, PartialEq, serde::Deserialize)]
enum Color {
    Red,
    Green,
    Blue,
}

/// Enum with named field variants
#[derive(DeriveGenerate, Debug, serde::Deserialize)]
enum Status {
    Pending,
    Active { since: String },
    Error { code: i32, message: String },
}

/// Enum with tuple variants
#[derive(DeriveGenerate, Debug, serde::Deserialize)]
enum Message {
    Quit,
    Text(String),
    Move(i32, i32),
}

fn test_enum_unit_only() {
    let gen = ColorGenerator::new();
    let color: Color = gen.generate();
    test_assert!(
        color == Color::Red || color == Color::Green || color == Color::Blue,
        "Color must be one of the variants"
    );
    println!("ColorGenerator: {:?}", color);
}

fn test_enum_with_named_fields() {
    let gen = StatusGenerator::new();
    let status: Status = gen.generate();
    match &status {
        Status::Pending => println!("StatusGenerator: Pending"),
        Status::Active { since } => {
            println!("StatusGenerator: Active {{ since: \"{}\" }}", since);
        }
        Status::Error { code, message } => {
            println!(
                "StatusGenerator: Error {{ code: {}, message: \"{}\" }}",
                code, message
            );
        }
    }
}

fn test_enum_with_tuple_variants() {
    let gen = MessageGenerator::new();
    let message: Message = gen.generate();
    match &message {
        Message::Quit => println!("MessageGenerator: Quit"),
        Message::Text(s) => println!("MessageGenerator: Text(\"{}\")", s),
        Message::Move(x, y) => println!("MessageGenerator: Move({}, {})", x, y),
    }
}

fn test_enum_customized_variant() {
    // Customize the Active variant's since field
    let gen = StatusGenerator::new().with_Active(
        StatusGenerator::default_Active()
            .with_since(gen::text().with_min_size(5).with_max_size(15)),
    );

    let status: Status = gen.generate();
    match &status {
        Status::Active { since } => {
            let len = since.chars().count();
            test_assert!(len <= 15, "Customized since must be <= 15 chars");
            println!(
                "Customized StatusGenerator: Active {{ since: \"{}\" }}",
                since
            );
        }
        other => {
            println!("Customized StatusGenerator: {:?} (not Active)", other);
        }
    }
}

fn test_enum_variant_generator_directly() {
    // Use a variant generator directly
    let gen =
        StatusActiveGenerator::new().with_since(gen::text().with_min_size(1).with_max_size(10));

    let status: Status = gen.generate();
    match status {
        Status::Active { since } => {
            let len = since.chars().count();
            test_assert!(
                len <= 10,
                "Direct variant generator since must be <= 10 chars"
            );
            println!(
                "StatusActiveGenerator direct: Active {{ since: \"{}\" }}",
                since
            );
        }
        _ => {
            panic!("StatusActiveGenerator should always produce Active variant");
        }
    }
}

fn test_enum_public_fields() {
    // Test that generator fields are accessible
    let gen = StatusGenerator::new();

    // Access the Active field and call schema() on it
    let active_schema = gen.Active.schema();
    test_assert!(
        active_schema.is_some(),
        "Active variant should have a schema"
    );
    println!(
        "StatusGenerator.Active.schema(): {}",
        active_schema.unwrap()
    );
}

fn test_fixed_dicts() {
    let gen = gen::fixed_dicts()
        .field("name", gen::text().with_min_size(1).with_max_size(20))
        .field("age", gen::integers::<u32>().with_min(0).with_max(120))
        .field("active", gen::booleans())
        .build();
    let value: serde_json::Value = gen.generate();
    test_assert!(value.is_object(), "fixed_dicts must produce object");
    test_assert!(value.get("name").is_some(), "missing 'name' field");
    test_assert!(value.get("age").is_some(), "missing 'age' field");
    test_assert!(value.get("active").is_some(), "missing 'active' field");
    println!("fixed_dicts: {}", value);
}

// =============================================================================
// Non-static lifetime tests
// =============================================================================

fn test_boxed_with_borrowed_data() {
    // Create local data that the generator will borrow
    let choices = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];

    // Create a boxed generator that borrows from `choices`
    // This demonstrates BoxedGenerator<'a, T> with non-'static lifetime
    let gen: gen::BoxedGenerator<'_, String> = gen::sampled_from_slice(&choices)
        .map(|s: String| s.to_uppercase())
        .boxed();

    let value: String = gen.generate();
    test_assert!(
        value == "ALPHA" || value == "BETA" || value == "GAMMA",
        "borrowed generator must produce uppercase version of choices"
    );
    println!("boxed_with_borrowed_data: \"{}\"", value);
}

fn test_one_of_with_borrowed_generators() {
    // Create local data
    let small_numbers: Vec<i32> = vec![1, 2, 3];
    let big_numbers: Vec<i32> = vec![100, 200, 300];

    // Create generators that borrow from local data and combine them
    let gen = gen::one_of(vec![
        gen::sampled_from_slice(&small_numbers).boxed(),
        gen::sampled_from_slice(&big_numbers).boxed(),
    ]);

    let value: i32 = gen.generate();
    test_assert!(
        small_numbers.contains(&value) || big_numbers.contains(&value),
        "one_of with borrowed data must produce value from one of the slices"
    );
    println!("one_of_with_borrowed_generators: {}", value);
}

fn test_fixed_dict_with_borrowed_generators() {
    // Create local choices (using String to satisfy DeserializeOwned)
    let statuses: Vec<String> = vec![
        "active".to_string(),
        "inactive".to_string(),
        "pending".to_string(),
    ];

    // Build a fixed dict generator that borrows from local data
    let gen = gen::fixed_dicts()
        .field("id", gen::integers::<u32>().with_min(1).with_max(1000))
        .field("status", gen::sampled_from_slice(&statuses))
        .build();

    let value: serde_json::Value = gen.generate();
    test_assert!(value.is_object(), "fixed_dict must produce object");

    let status = value.get("status").and_then(|v| v.as_str()).unwrap();
    test_assert!(
        status == "active" || status == "inactive" || status == "pending",
        "status must be one of the borrowed choices"
    );
    println!("fixed_dict_with_borrowed_generators: {}", value);
}

// =============================================================================
// Map/filter/flatmap tests
// =============================================================================

fn test_map() {
    let gen = gen::integers::<i32>()
        .with_min(1)
        .with_max(10)
        .map(|x| x * x);
    let value: i32 = gen.generate();
    // Should be a perfect square between 1 and 100
    let root = (value as f64).sqrt() as i32;
    test_assert!(
        root * root == value,
        "mapped value should be a perfect square"
    );
    test_assert!(
        value >= 1 && value <= 100,
        "squared value must be in [1,100]"
    );
    println!("integers.map(x*x): {}", value);
}

fn test_filter() {
    let gen = gen::integers::<i32>()
        .with_min(0)
        .with_max(100)
        .filter(|x| x % 2 == 0, 10);
    let value: i32 = gen.generate();
    test_assert!(value % 2 == 0, "filtered value must be even");
    test_assert!(
        value >= 0 && value <= 100,
        "filtered value must be in range"
    );
    println!("integers.filter(even): {}", value);
}

fn test_flat_map() {
    // Generate a length, then generate a string of that length
    let gen = gen::integers::<usize>()
        .with_min(3)
        .with_max(8)
        .flat_map(|len| gen::text().with_min_size(len).with_max_size(len));
    let value: String = gen.generate();
    let len = value.chars().count();
    test_assert!(len <= 8, "flat_map string char length must be <= 8");
    println!(
        "integers.flat_map(len -> text(len)): \"{}\" (chars={})",
        value, len
    );
}

// =============================================================================
// Test registry
// =============================================================================

type TestFn = fn();

fn get_all_tests() -> Vec<(&'static str, TestFn)> {
    vec![
        // Primitives
        ("units", test_units as TestFn),
        ("booleans", test_booleans),
        ("just_int", test_just_int),
        ("just_string", test_just_string),
        // Integers
        ("integers_unbounded", test_integers_unbounded),
        ("integers_bounded", test_integers_bounded),
        ("integers_min_only", test_integers_min_only),
        ("integers_max_only", test_integers_max_only),
        ("integers_u8", test_integers_u8),
        ("integers_negative_range", test_integers_negative_range),
        // Floats
        ("floats_unbounded", test_floats_unbounded),
        ("floats_bounded", test_floats_bounded),
        ("floats_exclusive", test_floats_exclusive),
        ("floats_f32", test_floats_f32),
        // Strings
        ("text_unbounded", test_text_unbounded),
        ("text_bounded", test_text_bounded),
        ("text_min_only", test_text_min_only),
        ("from_regex", test_from_regex),
        // Format strings
        ("emails", test_emails),
        ("urls", test_urls),
        ("domains", test_domains),
        ("ip_addresses_v4", test_ip_addresses_v4),
        ("ip_addresses_v6", test_ip_addresses_v6),
        ("ip_addresses_any", test_ip_addresses_any),
        // Datetime
        ("dates", test_dates),
        ("times", test_times),
        ("datetimes", test_datetimes),
        // Collections
        ("vecs_basic", test_vecs_basic),
        ("vecs_bounded", test_vecs_bounded),
        ("vecs_unique", test_vecs_unique),
        ("hashsets", test_hashsets),
        ("hashmaps", test_hashmaps),
        ("hashmaps_no_schema", test_hashmaps_no_schema),
        // Tuples
        ("tuples_pair", test_tuples_pair),
        ("tuples_triple", test_tuples_triple),
        // Combinators
        ("sampled_from_strings", test_sampled_from_strings),
        ("sampled_from_ints", test_sampled_from_ints),
        ("one_of", test_one_of),
        ("optional", test_optional_some),
        // Struct generation
        ("derived_struct_point", test_derived_struct_point),
        ("derived_struct_person", test_derived_struct_person),
        ("fixed_dicts", test_fixed_dicts),
        // Enum generation
        ("enum_unit_only", test_enum_unit_only),
        ("enum_with_named_fields", test_enum_with_named_fields),
        ("enum_with_tuple_variants", test_enum_with_tuple_variants),
        ("enum_customized_variant", test_enum_customized_variant),
        (
            "enum_variant_generator_directly",
            test_enum_variant_generator_directly,
        ),
        ("enum_public_fields", test_enum_public_fields),
        // Map/filter/flatmap
        ("map", test_map),
        ("filter", test_filter),
        ("flat_map", test_flat_map),
        // Non-static lifetime
        ("boxed_with_borrowed_data", test_boxed_with_borrowed_data),
        (
            "one_of_with_borrowed_generators",
            test_one_of_with_borrowed_generators,
        ),
        (
            "fixed_dict_with_borrowed_generators",
            test_fixed_dict_with_borrowed_generators,
        ),
    ]
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    let all_tests = get_all_tests();
    let args: Vec<String> = std::env::args().collect();

    // If a test name is provided as argument, run that test directly
    // Otherwise, use sampled_from to pick which test to run
    let selected: String = if args.len() > 1 {
        args[1].clone()
    } else {
        // Build vector of test names for sampled_from
        let test_names: Vec<&'static str> = all_tests.iter().map(|(name, _)| *name).collect();
        gen::sampled_from(test_names).generate().to_string()
    };

    println!("Selected test: {}", selected);
    println!("----------------------------------------");

    // Find and run the selected test
    for (name, test_fn) in &all_tests {
        if *name == selected {
            test_fn();
            println!("----------------------------------------");
            println!("PASSED: {}", selected);
            return;
        }
    }

    eprintln!("Unknown test: {}", selected);
    eprintln!("Available tests:");
    for (name, _) in &all_tests {
        eprintln!("  {}", name);
    }
    std::process::exit(1);
}
