use std::fs::OpenOptions;
use std::io::Write;

pub struct TestLocation {
    pub function: String,
    pub file: String,
    pub class: String,
    pub begin_line: u32,
}

pub(crate) fn emit_assertion(location: &TestLocation, passed: bool) {
    let output_dir = match std::env::var("ANTITHESIS_OUTPUT_DIR") {
        Ok(dir) if !dir.is_empty() => dir,
        _ => return,
    };

    let path = format!("{}/sdk.jsonl", output_dir);

    let id = format!("{}::{} passes properties", location.class, location.function);

    let location_obj = serde_json::json!({
        "class": location.class,
        "function": location.function,
        "file": location.file,
        "begin_line": location.begin_line,
        "begin_column": 0,
    });

    let declaration = serde_json::json!({
        "antithesis_assert": {
            "hit": false,
            "must_hit": true,
            "assert_type": "always",
            "display_type": "Always",
            "condition": false,
            "id": id,
            "message": id,
            "location": location_obj,
        }
    });

    let evaluation = serde_json::json!({
        "antithesis_assert": {
            "hit": true,
            "must_hit": true,
            "assert_type": "always",
            "display_type": "Always",
            "condition": passed,
            "id": id,
            "message": id,
            "location": location_obj,
        }
    });

    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) else {
        return;
    };

    let Ok(decl_line) = serde_json::to_string(&declaration) else {
        return;
    };
    let Ok(eval_line) = serde_json::to_string(&evaluation) else {
        return;
    };

    let _ = writeln!(file, "{}", decl_line);
    let _ = writeln!(file, "{}", eval_line);
}
