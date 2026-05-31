#[test]
fn handler_macro_rejects_invalid_inputs() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/handler_macro/*.rs");
}
