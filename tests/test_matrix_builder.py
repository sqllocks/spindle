from tests.fixtures.validation_matrix import build_matrix, DOMAINS, SINKS, SIZES, MODES, INFERENCE_CAPABLE_DOMAINS

def test_build_matrix_returns_list():
    matrix = build_matrix()
    assert isinstance(matrix, list)
    assert len(matrix) > 100

def test_matrix_no_duplicates():
    matrix = build_matrix()
    assert len(matrix) == len(set(matrix))

def test_matrix_filters_streaming_sql_server():
    matrix = build_matrix()
    bad = [(d, s, sz, m) for d, s, sz, m in matrix if s == "sql-server" and m == "streaming"]
    assert bad == [], f"streaming+sql-server should be filtered: {bad}"

def test_matrix_filters_fabric_demo_sql_server():
    matrix = build_matrix()
    for d, s, sz, m in matrix:
        if s == "sql-server":
            assert sz != "fabric_demo", f"fabric_demo+sql-server should be filtered: {(d,s,sz,m)}"

def test_matrix_inference_only_capable_domains():
    matrix = build_matrix()
    for d, s, sz, m in matrix:
        if m == "inference":
            assert d in INFERENCE_CAPABLE_DOMAINS, f"inference should only appear for capable domains, got: {d}"

def test_all_domains_in_matrix():
    matrix = build_matrix()
    domains_in_matrix = {d for d, _, _, _ in matrix}
    for domain in DOMAINS:
        assert domain in domains_in_matrix, f"{domain} missing from matrix"
