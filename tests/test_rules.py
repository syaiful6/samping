from samping.routes import Rule, route


rules = [
    Rule("backend_*", "backend"),
    Rule("ml_*", "ml"),
]


def test_task_routes():
    queue = route("ml_predict", rules)
    assert queue == "ml"


def test_task_no_match():
    queue = route("ml_predict", [])
    assert queue is None


def test_route_exact_match():
    rules = [
        Rule("backend_*", "backend"),
        Rule("ml_predict", "ml_predict"),  # order is important
        Rule("ml_*", "ml"),
    ]
    queue = route("ml_predict", rules)
    assert queue == "ml_predict"
