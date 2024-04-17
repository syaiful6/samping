import re
from typing import Optional, Iterable


class Rule:
    def __init__(self, pattern: str, queue: str):
        self.pattern = re.compile(pattern)
        self.queue = queue

    def is_match(self, task_name: str) -> bool:
        return self.pattern.match(task_name)


def route(task_name: str, rules: Iterable[Rule]) -> Optional[str]:
    for rule in rules:
        if rule.is_match(task_name):
            return rule.queue
