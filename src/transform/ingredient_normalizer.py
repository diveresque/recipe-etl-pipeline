import re

# Common plurals → singular
PLURAL_REPLACEMENTS = {
    "eggs": "egg",
    "tomatoes": "tomato",
    "potatoes": "potato",
    "chilies": "chili",
    "berries": "berry",
    "avocados": "avocado",
    "coconuts": "coconut",
    "cucumbers": "cucumber",
    "leeks": "leek",
    "onions": "onion",
    "pineapples": "pineapple",
    "potatoes": "potato",
    "pumpkins": "pumpkin",
    "radishes": "radish",
    "strawberries": "strawberry",
    "bananas": "banana",
    "apples": "apple",
    "oranges": "orange",
    "pears": "pear",
    "plums": "plum",
    "cherries": "cherry",
    "grapes": "grape",
    "melons": "melon",
    "nectarines": "nectarine",
}

# Garbage prefixes that indicate "measure accidentally merged into name"
GARBAGE_PREFIXES = [
    r"^\d+\s*[a-zA-Z]*\s+of\s+",     # e.g. "3 oz of pearl tapioca"
    r"^\d+\s+[a-zA-Z]+\s+",         # e.g. "4 large onions"
    r"^\d+[\./]?\d*\s+[a-zA-Z]+\s+" # e.g. "1 1/2 cups flour"
]

GARBAGE_PREFIX_RE = re.compile("|".join(GARBAGE_PREFIXES))


def normalize_ingredient_name(name: str) -> str | None:
    """
    Returns a normalized ingredient name:
        - trims whitespace
        - lowercases
        - removes leading measurements accidentally merged into the name
        - converts known plurals to singular
        - filters out obviously invalid names
    """

    if not name or not name.strip():
        return None

    # Normalize spacing + lowercase
    name = name.strip().lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    # Remove garbage prefixes (e.g. "3 oz of ...")
    name = GARBAGE_PREFIX_RE.sub("", name).strip()

    # Replace plurals
    for plural, singular in PLURAL_REPLACEMENTS.items():
        if name == plural:
            name = singular

    # Basic validity check
    if len(name) < 2:
        return None
    if name in {"n/a", "none", "null", "unknown"}:
        return None

    return name


def normalize_measure(measure: str) -> str | None:
    """
    Light normalization for measure.
    Do NOT attempt full parsing here.
    """
    if not measure or not measure.strip():
        return None

    measure = measure.strip().lower()

    # Remove redundant words
    measure = measure.replace("to taste", "").strip()

    if measure in {"", "n/a", "null"}:
        return None

    return measure
